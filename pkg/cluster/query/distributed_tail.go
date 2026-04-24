package query

import (
	"container/heap"
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/lynxbase/lynxdb/pkg/cluster"
	"github.com/lynxbase/lynxdb/pkg/cluster/rpc"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/cluster/sharding"
	"github.com/lynxbase/lynxdb/pkg/event"
)

// DistributedTailMerger subscribes to live tail streams from all ingest nodes
// and merges events in timestamp order using a min-heap. Events are delivered
// only when their timestamp is <= the global watermark.
type DistributedTailMerger struct {
	clientPool    *rpc.ClientPool
	shardMapCache *cluster.ShardMapCache
	watermarks    *WatermarkTracker
	nodeAddrs     func() map[sharding.NodeID]string
	logger        *slog.Logger
}

// NewDistributedTailMerger creates a new merger.
func NewDistributedTailMerger(
	clientPool *rpc.ClientPool,
	shardMapCache *cluster.ShardMapCache,
	watermarks *WatermarkTracker,
	nodeAddrs func() map[sharding.NodeID]string,
	logger *slog.Logger,
) *DistributedTailMerger {
	return &DistributedTailMerger{
		clientPool:    clientPool,
		shardMapCache: shardMapCache,
		watermarks:    watermarks,
		nodeAddrs:     nodeAddrs,
		logger:        logger,
	}
}

// tailEventWithTime wraps a decoded event with its timestamp for heap ordering.
type tailEventWithTime struct {
	fields map[string]event.Value
	timeNs int64
}

// tailEventHeap is a min-heap ordered by timestamp (ascending).
type tailEventHeap []tailEventWithTime

func (h tailEventHeap) Len() int            { return len(h) }
func (h tailEventHeap) Less(i, j int) bool  { return h[i].timeNs < h[j].timeNs }
func (h tailEventHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *tailEventHeap) Push(x interface{}) { *h = append(*h, x.(tailEventWithTime)) }
func (h *tailEventHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]

	return item
}

// Subscribe opens tail streams to all ingest nodes and returns a channel
// of merged events. Events are delivered in approximate timestamp order
// (bounded by watermark skew). The channel is closed when ctx is cancelled.
func (m *DistributedTailMerger) Subscribe(
	ctx context.Context, query string, fromNs int64,
) (<-chan *event.Event, error) {
	addrs := m.nodeAddrs()
	if len(addrs) == 0 {
		return nil, fmt.Errorf("DistributedTailMerger.Subscribe: no ingest nodes available")
	}

	// Channel for events from all node streams.
	rawEvents := make(chan tailEventWithTime, 1024)

	var wg sync.WaitGroup
	for nodeID, addr := range addrs {
		wg.Add(1)
		go func(nid sharding.NodeID, a string) {
			defer wg.Done()
			m.streamFromNode(ctx, nid, a, query, fromNs, rawEvents)
		}(nodeID, addr)
	}

	// Close rawEvents when all node streams complete.
	go func() {
		wg.Wait()
		close(rawEvents)
	}()

	// Output channel for merged events.
	out := make(chan *event.Event, 256)

	go m.mergeLoop(ctx, rawEvents, out)

	return out, nil
}

// streamFromNode opens a SubscribeTail stream to a single node and forwards
// events and watermark updates.
func (m *DistributedTailMerger) streamFromNode(
	ctx context.Context,
	nodeID sharding.NodeID,
	addr string,
	query string,
	fromNs int64,
	out chan<- tailEventWithTime,
) {
	defer m.watermarks.Remove(nodeID)

	conn, err := m.clientPool.GetConn(ctx, addr)
	if err != nil {
		m.logger.Warn("tail: connect failed", "node", nodeID, "addr", addr, "error", err)

		return
	}

	client := clusterpb.NewQueryServiceClient(conn)
	stream, err := client.SubscribeTail(ctx, &clusterpb.SubscribeTailRequest{
		Query:      query,
		FromUnixNs: fromNs,
	})
	if err != nil {
		m.logger.Warn("tail: subscribe failed", "node", nodeID, "error", err)

		return
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			if err != io.EOF && ctx.Err() == nil {
				m.logger.Warn("tail: stream error", "node", nodeID, "error", err)
			}

			return
		}

		if msg.WatermarkNs > 0 {
			m.watermarks.Update(nodeID, msg.WatermarkNs)
		}

		// Decode and forward event if present.
		if len(msg.Event) > 0 {
			var fields map[string]event.Value
			if err := msgpack.Unmarshal(msg.Event, &fields); err != nil {
				m.logger.Warn("tail: decode event failed", "error", err)

				continue
			}

			timeNs := msg.WatermarkNs
			if tv, ok := fields["_time"]; ok {
				if t, tok := tv.TryAsTimestamp(); tok {
					timeNs = t.UnixNano()
				}
			}

			select {
			case out <- tailEventWithTime{fields: fields, timeNs: timeNs}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// mergeLoop reads events from all node streams, buffers them in a min-heap,
// and delivers events whose timestamp is <= the global watermark.
func (m *DistributedTailMerger) mergeLoop(
	ctx context.Context,
	in <-chan tailEventWithTime,
	out chan<- *event.Event,
) {
	defer close(out)

	h := &tailEventHeap{}
	heap.Init(h)

	for {
		select {
		case <-ctx.Done():
			return
		case tev, ok := <-in:
			if !ok {
				// All streams closed — flush remaining events.
				for h.Len() > 0 {
					item := heap.Pop(h).(tailEventWithTime)
					ev := fieldsToEvent(item.fields)
					select {
					case out <- ev:
					case <-ctx.Done():
						return
					}
				}

				return
			}

			heap.Push(h, tev)

			// Deliver events that are safe (below global watermark).
			watermark := m.watermarks.GlobalWatermark()
			for h.Len() > 0 && (*h)[0].timeNs <= watermark {
				item := heap.Pop(h).(tailEventWithTime)
				ev := fieldsToEvent(item.fields)
				select {
				case out <- ev:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// fieldsToEvent converts a decoded field map back to an event.
func fieldsToEvent(fields map[string]event.Value) *event.Event {
	ev := &event.Event{
		Fields: make(map[string]event.Value, len(fields)),
	}
	for k, v := range fields {
		switch k {
		case "_time":
			if t, ok := v.TryAsTimestamp(); ok {
				ev.Time = t
			}
		case "_raw":
			s, _ := v.TryAsString()
			ev.Raw = s
		case "source", "_source":
			s, _ := v.TryAsString()
			ev.Source = s
		case "host":
			s, _ := v.TryAsString()
			ev.Host = s
		case "index":
			s, _ := v.TryAsString()
			ev.Index = s
		default:
			ev.Fields[k] = v
		}
	}

	return ev
}
