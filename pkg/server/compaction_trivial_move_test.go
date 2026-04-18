package server

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
	storagecompaction "github.com/lynxbase/lynxdb/pkg/storage/compaction"
)

func TestEngine_ExecuteTrivialMove_PromotesAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()

	newDiskEngine := func(t *testing.T) (*Engine, context.CancelFunc) {
		t.Helper()

		e := NewEngine(Config{
			DataDir: dataDir,
			Storage: config.DefaultConfig().Storage,
			Logger:  discardLogger(),
			Query:   queryCfg,
		})

		ctx, cancel := context.WithCancel(context.Background())
		if err := e.Start(ctx); err != nil {
			cancel()
			t.Fatalf("engine start: %v", err)
		}

		return e, cancel
	}

	e, cancel := newDiskEngine(t)

	base := time.Now().UTC()
	events := make([]*event.Event, 32)
	for i := range events {
		ev := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond), fmt.Sprintf("event-%d", i))
		ev.Index = "main"
		events[i] = ev
	}
	if err := e.Ingest(events); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if err := e.FlushBatcher(); err != nil {
		t.Fatalf("FlushBatcher: %v", err)
	}

	segs := e.Segments()
	if len(segs) != 1 {
		t.Fatalf("segments before trivial move: got %d, want 1", len(segs))
	}
	oldMeta := segs[0]
	oldPath := oldMeta.Path
	oldID := oldMeta.ID

	e.executeTrivialMove(context.Background(), oldMeta.Index, oldMeta.Partition, &storagecompaction.Plan{
		InputSegments: []*storagecompaction.SegmentInfo{{
			Meta: oldMeta,
			Path: oldMeta.Path,
		}},
		OutputLevel: storagecompaction.L1,
		TrivialMove: true,
	})

	segs = e.Segments()
	if len(segs) != 1 {
		t.Fatalf("segments after trivial move: got %d, want 1", len(segs))
	}

	newMeta := segs[0]
	if newMeta.Level != storagecompaction.L1 {
		t.Fatalf("new level: got %d, want %d", newMeta.Level, storagecompaction.L1)
	}
	if newMeta.ID == oldID {
		t.Fatalf("segment ID did not change after trivial move: %s", newMeta.ID)
	}
	if newMeta.Path == oldPath {
		t.Fatalf("segment path did not change after trivial move: %s", newMeta.Path)
	}
	if _, err := os.Stat(newMeta.Path); err != nil {
		t.Fatalf("new segment path missing: %v", err)
	}
	if _, err := os.Stat(oldPath); !os.IsNotExist(err) {
		t.Fatalf("old segment path still present, err=%v", err)
	}
	if e.partRegistry.Get(oldID) != nil {
		t.Fatalf("old part registry entry still present: %s", oldID)
	}
	if e.partRegistry.Get(newMeta.ID) == nil {
		t.Fatalf("new part registry entry missing: %s", newMeta.ID)
	}

	cancel()
	if err := e.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	e2, cancel2 := newDiskEngine(t)
	defer cancel2()
	defer func() {
		if err := e2.Shutdown(5 * time.Second); err != nil {
			t.Fatalf("Shutdown restart engine: %v", err)
		}
	}()

	recovered := e2.Segments()
	if len(recovered) != 1 {
		t.Fatalf("recovered segments: got %d, want 1", len(recovered))
	}
	if recovered[0].Level != storagecompaction.L1 {
		t.Fatalf("recovered level: got %d, want %d", recovered[0].Level, storagecompaction.L1)
	}
}

func TestEngine_ShutdownStopsCompactionAndTieringWithoutParentCancel(t *testing.T) {
	queryCfg := config.DefaultConfig().Query
	queryCfg.SpillDir = t.TempDir()

	e := NewEngine(Config{
		DataDir: t.TempDir(),
		Storage: config.DefaultConfig().Storage,
		Logger:  discardLogger(),
		Query:   queryCfg,
	})

	if err := e.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := e.Shutdown(5 * time.Second); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	waitDone := func(t *testing.T, name string, wg *sync.WaitGroup) {
		t.Helper()

		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatalf("%s goroutine still running after shutdown", name)
		}
	}

	waitDone(t, "compaction", &e.compactionWG)
	waitDone(t, "tiering", &e.tieringWG)
}
