package compaction

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultDeletionRate  = 200 << 20 // 200 MB/s
	deletionTickInterval = 100 * time.Millisecond
)

type pendingDelete struct {
	path      string
	sizeBytes int64
}

// DeletionPacer rate-limits file deletion to avoid SSD TRIM latency spikes.
// When compaction or retention removes segments, files are enqueued here
// instead of being deleted synchronously. A background goroutine drains the
// queue at the configured rate.
type DeletionPacer struct {
	rate    int64 // bytes per second
	mu      sync.Mutex
	pending []pendingDelete
	notify  chan struct{} // poked on enqueue to wake drainer
	logger  *slog.Logger
}

// NewDeletionPacer creates a pacer with the given deletion rate in bytes/sec.
// Use 0 for the default (200 MB/s).
func NewDeletionPacer(rateBytesPerSec int64) *DeletionPacer {
	if rateBytesPerSec <= 0 {
		rateBytesPerSec = defaultDeletionRate
	}
	return &DeletionPacer{
		rate:   rateBytesPerSec,
		notify: make(chan struct{}, 1),
	}
}

// SetLogger sets the logger for the deletion pacer.
func (p *DeletionPacer) SetLogger(logger *slog.Logger) {
	p.logger = logger
}

// Enqueue schedules a file for rate-limited deletion.
func (p *DeletionPacer) Enqueue(path string, sizeBytes int64) {
	p.mu.Lock()
	p.pending = append(p.pending, pendingDelete{path: path, sizeBytes: sizeBytes})
	p.mu.Unlock()

	if p.logger != nil {
		p.logger.Debug("deletion pacer enqueue",
			"path", path,
			"size", sizeBytes,
		)
	}

	// Non-blocking poke.
	select {
	case p.notify <- struct{}{}:
	default:
	}
}

// DrainLoop runs as a background goroutine, deleting files at the configured
// rate. It processes up to `rate * tickInterval` bytes per tick. On context
// cancellation, all remaining files are deleted immediately (shutdown flush).
func (p *DeletionPacer) DrainLoop(ctx context.Context) {
	ticker := time.NewTicker(deletionTickInterval)
	defer ticker.Stop()

	bytesPerTick := int64(float64(p.rate) * deletionTickInterval.Seconds())
	if bytesPerTick < 1 {
		bytesPerTick = 1
	}

	for {
		select {
		case <-ctx.Done():
			// Shutdown: flush all remaining deletions.
			p.flushAll()
			return
		case <-ticker.C:
			p.drainBudget(bytesPerTick)
		case <-p.notify:
			// Something was enqueued; drain on next tick.
		}
	}
}

// drainBudget deletes files up to the given byte budget.
func (p *DeletionPacer) drainBudget(budget int64) {
	p.mu.Lock()
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return
	}

	var remaining int64
	var toDelete []pendingDelete
	for _, pd := range p.pending {
		if remaining+pd.sizeBytes <= budget || len(toDelete) == 0 {
			// Always delete at least one file per tick to ensure progress.
			toDelete = append(toDelete, pd)
			remaining += pd.sizeBytes
		} else {
			break
		}
	}
	p.pending = p.pending[len(toDelete):]
	p.mu.Unlock()

	for _, pd := range toDelete {
		deleteFileAndDir(pd.path)
	}

	if p.logger != nil && len(toDelete) > 0 {
		p.logger.Debug("deletion pacer drain",
			"files_deleted", len(toDelete),
			"bytes_deleted", remaining,
		)
	}
}

// flushAll deletes all remaining files without rate limiting.
func (p *DeletionPacer) flushAll() {
	p.mu.Lock()
	remaining := p.pending
	p.pending = nil
	p.mu.Unlock()

	for _, pd := range remaining {
		deleteFileAndDir(pd.path)
	}

	if p.logger != nil && len(remaining) > 0 {
		p.logger.Debug("deletion pacer flush all",
			"files_flushed", len(remaining),
		)
	}
}

// Pending returns the number of files awaiting deletion.
func (p *DeletionPacer) Pending() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pending)
}

func deleteFileAndDir(path string) {
	if err := os.Remove(path); err == nil {
		// Try to clean up empty partition directory.
		os.Remove(filepath.Dir(path))
	}
}
