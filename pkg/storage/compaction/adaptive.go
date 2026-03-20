package compaction

import (
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

// AdaptiveController adjusts the compaction rate based on observed query
// latency and GC pressure. When P99 latency exceeds the target or GC CPU
// fraction exceeds the threshold, the rate is reduced. When both signals
// are healthy, the rate is increased toward the max.
type AdaptiveController struct {
	mu sync.Mutex

	// Configuration.
	maxRate            int64         // maximum compaction rate (bytes/sec)
	minRate            int64         // minimum compaction rate (bytes/sec)
	targetP99          time.Duration // target P99 query latency
	targetGCFraction   float64       // GC CPU fraction above which to reduce rate (default: 0.15)
	criticalGCFraction float64       // GC CPU fraction above which to auto-pause (default: 0.30)

	// Current state.
	currentRate  int64  // current compaction rate (bytes/sec)
	pausedReason string // reason for current pause, empty when not paused

	// Latency window: circular buffer of recent query latencies.
	window    []time.Duration
	windowIdx int
	windowLen int

	// Pause state.
	paused atomic.Bool

	// GC pressure monitor.
	gcMonitor *GCMonitor

	logger *slog.Logger
}

// AdaptiveConfig configures the adaptive controller.
type AdaptiveConfig struct {
	MaxRate            int64         // max compaction rate in bytes/sec (default: 200 MB/s)
	MinRate            int64         // min compaction rate in bytes/sec (default: 10 MB/s)
	TargetP99          time.Duration // target P99 query latency (default: 500ms)
	WindowSize         int           // number of recent queries to track (default: 100)
	TargetGCFraction   float64       // GC CPU fraction to start reducing rate (default: 0.15)
	CriticalGCFraction float64       // GC CPU fraction to auto-pause compaction (default: 0.30)
	EnableGCMonitor    *bool         // enable GC-aware throttling (default: true)
	Logger             *slog.Logger
}

// GCMonitor samples GC statistics and computes the fraction of wall-clock
// time spent in GC pauses. Uses delta-based computation (not cumulative)
// so the fraction reflects recent activity, not lifetime averages.
type GCMonitor struct {
	stats          debug.GCStats
	prevPauseTotal time.Duration
	prevSampleTime time.Time
	gcCPUFraction  float64

	// sampleFn overrides debug.ReadGCStats for testing. When nil, the
	// real runtime function is used.
	sampleFn func(stats *debug.GCStats)
}

// NewGCMonitor creates a GC monitor. Pre-allocates the Pause slice to
// avoid allocation on each Sample() call.
func NewGCMonitor() *GCMonitor {
	m := &GCMonitor{
		prevSampleTime: time.Now(),
	}
	// Pre-allocate pause slice so debug.ReadGCStats reuses it.
	m.stats.Pause = make([]time.Duration, 256)
	return m
}

// Sample reads current GC stats and returns the fraction of wall-clock time
// spent in GC pauses since the last call. The result is in [0, 1].
func (m *GCMonitor) Sample() float64 {
	if m.sampleFn != nil {
		m.sampleFn(&m.stats)
	} else {
		debug.ReadGCStats(&m.stats)
	}

	now := time.Now()
	delta := m.stats.PauseTotal - m.prevPauseTotal
	wall := now.Sub(m.prevSampleTime)
	m.prevPauseTotal = m.stats.PauseTotal
	m.prevSampleTime = now

	if wall > 0 {
		m.gcCPUFraction = float64(delta) / float64(wall)
	}

	return m.gcCPUFraction
}

// Fraction returns the most recently computed GC CPU fraction without
// taking a new sample.
func (m *GCMonitor) Fraction() float64 {
	return m.gcCPUFraction
}

// NewAdaptiveController creates an adaptive compaction throttle.
func NewAdaptiveController(cfg AdaptiveConfig) *AdaptiveController {
	if cfg.MaxRate <= 0 {
		cfg.MaxRate = 200 << 20 // 200 MB/s
	}
	if cfg.MinRate <= 0 {
		cfg.MinRate = 10 << 20 // 10 MB/s
	}
	if cfg.TargetP99 <= 0 {
		cfg.TargetP99 = 500 * time.Millisecond
	}
	windowSize := cfg.WindowSize
	if windowSize <= 0 {
		windowSize = 100
	}
	if cfg.TargetGCFraction <= 0 {
		cfg.TargetGCFraction = 0.15
	}
	if cfg.CriticalGCFraction <= 0 {
		cfg.CriticalGCFraction = 0.30
	}

	enableGC := true
	if cfg.EnableGCMonitor != nil {
		enableGC = *cfg.EnableGCMonitor
	}

	ac := &AdaptiveController{
		maxRate:            cfg.MaxRate,
		minRate:            cfg.MinRate,
		targetP99:          cfg.TargetP99,
		targetGCFraction:   cfg.TargetGCFraction,
		criticalGCFraction: cfg.CriticalGCFraction,
		currentRate:        cfg.MaxRate,
		window:             make([]time.Duration, windowSize),
	}

	if enableGC {
		ac.gcMonitor = NewGCMonitor()
	}

	ac.logger = cfg.Logger

	return ac
}

// RecordLatency records a query's execution latency.
// Thread-safe: called from query goroutines.
func (ac *AdaptiveController) RecordLatency(d time.Duration) {
	ac.mu.Lock()
	ac.window[ac.windowIdx] = d
	ac.windowIdx = (ac.windowIdx + 1) % len(ac.window)
	if ac.windowLen < len(ac.window) {
		ac.windowLen++
	}
	shouldLog := ac.logger != nil && ac.windowIdx%100 == 0
	windowLen := ac.windowLen
	ac.mu.Unlock()

	if shouldLog {
		ac.logger.Debug("adaptive latency sample",
			"latency_ms", d.Milliseconds(),
			"window_len", windowLen,
		)
	}
}

// Adjust recalculates the compaction rate based on observed latencies and
// GC pressure. Call periodically (e.g., every 15 seconds) from the
// compaction scheduler. Returns the new rate in bytes/sec.
//
// Uses a "worst of two signals" approach: if either P99 latency or GC
// fraction indicates pressure, the rate is reduced. Rate is only increased
// when both signals are healthy.
func (ac *AdaptiveController) Adjust() int64 {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.windowLen == 0 && ac.gcMonitor == nil {
		return ac.currentRate
	}

	// Sample P99 latency.
	var p99 time.Duration
	if ac.windowLen > 0 {
		p99 = ac.computeP99()
	}

	// Sample GC pressure.
	var gcFrac float64
	if ac.gcMonitor != nil {
		gcFrac = ac.gcMonitor.Sample()
	}

	p99High := p99 > ac.targetP99
	p99Critical := p99 > ac.targetP99*2
	gcHigh := ac.gcMonitor != nil && gcFrac > ac.targetGCFraction
	gcCritical := ac.gcMonitor != nil && gcFrac > ac.criticalGCFraction

	// Auto-pause: either signal critical → halt compaction.
	// Resume only when both signals are healthy.
	if p99Critical || gcCritical {
		ac.paused.Store(true)
		ac.pausedReason = pauseReason(p99, ac.targetP99, gcFrac, ac.criticalGCFraction)
	} else if p99 < ac.targetP99 && (!ac.gcMonitor.enabled() || gcFrac < ac.targetGCFraction) {
		ac.paused.Store(false)
		ac.pausedReason = ""
	}

	// Rate adjustment: either signal high → reduce. Both healthy → increase.
	if p99High || gcHigh {
		// Reduce rate by 25%.
		newRate := ac.currentRate * 3 / 4
		if newRate < ac.minRate {
			newRate = ac.minRate
		}
		ac.currentRate = newRate
	} else if p99 < ac.targetP99/2 && (!ac.gcMonitor.enabled() || gcFrac < ac.targetGCFraction/2) {
		// Both signals well below threshold: increase rate by 10%.
		newRate := ac.currentRate * 11 / 10
		if newRate > ac.maxRate {
			newRate = ac.maxRate
		}
		ac.currentRate = newRate
	}
	// else: one or both signals in acceptable range, keep current rate.

	if ac.logger != nil {
		decision := "hold"
		if p99High || gcHigh {
			decision = "decrease"
		} else if p99 < ac.targetP99/2 && (!ac.gcMonitor.enabled() || gcFrac < ac.targetGCFraction/2) {
			decision = "increase"
		}
		ac.logger.Debug("adaptive rate adjust",
			"p99_ms", p99.Milliseconds(),
			"target_p99_ms", ac.targetP99.Milliseconds(),
			"gc_frac", gcFrac,
			"decision", decision,
			"rate_bytes_per_sec", ac.currentRate,
		)
	}

	return ac.currentRate
}

// Rate returns the current compaction rate in bytes/sec.
func (ac *AdaptiveController) Rate() int64 {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	return ac.currentRate
}

// Paused returns whether compaction should be paused.
func (ac *AdaptiveController) Paused() bool {
	return ac.paused.Load()
}

// SetPaused sets whether compaction should be paused.
func (ac *AdaptiveController) SetPaused(p bool) {
	ac.paused.Store(p)
	if !p {
		ac.mu.Lock()
		ac.pausedReason = ""
		ac.mu.Unlock()
	}
}

// PausedReason returns a human-readable reason for the current pause.
// Returns empty string when not paused.
func (ac *AdaptiveController) PausedReason() string {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	return ac.pausedReason
}

// GCStats returns the current GC CPU fraction and paused reason.
// Safe for concurrent use.
func (ac *AdaptiveController) GCStats() (gcCPUFraction float64, pausedReason string) {
	ac.mu.Lock()
	defer ac.mu.Unlock()

	if ac.gcMonitor != nil {
		gcCPUFraction = ac.gcMonitor.Fraction()
	}
	pausedReason = ac.pausedReason

	return
}

// computeP99 computes the P99 latency from the window.
// Must be called under ac.mu.Lock().
func (ac *AdaptiveController) computeP99() time.Duration {
	if ac.windowLen == 0 {
		return 0
	}

	// Copy and sort the active portion of the window.
	active := make([]time.Duration, ac.windowLen)
	copy(active, ac.window[:ac.windowLen])

	// Insertion sort for small windows (typically ~100 entries).
	for i := 1; i < len(active); i++ {
		for j := i; j > 0 && active[j] < active[j-1]; j-- {
			active[j], active[j-1] = active[j-1], active[j]
		}
	}

	// P99 index.
	idx := int(float64(len(active)) * 0.99)
	if idx >= len(active) {
		idx = len(active) - 1
	}

	return active[idx]
}

// enabled returns true if the GCMonitor is non-nil (i.e., GC monitoring is active).
// Convenience method to allow nil-safe checks.
func (m *GCMonitor) enabled() bool {
	return m != nil
}

// pauseReason builds a human-readable pause reason from current signals.
func pauseReason(p99, targetP99 time.Duration, gcFrac, criticalGC float64) string {
	p99Crit := p99 > targetP99*2
	gcCrit := gcFrac > criticalGC

	switch {
	case p99Crit && gcCrit:
		return "p99_latency_critical,gc_cpu_critical"
	case p99Crit:
		return "p99_latency_critical"
	case gcCrit:
		return "gc_cpu_critical"
	default:
		return ""
	}
}
