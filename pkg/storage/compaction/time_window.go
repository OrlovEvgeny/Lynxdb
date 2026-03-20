package compaction

import (
	"log/slog"
	"sort"
	"time"
)

// DefaultMaxMergeSegments is the maximum number of L2 segments merged into
// a single L3 segment. Capping this prevents excessively large merges that
// can exceed shutdown drain timeouts and cause repeated recompaction.
const DefaultMaxMergeSegments = 20

// TimeWindow implements a time-window compaction strategy for L2->L3.
// It consolidates multiple L2 segments within a cold partition (no writes
// for ColdThreshold duration) into L3 archive segments.
//
// A partition is considered "cold" when all its L2 segments have a
// CreatedAt older than ColdThreshold. This avoids archiving data that
// is still being actively compacted at lower levels.
type TimeWindow struct {
	ColdThreshold    time.Duration // partition must be idle for this long (default 48h)
	MaxMergeSegments int           // max segments per L3 merge (default 20)
	Logger           *slog.Logger
}

// Plan returns plans merging L2 segments into L3 segments if the partition
// is cold. Segments are sorted by MinTime and chunked into groups of at
// most MaxMergeSegments for temporal locality and bounded merge size.
// Returns nil if fewer than 2 L2 segments exist or if any segment was
// created within the ColdThreshold window.
func (tw *TimeWindow) Plan(segments []*SegmentInfo) []*Plan {
	threshold := tw.ColdThreshold
	if threshold == 0 {
		threshold = 48 * time.Hour
	}

	maxMerge := tw.MaxMergeSegments
	if maxMerge <= 0 {
		maxMerge = DefaultMaxMergeSegments
	}

	// Only consider L2 segments.
	var l2 []*SegmentInfo
	for _, s := range segments {
		if s.Meta.Level == L2 {
			l2 = append(l2, s)
		}
	}

	// Need at least 2 L2 segments to justify consolidation.
	if len(l2) < 2 {
		return nil
	}

	if tw.Logger != nil {
		tw.Logger.Debug("time window plan",
			"l2_count", len(l2),
			"cold_threshold", threshold,
		)
	}

	// Check if partition is cold: all segments must be older than ColdThreshold.
	now := time.Now()
	for _, s := range l2 {
		if now.Sub(s.Meta.CreatedAt) < threshold {
			return nil // partition still warm
		}
	}

	if tw.Logger != nil {
		tw.Logger.Debug("time window partition is cold",
			"l2_count", len(l2),
		)
	}

	// Sort by MinTime for temporal locality before chunking.
	sort.Slice(l2, func(i, j int) bool {
		return l2[i].Meta.MinTime.Before(l2[j].Meta.MinTime)
	})

	// Chunk into bounded merge groups.
	var plans []*Plan
	for i := 0; i < len(l2); i += maxMerge {
		end := i + maxMerge
		if end > len(l2) {
			end = len(l2)
		}

		chunk := l2[i:end]
		if len(chunk) < 2 {
			break
		}

		plans = append(plans, &Plan{
			InputSegments: append([]*SegmentInfo(nil), chunk...),
			OutputLevel:   L3,
		})
	}

	return plans
}
