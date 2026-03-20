package compaction

import (
	"sort"
)

// IntraL0 merges L0 segments among themselves when L0 segment count is high
// and no L0->L1 compaction is possible (e.g., L1 is busy). This reduces
// read amplification without waiting for cross-level compaction.
// Output segments remain at L0.
type IntraL0 struct {
	// Threshold is the minimum number of L0 segments to trigger intra-L0 merge.
	// Default: 2 * L0CompactionThreshold = 8.
	Threshold int
}

// Plan returns merge plans for L0 segments when the count exceeds the threshold.
// Groups adjacent (by time) L0 segments into merge batches.
func (il *IntraL0) Plan(segments []*SegmentInfo) []*Plan {
	threshold := il.Threshold
	if threshold <= 0 {
		threshold = 2 * L0CompactionThreshold
	}

	// Filter to L0 segments only.
	var l0 []*SegmentInfo
	for _, s := range segments {
		if s.Meta.Level == L0 {
			l0 = append(l0, s)
		}
	}

	if len(l0) < threshold {
		return nil
	}

	// Sort by MinTime for time-adjacent grouping.
	sort.Slice(l0, func(i, j int) bool {
		return l0[i].Meta.MinTime.Before(l0[j].Meta.MinTime)
	})

	// Group into batches of L0CompactionThreshold (4) adjacent segments.
	batchSize := L0CompactionThreshold
	var plans []*Plan
	for i := 0; i+batchSize <= len(l0); i += batchSize {
		batch := l0[i : i+batchSize]
		plans = append(plans, &Plan{
			InputSegments: batch,
			OutputLevel:   L0, // stays at L0
		})
	}

	return plans
}
