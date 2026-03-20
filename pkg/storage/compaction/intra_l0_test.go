package compaction

import (
	"fmt"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/model"
)

func TestIntraL0_BelowThreshold(t *testing.T) {
	il := &IntraL0{Threshold: 8}
	// Only 4 L0 segments — below threshold.
	segments := makeL0Segments(4)
	plans := il.Plan(segments)
	if len(plans) != 0 {
		t.Fatalf("expected 0 plans for %d L0 segments, got %d", len(segments), len(plans))
	}
}

func TestIntraL0_AtThreshold(t *testing.T) {
	il := &IntraL0{Threshold: 8}
	segments := makeL0Segments(8)
	plans := il.Plan(segments)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans for 8 L0 segments, got %d", len(plans))
	}
	for _, p := range plans {
		if p.OutputLevel != L0 {
			t.Errorf("intra-L0 plan should output L0, got %d", p.OutputLevel)
		}
		if len(p.InputSegments) != L0CompactionThreshold {
			t.Errorf("expected %d inputs, got %d", L0CompactionThreshold, len(p.InputSegments))
		}
	}
}

func TestIntraL0_AboveThreshold(t *testing.T) {
	il := &IntraL0{Threshold: 8}
	segments := makeL0Segments(12)
	plans := il.Plan(segments)
	if len(plans) != 3 {
		t.Fatalf("expected 3 plans for 12 L0 segments, got %d", len(plans))
	}
}

func TestIntraL0_IgnoresNonL0(t *testing.T) {
	il := &IntraL0{Threshold: 8}
	segments := makeL0Segments(8)
	// Add L1 segments — should be ignored.
	for i := 0; i < 4; i++ {
		segments = append(segments, &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:    fmt.Sprintf("l1-seg-%d", i),
				Level: L1,
			},
		})
	}
	plans := il.Plan(segments)
	// Should still produce 2 plans (only L0 segments counted).
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans, got %d", len(plans))
	}
}

func TestIntraL0_EmptyInput(t *testing.T) {
	il := &IntraL0{Threshold: 8}
	plans := il.Plan(nil)
	if len(plans) != 0 {
		t.Fatalf("expected 0 plans for empty input, got %d", len(plans))
	}
}

func TestIntraL0_DefaultThreshold(t *testing.T) {
	il := &IntraL0{} // zero value
	segments := makeL0Segments(8)
	plans := il.Plan(segments)
	if len(plans) != 2 {
		t.Fatalf("expected 2 plans with default threshold, got %d", len(plans))
	}
}

func TestIntraL0_TimeOrdering(t *testing.T) {
	il := &IntraL0{Threshold: 8}
	segments := makeL0Segments(8)
	plans := il.Plan(segments)
	// First plan should contain earliest 4 segments.
	for i, seg := range plans[0].InputSegments {
		if seg.Meta.ID != fmt.Sprintf("l0-seg-%d", i) {
			t.Errorf("plan[0] segment %d: expected l0-seg-%d, got %s", i, i, seg.Meta.ID)
		}
	}
}

func makeL0Segments(n int) []*SegmentInfo {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	segments := make([]*SegmentInfo, n)
	for i := 0; i < n; i++ {
		segments[i] = &SegmentInfo{
			Meta: model.SegmentMeta{
				ID:      fmt.Sprintf("l0-seg-%d", i),
				Index:   "main",
				Level:   L0,
				MinTime: base.Add(time.Duration(i) * time.Hour),
				MaxTime: base.Add(time.Duration(i)*time.Hour + 30*time.Minute),
			},
		}
	}
	return segments
}
