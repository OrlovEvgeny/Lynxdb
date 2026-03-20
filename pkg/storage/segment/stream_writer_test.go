package segment

import (
	"bytes"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
)

func TestStreamWriter_RoundTrip(t *testing.T) {
	// Write N row groups via StreamWriter, read with Reader, verify all events match.
	allEvents := generateTestEvents(500)

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)
	sw.SetRowGroupSize(200)

	// Write 3 row groups: 200, 200, 100.
	if err := sw.WriteRowGroup(allEvents[0:200]); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(allEvents[200:400]); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}
	if err := sw.WriteRowGroup(allEvents[400:500]); err != nil {
		t.Fatalf("WriteRowGroup(2): %v", err)
	}

	written, err := sw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if written != int64(buf.Len()) {
		t.Fatalf("written=%d, buf.Len()=%d", written, buf.Len())
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != int64(len(allEvents)) {
		t.Fatalf("EventCount: got %d, want %d", r.EventCount(), len(allEvents))
	}

	if r.RowGroupCount() != 3 {
		t.Fatalf("RowGroupCount: got %d, want 3", r.RowGroupCount())
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(allEvents) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(allEvents))
	}

	for i := range allEvents {
		if !readEvents[i].Time.Equal(allEvents[i].Time) {
			t.Errorf("event[%d].Time mismatch", i)
			break
		}
		if readEvents[i].Raw != allEvents[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)
			break
		}
		if readEvents[i].Host != allEvents[i].Host {
			t.Errorf("event[%d].Host mismatch", i)
			break
		}
		if readEvents[i].Source != allEvents[i].Source {
			t.Errorf("event[%d].Source mismatch", i)
			break
		}

		// Verify user fields.
		origLevel := allEvents[i].GetField("level")
		gotLevel := readEvents[i].GetField("level")
		if !origLevel.IsNull() && gotLevel.String() != origLevel.String() {
			t.Errorf("event[%d].level: got %q, want %q", i, gotLevel, origLevel)
			break
		}

		origStatus := allEvents[i].GetField("status")
		gotStatus := readEvents[i].GetField("status")
		if !origStatus.IsNull() {
			origInt, _ := origStatus.TryAsInt()
			gotInt, _ := gotStatus.TryAsInt()
			if gotInt != origInt {
				t.Errorf("event[%d].status: got %d, want %d", i, gotInt, origInt)
				break
			}
		}

		origLatency := allEvents[i].GetField("latency")
		gotLatency := readEvents[i].GetField("latency")
		if !origLatency.IsNull() {
			origF, _ := origLatency.TryAsFloat()
			gotF, _ := gotLatency.TryAsFloat()
			if math.Abs(gotF-origF) > 1e-10 {
				t.Errorf("event[%d].latency: got %v, want %v", i, gotF, origF)
				break
			}
		}
	}
}

func TestStreamWriter_SingleRG_MatchesWriter(t *testing.T) {
	// A single row group via StreamWriter should produce the same readable
	// result as the batch Writer.Write() method.
	events := generateTestEvents(100)

	// Write with batch Writer.
	var batchBuf bytes.Buffer
	bw := NewWriter(&batchBuf)
	if _, err := bw.Write(events); err != nil {
		t.Fatalf("batch Write: %v", err)
	}

	// Write with StreamWriter (single RG).
	var streamBuf bytes.Buffer
	sw := NewStreamWriter(&streamBuf, CompressionLZ4)
	if err := sw.WriteRowGroup(events); err != nil {
		t.Fatalf("StreamWriter.WriteRowGroup: %v", err)
	}
	if _, err := sw.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Open both segments.
	batchReader, err := OpenSegment(batchBuf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment(batch): %v", err)
	}
	streamReader, err := OpenSegment(streamBuf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment(stream): %v", err)
	}

	// Compare event counts.
	if batchReader.EventCount() != streamReader.EventCount() {
		t.Fatalf("EventCount mismatch: batch=%d, stream=%d",
			batchReader.EventCount(), streamReader.EventCount())
	}

	// Compare row group counts.
	if batchReader.RowGroupCount() != streamReader.RowGroupCount() {
		t.Fatalf("RowGroupCount mismatch: batch=%d, stream=%d",
			batchReader.RowGroupCount(), streamReader.RowGroupCount())
	}

	// Read events from both.
	batchEvents, err := batchReader.ReadEvents()
	if err != nil {
		t.Fatalf("batch ReadEvents: %v", err)
	}
	streamEvents, err := streamReader.ReadEvents()
	if err != nil {
		t.Fatalf("stream ReadEvents: %v", err)
	}

	if len(batchEvents) != len(streamEvents) {
		t.Fatalf("event count mismatch: batch=%d, stream=%d",
			len(batchEvents), len(streamEvents))
	}

	// Compare every event.
	for i := range batchEvents {
		be := batchEvents[i]
		se := streamEvents[i]

		if !be.Time.Equal(se.Time) {
			t.Errorf("event[%d].Time mismatch", i)
		}
		if be.Raw != se.Raw {
			t.Errorf("event[%d].Raw mismatch", i)
		}
		if be.Host != se.Host {
			t.Errorf("event[%d].Host mismatch", i)
		}
		if be.Source != se.Source {
			t.Errorf("event[%d].Source mismatch", i)
		}
		if be.SourceType != se.SourceType {
			t.Errorf("event[%d].SourceType mismatch", i)
		}
		if be.Index != se.Index {
			t.Errorf("event[%d].Index mismatch", i)
		}

		// User fields.
		for _, field := range []string{"level", "status", "latency"} {
			bv := be.GetField(field)
			sv := se.GetField(field)
			if bv.String() != sv.String() {
				t.Errorf("event[%d].%s: batch=%q, stream=%q", i, field, bv.String(), sv.String())
			}
		}
	}

	// Compare column names.
	batchCols := batchReader.ColumnNames()
	streamCols := streamReader.ColumnNames()
	if len(batchCols) != len(streamCols) {
		t.Errorf("column count mismatch: batch=%d, stream=%d", len(batchCols), len(streamCols))
	}
}

func TestStreamWriter_FinalizeWithoutWriteRowGroup(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)

	_, err := sw.Finalize()
	if err != ErrNoEvents {
		t.Fatalf("expected ErrNoEvents, got %v", err)
	}
}

func TestStreamWriter_WriteAfterFinalize(t *testing.T) {
	events := generateTestEvents(10)

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)

	if err := sw.WriteRowGroup(events); err != nil {
		t.Fatalf("WriteRowGroup: %v", err)
	}
	if _, err := sw.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	// Writing after Finalize should fail.
	err := sw.WriteRowGroup(events)
	if err != ErrFinalized {
		t.Fatalf("expected ErrFinalized, got %v", err)
	}

	// Double Finalize should also fail.
	_, err = sw.Finalize()
	if err != ErrFinalized {
		t.Fatalf("expected ErrFinalized on double Finalize, got %v", err)
	}
}

func TestStreamWriter_ProgressiveFieldDiscovery(t *testing.T) {
	// Test that fields appearing in later row groups that weren't
	// in earlier ones are handled correctly.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// RG0: events with fields [level, status]
	rg0Events := make([]*event.Event, 50)
	for i := range rg0Events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d level=info status=200", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue("info"))
		e.SetField("status", event.IntValue(200))
		rg0Events[i] = e
	}

	// RG1: events with fields [level, status, region] (new field "region")
	rg1Events := make([]*event.Event, 50)
	for i := range rg1Events {
		ts := base.Add(time.Duration(50+i) * time.Millisecond)
		e := event.NewEvent(ts,
			fmt.Sprintf("event %d level=warn status=500 region=us-east-1", 50+i))
		e.Host = "web-02"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue("warn"))
		e.SetField("status", event.IntValue(500))
		e.SetField("region", event.StringValue("us-east-1"))
		rg1Events[i] = e
	}

	// RG2: events with fields [level, action] (new field "action", no status/region)
	rg2Events := make([]*event.Event, 30)
	for i := range rg2Events {
		ts := base.Add(time.Duration(100+i) * time.Millisecond)
		e := event.NewEvent(ts,
			fmt.Sprintf("event %d level=error action=deploy", 100+i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		e.SetField("level", event.StringValue("error"))
		e.SetField("action", event.StringValue("deploy"))
		rg2Events[i] = e
	}

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)

	if err := sw.WriteRowGroup(rg0Events); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(rg1Events); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}
	if err := sw.WriteRowGroup(rg2Events); err != nil {
		t.Fatalf("WriteRowGroup(2): %v", err)
	}

	written, err := sw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	t.Logf("progressive field discovery: 130 events, %d bytes, 3 RGs", written)

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != 130 {
		t.Fatalf("EventCount: got %d, want 130", r.EventCount())
	}
	if r.RowGroupCount() != 3 {
		t.Fatalf("RowGroupCount: got %d, want 3", r.RowGroupCount())
	}

	// Read all events and verify fields.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != 130 {
		t.Fatalf("ReadEvents: got %d, want 130", len(readEvents))
	}

	// Verify RG0 events (level + status, no region or action).
	for i := 0; i < 50; i++ {
		e := readEvents[i]
		if e.GetField("level").String() != "info" {
			t.Errorf("event[%d].level: got %q, want %q", i, e.GetField("level").String(), "info")
			break
		}
		v := e.GetField("status")
		if v.IsNull() {
			t.Errorf("event[%d].status: expected non-null", i)
			break
		}
		n, _ := v.TryAsInt()
		if n != 200 {
			t.Errorf("event[%d].status: got %d, want 200", i, n)
			break
		}
	}

	// Verify RG1 events (level + status + region).
	for i := 50; i < 100; i++ {
		e := readEvents[i]
		if e.GetField("level").String() != "warn" {
			t.Errorf("event[%d].level: got %q, want %q", i, e.GetField("level").String(), "warn")
			break
		}
		regV := e.GetField("region")
		if regV.IsNull() {
			t.Errorf("event[%d].region: expected non-null", i)
			break
		}
		if regV.String() != "us-east-1" {
			t.Errorf("event[%d].region: got %q, want %q", i, regV.String(), "us-east-1")
			break
		}
	}

	// Verify RG2 events (level + action).
	for i := 100; i < 130; i++ {
		e := readEvents[i]
		if e.GetField("level").String() != "error" {
			t.Errorf("event[%d].level: got %q, want %q", i, e.GetField("level").String(), "error")
			break
		}
		actV := e.GetField("action")
		if actV.IsNull() {
			t.Errorf("event[%d].action: expected non-null", i)
			break
		}
		if actV.String() != "deploy" {
			t.Errorf("event[%d].action: got %q, want %q", i, actV.String(), "deploy")
			break
		}
	}

	// Verify the presence bitmap: "region" should be present in RG1 but not RG0.
	if r.HasColumnInRowGroup(0, "region") {
		t.Error("RG0 should NOT have 'region' column")
	}
	if !r.HasColumnInRowGroup(1, "region") {
		t.Error("RG1 should have 'region' column")
	}

	// "action" should be present in RG2 but not RG0 or RG1.
	if r.HasColumnInRowGroup(0, "action") {
		t.Error("RG0 should NOT have 'action' column")
	}
	if r.HasColumnInRowGroup(1, "action") {
		t.Error("RG1 should NOT have 'action' column")
	}
	if !r.HasColumnInRowGroup(2, "action") {
		t.Error("RG2 should have 'action' column")
	}
}

func TestStreamWriter_BloomAndInvertedIndex(t *testing.T) {
	// Verify that bloom filters and inverted index work correctly
	// with streaming writes.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// RG0: contains "alpha" but not "beta".
	rg0Events := make([]*event.Event, 100)
	for i := range rg0Events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d alpha host=web-01", i))
		e.Host = "web-01"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		rg0Events[i] = e
	}

	// RG1: contains "beta" but not "alpha".
	rg1Events := make([]*event.Event, 100)
	for i := range rg1Events {
		ts := base.Add(time.Duration(100+i) * time.Millisecond)
		e := event.NewEvent(ts,
			fmt.Sprintf("event %d beta host=web-02", 100+i))
		e.Host = "web-02"
		e.Source = "/var/log/app.log"
		e.SourceType = "json"
		e.Index = "main"
		rg1Events[i] = e
	}

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)

	if err := sw.WriteRowGroup(rg0Events); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(rg1Events); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}

	if _, err := sw.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.RowGroupCount() != 2 {
		t.Fatalf("RowGroupCount: got %d, want 2", r.RowGroupCount())
	}

	// Bloom filter: "alpha" should match only RG0.
	alphaRGs, err := r.CheckBloomForRowGroups("alpha")
	if err != nil {
		t.Fatalf("CheckBloomForRowGroups(alpha): %v", err)
	}
	if len(alphaRGs) != 1 || alphaRGs[0] != 0 {
		t.Errorf("expected [0] for 'alpha', got %v", alphaRGs)
	}

	// "beta" should match only RG1.
	betaRGs, err := r.CheckBloomForRowGroups("beta")
	if err != nil {
		t.Fatalf("CheckBloomForRowGroups(beta): %v", err)
	}
	if len(betaRGs) != 1 || betaRGs[0] != 1 {
		t.Errorf("expected [1] for 'beta', got %v", betaRGs)
	}

	// Inverted index: search for "alpha" should return only RG0 events.
	inv, err := r.InvertedIndex()
	if err != nil {
		t.Fatalf("InvertedIndex: %v", err)
	}
	if inv == nil {
		t.Fatal("expected inverted index")
	}

	alphaBm, err := inv.Search("alpha")
	if err != nil {
		t.Fatalf("Search(alpha): %v", err)
	}
	if alphaBm.GetCardinality() != 100 {
		t.Errorf("expected 100 matches for 'alpha', got %d", alphaBm.GetCardinality())
	}

	betaBm, err := inv.Search("beta")
	if err != nil {
		t.Fatalf("Search(beta): %v", err)
	}
	if betaBm.GetCardinality() != 100 {
		t.Errorf("expected 100 matches for 'beta', got %d", betaBm.GetCardinality())
	}
}

func TestStreamWriter_EmptyRowGroup(t *testing.T) {
	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)

	err := sw.WriteRowGroup(nil)
	if err != ErrNoEvents {
		t.Fatalf("expected ErrNoEvents for nil events, got %v", err)
	}

	err = sw.WriteRowGroup([]*event.Event{})
	if err != ErrNoEvents {
		t.Fatalf("expected ErrNoEvents for empty events, got %v", err)
	}
}

func TestStreamWriter_LargeMultiRG(t *testing.T) {
	// Test with enough events to span multiple row groups at default size.
	events := generateTestEvents(DefaultRowGroupSize + 500)

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)

	// Write in chunks of default row group size.
	sw.SetRowGroupSize(DefaultRowGroupSize)
	if err := sw.WriteRowGroup(events[:DefaultRowGroupSize]); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(events[DefaultRowGroupSize:]); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}

	written, err := sw.Finalize()
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	t.Logf("large multi-RG: %d events, %d bytes, 2 RGs", len(events), written)

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	if r.EventCount() != int64(len(events)) {
		t.Fatalf("EventCount: got %d, want %d", r.EventCount(), len(events))
	}
	if r.RowGroupCount() != 2 {
		t.Fatalf("RowGroupCount: got %d, want 2", r.RowGroupCount())
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents count: got %d, want %d", len(readEvents), len(events))
	}

	// Spot-check a few events.
	for _, idx := range []int{0, 1000, DefaultRowGroupSize, len(events) - 1} {
		if !readEvents[idx].Time.Equal(events[idx].Time) {
			t.Errorf("event[%d].Time mismatch", idx)
		}
		if readEvents[idx].Raw != events[idx].Raw {
			t.Errorf("event[%d].Raw mismatch", idx)
		}
	}
}

func TestStreamWriter_ZSTD_Compression(t *testing.T) {
	events := generateTestEvents(200)

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionZSTD)
	sw.SetRowGroupSize(100)

	if err := sw.WriteRowGroup(events[:100]); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(events[100:]); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}

	if _, err := sw.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	if len(readEvents) != len(events) {
		t.Fatalf("ReadEvents: got %d, want %d", len(readEvents), len(events))
	}

	for i := range events {
		if readEvents[i].Raw != events[i].Raw {
			t.Errorf("event[%d].Raw mismatch", i)
			break
		}
	}
}

func TestStreamWriter_ConstColumns(t *testing.T) {
	// All events have the same _source, _sourcetype, and index,
	// which should be detected as const columns.
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := make([]*event.Event, 100)
	for i := range events {
		e := event.NewEvent(base.Add(time.Duration(i)*time.Millisecond),
			fmt.Sprintf("event %d", i))
		e.Host = "web-01"
		e.Source = "nginx"
		e.SourceType = "access"
		e.Index = "main"
		e.SetField("env", event.StringValue("production"))
		events[i] = e
	}

	var buf bytes.Buffer
	sw := NewStreamWriter(&buf, CompressionLZ4)
	sw.SetRowGroupSize(50)

	if err := sw.WriteRowGroup(events[:50]); err != nil {
		t.Fatalf("WriteRowGroup(0): %v", err)
	}
	if err := sw.WriteRowGroup(events[50:]); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}

	if _, err := sw.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	r, err := OpenSegment(buf.Bytes())
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}

	// _source, _sourcetype, host, index, and env should all be const in both RGs.
	for rgIdx := 0; rgIdx < 2; rgIdx++ {
		for _, col := range []string{"_source", "_sourcetype", "host", "index", "env"} {
			if !r.IsConstColumn(rgIdx, col) {
				t.Errorf("RG%d: expected %q to be const column", rgIdx, col)
			}
		}
	}

	// Verify the const values.
	val, ok := r.GetConstValue(0, "_source")
	if !ok || val != "nginx" {
		t.Errorf("_source const: got %q, want %q", val, "nginx")
	}
	val, ok = r.GetConstValue(0, "env")
	if !ok || val != "production" {
		t.Errorf("env const: got %q, want %q", val, "production")
	}

	// Full round-trip.
	readEvents, err := r.ReadEvents()
	if err != nil {
		t.Fatalf("ReadEvents: %v", err)
	}
	for i, e := range readEvents {
		if e.Source != "nginx" {
			t.Errorf("event[%d].Source: got %q, want %q", i, e.Source, "nginx")
			break
		}
		if e.GetField("env").String() != "production" {
			t.Errorf("event[%d].env: got %q, want %q", i, e.GetField("env").String(), "production")
			break
		}
	}
}
