package part

import (
	"context"
	"os"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/storage/segment"
)

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()

	if err := syncDir(dir); err != nil {
		t.Fatalf("syncDir: %v", err)
	}
}

func TestPartStreamWriter_FinalizeWritesSegment(t *testing.T) {
	dir := t.TempDir()
	layout := NewLayout(dir)

	psw, err := NewPartStreamWriter(layout, "main", 0)
	if err != nil {
		t.Fatalf("NewPartStreamWriter: %v", err)
	}

	events := generateTestEvents(25)
	if err := psw.WriteRowGroup(context.Background(), events[:10]); err != nil {
		t.Fatalf("WriteRowGroup(1): %v", err)
	}
	if err := psw.WriteRowGroup(context.Background(), events[10:]); err != nil {
		t.Fatalf("WriteRowGroup(2): %v", err)
	}

	meta, err := psw.Finalize(context.Background())
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	if _, err := os.Stat(meta.Path); err != nil {
		t.Fatalf("Stat(%s): %v", meta.Path, err)
	}

	ms, err := segment.OpenSegmentFile(meta.Path)
	if err != nil {
		t.Fatalf("OpenSegmentFile: %v", err)
	}
	defer ms.Close()

	if got := ms.Reader().EventCount(); got != int64(len(events)) {
		t.Fatalf("EventCount: got %d, want %d", got, len(events))
	}
}
