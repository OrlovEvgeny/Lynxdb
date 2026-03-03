package main

import (
	"strings"
	"testing"
)

func TestBench_SmallRun_CompletesWithoutError(t *testing.T) {
	stdout, _, err := runCmd(t, "bench", "--events", "1000")
	if err != nil {
		t.Fatalf("bench command failed: %v", err)
	}

	if !strings.Contains(stdout, "Ingest:") {
		t.Errorf("expected 'Ingest:' in bench output, got: %q", stdout)
	}
}

func TestBench_OutputContainsQueryResults(t *testing.T) {
	stdout, _, err := runCmd(t, "bench", "--events", "1000")
	if err != nil {
		t.Fatalf("bench command failed: %v", err)
	}

	// Bench output should contain query result lines.
	expectedQueries := []string{
		"Filtered aggregate",
		"Full scan aggregate",
		"Full-text search",
	}
	for _, q := range expectedQueries {
		if !strings.Contains(stdout, q) {
			t.Errorf("expected %q in bench output", q)
		}
	}
}

func TestBench_OutputContainsEventCount(t *testing.T) {
	stdout, _, err := runCmd(t, "bench", "--events", "500")
	if err != nil {
		t.Fatalf("bench command failed: %v", err)
	}

	// Should show the event count.
	if !strings.Contains(stdout, "500") {
		t.Errorf("expected '500' in bench output, got: %q", stdout)
	}
}
