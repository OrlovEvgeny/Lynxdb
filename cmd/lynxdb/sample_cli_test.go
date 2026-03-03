package main

import (
	"testing"
)

func TestSample_Default_JSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "sample", "--format", "json")
	if err != nil {
		t.Fatalf("sample command failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 event from sample")
	}

	if len(rows) > 5 {
		t.Errorf("default sample should return at most 5 events, got %d", len(rows))
	}
}

func TestSample_CustomCount_JSON(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "sample", "--format", "json", "3")
	if err != nil {
		t.Fatalf("sample 3 failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) != 3 {
		t.Errorf("expected 3 sampled events, got %d", len(rows))
	}
}

func TestSample_EventsHaveFields(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "sample", "--format", "json", "1")
	if err != nil {
		t.Fatalf("sample 1 failed: %v", err)
	}

	rows := mustParseJSON(t, stdout)
	if len(rows) == 0 {
		t.Fatal("expected at least 1 event")
	}

	// Each event should have at least a _raw or level field.
	row := rows[0]
	if len(row) == 0 {
		t.Error("sampled event has no fields")
	}
}
