package main

import (
	"strings"
	"testing"
)

func TestCount_All_AfterIngest(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "count")
	if err != nil {
		t.Fatalf("count command failed: %v", err)
	}

	// Output should contain the total event count (1000 or 1,000).
	if !strings.Contains(stdout, "1000") && !strings.Contains(stdout, "1,000") {
		t.Errorf("expected '1000' or '1,000' in count output, got: %q", stdout)
	}
}

func TestCount_WithFilter(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "count",
		"where level=\""+testLevelError+"\"")
	if err != nil {
		t.Fatalf("count with filter failed: %v", err)
	}

	if !strings.Contains(stdout, "294") {
		t.Errorf("expected '294' in filtered count output, got: %q", stdout)
	}
}

func TestCount_JSON_Format(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "count", "--format", "json")
	if err != nil {
		t.Fatalf("count --format json failed: %v", err)
	}

	got := jsonCount(t, stdout)
	if got != 1000 {
		t.Errorf("expected count=1000 in JSON, got %d", got)
	}
}
