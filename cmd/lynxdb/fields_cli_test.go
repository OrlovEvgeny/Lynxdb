package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestFields_AfterIngest_ContainsKnownFields(t *testing.T) {
	baseURL := setupServerWithData(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "fields", "--format", "json")
	if err != nil {
		t.Fatalf("fields command failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	if len(lines) == 0 {
		t.Fatal("expected at least one field in output")
	}

	fieldNames := make(map[string]bool)

	for _, line := range lines {
		var entry map[string]interface{}
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("parse field JSON line: %v\nline: %q", err, line)
		}

		name, _ := entry["name"].(string)
		if name != "" {
			fieldNames[name] = true
		}
	}

	// The access.log has a "level" field that the field catalog should discover.
	if !fieldNames["level"] {
		t.Errorf("expected 'level' field in catalog, got fields: %v", fieldNames)
	}
}

func TestFields_EmptyServer_NoError(t *testing.T) {
	baseURL := newTestServer(t)

	stdout, _, err := runCmd(t, "--server", baseURL, "fields", "--format", "json")
	if err != nil {
		t.Fatalf("fields command failed on empty server: %v", err)
	}

	// Empty server should return no fields — output should be empty or blank.
	if trimmed := strings.TrimSpace(stdout); trimmed != "" {
		// If there's output, each line should still be valid JSON.
		for _, line := range strings.Split(trimmed, "\n") {
			var entry map[string]interface{}
			if err := json.Unmarshal([]byte(line), &entry); err != nil {
				t.Errorf("unexpected non-JSON output on empty server: %q", line)
			}
		}
	}
}
