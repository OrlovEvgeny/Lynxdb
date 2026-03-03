package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestVersion_ContainsLynxDB(t *testing.T) {
	stdout, _, err := runCmd(t, "version")
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	// In non-TTY (test) mode, version outputs JSON. The JSON should
	// contain all build info fields.
	if !strings.Contains(stdout, "version") {
		t.Errorf("expected 'version' key in JSON output, got: %q", stdout)
	}
}

func TestVersion_ContainsGoVersion(t *testing.T) {
	stdout, _, err := runCmd(t, "version")
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	if !strings.Contains(stdout, "go") {
		t.Errorf("expected Go version in output, got: %q", stdout)
	}
}

func TestVersion_Short(t *testing.T) {
	stdout, _, err := runCmd(t, "version", "--short")
	if err != nil {
		t.Fatalf("version --short failed: %v", err)
	}

	got := strings.TrimSpace(stdout)
	if got != "dev" {
		t.Errorf("version --short = %q, want %q", got, "dev")
	}
}

func TestVersion_JSON(t *testing.T) {
	stdout, _, err := runCmd(t, "version", "--json")
	if err != nil {
		t.Fatalf("version --json failed: %v", err)
	}

	var info map[string]string
	if err := json.Unmarshal([]byte(stdout), &info); err != nil {
		t.Fatalf("version --json output is not valid JSON: %v\noutput: %q", err, stdout)
	}

	requiredKeys := []string{"version", "commit", "date", "go", "os", "arch"}
	for _, key := range requiredKeys {
		if _, ok := info[key]; !ok {
			t.Errorf("version --json missing key %q", key)
		}
	}
}

func TestVersion_NonTTY_DefaultsToJSON(t *testing.T) {
	// runCmd captures stdout (non-TTY), so default should be JSON.
	stdout, _, err := runCmd(t, "version")
	if err != nil {
		t.Fatalf("version command failed: %v", err)
	}

	var info map[string]string
	if err := json.Unmarshal([]byte(stdout), &info); err != nil {
		t.Fatalf("non-TTY version output is not valid JSON: %v\noutput: %q", err, stdout)
	}
}
