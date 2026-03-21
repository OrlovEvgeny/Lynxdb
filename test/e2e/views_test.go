//go:build e2e

package e2e

import (
	"context"
	"testing"

	"github.com/lynxbase/lynxdb/pkg/client"
)

func TestE2E_Views_CRUD(t *testing.T) {
	h := NewHarness(t, WithDisk())
	ctx := context.Background()

	// Create.
	view, err := h.Client().CreateView(ctx, client.ViewInput{
		Name: "mv_test_hosts",
		Q:    `FROM main | stats count by host`,
	})
	if err != nil {
		t.Fatalf("CreateView: %v", err)
	}
	if view.Name != "mv_test_hosts" {
		t.Errorf("expected name=mv_test_hosts, got %s", view.Name)
	}

	// List.
	views, err := h.Client().ListViews(ctx)
	if err != nil {
		t.Fatalf("ListViews: %v", err)
	}
	found := false
	for _, v := range views {
		if v.Name == "mv_test_hosts" {
			found = true
		}
	}
	if !found {
		t.Error("created view not found in ListViews")
	}

	// Get.
	detail, err := h.Client().GetView(ctx, "mv_test_hosts")
	if err != nil {
		t.Fatalf("GetView: %v", err)
	}
	if detail.Name != "mv_test_hosts" {
		t.Errorf("GetView: expected name=mv_test_hosts, got %s", detail.Name)
	}

	// Patch.
	retention := "720h"
	patched, err := h.Client().PatchView(ctx, "mv_test_hosts", client.ViewPatchInput{
		Retention: &retention,
	})
	if err != nil {
		t.Fatalf("PatchView: %v", err)
	}
	if patched.Retention != "720h" {
		// BUG: PatchView returns empty retention instead of "720h".
		// The server may not be persisting or returning the patched retention field.
		// The application code must be fixed — do not weaken this assertion.
		t.Errorf("PatchView retention: got %q, want %q", patched.Retention, "720h")
	}

	// Delete.
	err = h.Client().DeleteView(ctx, "mv_test_hosts")
	if err != nil {
		t.Fatalf("DeleteView: %v", err)
	}

	// Verify deleted.
	_, err = h.Client().GetView(ctx, "mv_test_hosts")
	if err == nil {
		t.Error("expected error after deleting view, got nil")
	} else if !client.IsNotFound(err) {
		t.Errorf("GetView after delete: expected NotFound, got %v", err)
	}
}

func TestE2E_Views_CreateDuplicate_ReturnsAlreadyExists(t *testing.T) {
	h := NewHarness(t, WithDisk())
	ctx := context.Background()

	input := client.ViewInput{
		Name: "mv_dup_test",
		Q:    `FROM main | stats count`,
	}

	_, err := h.Client().CreateView(ctx, input)
	if err != nil {
		t.Fatalf("first CreateView: %v", err)
	}
	t.Cleanup(func() {
		_ = h.Client().DeleteView(ctx, "mv_dup_test")
	})

	_, err = h.Client().CreateView(ctx, input)
	if err == nil {
		t.Fatal("expected error for duplicate view, got nil")
	}
	if !client.IsAlreadyExists(err) {
		t.Errorf("duplicate create: expected AlreadyExists, got %v", err)
	}
}

func TestE2E_Views_DeleteNonexistent_ReturnsNotFound(t *testing.T) {
	h := NewHarness(t)
	ctx := context.Background()

	err := h.Client().DeleteView(ctx, "nonexistent_view_xyz")
	if err == nil {
		t.Fatal("expected error deleting nonexistent view, got nil")
	}
	if !client.IsNotFound(err) {
		t.Errorf("delete nonexistent: expected NotFound, got %v", err)
	}
}
