package objectstore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/objectstoretest"
)

func TestLocalStore(t *testing.T) {
	objectstoretest.Run(t, func(t *testing.T) (objectstore.Store, func(string) string) {
		dir := t.TempDir()
		return objectstore.NewLocalStore(), func(name string) string {
			return filepath.Join(dir, filepath.FromSlash(name))
		}
	})
}

// TestLocalStore_InFlightWriteNotVisibleInList guards against an
// in-progress (or crash-orphaned) commit temp file being listed as if it
// were the committed object it's named after: a listing by the object's
// own prefix must not surface it before Close.
func TestLocalStore_InFlightWriteNotVisibleInList(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewLocalStore()
	target := filepath.Join(dir, "orders.parquet")

	w, err := store.Create(ctx, target)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("partial")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	defer w.Abort()

	for info, err := range store.List(ctx, target) {
		t.Fatalf("List saw an in-flight write before Close: %v, %v", info, err)
	}
}

// TestLocalStore_ListMatchesSiblingByStringPrefix asserts local's List
// matches the mem backend's pure string-prefix semantics even when the
// prefix also happens to name an existing directory: listing prefix "a"
// must still find a sibling file "ab", not just "a"'s own contents.
func TestLocalStore_ListMatchesSiblingByStringPrefix(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store := objectstore.NewLocalStore()

	inside := filepath.Join(dir, "a", "x")
	sibling := filepath.Join(dir, "ab")
	for _, p := range []string{inside, sibling} {
		w, err := store.Create(ctx, p)
		if err != nil {
			t.Fatalf("Create(%q): %v", p, err)
		}
		if _, err := w.Write([]byte("v")); err != nil {
			t.Fatalf("Write(%q): %v", p, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close(%q): %v", p, err)
		}
	}

	got := map[string]bool{}
	for info, err := range store.List(ctx, filepath.Join(dir, "a")) {
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		got[info.Path] = true
	}
	if !got[inside] {
		t.Errorf("List(prefix=%q) missing %q (directory contents)", filepath.Join(dir, "a"), inside)
	}
	if !got[sibling] {
		t.Errorf("List(prefix=%q) missing %q (sibling matching by string prefix, like the mem backend)", filepath.Join(dir, "a"), sibling)
	}
}
