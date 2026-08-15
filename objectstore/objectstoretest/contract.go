// Package objectstoretest is a shared behavior contract for
// objectstore.Store implementations: every backend (local, mem, and the
// gocloud-backed cloud adapters) must pass the same suite, so scan and
// insert code that only depends on the objectstore.Store interface
// behaves identically regardless of which backend is behind it.
package objectstoretest

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/cedricziel/datafusion-golang/objectstore"
)

// Factory returns a Store to test, plus a function that maps a logical
// object name to a location string valid for that Store (e.g. joined
// onto a temp directory for the local backend, or returned unchanged for
// an in-memory backend).
type Factory func(t *testing.T) (store objectstore.Store, path func(name string) string)

// Run exercises the object-store capability's requirements
// (openspec/specs/object-store) against the Store newStore produces.
func Run(t *testing.T, newStore Factory) {
	t.Helper()
	ctx := context.Background()

	t.Run("OpenMissingFails", func(t *testing.T) {
		store, path := newStore(t)
		if _, err := store.Open(ctx, path("missing")); err == nil {
			t.Fatal("Open on a missing object returned no error")
		}
	})

	t.Run("OpenDoesNotReadData", func(t *testing.T) {
		store, path := newStore(t)
		p := path("open-no-read")
		mustWrite(t, ctx, store, p, []byte("hello world"))

		obj, err := store.Open(ctx, p)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = obj.Close() }()
		if got, want := obj.Size(), int64(len("hello world")); got != want {
			t.Fatalf("Size() = %d, want %d (available without reading data)", got, want)
		}
	})

	t.Run("RangedRead", func(t *testing.T) {
		store, path := newStore(t)
		p := path("ranged")
		mustWrite(t, ctx, store, p, []byte("0123456789"))

		obj, err := store.Open(ctx, p)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		defer func() { _ = obj.Close() }()

		buf := make([]byte, 3)
		n, err := obj.ReadAt(buf, 7)
		if err != nil && err != io.EOF {
			t.Fatalf("ReadAt: %v", err)
		}
		if n != 3 || string(buf) != "789" {
			t.Fatalf("ReadAt(off=7, len=3) = %q (n=%d), want %q", buf, n, "789")
		}
	})

	t.Run("CommitOnCloseVisibility", func(t *testing.T) {
		store, path := newStore(t)
		p := path("commit")

		w, err := store.Create(ctx, p)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := w.Write([]byte("partial")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if _, err := store.Open(ctx, p); err == nil {
			t.Fatal("object visible before Close")
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		got := mustRead(t, ctx, store, p)
		if string(got) != "partial" {
			t.Fatalf("after Close, object = %q, want %q", got, "partial")
		}
	})

	t.Run("AbortLeavesPriorState", func(t *testing.T) {
		store, path := newStore(t)
		p := path("abort")
		mustWrite(t, ctx, store, p, []byte("original"))

		w, err := store.Create(ctx, p)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if _, err := w.Write([]byte("replacement")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Abort(); err != nil {
			t.Fatalf("Abort: %v", err)
		}

		got := mustRead(t, ctx, store, p)
		if string(got) != "original" {
			t.Fatalf("aborted write changed the object: got %q, want %q", got, "original")
		}
	})

	t.Run("RemoveThenOpenFails", func(t *testing.T) {
		store, path := newStore(t)
		p := path("remove")
		mustWrite(t, ctx, store, p, []byte("x"))

		if err := store.Remove(ctx, p); err != nil {
			t.Fatalf("Remove: %v", err)
		}
		if _, err := store.Open(ctx, p); err == nil {
			t.Fatal("Open after Remove returned no error")
		}
	})

	t.Run("ListPrefix", func(t *testing.T) {
		store, path := newStore(t)
		a1, a2 := path("list/a/1"), path("list/a/2")
		mustWrite(t, ctx, store, a1, []byte("x"))
		mustWrite(t, ctx, store, a2, []byte("yy"))

		got := map[string]int64{}
		for info, err := range store.List(ctx, path("list/a/")) {
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			got[info.Path] = info.Size
		}
		want := map[string]int64{a1: 1, a2: 2}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("List = %v, want %v", got, want)
		}
	})

	t.Run("ListEmptyPrefixIsNotError", func(t *testing.T) {
		store, path := newStore(t)
		for info, err := range store.List(ctx, path("nothing-here/")) {
			t.Fatalf("List on an empty prefix yielded %v, %v; want nothing", info, err)
		}
	})
}

func mustWrite(t *testing.T, ctx context.Context, store objectstore.Store, path string, data []byte) {
	t.Helper()
	w, err := store.Create(ctx, path)
	if err != nil {
		t.Fatalf("Create(%q): %v", path, err)
	}
	if _, err := w.Write(data); err != nil {
		t.Fatalf("Write(%q): %v", path, err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close(%q): %v", path, err)
	}
}

func mustRead(t *testing.T, ctx context.Context, store objectstore.Store, path string) []byte {
	t.Helper()
	obj, err := store.Open(ctx, path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	defer func() { _ = obj.Close() }()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, io.NewSectionReader(obj, 0, obj.Size())); err != nil {
		t.Fatalf("reading %q: %v", path, err)
	}
	return buf.Bytes()
}
