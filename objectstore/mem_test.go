package objectstore_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/objectstoretest"
)

func TestMemStore(t *testing.T) {
	bucket := 0
	objectstoretest.Run(t, func(t *testing.T) (objectstore.Store, func(string) string) {
		bucket++
		store, _, err := objectstore.Resolve(context.Background(), fmt.Sprintf("mem://bucket-%d/x", bucket))
		if err != nil {
			t.Fatalf("Resolve mem://: %v", err)
		}
		return store, func(name string) string { return name }
	})
}

func TestMemStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, path, err := objectstore.Resolve(ctx, "mem://t/x")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if path != "x" {
		t.Fatalf("path = %q, want %q", path, "x")
	}

	w, err := store.Create(ctx, path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store2, path2, err := objectstore.Resolve(ctx, "mem://t/x")
	if err != nil {
		t.Fatalf("Resolve (second): %v", err)
	}
	obj, err := store2.Open(ctx, path2)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer obj.Close()

	buf := make([]byte, obj.Size())
	if _, err := obj.Read(buf); err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf) != "payload" {
		t.Fatalf("got %q, want %q", buf, "payload")
	}
}

func TestMemStoreDistinctBucketsAreIsolated(t *testing.T) {
	ctx := context.Background()
	storeA, pathA, err := objectstore.Resolve(ctx, "mem://bucket-a/shared-key")
	if err != nil {
		t.Fatalf("Resolve bucket-a: %v", err)
	}
	w, err := storeA.Create(ctx, pathA)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, err := w.Write([]byte("a")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	storeB, pathB, err := objectstore.Resolve(ctx, "mem://bucket-b/shared-key")
	if err != nil {
		t.Fatalf("Resolve bucket-b: %v", err)
	}
	if _, err := storeB.Open(ctx, pathB); err == nil {
		t.Fatal("bucket-b sees an object written to bucket-a")
	}
}
