package objectstore_test

import (
	"context"
	"errors"
	"net/url"
	"testing"

	"github.com/cedricziel/datafusion-golang/objectstore"
)

func TestResolveBarePathIsLocal(t *testing.T) {
	store, path, err := objectstore.Resolve(context.Background(), "/data/orders.parquet")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if store != objectstore.Local {
		t.Fatal("bare path did not resolve to the local backend")
	}
	if path != "/data/orders.parquet" {
		t.Fatalf("path = %q, want %q", path, "/data/orders.parquet")
	}
}

func TestResolveFileSchemeIsLocal(t *testing.T) {
	store, path, err := objectstore.Resolve(context.Background(), "file:///data/orders.parquet")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if store != objectstore.Local {
		t.Fatal("file:// did not resolve to the local backend")
	}
	if path != "/data/orders.parquet" {
		t.Fatalf("path = %q, want %q", path, "/data/orders.parquet")
	}
}

func TestRegisterAndResolveSingleLetterScheme(t *testing.T) {
	// x://host/key is a valid URI with a genuine one-character scheme
	// (url.Parse gives it a non-empty Host) — distinct from a
	// drive-letter path like C:\data\x, which has no "://" authority and
	// so an empty Host. A single-letter scheme must be registrable and
	// must resolve through its own opener, not fall back to local.
	const scheme = "x"
	called := false
	if err := objectstore.Register(scheme, func(_ context.Context, u *url.URL) (objectstore.Store, error) {
		called = true
		return objectstore.Local, nil
	}); err != nil {
		t.Fatalf("Register(%q, ...): %v", scheme, err)
	}

	store, path, err := objectstore.Resolve(context.Background(), "x://host/key")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !called {
		t.Fatal("registered opener for single-letter scheme was not invoked")
	}
	if store != objectstore.Local {
		t.Fatal("Resolve did not return the store produced by the registered opener")
	}
	if path != "key" {
		t.Fatalf("path = %q, want %q", path, "key")
	}
}

func TestResolveDriveLetterIsLocal(t *testing.T) {
	store, path, err := objectstore.Resolve(context.Background(), `C:\data\orders.parquet`)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if store != objectstore.Local {
		t.Fatal("drive-letter path did not resolve to the local backend")
	}
	if path != `C:\data\orders.parquet` {
		t.Fatalf("path = %q, want unchanged drive-letter path", path)
	}
}

func TestResolveUnregisteredSchemeFails(t *testing.T) {
	_, _, err := objectstore.Resolve(context.Background(), "s3://bucket/key")
	if err == nil {
		t.Fatal("expected an error for an unregistered scheme")
	}
	var unregistered *objectstore.UnregisteredSchemeError
	if !errors.As(err, &unregistered) {
		t.Fatalf("error = %v, want an *UnregisteredSchemeError", err)
	}
	if unregistered.Scheme != "s3" {
		t.Fatalf("Scheme = %q, want %q", unregistered.Scheme, "s3")
	}
	if got := err.Error(); got == "" {
		t.Fatal("error message is empty")
	}
}

func TestRegisterRejectsReservedSchemes(t *testing.T) {
	for _, scheme := range []string{"", "file"} {
		if err := objectstore.Register(scheme, func(_ context.Context, _ *url.URL) (objectstore.Store, error) {
			return objectstore.Local, nil
		}); err == nil {
			t.Errorf("Register(%q, ...) succeeded, want an error (reserved scheme)", scheme)
		}
	}
}

func TestResolveWithOptions_WithStoreBypassesRegistry(t *testing.T) {
	mem := objectstore.NewLocalStore() // any Store value works as the override
	store, path, err := objectstore.ResolveWithOptions(context.Background(), "s3://bucket/key", objectstore.WithStore(mem))
	if err != nil {
		t.Fatalf("ResolveWithOptions: %v", err)
	}
	if store != mem {
		t.Fatal("WithStore override was not used")
	}
	if path != "s3://bucket/key" {
		t.Fatalf("path = %q, want the location used as-is", path)
	}
}

func TestResolveWithOptions_NoOptionsFallsBackToResolve(t *testing.T) {
	store, path, err := objectstore.ResolveWithOptions(context.Background(), "/data/x.parquet")
	if err != nil {
		t.Fatalf("ResolveWithOptions: %v", err)
	}
	if store != objectstore.Local {
		t.Fatal("expected the ordinary Resolve fallback for a bare path")
	}
	if path != "/data/x.parquet" {
		t.Fatalf("path = %q, want %q", path, "/data/x.parquet")
	}
}

func TestRegisterCustomOpener(t *testing.T) {
	const scheme = "custom-test-scheme"
	called := false
	if err := objectstore.Register(scheme, func(_ context.Context, u *url.URL) (objectstore.Store, error) {
		called = true
		return objectstore.Local, nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	store, _, err := objectstore.Resolve(context.Background(), scheme+"://host/key")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if !called {
		t.Fatal("custom opener was not invoked")
	}
	if store != objectstore.Local {
		t.Fatal("Resolve did not return the store produced by the custom opener")
	}
}
