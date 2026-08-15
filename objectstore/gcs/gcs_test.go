package gcs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cedricziel/datafusion-golang/objectstore"
	_ "github.com/cedricziel/datafusion-golang/objectstore/gcs"
)

// TestSchemeRegistered confirms importing this package registers the gs
// scheme, so Resolve routes to it instead of failing with
// UnregisteredSchemeError. Bounded by a short deadline — see the
// equivalent objectstore/s3 test for why this needs no network access to
// pass.
func TestSchemeRegistered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := objectstore.Resolve(ctx, "gs://some-bucket/key")
	var unregistered *objectstore.UnregisteredSchemeError
	if errors.As(err, &unregistered) {
		t.Fatalf("gs scheme not registered: %v", err)
	}
}
