package s3_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cedricziel/datafusion-golang/objectstore"
	_ "github.com/cedricziel/datafusion-golang/objectstore/s3"
)

// TestSchemeRegistered confirms importing this package registers the s3
// scheme, so Resolve routes to it instead of failing with
// UnregisteredSchemeError. Bounded by a short deadline: resolving loads
// the AWS SDK's default config (env/shared-config/IMDS with a short
// timeout), which needs no network access to succeed, but should never
// be allowed to hang the test suite if IMDS probing is slow in some CI
// environment.
func TestSchemeRegistered(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, _, err := objectstore.Resolve(ctx, "s3://some-bucket/key")
	var unregistered *objectstore.UnregisteredSchemeError
	if errors.As(err, &unregistered) {
		t.Fatalf("s3 scheme not registered: %v", err)
	}
	// Any other error (e.g. from AWS config loading with no credentials
	// present) is fine here — this test only asserts the scheme routes
	// to this package's opener, not that a real bucket is reachable.
}
