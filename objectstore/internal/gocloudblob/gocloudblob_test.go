package gocloudblob_test

import (
	"testing"

	"gocloud.dev/blob/memblob"

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/internal/gocloudblob"
	"github.com/cedricziel/datafusion-golang/objectstore/objectstoretest"
)

// TestStore runs the shared object-store contract suite against the
// gocloudblob adapter over gocloud's in-memory memblob driver — the code
// path S3, GCS, and Azure all share, exercised here with no cloud
// credentials.
func TestStore(t *testing.T) {
	objectstoretest.Run(t, func(t *testing.T) (objectstore.Store, func(string) string) {
		bucket := memblob.OpenBucket(nil)
		t.Cleanup(func() { _ = bucket.Close() })
		return gocloudblob.New(bucket), func(name string) string { return name }
	})
}
