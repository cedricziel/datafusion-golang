// Package gcs registers the gs:// scheme with objectstore, backed by
// gocloud.dev/blob/gcsblob. Importing this package for its side effect
// (blank import: `_ "github.com/cedricziel/datafusion-golang/objectstore/gcs"`)
// is enough to enable gs:// locations everywhere objectstore.Resolve is
// used.
//
// Credentials resolve via Google's Application Default Credentials
// (environment, workload identity, gcloud CLI login). See
// https://cloud.google.com/docs/authentication/production and
// gocloud.dev/blob/gcsblob's URLOpener for per-location query-parameter
// overrides.
package gcs

import (
	"context"
	"fmt"
	"net/url"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/gcsblob" // registers "gs" on gocloud's own URL mux, used below

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/internal/gocloudblob"
)

func init() {
	if err := objectstore.Register("gs", open); err != nil {
		panic(err)
	}
}

// open resolves u's authority and query — not its path, which is the
// in-store object key, handled by objectstore.Resolve itself — to a
// bucket, per design D2's "the opener receives the full URL (authority +
// query) and returns a store bound to that bucket".
func open(ctx context.Context, u *url.URL) (objectstore.Store, error) {
	bucketURL := &url.URL{Scheme: u.Scheme, Host: u.Host, RawQuery: u.RawQuery}
	bucket, err := blob.OpenBucket(ctx, bucketURL.String())
	if err != nil {
		return nil, fmt.Errorf("objectstore/gcs: opening bucket %q: %w", u.Host, err)
	}
	return gocloudblob.New(bucket), nil
}
