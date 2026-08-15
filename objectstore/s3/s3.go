// Package s3 registers the s3:// scheme with objectstore, backed by
// gocloud.dev/blob/s3blob. Importing this package for its side effect
// (blank import: `_ "github.com/cedricziel/datafusion-golang/objectstore/s3"`)
// is enough to enable s3:// locations everywhere objectstore.Resolve is
// used.
//
// Credentials resolve via the AWS SDK's standard chain (environment,
// shared config, IMDS). Connection parameters can be overridden per
// location through URL query parameters — notably region, endpoint, and
// use_path_style/s3ForcePathStyle for S3-compatible services such as
// MinIO — via gocloud's standard AWS URL parameter handling; see
// https://pkg.go.dev/gocloud.dev/aws#V2ConfigFromURLParams and
// gocloud.dev/blob/s3blob's URLOpener for the full set.
package s3

import (
	"context"
	"fmt"
	"net/url"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob" // registers "s3" on gocloud's own URL mux, used below

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/internal/gocloudblob"
)

func init() {
	if err := objectstore.Register("s3", open); err != nil {
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
		return nil, fmt.Errorf("objectstore/s3: opening bucket %q: %w", u.Host, err)
	}
	return gocloudblob.New(bucket), nil
}
