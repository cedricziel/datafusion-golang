// Package azure registers the azblob:// scheme with objectstore, backed
// by gocloud.dev/blob/azureblob. Importing this package for its side
// effect (blank import:
// `_ "github.com/cedricziel/datafusion-golang/objectstore/azure"`) is
// enough to enable azblob:// locations everywhere objectstore.Resolve is
// used.
//
// Credentials resolve via environment variables (AZURE_STORAGE_ACCOUNT +
// AZURE_STORAGE_KEY, a connection string, or a SAS token) or, absent
// those, azidentity's DefaultAzureCredential chain (CLI login, managed
// identity, environment). See gocloud.dev/blob/azureblob's package doc
// and URLOpener for the full set and per-location query-parameter
// overrides.
package azure

import (
	"context"
	"fmt"
	"net/url"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/azureblob" // registers "azblob" on gocloud's own URL mux, used below

	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/objectstore/internal/gocloudblob"
)

func init() {
	if err := objectstore.Register("azblob", open); err != nil {
		panic(err)
	}
}

// open resolves u's authority and query — not its path, which is the
// in-store object key, handled by objectstore.Resolve itself — to a
// bucket (Azure calls it a "container"), per design D2's "the opener
// receives the full URL (authority + query) and returns a store bound to
// that bucket".
func open(ctx context.Context, u *url.URL) (objectstore.Store, error) {
	bucketURL := &url.URL{Scheme: u.Scheme, Host: u.Host, RawQuery: u.RawQuery}
	bucket, err := blob.OpenBucket(ctx, bucketURL.String())
	if err != nil {
		return nil, fmt.Errorf("objectstore/azure: opening container %q: %w", u.Host, err)
	}
	return gocloudblob.New(bucket), nil
}
