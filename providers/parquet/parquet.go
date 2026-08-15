// Package parquet provides a datafusion.TableProvider backed by a local
// Parquet file, so it can be registered on a SessionContext and queried via
// SQL with no hand-written Parquet-reading code.
//
// The provider implements datafusion.PushdownTableProvider, so it is
// registered with scan pushdown enabled: projections prune column
// decoding exactly, pushed filters skip row groups whose column
// statistics or bloom filters prove no row can match, and the limit hint
// truncates the scan. Pushed filters are strictly advisory per the
// table-provider contract — they are used only to omit rows that cannot
// satisfy them (the engine re-applies every filter), and any missing or
// inconclusive metadata keeps the data, so results are identical with and
// without pruning.
package parquet

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
)

var readProps = pqarrow.ArrowReadProperties{BatchSize: 1024}

// tableProvider is a datafusion.TableProvider backed by a Parquet object
// at path within store. Each Scan opens its own file.Reader so concurrent
// and repeated scans never share state (see design D4).
type tableProvider struct {
	store  objectstore.Store
	path   string
	schema *arrow.Schema

	// insertMu serializes InsertInto calls against this instance so two
	// concurrent inserts can never race the rewrite-then-rename commit
	// (design D7 of wire-parquet-and-iceberg-insert). Scans never take it.
	insertMu sync.Mutex
}

// Option configures NewTableProvider.
type Option = objectstore.Option

// WithStore overrides the backend location resolves against, bypassing
// the objectstore registry: location is then used as-is as the in-store
// path. Intended for tests and explicitly configured buckets.
func WithStore(store objectstore.Store) Option { return objectstore.WithStore(store) }

// NewTableProvider opens the Parquet object at location — a bare local
// path or a URL whose scheme is registered with the objectstore package
// (file://, mem://, s3://, ...) — validating it and caching its Arrow
// schema. Construction fails if the location's scheme is not registered,
// the object does not exist, is not readable, or is not valid Parquet.
func NewTableProvider(ctx context.Context, location string, opts ...Option) (datafusion.TableProvider, error) {
	store, path, err := objectstore.ResolveWithOptions(ctx, location, opts...)
	if err != nil {
		return nil, fmt.Errorf("parquet: resolving %s: %w", location, err)
	}

	rdr, err := openReader(ctx, store, path)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, readProps, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("parquet: read schema of %s: %w", location, err)
	}
	schema, err := fr.Schema()
	if err != nil {
		return nil, fmt.Errorf("parquet: derive arrow schema of %s: %w", location, err)
	}

	return &tableProvider{store: store, path: path, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// openReader opens the Parquet object at path within store and wraps it
// in a file.Reader, closing the object if wrapping fails. Shared by
// NewTableProvider, Scan, and ScanWithOptions (pushdown.go) — every call
// site that needs a fresh reader over the object.
func openReader(ctx context.Context, store objectstore.Store, path string) (*file.Reader, error) {
	obj, err := store.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %s: %w", path, err)
	}
	rdr, err := file.NewParquetReader(obj)
	if err != nil {
		_ = obj.Close()
		return nil, fmt.Errorf("parquet: open %s: %w", path, err)
	}
	return rdr, nil
}

// Scan opens a fresh reader over the object so this scan is independent
// of any other concurrent or subsequent scan of the same provider (design
// D4). The returned reader closes the underlying object when it is
// released or exhausted (design D6).
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	rdr, err := openReader(ctx, t.store, t.path)
	if err != nil {
		return nil, err
	}

	fr, err := pqarrow.NewFileReader(rdr, readProps, memory.DefaultAllocator)
	if err != nil {
		_ = rdr.Close()
		return nil, fmt.Errorf("parquet: read %s: %w", t.path, err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		_ = rdr.Close()
		return nil, fmt.Errorf("parquet: scan %s: %w", t.path, err)
	}

	return &closingRecordReader{RecordReader: rr, file: rdr}, nil
}

// closingRecordReader wraps a pqarrow record reader so that Release, or
// running the underlying stream to exhaustion, also closes the parquet
// file this scan opened.
type closingRecordReader struct {
	array.RecordReader
	file   *file.Reader
	closed bool
}

func (r *closingRecordReader) Next() bool {
	ok := r.RecordReader.Next()
	if !ok {
		r.closeFile()
	}
	return ok
}

func (r *closingRecordReader) Release() {
	r.RecordReader.Release()
	r.closeFile()
}

func (r *closingRecordReader) closeFile() {
	if r.closed {
		return
	}
	r.closed = true
	_ = r.file.Close()
}
