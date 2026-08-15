// Package jsonl provides a datafusion.TableProvider backed by a local
// newline-delimited JSON (JSON Lines) file, so it can be registered on a
// SessionContext and queried via SQL with no hand-written JSON-reading
// code.
//
// The file must contain one JSON object per row; arrow-go has no schema
// inference for JSON, so schema is always caller-supplied. The provider
// does not implement datafusion.PushdownTableProvider — every scan reads
// and decodes the whole file.
package jsonl

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/providers/internal/closingreader"
)

const batchSize = 1024

// tableProvider is a datafusion.TableProvider backed by a JSON Lines
// object at path within store. Each Scan opens its own reader so
// concurrent and repeated scans never share state, mirroring
// providers/parquet.
type tableProvider struct {
	store  objectstore.Store
	path   string
	schema *arrow.Schema
}

// Option configures NewTableProvider.
type Option = objectstore.Option

// WithStore overrides the backend location resolves against, bypassing
// the objectstore registry: location is then used as-is as the in-store
// path. Intended for tests and explicitly configured buckets.
func WithStore(store objectstore.Store) Option { return objectstore.WithStore(store) }

// NewTableProvider opens the JSON Lines object at location against
// schema, decoding the first object to validate it against schema and
// caching the schema. Construction fails if location's scheme is not
// registered, the object does not exist, is not readable, or its first
// line does not decode against schema.
func NewTableProvider(ctx context.Context, location string, schema *arrow.Schema, opts ...Option) (datafusion.TableProvider, error) {
	store, path, err := objectstore.ResolveWithOptions(ctx, location, opts...)
	if err != nil {
		return nil, fmt.Errorf("jsonl: resolving %s: %w", location, err)
	}

	obj, err := store.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("jsonl: open %s: %w", location, err)
	}
	defer obj.Close()

	rdr := array.NewJSONReader(obj, schema)
	defer rdr.Release()
	rdr.Next()
	if err := rdr.Err(); err != nil {
		return nil, fmt.Errorf("jsonl: decode first object of %s: %w", location, err)
	}

	return &tableProvider{store: store, path: path, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan opens a fresh reader over the object so this scan is independent
// of any other concurrent or subsequent scan of the same provider. The
// returned reader closes the underlying object when it is released or
// exhausted.
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	obj, err := t.store.Open(ctx, t.path)
	if err != nil {
		return nil, fmt.Errorf("jsonl: open %s: %w", t.path, err)
	}

	rdr := array.NewJSONReader(obj, t.schema, array.WithChunk(batchSize))
	return closingreader.New(rdr, obj), nil
}
