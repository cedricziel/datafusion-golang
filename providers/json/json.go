// Package json provides a datafusion.TableProvider backed by a local JSON
// file whose top level is an array of objects, so it can be registered on
// a SessionContext and queried via SQL with no hand-written JSON-reading
// code.
//
// The whole file decodes into a single record batch — there is no
// chunking for this format, unlike providers/jsonl. arrow-go has no
// schema inference for JSON, so schema is always caller-supplied. The
// provider does not implement datafusion.PushdownTableProvider.
package json

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
)

// tableProvider is a datafusion.TableProvider backed by a JSON
// array-of-objects object at path within store.
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

// NewTableProvider opens the JSON object at location, decoding it against
// schema to validate content and caching the schema. Construction fails
// if location's scheme is not registered, the object does not exist, is
// not readable, its top level is not a JSON array, or its content does
// not decode against schema.
func NewTableProvider(ctx context.Context, location string, schema *arrow.Schema, opts ...Option) (datafusion.TableProvider, error) {
	store, path, err := objectstore.ResolveWithOptions(ctx, location, opts...)
	if err != nil {
		return nil, fmt.Errorf("json: resolving %s: %w", location, err)
	}

	obj, err := store.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("json: open %s: %w", location, err)
	}
	defer obj.Close()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, obj)
	if err != nil {
		return nil, fmt.Errorf("json: decode %s: %w", location, err)
	}
	rec.Release()

	return &tableProvider{store: store, path: path, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan decodes the whole object into one record batch and wraps it in a
// RecordReader that owns no open handle: the object is opened, decoded,
// and closed before Scan returns, so there is no window during which the
// returned reader holds an open handle.
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	obj, err := t.store.Open(ctx, t.path)
	if err != nil {
		return nil, fmt.Errorf("json: open %s: %w", t.path, err)
	}
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, t.schema, obj)
	closeErr := obj.Close()
	if err != nil {
		return nil, fmt.Errorf("json: decode %s: %w", t.path, err)
	}
	if closeErr != nil {
		rec.Release()
		return nil, fmt.Errorf("json: close %s: %w", t.path, closeErr)
	}
	defer rec.Release()

	return array.NewRecordReader(t.schema, []arrow.RecordBatch{rec})
}
