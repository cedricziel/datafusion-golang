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
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// tableProvider is a datafusion.TableProvider backed by a local JSON
// array-of-objects file at path.
type tableProvider struct {
	path   string
	schema *arrow.Schema
}

// NewTableProvider opens the JSON file at path, decoding it against
// schema to validate content and caching the schema. Construction fails
// if the file does not exist, is not readable, its top level is not a
// JSON array, or its content does not decode against schema.
func NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("json: open %s: %w", path, err)
	}
	defer f.Close()

	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema, f)
	if err != nil {
		return nil, fmt.Errorf("json: decode %s: %w", path, err)
	}
	rec.Release()

	return &tableProvider{path: path, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan decodes the whole file into one record batch and wraps it in a
// RecordReader that owns no file handle: the file is opened, decoded, and
// closed before Scan returns, so there is no window during which the
// returned reader holds an open file descriptor.
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	f, err := os.Open(t.path)
	if err != nil {
		return nil, fmt.Errorf("json: open %s: %w", t.path, err)
	}
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, t.schema, f)
	closeErr := f.Close()
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
