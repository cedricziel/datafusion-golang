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
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

const batchSize = 1024

// tableProvider is a datafusion.TableProvider backed by a local JSON
// Lines file at path. Each Scan opens its own reader so concurrent and
// repeated scans never share state, mirroring providers/parquet.
type tableProvider struct {
	path   string
	schema *arrow.Schema
}

// NewTableProvider opens the JSON Lines file at path against schema,
// decoding the first object to validate it against schema and caching
// the schema. Construction fails if the file does not exist, is not
// readable, or its first object does not decode against schema.
func NewTableProvider(path string, schema *arrow.Schema) (datafusion.TableProvider, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("jsonl: open %s: %w", path, err)
	}
	defer f.Close()

	rdr := array.NewJSONReader(f, schema)
	defer rdr.Release()
	rdr.Next()
	if err := rdr.Err(); err != nil {
		return nil, fmt.Errorf("jsonl: decode first object of %s: %w", path, err)
	}

	return &tableProvider{path: path, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan opens a fresh reader over the file so this scan is independent of
// any other concurrent or subsequent scan of the same provider. The
// returned reader closes the underlying file when it is released or
// exhausted, matching providers/parquet's closingRecordReader pattern.
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	f, err := os.Open(t.path)
	if err != nil {
		return nil, fmt.Errorf("jsonl: open %s: %w", t.path, err)
	}

	rdr := array.NewJSONReader(f, t.schema, array.WithChunk(batchSize))
	return &closingRecordReader{RecordReader: rdr, file: f}, nil
}

// closingRecordReader wraps a JSON Lines record reader so that Release,
// or running the underlying stream to exhaustion, also closes the file
// this scan opened.
type closingRecordReader struct {
	array.RecordReader
	file   *os.File
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
