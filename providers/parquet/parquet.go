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

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

var readProps = pqarrow.ArrowReadProperties{BatchSize: 1024}

// tableProvider is a datafusion.TableProvider backed by a local Parquet
// file at path. Each Scan opens its own file.Reader so concurrent and
// repeated scans never share state (see design D4).
type tableProvider struct {
	path   string
	schema *arrow.Schema
}

// NewTableProvider opens the Parquet file at path, validating it and
// caching its Arrow schema. Construction fails if the file does not exist,
// is not readable, or is not a valid Parquet file.
func NewTableProvider(path string) (datafusion.TableProvider, error) {
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %s: %w", path, err)
	}
	defer rdr.Close()

	fr, err := pqarrow.NewFileReader(rdr, readProps, memory.DefaultAllocator)
	if err != nil {
		return nil, fmt.Errorf("parquet: read schema of %s: %w", path, err)
	}
	schema, err := fr.Schema()
	if err != nil {
		return nil, fmt.Errorf("parquet: derive arrow schema of %s: %w", path, err)
	}

	return &tableProvider{path: path, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan opens a fresh reader over the file so this scan is independent of
// any other concurrent or subsequent scan of the same provider (design D4).
// The returned reader closes the underlying file when it is released or
// exhausted (design D6).
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	rdr, err := file.OpenParquetFile(t.path, false)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %s: %w", t.path, err)
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
