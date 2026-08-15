// Package csv provides a datafusion.TableProvider backed by a local CSV
// file, so it can be registered on a SessionContext and queried via SQL
// with no hand-written CSV-reading code.
//
// A CSV file is read against either a caller-supplied schema
// (NewTableProvider) or a schema inferred from the file's header and
// first data row (NewTableProviderWithInferredSchema). The file must have
// a header row; there is no headerless-CSV or custom-delimiter support,
// and the provider does not implement datafusion.PushdownTableProvider —
// every scan reads and decodes the whole file.
package csv

import (
	"bufio"
	"context"
	stdcsv "encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	arrowcsv "github.com/apache/arrow-go/v18/arrow/csv"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
	"github.com/cedricziel/datafusion-golang/providers/internal/closingreader"
)

const batchSize = 1024

// tableProvider is a datafusion.TableProvider backed by a CSV object at
// path within store. Each Scan opens its own reader so concurrent and
// repeated scans never share state, mirroring providers/parquet.
type tableProvider struct {
	store    objectstore.Store
	path     string
	schema   *arrow.Schema
	inferred bool
}

// Option configures NewTableProvider and NewTableProviderWithInferredSchema.
type Option = objectstore.Option

// WithStore overrides the backend location resolves against, bypassing
// the objectstore registry: location is then used as-is as the in-store
// path. Intended for tests and explicitly configured buckets.
func WithStore(store objectstore.Store) Option { return objectstore.WithStore(store) }

// NewTableProvider opens the CSV object at location against schema,
// validating the object's header row column count against it and caching
// schema exactly as supplied. The header row is used only for that count
// check — field names and types always come from schema, never from the
// header's own text.
//
// (arrow-go's own csv.Reader, given WithHeader(true), renames schema
// fields to the header row's text; this package reads and discards the
// header itself instead, so the caller-supplied schema's field names are
// never overwritten.) Construction fails if location's scheme is not
// registered, the object does not exist, is not readable, has no header
// row, or its header column count does not match schema.
func NewTableProvider(ctx context.Context, location string, schema *arrow.Schema, opts ...Option) (datafusion.TableProvider, error) {
	store, path, err := objectstore.ResolveWithOptions(ctx, location, opts...)
	if err != nil {
		return nil, fmt.Errorf("csv: resolving %s: %w", location, err)
	}

	obj, err := store.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("csv: open %s: %w", location, err)
	}
	defer obj.Close()

	if _, err := skipHeader(location, obj, schema); err != nil {
		return nil, err
	}

	return &tableProvider{store: store, path: path, schema: schema}, nil
}

// skipHeader reads and validates one header line from r — check its
// column count against schema — and returns a *bufio.Reader positioned
// right after that line, ready to feed a csv.Reader configured for no
// further header handling (schema field names stay exactly as supplied).
// path is used only for error messages.
func skipHeader(path string, r io.Reader, schema *arrow.Schema) (*bufio.Reader, error) {
	br := bufio.NewReader(r)
	line, err := br.ReadString('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("csv: read header of %s: %w", path, err)
	}
	if line == "" {
		return nil, fmt.Errorf("csv: %s has no header row", path)
	}
	fields, err := stdcsv.NewReader(strings.NewReader(line)).Read()
	if err != nil {
		return nil, fmt.Errorf("csv: parse header of %s: %w", path, err)
	}
	if len(fields) != schema.NumFields() {
		return nil, fmt.Errorf("csv: %s header has %d column(s), schema has %d", path, len(fields), schema.NumFields())
	}
	return br, nil
}

// NewTableProviderWithInferredSchema opens the CSV object at location and
// infers its schema from the header row (field names) and the first data
// row (field types, via arrow-go's fixed type ladder). Construction fails
// if location's scheme is not registered, the object does not exist, is
// not readable, or has no data rows to infer types from.
//
// Inference looks only at the first data row: a column whose later values
// don't parse under the type inferred from row one produces null fields
// from that row onward and ends the scan early, surfaced through the
// returned reader's Err() — not silently. A caller with heterogeneous
// data should use NewTableProvider with an explicit schema instead.
func NewTableProviderWithInferredSchema(ctx context.Context, location string, opts ...Option) (datafusion.TableProvider, error) {
	store, path, err := objectstore.ResolveWithOptions(ctx, location, opts...)
	if err != nil {
		return nil, fmt.Errorf("csv: resolving %s: %w", location, err)
	}

	obj, err := store.Open(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("csv: open %s: %w", location, err)
	}
	defer obj.Close()

	rdr := arrowcsv.NewInferringReader(obj, arrowcsv.WithHeader(true))
	defer rdr.Release()
	rdr.Next()
	if err := rdr.Err(); err != nil {
		return nil, fmt.Errorf("csv: infer schema of %s: %w", location, err)
	}
	schema := rdr.Schema()
	if schema == nil {
		return nil, fmt.Errorf("csv: infer schema of %s: no data rows to infer types from", location)
	}

	return &tableProvider{store: store, path: path, schema: schema, inferred: true}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan opens a fresh reader over the object so this scan is independent
// of any other concurrent or subsequent scan of the same provider. The
// returned reader closes the underlying object when it is released or
// exhausted.
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	obj, err := t.store.Open(ctx, t.path)
	if err != nil {
		return nil, fmt.Errorf("csv: open %s: %w", t.path, err)
	}

	var rdr *arrowcsv.Reader
	if t.inferred {
		rdr = arrowcsv.NewInferringReader(obj, arrowcsv.WithHeader(true), arrowcsv.WithChunk(batchSize))
	} else {
		br, err := skipHeader(t.path, obj, t.schema)
		if err != nil {
			_ = obj.Close()
			return nil, err
		}
		rdr = arrowcsv.NewReader(br, t.schema, arrowcsv.WithChunk(batchSize))
	}

	return closingreader.New(rdr, obj), nil
}
