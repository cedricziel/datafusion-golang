// Package iceberg provides a datafusion.TableProvider backed by a
// local-filesystem Apache Iceberg table, opened directly from its
// metadata.json location with no catalog service required.
//
// The provider implements datafusion.PushdownTableProvider, so it is
// registered with scan pushdown enabled: pushed filters are converted to
// Iceberg expressions and drive iceberg-go's own manifest, data-file, and
// row-group pruning plus exact row filtering; projection is pushed via
// selected fields; the limit hint is passed through. Pushed filters are
// strictly advisory per the table-provider contract — a filter that
// cannot be converted faithfully is dropped whole rather than
// approximated, so pushdown can only ever widen the scan (the engine
// re-applies every filter), and results are identical either way.
package iceberg

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// tableProvider is a datafusion.TableProvider backed by a local Iceberg
// table's metadata.json location. Each Scan re-opens the table and starts
// a fresh scan so concurrent and repeated scans never share state (design
// D4), mirroring providers/parquet.
type tableProvider struct {
	metadataLocation string
	schema           *arrow.Schema
}

// NewTableProvider opens the Iceberg table whose current metadata lives at
// metadataLocation, validating it and caching the Arrow-equivalent of its
// current schema. Construction fails if the metadata cannot be read or
// parsed as valid Iceberg table metadata.
func NewTableProvider(ctx context.Context, metadataLocation string) (datafusion.TableProvider, error) {
	tbl, err := loadTable(ctx, metadataLocation)
	if err != nil {
		return nil, err
	}

	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: derive arrow schema of %s: %w", metadataLocation, err)
	}

	return &tableProvider{metadataLocation: metadataLocation, schema: schema}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan opens a fresh table handle and scan over the table's current
// snapshot, independent of any other concurrent or subsequent scan of the
// same provider (design D4). Errors encountered while reading a data file
// surface lazily through the returned reader's Err(), not swallowed
// (design D5).
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	tbl, err := loadTable(ctx, t.metadataLocation)
	if err != nil {
		return nil, err
	}

	scan := tbl.Scan()
	schema, itr, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("iceberg: scan %s: %w", t.metadataLocation, err)
	}

	return readerFromSeq(schema, itr), nil
}

// loadTable loads the Iceberg table at metadataLocation with no catalog
// service (design D3). The identifier is not validated against the
// metadata content by iceberg-go; it is only used for display purposes, so
// a name derived from the metadata path is fine here.
func loadTable(ctx context.Context, metadataLocation string) (*table.Table, error) {
	fsysF := icebergio.LoadFSFunc(nil, metadataLocation)
	ident := table.Identifier{"default", tableNameFromMetadataLocation(metadataLocation)}
	tbl, err := table.NewFromLocation(ctx, ident, metadataLocation, fsysF, nil)
	if err != nil {
		return nil, fmt.Errorf("iceberg: load table metadata at %s: %w", metadataLocation, err)
	}
	return tbl, nil
}

// tableNameFromMetadataLocation derives a display name for the table
// identifier from a metadata.json path shaped like
// <table-root>/metadata/<file>.metadata.json.
func tableNameFromMetadataLocation(metadataLocation string) string {
	dir := filepath.Dir(metadataLocation)
	name := filepath.Base(filepath.Dir(dir))
	if name == "" || name == "." || name == string(filepath.Separator) {
		return "table"
	}
	return name
}
