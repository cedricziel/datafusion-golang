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
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// tableProvider is a datafusion.TableProvider backed by an Iceberg table,
// opened either from a direct metadata.json location or through a catalog
// client. Each Scan calls load to obtain a fresh table handle and starts a
// fresh scan so concurrent and repeated scans never share state (design
// D4 of add-parquet-and-iceberg-table-providers), mirroring
// providers/parquet.
type tableProvider struct {
	schema   *arrow.Schema
	load     func(ctx context.Context) (*table.Table, error)
	describe func() string // for error messages, e.g. "metadata.json at ..." or "catalog table db.orders"
}

func newProvider(ctx context.Context, load func(context.Context) (*table.Table, error), describe func() string) (*tableProvider, error) {
	tbl, err := load(ctx)
	if err != nil {
		return nil, err
	}

	schema, err := table.SchemaToArrowSchema(tbl.Schema(), nil, false, false)
	if err != nil {
		return nil, fmt.Errorf("iceberg: derive arrow schema of %s: %w", describe(), err)
	}

	return &tableProvider{schema: schema, load: load, describe: describe}, nil
}

// Option configures NewTableProvider.
type Option func(*options)

type options struct {
	ioProps map[string]string
}

// WithIOProps supplies properties (endpoint overrides, credentials-related
// settings, and any other iceberg-go file-IO property) to the file IO used
// for every metadata and data read the provider performs. Omitted or nil,
// behavior is unchanged from before this option existed: iceberg-go's
// default file-IO resolution for the location's scheme. Cloud schemes
// (s3://, gs://, abfs://, ...) additionally require the corresponding
// iceberg-go file-IO implementation to be enabled — see
// github.com/apache/iceberg-go/io/gocloud's package doc.
func WithIOProps(props map[string]string) Option {
	return func(o *options) { o.ioProps = props }
}

// NewTableProvider opens the Iceberg table whose current metadata lives at
// metadataLocation, validating it and caching the Arrow-equivalent of its
// current schema. Construction fails if the metadata cannot be read or
// parsed as valid Iceberg table metadata, or if metadataLocation's scheme
// has no registered iceberg-go file-IO implementation.
func NewTableProvider(ctx context.Context, metadataLocation string, opts ...Option) (datafusion.TableProvider, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	load := func(ctx context.Context) (*table.Table, error) {
		return loadTableFromLocation(ctx, metadataLocation, o.ioProps)
	}
	return newProvider(ctx, load, func() string { return metadataLocation })
}

// NewTableProviderFromCatalog resolves and opens the Iceberg table
// identified by identifier (namespace levels followed by the table name,
// e.g. "db", "orders") through cat, an already-configured Iceberg catalog
// client. providers/iceberg depends only on the catalog.Catalog interface,
// never a specific catalog implementation — the caller builds and
// configures whichever concrete client (REST, Hive, Glue, SQL, Hadoop, ...)
// it needs.
//
// Unlike NewTableProvider, a provider constructed this way re-resolves the
// table through the catalog on every scan, so a commit made to the table
// between two scans becomes visible without reconstructing the provider.
//
// The returned provider also implements datafusion.WritableTableProvider
// (design D4 of wire-parquet-and-iceberg-insert): a catalog is required to
// commit an insert, so only catalog-backed tables are writable —
// NewTableProvider's metadata-location tables stay read-only, since a
// pinned metadata.json path has no channel to observe a new snapshot.
func NewTableProviderFromCatalog(ctx context.Context, cat catalog.Catalog, identifier ...string) (datafusion.TableProvider, error) {
	ident := table.Identifier(identifier)
	load := func(ctx context.Context) (*table.Table, error) {
		tbl, err := cat.LoadTable(ctx, ident)
		if err != nil {
			return nil, fmt.Errorf("iceberg: load catalog table %s: %w", strings.Join(identifier, "."), err)
		}
		return tbl, nil
	}
	base, err := newProvider(ctx, load, func() string { return "catalog table " + strings.Join(identifier, ".") })
	if err != nil {
		return nil, err
	}
	return &writableTableProvider{tableProvider: base, cat: cat, ident: ident}, nil
}

func (t *tableProvider) Schema() *arrow.Schema { return t.schema }

// Scan calls load to obtain a fresh table handle and scan over the table's
// current snapshot, independent of any other concurrent or subsequent scan
// of the same provider (design D4). Errors encountered while reading a
// data file surface lazily through the returned reader's Err(), not
// swallowed (design D5).
func (t *tableProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	tbl, err := t.load(ctx)
	if err != nil {
		return nil, err
	}

	scan := tbl.Scan()
	schema, itr, err := scan.ToArrowRecords(ctx)
	if err != nil {
		return nil, fmt.Errorf("iceberg: scan %s: %w", t.describe(), err)
	}

	return readerFromSeq(schema, itr), nil
}

// loadTableFromLocation loads the Iceberg table at metadataLocation with
// no catalog service (design D3 of add-parquet-and-iceberg-table-providers).
// ioProps is passed through to the file IO (design D6 of add-object-store);
// nil behaves exactly as before that option existed. The identifier is not
// validated against the metadata content by iceberg-go; it is only used
// for display purposes, so a name derived from the metadata path is fine
// here.
func loadTableFromLocation(ctx context.Context, metadataLocation string, ioProps map[string]string) (*table.Table, error) {
	fsysF := icebergio.LoadFSFunc(ioProps, metadataLocation)
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
