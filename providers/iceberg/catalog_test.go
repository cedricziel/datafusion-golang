package iceberg_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

func TestNewTableProviderFromCatalog_ValidTable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	if got, want := p.Schema().NumFields(), 2; got != want {
		t.Fatalf("expected %d fields, got %d", want, got)
	}
	if p.Schema().Field(0).Name != "id" || p.Schema().Field(1).Name != "name" {
		t.Fatalf("unexpected field names: %v", p.Schema())
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestNewTableProviderFromCatalog_TableNotFound(t *testing.T) {
	schema := peopleSchema()
	cat, _ := newIcebergCatalogFixture(t, "people", schema)

	_, err := provider.NewTableProviderFromCatalog(context.Background(), cat, "default", "does-not-exist")
	if err == nil {
		t.Fatalf("expected an error for a missing table")
	}
	if !errors.Is(err, catalog.ErrNoSuchTable) {
		t.Fatalf("expected errors.Is(err, catalog.ErrNoSuchTable), got: %v", err)
	}
}

func TestNewTableProviderFromCatalog_NamespaceNotFound(t *testing.T) {
	schema := peopleSchema()
	cat, _ := newIcebergCatalogFixture(t, "people", schema)

	_, err := provider.NewTableProviderFromCatalog(context.Background(), cat, "missing-namespace", "t")
	if err == nil {
		t.Fatalf("expected an error for a missing namespace")
	}
}

// stubCatalog wraps a real catalog.Catalog and forces LoadTable to fail,
// simulating a connectivity or authentication failure independent of
// whether the table actually exists.
type stubCatalog struct {
	catalog.Catalog
	loadErr error
}

func (s *stubCatalog) LoadTable(ctx context.Context, identifier table.Identifier) (*table.Table, error) {
	return nil, s.loadErr
}

func TestNewTableProviderFromCatalog_CatalogClientError(t *testing.T) {
	wantErr := errors.New("connection refused")
	cat := &stubCatalog{loadErr: wantErr}

	_, err := provider.NewTableProviderFromCatalog(context.Background(), cat, "default", "t")
	if err == nil {
		t.Fatalf("expected an error when the catalog client fails")
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected errors.Is(err, wantErr), got: %v", err)
	}
}

func TestScan_CatalogBackedProvider_CommitBetweenScansIsVisible(t *testing.T) {
	schema := peopleSchema()
	b1 := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer b1.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, b1)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}

	reader, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("first Scan: %v", err)
	}
	firstIDs, _ := collectRows(t, reader)
	if len(firstIDs) != 2 {
		t.Fatalf("expected 2 rows before the second commit, got %v", firstIDs)
	}

	// Commit a new snapshot through the catalog, independent of the
	// already-constructed provider.
	ctx := context.Background()
	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable for append: %v", err)
	}
	b2 := peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"})
	defer b2.Release()
	b2.Retain()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{b2})
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	if _, err := tbl.Append(ctx, rdr, nil); err != nil {
		rdr.Release()
		t.Fatalf("Append: %v", err)
	}
	rdr.Release()

	reader2, err := p.Scan(context.Background())
	if err != nil {
		t.Fatalf("second Scan: %v", err)
	}
	secondIDs, _ := collectRows(t, reader2)
	if len(secondIDs) != 4 {
		t.Fatalf("expected the second scan to see the committed snapshot (4 rows), got %v", secondIDs)
	}
}

// TestCatalogBackedProvider_ScanWithOptionsComposesAutomatically proves the
// design's claim that a catalog-backed provider gets scan pushdown for
// free: NewTableProviderFromCatalog's tableProvider is the same type
// NewTableProvider returns, so it already implements
// datafusion.PushdownTableProvider, with no catalog-specific pushdown code.
func TestCatalogBackedProvider_ScanWithOptionsComposesAutomatically(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2, 3}, []string{"alice", "bob", "carol"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	if _, ok := p.(datafusion.PushdownTableProvider); !ok {
		t.Fatalf("expected a catalog-backed provider to implement datafusion.PushdownTableProvider")
	}

	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("SELECT name FROM people WHERE id > 1 ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	defer reader.Release()
	var names []string
	for reader.Next() {
		rec := reader.RecordBatch()
		col := rec.Column(0).(*array.String)
		for i := 0; i < int(rec.NumRows()); i++ {
			names = append(names, strings.Clone(col.Value(i)))
		}
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	if len(names) != 2 || names[0] != "bob" || names[1] != "carol" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestScan_CatalogBackedProvider_ConcurrentScans(t *testing.T) {
	schema := peopleSchema()
	b1 := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer b1.Release()
	b2 := peopleBatch(t, schema, []int64{3, 4}, []string{"carol", "dave"})
	defer b2.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, b1, b2)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := p.Scan(context.Background())
			if err != nil {
				errs <- err
				return
			}
			ids, _ := collectRows(t, reader)
			if len(ids) != 4 {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("goroutine error: %v", err)
		}
	}
}
