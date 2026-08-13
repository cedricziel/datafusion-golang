package datafusion_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// memCatalog is an in-memory datafusion.CatalogProvider/SchemaProvider
// backed by a map of *memSchema, keyed by schema name, so a test can reach
// in and set a schema's tableErr directly (e.g. cat.schemas["sch"].tableErr
// = err) without a second CatalogProvider implementation.
//
// SchemaNames/TableNames are only invoked by DataFusion's
// information_schema machinery, not by a direct catalog.schema.table
// reference (see design.md D8) — nothing in this project's Go-facing API
// enables information_schema, so schemaCalls is the counter tests actually
// need; SchemaNames/TableNames exist here only to satisfy the interfaces.
type memCatalog struct {
	mu      sync.Mutex
	schemas map[string]*memSchema

	schemaCalls atomic.Int64
	schemaErr   error
}

func newMemCatalog() *memCatalog {
	return &memCatalog{schemas: map[string]*memSchema{}}
}

func (c *memCatalog) addTable(schema, table string, provider datafusion.TableProvider) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.schemas[schema] == nil {
		c.schemas[schema] = &memSchema{tables: map[string]datafusion.TableProvider{}}
	}
	c.schemas[schema].tables[table] = provider
}

func (c *memCatalog) SchemaNames(ctx context.Context) ([]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.schemas))
	for name := range c.schemas {
		names = append(names, name)
	}
	return names, nil
}

func (c *memCatalog) Schema(ctx context.Context, name string) (datafusion.SchemaProvider, bool, error) {
	c.schemaCalls.Add(1)
	if c.schemaErr != nil {
		return nil, false, c.schemaErr
	}
	c.mu.Lock()
	schema, ok := c.schemas[name]
	c.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	return schema, true, nil
}

// memSchema is the schema-level counterpart of memCatalog.
type memSchema struct {
	tables   map[string]datafusion.TableProvider
	tableErr error
}

func (s *memSchema) TableNames(ctx context.Context) ([]string, error) {
	names := make([]string, 0, len(s.tables))
	for name := range s.tables {
		names = append(names, name)
	}
	return names, nil
}

func (s *memSchema) Table(ctx context.Context, name string) (datafusion.TableProvider, bool, error) {
	if s.tableErr != nil {
		return nil, false, s.tableErr
	}
	t, ok := s.tables[name]
	if !ok {
		return nil, false, nil
	}
	return t, true, nil
}

func collectAll(t *testing.T, reader array.RecordReader) []arrow.RecordBatch {
	t.Helper()
	defer reader.Release()
	var batches []arrow.RecordBatch
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain()
		batches = append(batches, rec)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading result: %v", err)
	}
	return batches
}

func totalRows(batches []arrow.RecordBatch) int64 {
	var n int64
	for _, b := range batches {
		n += b.NumRows()
	}
	return n
}

func newSession(t *testing.T) *datafusion.SessionContext {
	t.Helper()
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	t.Cleanup(func() { ctx.Close() })
	return ctx
}

func TestRegisterCatalog_QueryCatalogSchemaTable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	table := newPeopleTable(t, batch)

	cat := newMemCatalog()
	cat.addTable("sch", "people", table)

	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	reader, err := ctx.SQL("SELECT id, name FROM cat.sch.people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	batches := collectAll(t, reader)
	if got, want := totalRows(batches), int64(2); got != want {
		t.Fatalf("expected %d rows, got %d", want, got)
	}
	for _, b := range batches {
		b.Release()
	}
}

func TestRegisterCatalog_NilCatalogRejected(t *testing.T) {
	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", nil); err == nil {
		t.Fatalf("expected an error registering a nil catalog")
	}
}

func TestRegisterCatalog_DuplicateNameRejected(t *testing.T) {
	ctx := newSession(t)
	first := newMemCatalog()
	second := newMemCatalog()
	if err := ctx.RegisterCatalog("dup", first); err != nil {
		t.Fatalf("first RegisterCatalog: %v", err)
	}
	if err := ctx.RegisterCatalog("dup", second); err == nil {
		t.Fatalf("expected duplicate catalog name to be rejected")
	}
}

func TestRegisterCatalog_DefaultNameReplacesIt(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	table := newPeopleTable(t, batch)

	ctx := newSession(t)
	if err := ctx.RegisterTable("people", table); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}
	if reader, err := ctx.SQL("SELECT * FROM people"); err != nil {
		t.Fatalf("query before replacement: %v", err)
	} else {
		reader.Release()
	}

	cat := newMemCatalog()
	catTable := newPeopleTable(t, batch)
	cat.addTable("sch", "t", catTable)
	if err := ctx.RegisterCatalog("datafusion", cat); err != nil {
		t.Fatalf("registering under the default catalog name must succeed: %v", err)
	}

	if _, err := ctx.SQL("SELECT * FROM people"); err == nil {
		t.Fatalf("expected the previously-registered table to become unreachable")
	}

	reader, err := ctx.SQL("SELECT * FROM datafusion.sch.t")
	if err != nil {
		t.Fatalf("querying the Go catalog under the default name: %v", err)
	}
	batches := collectAll(t, reader)
	if got, want := totalRows(batches), int64(2); got != want {
		t.Fatalf("expected %d rows, got %d", want, got)
	}
	for _, b := range batches {
		b.Release()
	}
}

func TestRegisterCatalog_UnknownSchemaAndTable(t *testing.T) {
	cat := newMemCatalog()
	cat.addTable("sch", "t", newPeopleTable(t))
	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	if _, err := ctx.SQL("SELECT * FROM cat.missing_schema.t"); err == nil {
		t.Fatalf("expected an error for an unknown schema")
	}
	if _, err := ctx.SQL("SELECT * FROM cat.sch.missing_table"); err == nil {
		t.Fatalf("expected an error for an unknown table")
	}
	// session remains usable
	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable: %v", err)
	}
	reader.Release()
}

func TestRegisterCatalog_SchemaLookupErrorSurfacesAndSessionStaysUsable(t *testing.T) {
	cat := newMemCatalog()
	cat.schemaErr = errors.New("schema backend unavailable")
	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	_, err := ctx.SQL("SELECT * FROM cat.sch.t")
	if err == nil {
		t.Fatalf("expected the schema lookup error to abort the query")
	}
	if !strings.Contains(err.Error(), "schema backend unavailable") {
		t.Fatalf("expected the error to describe the failure, got: %v", err)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable: %v", err)
	}
	reader.Release()
}

func TestRegisterCatalog_TableLookupErrorSurfacesAndSessionStaysUsable(t *testing.T) {
	cat := newMemCatalog()
	cat.addTable("sch", "placeholder", newPeopleTable(t))
	cat.schemas["sch"].tableErr = errors.New("table backend unavailable")

	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	_, err := ctx.SQL("SELECT * FROM cat.sch.t")
	if err == nil {
		t.Fatalf("expected the table lookup error to abort the query")
	}
	if !strings.Contains(err.Error(), "table backend unavailable") {
		t.Fatalf("expected the error to describe the failure, got: %v", err)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable: %v", err)
	}
	reader.Release()
}

func TestRegisterCatalog_TableAddedAfterRegistrationBecomesQueryable(t *testing.T) {
	cat := newMemCatalog()
	cat.addTable("sch", "first", newPeopleTable(t))
	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	if _, err := ctx.SQL("SELECT * FROM cat.sch.second"); err == nil {
		t.Fatalf("expected 'second' to be unknown before it's added")
	}

	batch := peopleBatch(t, peopleSchema(), []int64{9}, []string{"zed"})
	defer batch.Release()
	cat.addTable("sch", "second", newPeopleTable(t, batch))

	reader, err := ctx.SQL("SELECT * FROM cat.sch.second")
	if err != nil {
		t.Fatalf("table added after registration must become queryable: %v", err)
	}
	batches := collectAll(t, reader)
	if got, want := totalRows(batches), int64(1); got != want {
		t.Fatalf("expected %d rows, got %d", want, got)
	}
	for _, b := range batches {
		b.Release()
	}
}

func TestRegisterCatalog_NotInvokedAfterClose(t *testing.T) {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}

	cat := newMemCatalog()
	cat.addTable("sch", "t", newPeopleTable(t))
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}
	if err := ctx.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := ctx.SQL("SELECT * FROM cat.sch.t"); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed, got: %v", err)
	}
	if got := cat.schemaCalls.Load(); got != 0 {
		t.Fatalf("catalog must not be invoked after Close, got %d schema calls", got)
	}

	other := newMemCatalog()
	if err := ctx.RegisterCatalog("other", other); !errors.Is(err, datafusion.ErrSessionClosed) {
		t.Fatalf("expected ErrSessionClosed for registration after Close, got: %v", err)
	}
}

func TestRegisterCatalog_PushdownTableProviderComposesAutomatically(t *testing.T) {
	pd := newPushdownTable(t, fourPeopleBatches(t)...)
	pd.prune = true

	cat := newMemCatalog()
	cat.addTable("sch", "t", pd)

	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	reader, err := ctx.SQL("SELECT name FROM cat.sch.t WHERE id > 2")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	batches := collectAll(t, reader)
	if totalRows(batches) == 0 {
		t.Fatalf("expected at least one matching row")
	}
	if len(pd.recordedScans()) == 0 {
		t.Fatalf("expected ScanWithOptions to be called on a catalog-discovered pushdown table")
	}
	scan := pd.recordedScans()[0]
	if scan.Projection == nil {
		t.Fatalf("expected the projection to be delivered exactly as for a directly-registered pushdown table")
	}
	for _, b := range batches {
		b.Release()
	}
}

func TestRegisterCatalog_ConcurrentQueries(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	cat := newMemCatalog()
	cat.addTable("sch", "t", newPeopleTable(t, batch))

	ctx := newSession(t)
	if err := ctx.RegisterCatalog("cat", cat); err != nil {
		t.Fatalf("RegisterCatalog: %v", err)
	}

	const goroutines = 8
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reader, err := ctx.SQL("SELECT * FROM cat.sch.t")
			if err != nil {
				errs <- err
				return
			}
			batches := collectAll(t, reader)
			if totalRows(batches) != 2 {
				errs <- fmt.Errorf("expected 2 rows, got %d", totalRows(batches))
			}
			for _, b := range batches {
				b.Release()
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
