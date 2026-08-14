package iceberg_test

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
	"github.com/cedricziel/datafusion-golang/providers/internal/providertest"
)

func TestInsertInto_SQL_ValuesThenSelectSeesRows(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO people VALUES (2, 'bob')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := providertest.CountResult(t, reader); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	ids, names := collectRows(t, selectReader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected names: %v", names)
	}
}

func TestInsertInto_SQL_SelectFromAnotherRegisteredTable(t *testing.T) {
	schema := peopleSchema()

	srcBatch := peopleBatch(t, schema, []int64{10, 20}, []string{"src1", "src2"})
	defer srcBatch.Release()
	srcCat, srcIdent := newIcebergCatalogFixture(t, "src", schema, srcBatch)
	src, err := provider.NewTableProviderFromCatalog(context.Background(), srcCat, srcIdent...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog(src): %v", err)
	}

	dstBatch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer dstBatch.Release()
	dstCat, dstIdent := newIcebergCatalogFixture(t, "dst", schema, dstBatch)
	dst, err := provider.NewTableProviderFromCatalog(context.Background(), dstCat, dstIdent...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog(dst): %v", err)
	}

	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("src", src); err != nil {
		t.Fatalf("RegisterTable(src): %v", err)
	}
	if err := ctx.RegisterTable("dst", dst); err != nil {
		t.Fatalf("RegisterTable(dst): %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO dst SELECT * FROM src")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := providertest.CountResult(t, reader); got != 2 {
		t.Fatalf("expected count=2, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT COUNT(*) FROM dst")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer selectReader.Release()
	if !selectReader.Next() {
		t.Fatalf("expected a count row")
	}
	if got := selectReader.RecordBatch().Column(0).(*array.Int64).Value(0); got != 3 {
		t.Fatalf("expected 3 combined rows in dst, got %d", got)
	}
}

func TestInsertInto_SQL_ColumnSubsetFillsOmittedWithNull(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("INSERT INTO people (name) VALUES ('nameless')")
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if got := providertest.CountResult(t, reader); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT id, name FROM people WHERE name = 'nameless'")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	defer selectReader.Release()
	if !selectReader.Next() {
		t.Fatalf("expected the inserted row")
	}
	rec := selectReader.RecordBatch()
	if !rec.Column(0).(*array.Int64).IsNull(0) {
		t.Fatalf("expected the omitted id column to be NULL")
	}
}

func TestInsertInto_SQL_Overwrite(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := ctx.SQL("INSERT OVERWRITE people VALUES (9, 'zed')")
	if err != nil {
		t.Fatalf("INSERT OVERWRITE: %v", err)
	}
	if got := providertest.CountResult(t, reader); got != 1 {
		t.Fatalf("expected count=1, got %d", got)
	}

	selectReader, err := ctx.SQL("SELECT id, name FROM people")
	if err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	ids, names := collectRows(t, selectReader)
	if len(ids) != 1 || ids[0] != 9 || names[0] != "zed" {
		t.Fatalf("expected only (9, zed) after overwrite, got ids=%v names=%v", ids, names)
	}
}

func TestInsertInto_SQL_ReplaceFailsCleanlyAndSessionStaysUsable(t *testing.T) {
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1}, []string{"alice"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)
	before := currentSnapshotID(t, cat, ident)

	p, err := provider.NewTableProviderFromCatalog(context.Background(), cat, ident...)
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()
	if err := ctx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	_, err = ctx.SQL("REPLACE INTO people VALUES (2, 'bob')")
	if err == nil {
		t.Fatalf("expected REPLACE INTO to fail (only Append/Overwrite are supported)")
	}
	if !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("expected a clear unsupported-mode message, got: %v", err)
	}
	if got := currentSnapshotID(t, cat, ident); got != before {
		t.Fatalf("a rejected REPLACE must leave the table's snapshot unchanged: before=%d after=%d", before, got)
	}

	reader, err := ctx.SQL("SELECT 1 AS one")
	if err != nil {
		t.Fatalf("session should remain usable after a rejected insert: %v", err)
	}
	reader.Release()
}
