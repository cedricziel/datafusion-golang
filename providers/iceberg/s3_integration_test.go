package iceberg_test

import (
	"context"
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/iceberg-go/catalog/hadoop"
	icebergtable "github.com/apache/iceberg-go/table"

	_ "github.com/apache/iceberg-go/io/gocloud" // registers s3/s3a/s3n/oss, gs, abfs/abfss with iceberg-go's file-IO registry
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

// s3TestWarehouse returns the OBJECTSTORE_S3_TEST_URL environment
// variable's value, or skips the test if it is unset. Reused from the
// same variable providers/parquet's S3 integration test uses — one
// endpoint serves both. Expected form: an s3:// bucket URL, e.g. against
// a local MinIO:
//
//	s3://test-bucket?endpoint=http://localhost:9000&use_path_style=true&region=us-east-1
func s3TestWarehouse(t *testing.T) string {
	t.Helper()
	bucket := os.Getenv("OBJECTSTORE_S3_TEST_URL")
	if bucket == "" {
		t.Skip("OBJECTSTORE_S3_TEST_URL not set; skipping iceberg S3 integration test (see providers/iceberg/s3_integration_test.go)")
	}
	return bucket
}

// newS3IcebergFixture creates a table under warehouse (an s3:// URL) via
// a Hadoop-style catalog backed by iceberg-go's S3 file IO, writes
// batches to it, and returns its metadata location — the S3 analog of
// iceberg_test.go's newIcebergFixture (which uses a local t.TempDir()).
func newS3IcebergFixture(t *testing.T, warehouse, tableName string, ioProps map[string]string, schema *arrow.Schema, batches ...arrow.RecordBatch) string {
	t.Helper()
	ctx := context.Background()

	hcat, err := hadoop.NewCatalog(tableName+"-catalog", warehouse, ioProps)
	if err != nil {
		t.Fatalf("hadoop.NewCatalog: %v", err)
	}
	_ = hcat.CreateNamespace(ctx, []string{"default"}, nil) // ignore AlreadyExists across runs

	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		t.Fatalf("ArrowSchemaToIcebergWithFreshIDs: %v", err)
	}
	ident := []string{"default", tableName}
	tbl, err := hcat.CreateTable(ctx, ident, icebergSchema)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	for i, batch := range batches {
		batch.Retain()
		rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{batch})
		if err != nil {
			t.Fatalf("NewRecordReader for batch %d: %v", i, err)
		}
		next, err := tbl.Append(ctx, rdr, nil)
		rdr.Release()
		if err != nil {
			t.Fatalf("Append batch %d: %v", i, err)
		}
		tbl = next
	}

	return tbl.MetadataLocation()
}

// TestS3Integration_MetadataLocationRead covers task 6.2: reading an
// Iceberg table whose metadata and data live on a real S3-compatible
// endpoint, via NewTableProvider's WithIOProps reaching iceberg-go's
// file-IO resolution (registered for s3:// by the blank import above).
func TestS3Integration_MetadataLocationRead(t *testing.T) {
	warehouse := s3TestWarehouse(t)
	// iceberg-go's S3 file IO defaults to path-style addressing whenever
	// a custom endpoint is set (s3.go's resolveUsePathStyle), so MinIO
	// needs no separate path-style property — just the endpoint.
	ioProps := map[string]string{
		"s3.endpoint": os.Getenv("OBJECTSTORE_S3_TEST_ENDPOINT"),
		"s3.region":   "us-east-1",
	}

	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	metaLoc := newS3IcebergFixture(t, warehouse, "s3-integration-people", ioProps, schema, batch)

	ctx := context.Background()
	p, err := provider.NewTableProvider(ctx, metaLoc, provider.WithIOProps(ioProps))
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}

	sess, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer sess.Close()
	if err := sess.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := sess.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 || names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("got ids=%v names=%v, want [1 2] [alice bob]", ids, names)
	}
}
