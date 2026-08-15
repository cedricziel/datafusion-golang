package parquet_test

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/cedricziel/datafusion-golang/datafusion"
	_ "github.com/cedricziel/datafusion-golang/objectstore/gcs"
	provider "github.com/cedricziel/datafusion-golang/providers/parquet"
)

// gcsTestBucket returns the OBJECTSTORE_GCS_TEST_URL environment
// variable's value, or skips the test if it is unset. Expected form: a
// gs:// bucket URL, e.g. against a local fake-gcs-server:
//
//	gs://test-bucket?endpoint=http://localhost:4443
func gcsTestBucket(t *testing.T) string {
	t.Helper()
	bucket := os.Getenv("OBJECTSTORE_GCS_TEST_URL")
	if bucket == "" {
		t.Skip("OBJECTSTORE_GCS_TEST_URL not set; skipping GCS integration test (see providers/parquet/gcs_integration_test.go)")
	}
	return bucket
}

// gcsLocation joins key onto bucket, preserving bucket's query string —
// design D2's per-location query-parameter passthrough.
func gcsLocation(t *testing.T, bucket, key string) string {
	t.Helper()
	u, err := url.Parse(bucket)
	if err != nil {
		t.Fatalf("OBJECTSTORE_GCS_TEST_URL %q: %v", bucket, err)
	}
	u.Path = "/" + key
	return u.String()
}

// TestGCSIntegration_ScanPushdownAndInsert covers construction, pushdown
// scan, and INSERT through SQL against a real GCS-compatible endpoint
// (task 4.2): the same behavior already proven against mem:// and s3://,
// now against the gs:// scheme end to end.
func TestGCSIntegration_ScanPushdownAndInsert(t *testing.T) {
	bucket := gcsTestBucket(t)
	location := gcsLocation(t, bucket, "gcs-integration-"+t.Name()+"/people.parquet")

	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	writeParquetToStore(t, location, schema, batch)

	ctx := context.Background()
	p, err := provider.NewTableProvider(ctx, location)
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

	reader, err := sess.SQL("SELECT id, name FROM people WHERE id > 1 ORDER BY id")
	if err != nil {
		t.Fatalf("SQL select: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 1 || ids[0] != 2 || names[0] != "bob" {
		t.Fatalf("got ids=%v names=%v, want [2] [bob]", ids, names)
	}

	insertReader, err := sess.SQL("INSERT INTO people VALUES (3, 'carol')")
	if err != nil {
		t.Fatalf("SQL insert: %v", err)
	}
	defer insertReader.Release()
	for insertReader.Next() {
	}
	if err := insertReader.Err(); err != nil {
		t.Fatalf("insert: %v", err)
	}

	afterReader, err := sess.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL select after insert: %v", err)
	}
	ids, names = collectRows(t, afterReader)
	if len(ids) != 3 || ids[2] != 3 || names[2] != "carol" {
		t.Fatalf("got ids=%v names=%v after insert, want a third row (3, carol)", ids, names)
	}
}
