package parquet_test

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/cedricziel/datafusion-golang/datafusion"
	_ "github.com/cedricziel/datafusion-golang/objectstore/s3"
	provider "github.com/cedricziel/datafusion-golang/providers/parquet"
)

func parseS3URL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("OBJECTSTORE_S3_TEST_URL %q: %v", raw, err)
	}
	return u
}

// s3TestBucket returns the OBJECTSTORE_S3_TEST_URL environment variable's
// value, or skips the test if it is unset. Expected form: an s3:// bucket
// URL with any query parameters an S3-compatible endpoint needs, e.g.
// against a local MinIO:
//
//	s3://test-bucket?endpoint=http://localhost:9000&use_path_style=true&region=us-east-1
func s3TestBucket(t *testing.T) string {
	t.Helper()
	bucket := os.Getenv("OBJECTSTORE_S3_TEST_URL")
	if bucket == "" {
		t.Skip("OBJECTSTORE_S3_TEST_URL not set; skipping S3 integration test (see providers/parquet/s3_integration_test.go)")
	}
	return bucket
}

// s3Location joins key onto bucket, preserving bucket's query string —
// design D2's per-location query-parameter passthrough.
func s3Location(t *testing.T, bucket, key string) string {
	t.Helper()
	u := parseS3URL(t, bucket)
	u.Path = "/" + key
	return u.String()
}

// TestS3Integration_ScanPushdownAndInsert covers construction, pushdown
// scan, and INSERT through SQL against a real S3-compatible endpoint
// (task 3.3): the same behavior already proven against mem:// in
// parquet_test.go/insert_test.go, now against the s3:// scheme end to
// end.
func TestS3Integration_ScanPushdownAndInsert(t *testing.T) {
	bucket := s3TestBucket(t)
	location := s3Location(t, bucket, "s3-integration-"+uniqueTestSuffix(t)+"/people.parquet")
	removeOnCleanup(t, location)

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
