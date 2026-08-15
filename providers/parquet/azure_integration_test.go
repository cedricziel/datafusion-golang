package parquet_test

import (
	"context"
	"net/url"
	"os"
	"testing"

	"github.com/cedricziel/datafusion-golang/datafusion"
	_ "github.com/cedricziel/datafusion-golang/objectstore/azure"
	provider "github.com/cedricziel/datafusion-golang/providers/parquet"
)

// azureTestContainer returns the OBJECTSTORE_AZURE_TEST_URL environment
// variable's value, or skips the test if it is unset. Expected form: an
// azblob:// container URL, e.g. against a local Azurite:
//
//	azblob://test-container?domain=127.0.0.1:10000&protocol=http&storage_account=devstoreaccount1
func azureTestContainer(t *testing.T) string {
	t.Helper()
	container := os.Getenv("OBJECTSTORE_AZURE_TEST_URL")
	if container == "" {
		t.Skip("OBJECTSTORE_AZURE_TEST_URL not set; skipping Azure integration test (see providers/parquet/azure_integration_test.go)")
	}
	return container
}

// azureLocation joins key onto container, preserving its query string —
// design D2's per-location query-parameter passthrough.
func azureLocation(t *testing.T, container, key string) string {
	t.Helper()
	u, err := url.Parse(container)
	if err != nil {
		t.Fatalf("OBJECTSTORE_AZURE_TEST_URL %q: %v", container, err)
	}
	u.Path = "/" + key
	return u.String()
}

// TestAzureIntegration_ScanPushdownAndInsert covers construction,
// pushdown scan, and INSERT through SQL against a real
// Azure-Blob-compatible endpoint (task 5.2): the same behavior already
// proven against mem://, s3://, and gs://, now against azblob:// end to
// end.
func TestAzureIntegration_ScanPushdownAndInsert(t *testing.T) {
	container := azureTestContainer(t)
	location := azureLocation(t, container, "azure-integration-"+t.Name()+"/people.parquet")

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
