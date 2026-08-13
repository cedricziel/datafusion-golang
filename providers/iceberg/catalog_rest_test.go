package iceberg_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/apache/iceberg-go/catalog/rest"
	"github.com/cedricziel/datafusion-golang/datafusion"
	provider "github.com/cedricziel/datafusion-golang/providers/iceberg"
)

// newRESTCatalogFake serves a minimal, wire-format-correct Iceberg REST
// catalog protocol (design D6): just enough of GET /v1/config and GET
// /v1/namespaces/{ns}/tables/{table} for catalog/rest's client to
// construct and load one table, backed by a real table created on disk via
// the existing hadoop-catalog fixture. The handler reads that table's real
// metadata.json off disk and returns it verbatim as the response's
// "metadata" field (the same shape catalog/rest's own test suite uses), so
// the client-under-test is exercised against a real, scannable local
// table rather than a schema-only fixture.
func newRESTCatalogFake(t *testing.T, ns, tableName, metadataLocation string) *httptest.Server {
	t.Helper()
	metadataBytes, err := os.ReadFile(metadataLocation)
	if err != nil {
		t.Fatalf("reading fixture metadata.json: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})
	mux.HandleFunc(fmt.Sprintf("/v1/namespaces/%s/tables/%s", ns, tableName), func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"metadata-location": %q, "metadata": %s}`, metadataLocation, metadataBytes)
	})

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestNewTableProviderFromCatalog_RESTCatalogEndToEnd(t *testing.T) {
	ctx := context.Background()
	schema := peopleSchema()
	batch := peopleBatch(t, schema, []int64{1, 2}, []string{"alice", "bob"})
	defer batch.Release()
	cat, ident := newIcebergCatalogFixture(t, "people", schema, batch)

	tbl, err := cat.LoadTable(ctx, ident)
	if err != nil {
		t.Fatalf("LoadTable (fixture setup): %v", err)
	}

	srv := newRESTCatalogFake(t, "default", "people", tbl.MetadataLocation())

	restCat, err := rest.NewCatalog(ctx, "test", srv.URL)
	if err != nil {
		t.Fatalf("rest.NewCatalog: %v", err)
	}

	p, err := provider.NewTableProviderFromCatalog(ctx, restCat, "default", "people")
	if err != nil {
		t.Fatalf("NewTableProviderFromCatalog: %v", err)
	}

	dfCtx, err := datafusion.NewSessionContext()
	if err != nil {
		t.Fatalf("NewSessionContext: %v", err)
	}
	defer dfCtx.Close()
	if err := dfCtx.RegisterTable("people", p); err != nil {
		t.Fatalf("RegisterTable: %v", err)
	}

	reader, err := dfCtx.SQL("SELECT id, name FROM people ORDER BY id")
	if err != nil {
		t.Fatalf("SQL: %v", err)
	}
	ids, names := collectRows(t, reader)
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("unexpected ids: %v", ids)
	}
	if names[0] != "alice" || names[1] != "bob" {
		t.Fatalf("unexpected names: %v", names)
	}
}
