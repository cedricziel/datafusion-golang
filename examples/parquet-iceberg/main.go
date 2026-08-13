// Command parquet-iceberg demonstrates registering a Parquet-backed table
// provider and an Iceberg-backed table provider on the same session, then
// joining across both with one SQL query. It also demonstrates opening the
// same Iceberg table a second way — through an Iceberg REST catalog client
// (NewTableProviderFromCatalog) instead of a direct metadata.json path.
//
// Both providers are opt-in packages, separate from the core datafusion
// module: providers/parquet needs only a local Parquet file, and
// providers/iceberg needs only a local table's metadata.json (no catalog
// service) or a caller-supplied catalog client. This example builds small
// fixtures for both, plus a minimal in-process REST catalog server, under a
// temp directory so it runs standalone; a real program would point at
// existing files and a real catalog service instead.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go/catalog/hadoop"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergtable "github.com/apache/iceberg-go/table"
	"github.com/cedricziel/datafusion-golang/datafusion"
	icebergprovider "github.com/cedricziel/datafusion-golang/providers/iceberg"
	parquetprovider "github.com/cedricziel/datafusion-golang/providers/parquet"
)

func main() {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "parquet-iceberg-example")
	if err != nil {
		log.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	productsPath := writeProductsParquet(dir)
	ordersMetaLoc := writeOrdersIceberg(ctx, dir)

	sess, err := datafusion.NewSessionContext()
	if err != nil {
		log.Fatalf("NewSessionContext: %v", err)
	}
	defer sess.Close()

	products, err := parquetprovider.NewTableProvider(productsPath)
	if err != nil {
		log.Fatalf("parquet.NewTableProvider: %v", err)
	}
	if err := sess.RegisterTable("products", products); err != nil {
		log.Fatalf("RegisterTable(products): %v", err)
	}

	orders, err := icebergprovider.NewTableProvider(ctx, ordersMetaLoc)
	if err != nil {
		log.Fatalf("iceberg.NewTableProvider: %v", err)
	}
	if err := sess.RegisterTable("orders", orders); err != nil {
		log.Fatalf("RegisterTable(orders): %v", err)
	}

	reader, err := sess.SQL(`
		SELECT p.name, o.quantity
		FROM orders o
		JOIN products p ON p.id = o.product_id
		ORDER BY p.name`)
	if err != nil {
		log.Fatalf("SQL: %v", err)
	}
	defer reader.Release()

	for reader.Next() {
		fmt.Println(reader.RecordBatch())
	}
	if err := reader.Err(); err != nil {
		log.Fatalf("reading result: %v", err)
	}

	// The same Iceberg table can also be opened through a catalog client
	// instead of its metadata.json path — resolving it by namespace and
	// table name via any github.com/apache/iceberg-go/catalog.Catalog
	// implementation. Here that's the REST catalog client against a
	// minimal in-process fake server; a real program would point at an
	// actual REST catalog service.
	restSrv := startRESTCatalogFake(ordersMetaLoc)
	defer restSrv.Close()

	restCat, err := rest.NewCatalog(ctx, "example", restSrv.URL)
	if err != nil {
		log.Fatalf("rest.NewCatalog: %v", err)
	}
	ordersViaCatalog, err := icebergprovider.NewTableProviderFromCatalog(ctx, restCat, "default", "orders")
	if err != nil {
		log.Fatalf("iceberg.NewTableProviderFromCatalog: %v", err)
	}
	if err := sess.RegisterTable("orders_via_catalog", ordersViaCatalog); err != nil {
		log.Fatalf("RegisterTable(orders_via_catalog): %v", err)
	}

	catalogReader, err := sess.SQL("SELECT product_id, quantity FROM orders_via_catalog ORDER BY product_id")
	if err != nil {
		log.Fatalf("SQL: %v", err)
	}
	defer catalogReader.Release()

	for catalogReader.Next() {
		fmt.Println(catalogReader.RecordBatch())
	}
	if err := catalogReader.Err(); err != nil {
		log.Fatalf("reading catalog result: %v", err)
	}
}

// startRESTCatalogFake serves just enough of the Iceberg REST catalog
// protocol (GET /v1/config, GET /v1/namespaces/{ns}/tables/{table}) for
// catalog/rest's client to load one table, by reading that table's real
// metadata.json off disk and returning it verbatim. A real REST catalog
// service implements the full protocol (namespace/table management,
// commits, credential vending, ...); this fake exists only so the example
// runs standalone with no external service.
func startRESTCatalogFake(metadataLocation string) *httptest.Server {
	metadataBytes, err := os.ReadFile(metadataLocation)
	if err != nil {
		log.Fatalf("reading orders metadata.json: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/config", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"defaults":  map[string]any{},
			"overrides": map[string]any{},
		})
	})
	mux.HandleFunc("/v1/namespaces/default/tables/orders", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"metadata-location": %q, "metadata": %s}`, metadataLocation, metadataBytes)
	})

	return httptest.NewServer(mux)
}

func productsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// writeProductsParquet writes a small Parquet file under dir and returns
// its path.
func writeProductsParquet(dir string) string {
	schema := productsSchema()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"widget", "gadget", "gizmo"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	path := filepath.Join(dir, "products.parquet")
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	fw, err := pqarrow.NewFileWriter(schema, f, pqparquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
	if err != nil {
		log.Fatalf("NewFileWriter: %v", err)
	}
	if err := fw.Write(rec); err != nil {
		log.Fatalf("Write: %v", err)
	}
	if err := fw.Close(); err != nil {
		log.Fatalf("Close writer: %v", err)
	}
	return path
}

func ordersSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "product_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "quantity", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	}, nil)
}

// writeOrdersIceberg creates a small local Iceberg table under dir using
// iceberg-go's own Hadoop (catalog-less, filesystem-only) catalog and
// write path, and returns its metadata.json location.
func writeOrdersIceberg(ctx context.Context, dir string) string {
	schema := ordersSchema()
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 1, 3}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{5, 2, 1, 8}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()

	// The Hadoop catalog creates namespace directories with a plain
	// os.Mkdir, so the warehouse root itself must already exist.
	warehouse := filepath.Join(dir, "warehouse")
	if err := os.MkdirAll(warehouse, 0o755); err != nil {
		log.Fatalf("MkdirAll %s: %v", warehouse, err)
	}
	cat, err := hadoop.NewCatalog("example", warehouse, nil)
	if err != nil {
		log.Fatalf("hadoop.NewCatalog: %v", err)
	}
	if err := cat.CreateNamespace(ctx, []string{"default"}, nil); err != nil {
		log.Fatalf("CreateNamespace: %v", err)
	}

	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		log.Fatalf("ArrowSchemaToIcebergWithFreshIDs: %v", err)
	}
	tbl, err := cat.CreateTable(ctx, []string{"default", "orders"}, icebergSchema)
	if err != nil {
		log.Fatalf("CreateTable: %v", err)
	}

	rec.Retain()
	rdr, err := array.NewRecordReader(schema, []arrow.RecordBatch{rec})
	if err != nil {
		log.Fatalf("NewRecordReader: %v", err)
	}
	defer rdr.Release()
	tbl, err = tbl.Append(ctx, rdr, nil)
	if err != nil {
		log.Fatalf("Append: %v", err)
	}

	return tbl.MetadataLocation()
}
