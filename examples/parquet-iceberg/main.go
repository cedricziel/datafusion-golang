// Command parquet-iceberg demonstrates registering a Parquet-backed table
// provider and an Iceberg-backed table provider on the same session, then
// joining across both with one SQL query.
//
// Both providers are opt-in packages, separate from the core datafusion
// module: providers/parquet needs only a local Parquet file, and
// providers/iceberg needs only a local table's metadata.json (no catalog
// service). This example builds small fixtures for both under a temp
// directory so it runs standalone; a real program would point at existing
// files instead.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go/catalog/hadoop"
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
