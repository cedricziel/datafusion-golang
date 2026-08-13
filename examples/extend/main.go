// Command extend demonstrates extending an embedded DataFusion session
// with a Go-implemented table provider and a Go-implemented scalar UDF,
// then using both in one SQL query. It also demonstrates a Go-implemented
// catalog (RegisterCatalog), whose schema lists tables discovered from an
// in-memory map that changes between two queries, addressed via SQL as
// catalog.schema.table.
package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// peopleTable is an in-memory table with columns (id BIGINT, name TEXT).
type peopleTable struct {
	schema *arrow.Schema
}

func (p *peopleTable) Schema() *arrow.Schema { return p.schema }

func (p *peopleTable) Scan(ctx context.Context) (array.RecordReader, error) {
	b := array.NewRecordBuilder(memory.DefaultAllocator, p.schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"alice", "bob", "carol", "dave"}, nil)
	rec := b.NewRecordBatch()
	defer rec.Release()
	return array.NewRecordReader(p.schema, []arrow.RecordBatch{rec})
}

// warehouseCatalog is a datafusion.CatalogProvider with a single "public"
// schema backed by an in-memory map of tables. Tables can be added at any
// time (addTable); the engine discovers them fresh on every query, so a
// table added after registration becomes queryable without re-registering
// the catalog.
type warehouseCatalog struct {
	mu     sync.Mutex
	tables map[string]datafusion.TableProvider
}

func newWarehouseCatalog() *warehouseCatalog {
	return &warehouseCatalog{tables: map[string]datafusion.TableProvider{}}
}

func (w *warehouseCatalog) addTable(name string, table datafusion.TableProvider) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.tables[name] = table
}

func (w *warehouseCatalog) SchemaNames(ctx context.Context) ([]string, error) {
	return []string{"public"}, nil
}

func (w *warehouseCatalog) Schema(ctx context.Context, name string) (datafusion.SchemaProvider, bool, error) {
	if name != "public" {
		return nil, false, nil
	}
	return w, true, nil
}

func (w *warehouseCatalog) TableNames(ctx context.Context) ([]string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	names := make([]string, 0, len(w.tables))
	for name := range w.tables {
		names = append(names, name)
	}
	return names, nil
}

func (w *warehouseCatalog) Table(ctx context.Context, name string) (datafusion.TableProvider, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	t, ok := w.tables[name]
	return t, ok, nil
}

// intTable is a minimal single-column table, used to keep the catalog
// demonstration focused on discovery rather than schema shape.
type intTable struct {
	values []int64
}

func (t *intTable) Schema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
}

func (t *intTable) Scan(ctx context.Context) (array.RecordReader, error) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.AppendValues(t.values, nil)
	arr := b.NewArray()
	defer arr.Release()
	rec := array.NewRecordBatch(t.Schema(), []arrow.Array{arr}, int64(len(t.values)))
	defer rec.Release()
	return array.NewRecordReader(t.Schema(), []arrow.RecordBatch{rec})
}

func main() {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		log.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	table := &peopleTable{schema: arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)}
	if err := ctx.RegisterTable("people", table); err != nil {
		log.Fatalf("RegisterTable: %v", err)
	}

	double, err := datafusion.NewScalarUDF(
		"double",
		[]arrow.DataType{arrow.PrimitiveTypes.Int64},
		arrow.PrimitiveTypes.Int64,
		func(args []arrow.Array) (arrow.Array, error) {
			in := args[0].(*array.Int64)
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			for i := 0; i < in.Len(); i++ {
				if in.IsNull(i) {
					b.AppendNull()
					continue
				}
				b.Append(in.Value(i) * 2)
			}
			return b.NewArray(), nil
		},
	)
	if err != nil {
		log.Fatalf("NewScalarUDF: %v", err)
	}
	if err := ctx.RegisterScalarUDF(double); err != nil {
		log.Fatalf("RegisterScalarUDF: %v", err)
	}

	reader, err := ctx.SQL(`
		SELECT name, double(id) AS doubled_id
		FROM people
		WHERE id > 1
		ORDER BY id`)
	if err != nil {
		log.Fatalf("SQL: %v", err)
	}
	defer reader.Release()

	fmt.Println("schema:", reader.Schema())
	for reader.Next() {
		rec := reader.RecordBatch()
		fmt.Printf("batch: %d rows\n", rec.NumRows())
		fmt.Println(rec)
	}
	if err := reader.Err(); err != nil {
		log.Fatalf("reading result: %v", err)
	}

	// A Go-implemented catalog: SQL addresses tables as catalog.schema.table
	// against content the catalog discovers dynamically, not pre-declared
	// via RegisterTable.
	warehouse := newWarehouseCatalog()
	warehouse.addTable("counts", &intTable{values: []int64{10, 20, 30}})
	if err := ctx.RegisterCatalog("warehouse", warehouse); err != nil {
		log.Fatalf("RegisterCatalog: %v", err)
	}

	catalogReader, err := ctx.SQL("SELECT n FROM warehouse.public.counts ORDER BY n")
	if err != nil {
		log.Fatalf("SQL: %v", err)
	}
	fmt.Println("warehouse.public.counts before adding 'more':")
	for catalogReader.Next() {
		fmt.Println(catalogReader.RecordBatch())
	}
	if err := catalogReader.Err(); err != nil {
		log.Fatalf("reading catalog result: %v", err)
	}
	catalogReader.Release()

	// A table added after registration becomes queryable without
	// re-registering the catalog — the engine discovers it fresh per query.
	warehouse.addTable("more", &intTable{values: []int64{99}})
	moreReader, err := ctx.SQL("SELECT n FROM warehouse.public.more")
	if err != nil {
		log.Fatalf("SQL: %v", err)
	}
	fmt.Println("warehouse.public.more, added after registration:")
	for moreReader.Next() {
		fmt.Println(moreReader.RecordBatch())
	}
	if err := moreReader.Err(); err != nil {
		log.Fatalf("reading catalog result: %v", err)
	}
	moreReader.Release()
}
