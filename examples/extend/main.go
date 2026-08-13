// Command extend demonstrates extending an embedded DataFusion session
// with a Go-implemented table provider and a Go-implemented scalar UDF,
// then using both in one SQL query.
package main

import (
	"context"
	"fmt"
	"log"

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
}
