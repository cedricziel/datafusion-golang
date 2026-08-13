package csv_test

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
	csvprovider "github.com/cedricziel/datafusion-golang/providers/csv"
)

// Example demonstrates registering a local CSV file and querying it via
// SQL, using an explicit schema.
func Example() {
	dir, err := os.MkdirTemp("", "csv-example")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	path := filepath.Join(dir, "people.csv")
	if err := os.WriteFile(path, []byte("id,name\n1,alice\n2,bob\n"), 0o644); err != nil {
		panic(err)
	}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	table, err := csvprovider.NewTableProvider(path, schema)
	if err != nil {
		panic(err)
	}

	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		panic(err)
	}
	defer ctx.Close()

	if err := ctx.RegisterTable("people", table); err != nil {
		panic(err)
	}

	reader, err := ctx.SQL("SELECT name FROM people WHERE id = 2")
	if err != nil {
		panic(err)
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.RecordBatch()
		fmt.Println(rec.Column(0))
	}
	if err := reader.Err(); err != nil {
		panic(err)
	}

	// Output:
	// ["bob"]
}
