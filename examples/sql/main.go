// Command sql demonstrates executing SQL against an embedded DataFusion
// session and consuming the result as Arrow record batches.
package main

import (
	"fmt"
	"log"

	"github.com/cedricziel/datafusion-golang/datafusion"
)

func main() {
	ctx, err := datafusion.NewSessionContext()
	if err != nil {
		log.Fatalf("NewSessionContext: %v", err)
	}
	defer ctx.Close()

	reader, err := ctx.SQL("SELECT 1 AS one, 2 AS two UNION ALL SELECT 3, 4")
	if err != nil {
		log.Fatalf("SQL: %v", err)
	}
	defer reader.Release()

	fmt.Println("schema:", reader.Schema())

	for reader.Next() {
		rec := reader.RecordBatch()
		fmt.Printf("batch: %d rows, %d columns\n", rec.NumRows(), rec.NumCols())
		fmt.Println(rec)
	}
	if err := reader.Err(); err != nil {
		log.Fatalf("reading result: %v", err)
	}
}
