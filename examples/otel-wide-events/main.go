// Command otel-wide-events demonstrates an OpenTelemetry-shaped wide-events
// table with separate physical and logical schemas: a single Parquet-backed
// table stores one row per event with a generic AnyValue-encoded attributes
// column (the physical schema), and a SQL view projects specific attributes
// out into flat, semantic-convention-conformant columns (the logical
// schema) — e.g. "http.request.method", "http.response.status_code".
//
// AnyValue (OTel's sum-typed attribute value: string, int, double, bool,
// ...) has no native Arrow union that plays well with Parquet, so it is
// encoded physically as a struct-of-nullable-typed-columns — one nullable
// field per variant, all but one NULL for any given value — nested inside
// a list of {key, value} pairs per event. This example covers the string,
// int, double, and bool variants; bytes/array/kvlist variants are
// self-referential (an AnyValue can contain AnyValues) and need a
// bounded-depth or JSON-encoded fallback beyond what this example covers.
//
// New events land in the physical table via INSERT INTO ... SELECT from
// another registered provider (an in-memory "staging" table standing in
// for a real event source) rather than SQL VALUES literals: DataFusion
// 54.1.0 cannot CAST a VALUES-literal's inferred List<Struct<...>> type to
// the registered table's exact physical type (which carries Parquet field
// IDs and internal element-field naming the literal's inferred type
// doesn't), but a provider-to-provider INSERT INTO ... SELECT sidesteps
// that cast entirely — and is the more realistic ingestion shape besides,
// since event batches arrive as Arrow data, not hand-typed SQL.
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
	"github.com/cedricziel/datafusion-golang/datafusion"
	parquetprovider "github.com/cedricziel/datafusion-golang/providers/parquet"
)

// anyValueType is the physical struct-of-nullable-columns encoding for one
// AnyValue: exactly one field is non-NULL for any given attribute value.
func anyValueType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "int_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "double_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	)
}

// wideEventsSchema is the physical schema: one row per event, attributes
// as a list of {key, value AnyValue} pairs — OTel's actual repeated-KeyValue
// shape, physically encoded in Arrow/Parquet.
func wideEventsSchema() *arrow.Schema {
	kv := arrow.StructOf(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "value", Type: anyValueType(), Nullable: true},
	)
	return arrow.NewSchema([]arrow.Field{
		{Name: "event_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "event_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "attributes", Type: arrow.ListOf(kv), Nullable: true},
	}, nil)
}

// attr is one key-value pair to attach to an event; exactly one of the
// value fields is set. Build one with strAttr/intAttr/dblAttr/boolAttr.
type attr struct {
	key string
	s   *string
	i   *int64
	d   *float64
	b   *bool
}

func strAttr(key, v string) attr         { return attr{key: key, s: &v} }
func intAttr(key string, v int64) attr   { return attr{key: key, i: &v} }
func dblAttr(key string, v float64) attr { return attr{key: key, d: &v} }
func boolAttr(key string, v bool) attr   { return attr{key: key, b: &v} }

// appendEvent appends one event row, with its attributes list, to b.
func appendEvent(b *array.RecordBuilder, id int64, traceID, name string, attrs []attr) {
	b.Field(0).(*array.Int64Builder).Append(id)
	b.Field(1).(*array.StringBuilder).Append(traceID)
	b.Field(2).(*array.StringBuilder).Append(name)

	listB := b.Field(3).(*array.ListBuilder)
	kvB := listB.ValueBuilder().(*array.StructBuilder)
	listB.Append(true)
	for _, a := range attrs {
		kvB.Append(true)
		kvB.FieldBuilder(0).(*array.StringBuilder).Append(a.key)

		valB := kvB.FieldBuilder(1).(*array.StructBuilder)
		valB.Append(true)
		sB := valB.FieldBuilder(0).(*array.StringBuilder)
		iB := valB.FieldBuilder(1).(*array.Int64Builder)
		dB := valB.FieldBuilder(2).(*array.Float64Builder)
		bB := valB.FieldBuilder(3).(*array.BooleanBuilder)
		switch {
		case a.s != nil:
			sB.Append(*a.s)
			iB.AppendNull()
			dB.AppendNull()
			bB.AppendNull()
		case a.i != nil:
			sB.AppendNull()
			iB.Append(*a.i)
			dB.AppendNull()
			bB.AppendNull()
		case a.d != nil:
			sB.AppendNull()
			iB.AppendNull()
			dB.Append(*a.d)
			bB.AppendNull()
		case a.b != nil:
			sB.AppendNull()
			iB.AppendNull()
			dB.AppendNull()
			bB.Append(*a.b)
		}
	}
}

// httpEventsView is the logical schema: a SQL view over wide_events that
// unnests attributes and pivots specific keys out into flat,
// semantic-convention-conformant columns (quoted, dotted names — exactly
// how OTel semconv names its attributes). MAX(CASE WHEN ...) per attribute
// is the standard unnest-then-pivot idiom; GROUP BY event_id collapses
// each event's attribute rows back into one output row per event.
const httpEventsView = `CREATE VIEW http_events AS
SELECT
    event_id,
    trace_id,
    event_name,
    MAX(CASE WHEN u.key = 'http.request.method' THEN u.value.string_value END) AS "http.request.method",
    MAX(CASE WHEN u.key = 'url.path' THEN u.value.string_value END) AS "url.path",
    MAX(CASE WHEN u.key = 'client.address' THEN u.value.string_value END) AS "client.address",
    MAX(CASE WHEN u.key = 'http.response.status_code' THEN u.value.int_value END) AS "http.response.status_code",
    MAX(CASE WHEN u.key = 'http.server.request.duration' THEN u.value.double_value END) AS "http.server.request.duration",
    MAX(CASE WHEN u.key = 'error' THEN u.value.bool_value END) AS "error"
FROM (SELECT event_id, trace_id, event_name, UNNEST(attributes) AS u FROM wide_events)
GROUP BY event_id, trace_id, event_name`

func main() {
	dir, err := os.MkdirTemp("", "otel-wide-events-example")
	if err != nil {
		log.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	schema := wideEventsSchema()
	path := writeInitialEvents(dir, schema)

	sess, err := datafusion.NewSessionContext()
	if err != nil {
		log.Fatalf("NewSessionContext: %v", err)
	}
	defer sess.Close()

	events, err := parquetprovider.NewTableProvider(path)
	if err != nil {
		log.Fatalf("parquet.NewTableProvider: %v", err)
	}
	if err := sess.RegisterTable("wide_events", events); err != nil {
		log.Fatalf("RegisterTable(wide_events): %v", err)
	}

	if _, err := sess.SQL(httpEventsView); err != nil {
		log.Fatalf("CREATE VIEW http_events: %v", err)
	}

	fmt.Println("--- http_events, before insert ---")
	printHTTPEvents(sess)

	// New events arrive as an Arrow batch — modeled here by a small
	// in-memory staging provider — and land in the physical table via
	// INSERT INTO ... SELECT (see the package doc comment for why not
	// SQL VALUES literals).
	staging := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	appendEvent(staging, 3, "trace-003", "http.request", []attr{
		strAttr("http.request.method", "POST"),
		strAttr("url.path", "/orders"),
		strAttr("client.address", "203.0.113.7"),
		intAttr("http.response.status_code", 404),
		dblAttr("http.server.request.duration", 0.031),
		boolAttr("error", true),
	})
	stagingRec := staging.NewRecordBatch()
	staging.Release()
	defer stagingRec.Release()

	if err := sess.RegisterTable("staging", &memProvider{schema: schema, batch: stagingRec}); err != nil {
		log.Fatalf("RegisterTable(staging): %v", err)
	}
	insertReader, err := sess.SQL("INSERT INTO wide_events SELECT * FROM staging")
	if err != nil {
		log.Fatalf("INSERT INTO wide_events: %v", err)
	}
	insertReader.Release()

	fmt.Println("--- http_events, after insert ---")
	printHTTPEvents(sess)
}

func printHTTPEvents(sess *datafusion.SessionContext) {
	reader, err := sess.SQL(`
		SELECT event_id, "http.request.method", "url.path", "client.address",
		       "http.response.status_code", "http.server.request.duration", "error"
		FROM http_events ORDER BY event_id`)
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

// writeInitialEvents writes two seed events (a 200 GET and a 500 GET) to a
// Parquet file under dir and returns its path.
func writeInitialEvents(dir string, schema *arrow.Schema) string {
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	appendEvent(b, 1, "trace-001", "http.request", []attr{
		strAttr("http.request.method", "GET"),
		strAttr("url.path", "/orders/1"),
		strAttr("client.address", "203.0.113.1"),
		intAttr("http.response.status_code", 200),
		dblAttr("http.server.request.duration", 0.008),
		boolAttr("error", false),
	})
	appendEvent(b, 2, "trace-002", "http.request", []attr{
		strAttr("http.request.method", "GET"),
		strAttr("url.path", "/orders/2"),
		strAttr("client.address", "203.0.113.2"),
		intAttr("http.response.status_code", 500),
		dblAttr("http.server.request.duration", 0.142),
		boolAttr("error", true),
	})
	rec := b.NewRecordBatch()
	defer rec.Release()

	path := filepath.Join(dir, "wide_events.parquet")
	f, err := os.Create(path)
	if err != nil {
		log.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	fw, err := pqarrow.NewFileWriter(schema, f, pqparquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
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

// memProvider is a minimal read-only datafusion.TableProvider over a
// single in-memory batch — standing in for a real event source (a
// collector export, a Kafka consumer batch, ...) as the source side of
// INSERT INTO ... SELECT.
type memProvider struct {
	schema *arrow.Schema
	batch  arrow.RecordBatch
}

func (m *memProvider) Schema() *arrow.Schema { return m.schema }

func (m *memProvider) Scan(ctx context.Context) (array.RecordReader, error) {
	m.batch.Retain()
	return array.NewRecordReader(m.schema, []arrow.RecordBatch{m.batch})
}
