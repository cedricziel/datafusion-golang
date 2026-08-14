// Command otel-wide-events demonstrates an OpenTelemetry-shaped wide-events
// table with separate physical and logical schemas: a single Parquet-backed
// table stores one row per event with a generic AnyValue-encoded attributes
// column (the physical schema), and a SQL view projects specific attributes
// out into flat, semantic-convention-conformant columns (the logical
// schema) — e.g. "http.request.method", "http.response.status_code".
//
// AnyValue (OTel's sum-typed attribute value) has no native Arrow union
// that plays well with Parquet, so it is encoded physically as a
// struct-of-nullable-typed-columns — one nullable field per variant, all
// but one NULL for any given value — nested inside a list of {key, value}
// pairs per event. This covers all 7 AnyValue variants: string, bool,
// int, double, bytes, array, and kvlist. array_value/kvlist_value are
// self-referential (an AnyValue can contain AnyValues), which Arrow
// cannot express as a true recursive type, so this example bounds nesting
// to one level — an array_value/kvlist_value's elements may only be
// scalar AnyValues (string/bool/int/double/bytes), never themselves
// array_value/kvlist_value. Deeper OTel payloads (nested arrays of
// arrays, arbitrarily nested kvlists) would need either more physical
// nesting levels (each one a distinct, larger Arrow type, same technique
// repeated) or a JSON/serialized-bytes fallback beyond some fixed depth.
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
//
// httpEventsView's own doc comment covers a second, independent DataFusion
// 54.1.0 limitation this example works around: MAX/FIRST_VALUE over a
// List-typed column fails once the underlying scan spans more than one
// batch (which two separate writes to the Parquet file — the seed data
// and the inserted event — guarantee here).
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

// anyValueLeafType is AnyValue bounded to its 5 scalar variants — the
// terminal case used inside array_value/kvlist_value, since this example
// does not support nesting an array/kvlist inside another one.
func anyValueLeafType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "int_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "double_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "bytes_value", Type: arrow.BinaryTypes.Binary, Nullable: true},
	)
}

// kvLeafType is a {key, value} pair whose value is leaf-only — the
// element type of kvlist_value.
func kvLeafType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "key", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "value", Type: anyValueLeafType(), Nullable: true},
	)
}

// anyValueType is the physical struct-of-nullable-columns encoding for a
// full, top-level AnyValue: all 7 variants, exactly one non-NULL. Its
// first 5 fields mirror anyValueLeafType's so appendScalar can fill
// either; array_value/kvlist_value are the two variants leaf values omit.
func anyValueType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "int_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "double_value", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "bytes_value", Type: arrow.BinaryTypes.Binary, Nullable: true},
		arrow.Field{Name: "array_value", Type: arrow.ListOf(anyValueLeafType()), Nullable: true},
		arrow.Field{Name: "kvlist_value", Type: arrow.ListOf(kvLeafType()), Nullable: true},
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

// anyValue is a Go-side AnyValue for building example data; exactly one
// field is set. array/kvlist elements are themselves anyValue but must be
// leaf-only (see appendLeafValue) — construct them with the scalar
// constructors (strVal, intVal, ...), never arrayVal/kvlistVal.
type anyValue struct {
	str     *string
	boolean *bool
	i       *int64
	d       *float64
	bytes   []byte
	array   []anyValue
	kvlist  []kv
}

// kv is one entry of a kvlist_value: a key paired with a leaf AnyValue.
type kv struct {
	key   string
	value anyValue
}

func strVal(v string) anyValue         { return anyValue{str: &v} }
func boolVal(v bool) anyValue          { return anyValue{boolean: &v} }
func intVal(v int64) anyValue          { return anyValue{i: &v} }
func dblVal(v float64) anyValue        { return anyValue{d: &v} }
func bytesVal(v []byte) anyValue       { return anyValue{bytes: v} }
func arrayVal(vs ...anyValue) anyValue { return anyValue{array: vs} }
func kvlistVal(kvs ...kv) anyValue     { return anyValue{kvlist: kvs} }
func kvPair(key string, v anyValue) kv { return kv{key: key, value: v} }

// attr is one attribute (key + AnyValue) to attach to an event.
type attr struct {
	key   string
	value anyValue
}

func strAttr(key, v string) attr                { return attr{key: key, value: strVal(v)} }
func boolAttr(key string, v bool) attr          { return attr{key: key, value: boolVal(v)} }
func intAttr(key string, v int64) attr          { return attr{key: key, value: intVal(v)} }
func dblAttr(key string, v float64) attr        { return attr{key: key, value: dblVal(v)} }
func bytesAttr(key string, v []byte) attr       { return attr{key: key, value: bytesVal(v)} }
func arrayAttr(key string, vs ...anyValue) attr { return attr{key: key, value: arrayVal(vs...)} }
func kvlistAttr(key string, kvs ...kv) attr     { return attr{key: key, value: kvlistVal(kvs...)} }

// appendScalar fills the 5 scalar-variant fields (indices 0-4, the layout
// shared by anyValueType and anyValueLeafType) from v, leaving exactly
// the set field non-NULL.
func appendScalar(valB *array.StructBuilder, v anyValue) {
	sB := valB.FieldBuilder(0).(*array.StringBuilder)
	boB := valB.FieldBuilder(1).(*array.BooleanBuilder)
	iB := valB.FieldBuilder(2).(*array.Int64Builder)
	dB := valB.FieldBuilder(3).(*array.Float64Builder)
	byB := valB.FieldBuilder(4).(*array.BinaryBuilder)

	if v.str != nil {
		sB.Append(*v.str)
	} else {
		sB.AppendNull()
	}
	if v.boolean != nil {
		boB.Append(*v.boolean)
	} else {
		boB.AppendNull()
	}
	if v.i != nil {
		iB.Append(*v.i)
	} else {
		iB.AppendNull()
	}
	if v.d != nil {
		dB.Append(*v.d)
	} else {
		dB.AppendNull()
	}
	if v.bytes != nil {
		byB.Append(v.bytes)
	} else {
		byB.AppendNull()
	}
}

// appendLeafValue writes v into valB, a leaf anyValueLeafType struct slot
// (used inside array_value/kvlist_value elements). Panics if v itself
// tries to carry an array/kvlist: this example's physical encoding bounds
// AnyValue nesting to one level (see the package doc comment).
func appendLeafValue(valB *array.StructBuilder, v anyValue) {
	if v.array != nil || v.kvlist != nil {
		panic("otel-wide-events: array_value/kvlist_value elements must be scalar AnyValues (nesting is bounded to one level)")
	}
	appendScalar(valB, v)
}

// appendAnyValue writes v into valB, a top-level anyValueType struct
// slot.
func appendAnyValue(valB *array.StructBuilder, v anyValue) {
	appendScalar(valB, v)

	arrB := valB.FieldBuilder(5).(*array.ListBuilder)
	if v.array != nil {
		leafB := arrB.ValueBuilder().(*array.StructBuilder)
		arrB.Append(true)
		for _, elem := range v.array {
			leafB.Append(true)
			appendLeafValue(leafB, elem)
		}
	} else {
		arrB.AppendNull()
	}

	kvlistB := valB.FieldBuilder(6).(*array.ListBuilder)
	if v.kvlist != nil {
		kvLeafB := kvlistB.ValueBuilder().(*array.StructBuilder)
		kvlistB.Append(true)
		for _, pair := range v.kvlist {
			kvLeafB.Append(true)
			kvLeafB.FieldBuilder(0).(*array.StringBuilder).Append(pair.key)
			leafValB := kvLeafB.FieldBuilder(1).(*array.StructBuilder)
			leafValB.Append(true)
			appendLeafValue(leafValB, pair.value)
		}
	} else {
		kvlistB.AppendNull()
	}
}

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
		appendAnyValue(valB, a.value)
	}
}

// httpEventsView is the logical schema: a SQL view over wide_events that
// unnests attributes and pivots specific keys out into flat,
// semantic-convention-conformant columns (quoted, dotted names — exactly
// how OTel semconv names its attributes). MAX(CASE WHEN ...) is the
// standard unnest-then-pivot idiom and is used for the 6 scalar variants
// (each event has at most one row per attribute key, so MAX just selects
// the single non-NULL value). It cannot be used for array_value/
// kvlist_value, though: DataFusion 54.1.0's MAX (and FIRST_VALUE)
// accumulator for List-typed columns fails with "not possible to
// concatenate arrays of different data types" once the underlying scan
// spans more than one batch — reproducible independent of this example
// (a plain multi-row-group Parquet scan is enough), so a real upstream
// limitation, not something wrong with the physical encoding. ARRAY_AGG's
// accumulator does not have this problem; wrapping it with a FILTER (at
// most one match per group, same as the CASE WHEN branches) and unwrapping
// the resulting one-element outer list with array_element(..., 1) gets
// back to the same shape MAX would have produced. GROUP BY event_id
// collapses each event's attribute rows back into one output row per
// event.
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
    MAX(CASE WHEN u.key = 'error' THEN u.value.bool_value END) AS "error",
    MAX(CASE WHEN u.key = 'debug.payload_prefix' THEN u.value.bytes_value END) AS "debug.payload_prefix",
    array_element(ARRAY_AGG(u.value.array_value) FILTER (WHERE u.key = 'http.request.header.accept'), 1) AS "http.request.header.accept",
    array_element(ARRAY_AGG(u.value.kvlist_value) FILTER (WHERE u.key = 'debug.context'), 1) AS "debug.context"
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
	// SQL VALUES literals). This event also exercises the 3 variants the
	// seed data doesn't: bytes_value, array_value, and kvlist_value.
	staging := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	appendEvent(staging, 3, "trace-003", "http.request", []attr{
		strAttr("http.request.method", "POST"),
		strAttr("url.path", "/orders"),
		strAttr("client.address", "203.0.113.7"),
		intAttr("http.response.status_code", 404),
		dblAttr("http.server.request.duration", 0.031),
		boolAttr("error", true),
		bytesAttr("debug.payload_prefix", []byte{0xDE, 0xAD, 0xBE, 0xEF}),
		arrayAttr("http.request.header.accept", strVal("text/html"), strVal("application/json")),
		kvlistAttr("debug.context", kvPair("region", strVal("us-west")), kvPair("retry_count", intVal(2))),
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
		       "http.response.status_code", "http.server.request.duration", "error",
		       "debug.payload_prefix", "http.request.header.accept", "debug.context"
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
