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
// but one NULL for any given value. This covers all 7 AnyValue variants:
// string, bool, int, double, bytes, array, and kvlist. array_value/
// kvlist_value are self-referential (an AnyValue can contain AnyValues),
// which Arrow cannot express as a true recursive type, so this example
// bounds nesting to one level — an array_value/kvlist_value's elements
// may only be scalar AnyValues (string/bool/int/double/bytes), never
// themselves array_value/kvlist_value. Deeper OTel payloads (nested
// arrays of arrays, arbitrarily nested kvlists) would need either more
// physical nesting levels (each one a distinct, larger Arrow type, same
// technique repeated) or a JSON/serialized-bytes fallback beyond some
// fixed depth.
//
// Each event's attributes are a Map<Utf8, AnyValue> — OTel's actual
// repeated-KeyValue shape, but keyed for direct lookup rather than stored
// as a List<Struct<key,value>>. An earlier version of this example used
// the list encoding with a CREATE VIEW that UNNEST-ed it and pivoted
// specific keys out with MAX(CASE WHEN ...) per attribute — it worked,
// but doesn't scale past a handful of keys, and it was the reason this
// example hit a real DataFusion 54.1.0 bug: MAX/FIRST_VALUE's accumulator
// for List-typed columns (array_value/kvlist_value) fails once the scan
// spans more than one batch. The Map encoding sidesteps both problems:
// each event has exactly one entry per attribute key already, so a direct
// subscript (httpEventsView's `attributes['key']`) replaces UNNEST, GROUP
// BY, and the aggregate entirely — there is no List-typed aggregation
// left to trip the bug.
//
// New events land in the physical table via INSERT INTO ... SELECT from
// another registered provider (an in-memory "staging" table standing in
// for a real event source) rather than SQL VALUES literals: DataFusion
// 54.1.0 cannot CAST a VALUES-literal's inferred nested-type schema to
// the registered table's exact physical type (which carries Parquet field
// IDs the literal's inferred type doesn't), but a provider-to-provider
// INSERT INTO ... SELECT sidesteps that cast entirely — and is the more
// realistic ingestion shape besides, since event batches arrive as Arrow
// data, not hand-typed SQL.
//
// attr_udf.go demonstrates a second way to read attributes: Go ScalarUDFs
// (otel_attr_string/bool/int/double/bytes) that extract one named
// attribute's value directly, callable from SQL with no view or subscript
// access at all. This needs no engine changes — scalar UDF registration
// already exists — and works purely by walking the attributes column's
// Arrow value in Go, so it is agnostic to the SQL-level access pattern
// httpEventsView uses.
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
// as a Map<Utf8, AnyValue> — OTel's repeated-KeyValue shape, keyed for
// direct lookup (see the package doc comment for why Map rather than
// List<Struct<key,value>>).
func wideEventsSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "event_id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "event_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "attributes", Type: arrow.MapOf(arrow.BinaryTypes.String, anyValueType()), Nullable: true},
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

// appendEvent appends one event row, with its attributes map, to b.
func appendEvent(b *array.RecordBuilder, id int64, traceID, name string, attrs []attr) {
	b.Field(0).(*array.Int64Builder).Append(id)
	b.Field(1).(*array.StringBuilder).Append(traceID)
	b.Field(2).(*array.StringBuilder).Append(name)

	mapB := b.Field(3).(*array.MapBuilder)
	keyB := mapB.KeyBuilder().(*array.StringBuilder)
	valB := mapB.ItemBuilder().(*array.StructBuilder)
	mapB.Append(true)
	for _, a := range attrs {
		keyB.Append(a.key)
		valB.Append(true)
		appendAnyValue(valB, a.value)
	}
}

// httpEventsView is the logical schema: a SQL view over wide_events that
// looks up specific attribute keys and projects them out into flat,
// semantic-convention-conformant columns (quoted, dotted names — exactly
// how OTel semconv names its attributes).
//
// Each subscript (attributes['key']) has to be aliased in the inner
// subquery before its fields can be dot-accessed in the outer SELECT —
// DataFusion 54.1.0's parser rejects chaining .field directly onto a
// subscript expression ("Dot access not supported for non-string expr"),
// the same limitation UNNEST(...).field hit in this example's first
// version. Aliasing first and dot-accessing the alias works cleanly, is
// resolved once per row (not once per key against every row, the way
// UNNEST+GROUP BY was), and — because there is no aggregation here at
// all — never touches the MAX-over-List bug that motivated moving off
// the List<Struct> encoding in the first place.
const httpEventsView = `CREATE VIEW http_events AS
SELECT
    event_id,
    trace_id,
    event_name,
    m_method.string_value AS "http.request.method",
    m_path.string_value AS "url.path",
    m_addr.string_value AS "client.address",
    m_status.int_value AS "http.response.status_code",
    m_dur.double_value AS "http.server.request.duration",
    m_err.bool_value AS "error",
    m_payload.bytes_value AS "debug.payload_prefix",
    m_accept.array_value AS "http.request.header.accept",
    m_ctx.kvlist_value AS "debug.context"
FROM (
    SELECT
        event_id, trace_id, event_name,
        attributes['http.request.method'] AS m_method,
        attributes['url.path'] AS m_path,
        attributes['client.address'] AS m_addr,
        attributes['http.response.status_code'] AS m_status,
        attributes['http.server.request.duration'] AS m_dur,
        attributes['error'] AS m_err,
        attributes['debug.payload_prefix'] AS m_payload,
        attributes['http.request.header.accept'] AS m_accept,
        attributes['debug.context'] AS m_ctx
    FROM wide_events
)`

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
	if err := registerAttrUDFs(sess, events.Schema().Field(3).Type); err != nil {
		log.Fatalf("registerAttrUDFs: %v", err)
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

	// Same physical attributes column, read a second way: a Go ScalarUDF
	// per scalar variant, called directly against wide_events with no
	// view or subscript access involved (see attr_udf.go).
	fmt.Println("--- direct attribute extraction via otel_attr_* UDFs ---")
	udfReader, err := sess.SQL(`
		SELECT event_id,
		       otel_attr_string(attributes, 'http.request.method') AS method,
		       otel_attr_int(attributes, 'http.response.status_code') AS status_code,
		       otel_attr_double(attributes, 'http.server.request.duration') AS duration,
		       otel_attr_bool(attributes, 'error') AS is_error,
		       otel_attr_bytes(attributes, 'debug.payload_prefix') AS payload_prefix
		FROM wide_events ORDER BY event_id`)
	if err != nil {
		log.Fatalf("SQL (otel_attr_*): %v", err)
	}
	defer udfReader.Release()
	for udfReader.Next() {
		fmt.Println(udfReader.RecordBatch())
	}
	if err := udfReader.Err(); err != nil {
		log.Fatalf("reading result: %v", err)
	}
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
