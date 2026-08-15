package main

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// httpAttrWant pairs a wanted attribute key with which scalar field of
// its matched AnyValue struct to read: index into anyValueType's 5
// scalar fields (string_value=0, bool_value=1, int_value=2,
// double_value=3, bytes_value=4). httpAttrsResultType's fields are laid
// out in that same order — each valueField index doubles as the result
// struct's field index — so the two must be kept in sync (this example
// has exactly one wanted attribute per AnyValue variant; a design with
// two wanted attributes of the same variant would need a separate
// result-field index).
type httpAttrWant struct {
	key        string
	valueField int
}

func httpAttrWants() []httpAttrWant {
	return []httpAttrWant{
		{key: "http.request.method", valueField: 0},          // -> method (string_value)
		{key: "error", valueField: 1},                        // -> is_error (bool_value)
		{key: "http.response.status_code", valueField: 2},    // -> status_code (int_value)
		{key: "http.server.request.duration", valueField: 3}, // -> duration (double_value)
		{key: "debug.payload_prefix", valueField: 4},         // -> payload_prefix (bytes_value)
	}
}

// httpAttrsResultType is otel_http_attrs' return type: one struct field
// per httpAttrWants() entry, field-index-aligned with anyValueType's
// scalar layout (see httpAttrWant's doc). array_value/kvlist_value are
// left to httpEventsView (see main.go's doc comment).
func httpAttrsResultType() *arrow.StructType {
	return arrow.StructOf(
		arrow.Field{Name: "method", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "is_error", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "status_code", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "duration", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "payload_prefix", Type: arrow.BinaryTypes.Binary, Nullable: true},
	)
}

// registerAttrUDFs registers otel_http_attrs on sess: a second,
// engine-capability-only way to read the physical attributes column,
// alongside httpEventsView's subscript-based SQL view.
// datafusion.RegisterScalarUDF is an existing capability — this needs no
// core library changes, and works identically regardless of whether
// attributes is Map or List<Struct> (a ScalarUDF's Evaluate walks the
// argument's Arrow value directly).
//
// One struct-returning UDF, not one UDF per attribute: an earlier version
// registered otel_attr_string/bool/int/double/bytes, one call per
// attribute, and calling all 5 against the same rows re-scanned each
// row's attribute list from scratch every time — O(attributes per row ×
// attributes wanted) per row. otel_http_attrs scans each row's attribute
// list exactly once: every entry's key is checked against a
// map[string]int built once outside the row loop (not rebuilt per row),
// and a match writes straight to its output field — O(attributes per
// row) total, independent of how many attributes are wanted. Calling it
// from SQL needs the same alias-then-dot-access step httpEventsView's
// subscripts do (`SELECT h.method FROM (SELECT otel_http_attrs(attributes)
// AS h FROM wide_events)`), for the same reason: DataFusion 54.1.0
// rejects chaining .field directly onto a function-call expression.
//
// attrsType must be the wide_events table's actual, registered attributes
// field type — see datafusion.ScalarUDF.ArgTypes's doc for why.
func registerAttrUDFs(sess *datafusion.SessionContext, attrsType arrow.DataType) error {
	udf, err := newOtelHTTPAttrsUDF(attrsType)
	if err != nil {
		return err
	}
	return sess.RegisterScalarUDF(udf)
}

func newOtelHTTPAttrsUDF(attrsType arrow.DataType) (datafusion.ScalarUDF, error) {
	wants := httpAttrWants()
	wantIndex := make(map[string]int, len(wants))
	for i, w := range wants {
		wantIndex[w.key] = i
	}
	resultType := httpAttrsResultType()

	return datafusion.NewScalarUDF("otel_http_attrs", []arrow.DataType{attrsType}, resultType,
		func(args []arrow.Array) (arrow.Array, error) {
			attrs := args[0].(*array.Map)
			keyArr := attrs.Keys().(*array.String)
			items := attrs.Items().(*array.Struct)

			strCol := items.Field(0).(*array.String)
			boolCol := items.Field(1).(*array.Boolean)
			intCol := items.Field(2).(*array.Int64)
			dblCol := items.Field(3).(*array.Float64)
			bytesCol := items.Field(4).(*array.Binary)

			b := array.NewStructBuilder(memory.DefaultAllocator, resultType)
			defer b.Release()
			strB := b.FieldBuilder(0).(*array.StringBuilder)
			boolB := b.FieldBuilder(1).(*array.BooleanBuilder)
			intB := b.FieldBuilder(2).(*array.Int64Builder)
			dblB := b.FieldBuilder(3).(*array.Float64Builder)
			bytesB := b.FieldBuilder(4).(*array.BinaryBuilder)

			matched := make([]int, len(wants))
			for i := 0; i < attrs.Len(); i++ {
				for w := range matched {
					matched[w] = -1
				}
				if !attrs.IsNull(i) {
					start, end := attrs.ValueOffsets(i)
					for j := start; j < end; j++ {
						if wi, ok := wantIndex[keyArr.Value(int(j))]; ok {
							matched[wi] = int(j)
						}
					}
				}

				b.Append(true)
				for wi, want := range wants {
					idx := matched[wi]
					switch want.valueField {
					case 0:
						appendMatched(strB, strCol, idx)
					case 1:
						appendMatched(boolB, boolCol, idx)
					case 2:
						appendMatched(intB, intCol, idx)
					case 3:
						appendMatched(dblB, dblCol, idx)
					case 4:
						appendMatched(bytesB, bytesCol, idx)
					}
				}
			}
			return b.NewArray(), nil
		})
}

// valueCol and valueBuilder are the common shape every anyValueType
// scalar column (string_value, bool_value, ...) and its matching Arrow
// builder already have.
type valueCol[V any] interface {
	IsNull(i int) bool
	Value(i int) V
}

type valueBuilder[V any] interface {
	Append(V)
	AppendNull()
}

// appendMatched appends col's value at idx to b, or NULL if idx is -1
// (no matching attribute key in this row) or col itself is NULL there.
func appendMatched[V any](b valueBuilder[V], col valueCol[V], idx int) {
	if idx < 0 || col.IsNull(idx) {
		b.AppendNull()
		return
	}
	b.Append(col.Value(idx))
}
