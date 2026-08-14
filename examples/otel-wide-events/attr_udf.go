package main

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// registerAttrUDFs registers otel_attr_string/bool/int/double/bytes on
// sess: a second, engine-capability-only way to read the physical
// attributes column, alongside httpEventsView's subscript-based SQL view.
// datafusion.RegisterScalarUDF is an existing capability — this needs no
// core library changes, and works identically regardless of whether
// attributes is Map or List<Struct> (a ScalarUDF's Evaluate walks the
// argument's Arrow value directly; it doesn't care how the query got it
// there).
//
// attrsType must be the wide_events table's actual, registered attributes
// field type (e.g. events.Schema().Field(3).Type), not an independently
// built arrow.MapOf(...) — see datafusion.ScalarUDF.ArgTypes's doc for
// why a declared argument type has to come from the table's own schema
// when it's a nested (List/Struct/Map) type read from a Parquet- or
// Iceberg-backed table.
//
// Only the 5 scalar variants get a UDF (array_value/kvlist_value are out
// of scope here): their return type would itself be List<Struct<...>>,
// and demonstrating the capability doesn't need every variant covered
// twice — httpEventsView already covers all 7.
func registerAttrUDFs(sess *datafusion.SessionContext, attrsType arrow.DataType) error {
	udfs := []func() (datafusion.ScalarUDF, error){
		func() (datafusion.ScalarUDF, error) {
			return newOtelAttrUDF("otel_attr_string", 0, attrsType, arrow.BinaryTypes.String,
				func(items *array.Struct, i int) *array.String { return items.Field(i).(*array.String) },
				func() *array.StringBuilder { return array.NewStringBuilder(memory.DefaultAllocator) })
		},
		func() (datafusion.ScalarUDF, error) {
			return newOtelAttrUDF("otel_attr_bool", 1, attrsType, arrow.FixedWidthTypes.Boolean,
				func(items *array.Struct, i int) *array.Boolean { return items.Field(i).(*array.Boolean) },
				func() *array.BooleanBuilder { return array.NewBooleanBuilder(memory.DefaultAllocator) })
		},
		func() (datafusion.ScalarUDF, error) {
			return newOtelAttrUDF("otel_attr_int", 2, attrsType, arrow.PrimitiveTypes.Int64,
				func(items *array.Struct, i int) *array.Int64 { return items.Field(i).(*array.Int64) },
				func() *array.Int64Builder { return array.NewInt64Builder(memory.DefaultAllocator) })
		},
		func() (datafusion.ScalarUDF, error) {
			return newOtelAttrUDF("otel_attr_double", 3, attrsType, arrow.PrimitiveTypes.Float64,
				func(items *array.Struct, i int) *array.Float64 { return items.Field(i).(*array.Float64) },
				func() *array.Float64Builder { return array.NewFloat64Builder(memory.DefaultAllocator) })
		},
		func() (datafusion.ScalarUDF, error) {
			return newOtelAttrUDF("otel_attr_bytes", 4, attrsType, arrow.BinaryTypes.Binary,
				func(items *array.Struct, i int) *array.Binary { return items.Field(i).(*array.Binary) },
				func() *array.BinaryBuilder {
					return array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
				})
		},
	}
	for _, build := range udfs {
		udf, err := build()
		if err != nil {
			return err
		}
		if err := sess.RegisterScalarUDF(udf); err != nil {
			return err
		}
	}
	return nil
}

// valueCol and valueBuilder are the common shape every anyValueType
// scalar column (string_value, bool_value, ...) and its matching Arrow
// builder already have — enough to write newOtelAttrUDF once instead of
// once per variant.
type valueCol[V any] interface {
	IsNull(i int) bool
	Value(i int) V
}

type valueBuilder[V any] interface {
	Append(V)
	AppendNull()
	NewArray() arrow.Array
}

// newOtelAttrUDF builds one otel_attr_* function via the core
// datafusion.NewScalarUDF constructor (datafusion/udf.go) — this needs no
// custom ScalarUDF implementation, matching how examples/extend builds
// its scalar UDFs. col selects which typed field of the matched
// attribute's AnyValue struct to read (see anyValueType's field layout);
// newBuilder constructs a fresh builder of the matching type per call.
func newOtelAttrUDF[V any, C valueCol[V], B valueBuilder[V]](
	name string, fieldIndex int, attrsType, returnType arrow.DataType,
	col func(items *array.Struct, fieldIndex int) C, newBuilder func() B,
) (datafusion.ScalarUDF, error) {
	return datafusion.NewScalarUDF(name, []arrow.DataType{attrsType, arrow.BinaryTypes.String}, returnType,
		func(args []arrow.Array) (arrow.Array, error) {
			attrs := args[0].(*array.Map)
			keys := args[1].(*array.String)
			items := attrs.Items().(*array.Struct)
			c := col(items, fieldIndex)

			b := newBuilder()
			for _, idx := range findAttrItemIndex(attrs, keys) {
				if idx < 0 || items.IsNull(idx) || c.IsNull(idx) {
					b.AppendNull()
					continue
				}
				b.Append(c.Value(idx))
			}
			return b.NewArray(), nil
		})
}

// findAttrItemIndex returns, for each row i, the index into attrs.Items()
// of the entry whose key matches keys[i] — or -1 if attrs is NULL, keys[i]
// is NULL, or no entry has that key. Shared by every otel_attr_* UDF.
func findAttrItemIndex(attrs *array.Map, keys *array.String) []int {
	keyArr := attrs.Keys().(*array.String)
	out := make([]int, attrs.Len())
	for i := 0; i < attrs.Len(); i++ {
		out[i] = -1
		if attrs.IsNull(i) || keys.IsNull(i) {
			continue
		}
		start, end := attrs.ValueOffsets(i)
		want := keys.Value(i)
		for j := start; j < end; j++ {
			if keyArr.Value(int(j)) == want {
				out[i] = int(j)
				break
			}
		}
	}
	return out
}
