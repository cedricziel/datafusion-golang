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
// built arrow.MapOf(...): DataFusion's function-signature matching is an
// exact type match, and a Parquet-backed table's registered schema
// carries PARQUET:field_id metadata on every nested field that a type
// built fresh in Go won't have, so a mismatched declared argument type
// fails planning with "No function matches the given name and argument
// types" even though the types are structurally identical.
//
// Only the 5 scalar variants get a UDF (array_value/kvlist_value are out
// of scope here): their return type would itself be List<Struct<...>>,
// and demonstrating the capability doesn't need every variant covered
// twice — httpEventsView already covers all 7.
func registerAttrUDFs(sess *datafusion.SessionContext, attrsType arrow.DataType) error {
	udfs := []datafusion.ScalarUDF{
		newOtelAttrStringUDF("otel_attr_string", 0, attrsType),
		newOtelAttrBoolUDF("otel_attr_bool", 1, attrsType),
		newOtelAttrIntUDF("otel_attr_int", 2, attrsType),
		newOtelAttrDoubleUDF("otel_attr_double", 3, attrsType),
		newOtelAttrBytesUDF("otel_attr_bytes", 4, attrsType),
	}
	for _, udf := range udfs {
		if err := sess.RegisterScalarUDF(udf); err != nil {
			return err
		}
	}
	return nil
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

// otelAttrUDF is the shared shape of every otel_attr_* function: (Map
// attributes, Utf8 key) -> one scalar column, extracted by matching key
// against the map and reading one field of the matched entry's AnyValue
// struct. Only extract differs per variant (which typed column of
// anyValueType's fields 0-4 it reads and appends into).
type otelAttrUDF struct {
	name       string
	attrsType  arrow.DataType
	returnType arrow.DataType
	extract    func(items *array.Struct, matched []int) arrow.Array
}

func (u otelAttrUDF) Name() string { return u.name }
func (u otelAttrUDF) ArgTypes() []arrow.DataType {
	return []arrow.DataType{u.attrsType, arrow.BinaryTypes.String}
}
func (u otelAttrUDF) ReturnType() arrow.DataType { return u.returnType }

func (u otelAttrUDF) Evaluate(args []arrow.Array) (arrow.Array, error) {
	attrs := args[0].(*array.Map)
	keys := args[1].(*array.String)
	items := attrs.Items().(*array.Struct)
	return u.extract(items, findAttrItemIndex(attrs, keys)), nil
}

func newOtelAttrStringUDF(name string, fieldIndex int, attrsType arrow.DataType) datafusion.ScalarUDF {
	return otelAttrUDF{
		name: name, attrsType: attrsType, returnType: arrow.BinaryTypes.String,
		extract: func(items *array.Struct, matched []int) arrow.Array {
			col := items.Field(fieldIndex).(*array.String)
			b := array.NewStringBuilder(memory.DefaultAllocator)
			defer b.Release()
			for _, idx := range matched {
				if idx < 0 || items.IsNull(idx) || col.IsNull(idx) {
					b.AppendNull()
					continue
				}
				b.Append(col.Value(idx))
			}
			return b.NewArray()
		},
	}
}

func newOtelAttrBoolUDF(name string, fieldIndex int, attrsType arrow.DataType) datafusion.ScalarUDF {
	return otelAttrUDF{
		name: name, attrsType: attrsType, returnType: arrow.FixedWidthTypes.Boolean,
		extract: func(items *array.Struct, matched []int) arrow.Array {
			col := items.Field(fieldIndex).(*array.Boolean)
			b := array.NewBooleanBuilder(memory.DefaultAllocator)
			defer b.Release()
			for _, idx := range matched {
				if idx < 0 || items.IsNull(idx) || col.IsNull(idx) {
					b.AppendNull()
					continue
				}
				b.Append(col.Value(idx))
			}
			return b.NewArray()
		},
	}
}

func newOtelAttrIntUDF(name string, fieldIndex int, attrsType arrow.DataType) datafusion.ScalarUDF {
	return otelAttrUDF{
		name: name, attrsType: attrsType, returnType: arrow.PrimitiveTypes.Int64,
		extract: func(items *array.Struct, matched []int) arrow.Array {
			col := items.Field(fieldIndex).(*array.Int64)
			b := array.NewInt64Builder(memory.DefaultAllocator)
			defer b.Release()
			for _, idx := range matched {
				if idx < 0 || items.IsNull(idx) || col.IsNull(idx) {
					b.AppendNull()
					continue
				}
				b.Append(col.Value(idx))
			}
			return b.NewArray()
		},
	}
}

func newOtelAttrDoubleUDF(name string, fieldIndex int, attrsType arrow.DataType) datafusion.ScalarUDF {
	return otelAttrUDF{
		name: name, attrsType: attrsType, returnType: arrow.PrimitiveTypes.Float64,
		extract: func(items *array.Struct, matched []int) arrow.Array {
			col := items.Field(fieldIndex).(*array.Float64)
			b := array.NewFloat64Builder(memory.DefaultAllocator)
			defer b.Release()
			for _, idx := range matched {
				if idx < 0 || items.IsNull(idx) || col.IsNull(idx) {
					b.AppendNull()
					continue
				}
				b.Append(col.Value(idx))
			}
			return b.NewArray()
		},
	}
}

func newOtelAttrBytesUDF(name string, fieldIndex int, attrsType arrow.DataType) datafusion.ScalarUDF {
	return otelAttrUDF{
		name: name, attrsType: attrsType, returnType: arrow.BinaryTypes.Binary,
		extract: func(items *array.Struct, matched []int) arrow.Array {
			col := items.Field(fieldIndex).(*array.Binary)
			b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
			defer b.Release()
			for _, idx := range matched {
				if idx < 0 || items.IsNull(idx) || col.IsNull(idx) {
					b.AppendNull()
					continue
				}
				b.Append(col.Value(idx))
			}
			return b.NewArray()
		},
	}
}
