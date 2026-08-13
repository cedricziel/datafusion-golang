package parquet

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

func TestBloomProbe_Allowlist(t *testing.T) {
	tsUS := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	tsNS := &arrow.TimestampType{Unit: arrow.Nanosecond}

	cases := []struct {
		name string
		lit  datafusion.Literal
		dt   arrow.DataType
		ok   bool
	}{
		{"utf8 on string", utf8("x"), arrow.BinaryTypes.String, true},
		{"utf8 on large string", utf8("x"), arrow.BinaryTypes.LargeString, true},
		{"binary on binary", datafusion.Literal{Type: datafusion.LiteralBinary, Value: []byte{1}}, arrow.BinaryTypes.Binary, true},
		{"binary on fixed-size binary", datafusion.Literal{Type: datafusion.LiteralBinary, Value: []byte{1, 2}}, &arrow.FixedSizeBinaryType{ByteWidth: 2}, true},
		{"int8 on int8", datafusion.Literal{Type: datafusion.LiteralInt8, Value: int8(-5)}, arrow.PrimitiveTypes.Int8, true},
		{"int32 on int32", datafusion.Literal{Type: datafusion.LiteralInt32, Value: int32(7)}, arrow.PrimitiveTypes.Int32, true},
		{"int64 on int64", i64(7), arrow.PrimitiveTypes.Int64, true},
		{"int64 too large for int32 column", i64(1 << 40), arrow.PrimitiveTypes.Int32, false},
		{"uint16 on uint16", datafusion.Literal{Type: datafusion.LiteralUint16, Value: uint16(9)}, arrow.PrimitiveTypes.Uint16, true},
		{"uint32 on uint32", datafusion.Literal{Type: datafusion.LiteralUint32, Value: uint32(1 << 31)}, arrow.PrimitiveTypes.Uint32, true},
		{"uint64 on uint64", u64(1 << 63), arrow.PrimitiveTypes.Uint64, true},
		{"uint64 too large for uint32 column", u64(1 << 40), arrow.PrimitiveTypes.Uint32, false},
		{"date32 on date32", datafusion.Literal{Type: datafusion.LiteralDate32, Value: int32(19000)}, arrow.FixedWidthTypes.Date32, true},
		{"timestamp unit match", ts(datafusion.TimeUnitMicrosecond, 1), tsUS, true},
		{"timestamp unit mismatch", ts(datafusion.TimeUnitSecond, 1), tsUS, false},
		{"timestamp ns match", ts(datafusion.TimeUnitNanosecond, 1), tsNS, true},
		// Floats: 0.0 == -0.0 in SQL but the bit patterns hash
		// differently, so float probes are excluded outright.
		{"float32 excluded", datafusion.Literal{Type: datafusion.LiteralFloat32, Value: float32(1)}, arrow.PrimitiveTypes.Float32, false},
		{"float64 excluded", f64(1), arrow.PrimitiveTypes.Float64, false},
		// Bools: excluded (a two-value domain gains nothing).
		{"bool excluded", datafusion.Literal{Type: datafusion.LiteralBool, Value: true}, arrow.FixedWidthTypes.Boolean, false},
		// Type-family mismatches: no probe.
		{"utf8 on int64", utf8("7"), arrow.PrimitiveTypes.Int64, false},
		{"int64 on string", i64(7), arrow.BinaryTypes.String, false},
		{"signed literal on unsigned column", i64(7), arrow.PrimitiveTypes.Uint64, false},
		{"unsigned literal on signed column", u64(7), arrow.PrimitiveTypes.Int64, false},
		{"wrong Go value inside literal", datafusion.Literal{Type: datafusion.LiteralInt64, Value: "7"}, arrow.PrimitiveTypes.Int64, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := bloomProbe(tc.lit, tc.dt)
			if ok != tc.ok {
				t.Fatalf("bloomProbe(%v, %s): got ok=%v, want %v", tc.lit, tc.dt, ok, tc.ok)
			}
		})
	}
}
