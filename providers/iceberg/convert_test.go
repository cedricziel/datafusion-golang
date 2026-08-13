package iceberg

import (
	"math"
	"testing"

	ib "github.com/apache/iceberg-go"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

func col(name string) datafusion.Column { return datafusion.Column{Name: name, Index: 0} }

func i64(v int64) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralInt64, Value: v}
}

func i32(v int32) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralInt32, Value: v}
}

func utf8(v string) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralUtf8, Value: v}
}

func tsLit(unit datafusion.TimeUnit, v int64) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralTimestamp, Value: v, Unit: unit}
}

func cmp(name string, op datafusion.CompareOp, lit datafusion.Literal) datafusion.Expr {
	return datafusion.Compare{Column: col(name), Op: op, Literal: lit}
}

func mustConvert(t *testing.T, e datafusion.Expr) ib.BooleanExpression {
	t.Helper()
	got, ok := convertExpr(e)
	if !ok {
		t.Fatalf("convertExpr(%#v) failed", e)
	}
	return got
}

func assertEq(t *testing.T, got, want ib.BooleanExpression) {
	t.Helper()
	if !got.Equals(want) {
		t.Fatalf("converted expression mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestConvert_CompareOperators(t *testing.T) {
	ref := ib.Reference("id")
	cases := []struct {
		op   datafusion.CompareOp
		want ib.BooleanExpression
	}{
		{datafusion.CompareEq, ib.EqualTo(ref, int64(5))},
		{datafusion.CompareNeq, ib.NotEqualTo(ref, int64(5))},
		{datafusion.CompareLt, ib.LessThan(ref, int64(5))},
		{datafusion.CompareLtEq, ib.LessThanEqual(ref, int64(5))},
		{datafusion.CompareGt, ib.GreaterThan(ref, int64(5))},
		{datafusion.CompareGtEq, ib.GreaterThanEqual(ref, int64(5))},
	}
	for _, tc := range cases {
		assertEq(t, mustConvert(t, cmp("id", tc.op, i64(5))), tc.want)
	}
	if _, ok := convertExpr(cmp("id", datafusion.CompareOp("like"), i64(5))); ok {
		t.Fatal("unknown operator must not convert")
	}
}

func TestConvert_LiteralWidenings(t *testing.T) {
	ref := ib.Reference("c")
	cases := []struct {
		name string
		lit  datafusion.Literal
		want ib.BooleanExpression
	}{
		{"bool", datafusion.Literal{Type: datafusion.LiteralBool, Value: true}, ib.EqualTo(ref, true)},
		{"int8 to int32", datafusion.Literal{Type: datafusion.LiteralInt8, Value: int8(-3)}, ib.EqualTo(ref, int32(-3))},
		{"int16 to int32", datafusion.Literal{Type: datafusion.LiteralInt16, Value: int16(-300)}, ib.EqualTo(ref, int32(-300))},
		{"int32", i32(7), ib.EqualTo(ref, int32(7))},
		{"int64", i64(7), ib.EqualTo(ref, int64(7))},
		{"uint8 to int64", datafusion.Literal{Type: datafusion.LiteralUint8, Value: uint8(200)}, ib.EqualTo(ref, int64(200))},
		{"uint16 to int64", datafusion.Literal{Type: datafusion.LiteralUint16, Value: uint16(60000)}, ib.EqualTo(ref, int64(60000))},
		{"uint32 to int64", datafusion.Literal{Type: datafusion.LiteralUint32, Value: uint32(math.MaxUint32)}, ib.EqualTo(ref, int64(math.MaxUint32))},
		{"uint64 in range", datafusion.Literal{Type: datafusion.LiteralUint64, Value: uint64(12)}, ib.EqualTo(ref, int64(12))},
		{"float32", datafusion.Literal{Type: datafusion.LiteralFloat32, Value: float32(1.5)}, ib.EqualTo(ref, float32(1.5))},
		{"float64", datafusion.Literal{Type: datafusion.LiteralFloat64, Value: 2.5}, ib.EqualTo(ref, 2.5)},
		{"utf8", utf8("x"), ib.EqualTo(ref, "x")},
		{"binary", datafusion.Literal{Type: datafusion.LiteralBinary, Value: []byte{1, 2}}, ib.EqualTo(ref, []byte{1, 2})},
		{"date32", datafusion.Literal{Type: datafusion.LiteralDate32, Value: int32(19000)}, ib.EqualTo(ref, ib.Date(19000))},
		{"timestamp s to us", tsLit(datafusion.TimeUnitSecond, 3), ib.EqualTo(ref, ib.Timestamp(3_000_000))},
		{"timestamp ms to us", tsLit(datafusion.TimeUnitMillisecond, 3), ib.EqualTo(ref, ib.Timestamp(3_000))},
		{"timestamp us", tsLit(datafusion.TimeUnitMicrosecond, 3), ib.EqualTo(ref, ib.Timestamp(3))},
		{"timestamp ns", tsLit(datafusion.TimeUnitNanosecond, 3), ib.EqualTo(ref, ib.TimestampNano(3))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertEq(t, mustConvert(t, cmp("c", datafusion.CompareEq, tc.lit)), tc.want)
		})
	}
}

func TestConvert_UnrepresentableLiterals(t *testing.T) {
	bad := []datafusion.Literal{
		{Type: datafusion.LiteralUint64, Value: uint64(math.MaxInt64) + 1},
		{Type: datafusion.LiteralDate64, Value: int64(1)},
		{Type: datafusion.LiteralTimestamp, Value: int64(math.MaxInt64/1_000_000 + 1), Unit: datafusion.TimeUnitSecond},
		{Type: datafusion.LiteralTimestamp, Value: int64(math.MinInt64/1_000 - 1), Unit: datafusion.TimeUnitMillisecond},
		{Type: datafusion.LiteralInt64, Value: "wrong go type"},
		{Type: datafusion.LiteralType("decimal"), Value: "1.5"},
	}
	for _, lit := range bad {
		if _, ok := convertExpr(cmp("c", datafusion.CompareEq, lit)); ok {
			t.Fatalf("literal %#v must not convert", lit)
		}
	}
	// Boundary cases that must survive.
	assertEq(t,
		mustConvert(t, cmp("c", datafusion.CompareEq, tsLit(datafusion.TimeUnitSecond, math.MaxInt64/1_000_000))),
		ib.EqualTo(ib.Reference("c"), ib.Timestamp(math.MaxInt64/1_000_000*1_000_000)))
	assertEq(t,
		mustConvert(t, cmp("c", datafusion.CompareEq, datafusion.Literal{Type: datafusion.LiteralUint64, Value: uint64(math.MaxInt64)})),
		ib.EqualTo(ib.Reference("c"), int64(math.MaxInt64)))
}

func TestConvert_IsNull(t *testing.T) {
	assertEq(t, mustConvert(t, datafusion.IsNull{Column: col("c")}), ib.IsNull(ib.Reference("c")))
	assertEq(t, mustConvert(t, datafusion.IsNull{Column: col("c"), Negated: true}), ib.NotNull(ib.Reference("c")))
}

func TestConvert_Between(t *testing.T) {
	ref := ib.Reference("c")
	assertEq(t,
		mustConvert(t, datafusion.Between{Column: col("c"), Low: i64(1), High: i64(9)}),
		ib.NewAnd(ib.GreaterThanEqual(ref, int64(1)), ib.LessThanEqual(ref, int64(9))))
	assertEq(t,
		mustConvert(t, datafusion.Between{Column: col("c"), Negated: true, Low: i64(1), High: i64(9)}),
		ib.NewOr(ib.LessThan(ref, int64(1)), ib.GreaterThan(ref, int64(9))))
	// One unconvertible bound poisons the whole node.
	if _, ok := convertExpr(datafusion.Between{Column: col("c"), Low: i64(1), High: datafusion.Literal{Type: datafusion.LiteralUint64, Value: uint64(math.MaxUint64)}}); ok {
		t.Fatal("Between with an unconvertible bound must not convert")
	}
}

func TestConvert_InList(t *testing.T) {
	ref := ib.Reference("c")
	assertEq(t,
		mustConvert(t, datafusion.InList{Column: col("c"), List: []datafusion.Literal{i64(1), i64(2)}}),
		ib.IsIn(ref, int64(1), int64(2)))
	assertEq(t,
		mustConvert(t, datafusion.InList{Column: col("c"), Negated: true, List: []datafusion.Literal{i64(1), i64(2)}}),
		ib.NotIn(ref, int64(1), int64(2)))
	// Mixed element types after conversion: drop.
	if _, ok := convertExpr(datafusion.InList{Column: col("c"), List: []datafusion.Literal{i64(1), utf8("x")}}); ok {
		t.Fatal("mixed-type IN list must not convert")
	}
	// One unconvertible element: drop.
	if _, ok := convertExpr(datafusion.InList{Column: col("c"), List: []datafusion.Literal{i64(1), {Type: datafusion.LiteralUint64, Value: uint64(math.MaxUint64)}}}); ok {
		t.Fatal("IN list with an unconvertible element must not convert")
	}
	// Empty list: engine never pushes it; drop rather than guess.
	if _, ok := convertExpr(datafusion.InList{Column: col("c")}); ok {
		t.Fatal("empty IN list must not convert")
	}
}

func TestConvert_Compound(t *testing.T) {
	a := cmp("a", datafusion.CompareEq, i64(1))
	b := cmp("b", datafusion.CompareGt, i64(2))
	ibA := ib.EqualTo(ib.Reference("a"), int64(1))
	ibB := ib.GreaterThan(ib.Reference("b"), int64(2))

	assertEq(t, mustConvert(t, datafusion.And{Left: a, Right: b}), ib.NewAnd(ibA, ibB))
	assertEq(t, mustConvert(t, datafusion.Or{Left: a, Right: b}), ib.NewOr(ibA, ibB))
	assertEq(t, mustConvert(t, datafusion.Not{Expr: a}), ib.NewNot(ibA))

	// All-or-nothing: a failure anywhere inside poisons the whole tree —
	// especially one side of an OR, which must never be pushed alone.
	bad := cmp("c", datafusion.CompareEq, datafusion.Literal{Type: datafusion.LiteralUint64, Value: uint64(math.MaxUint64)})
	if _, ok := convertExpr(datafusion.Or{Left: a, Right: bad}); ok {
		t.Fatal("OR with an unconvertible side must not convert at all")
	}
	if _, ok := convertExpr(datafusion.And{Left: a, Right: bad}); ok {
		t.Fatal("AND with an unconvertible side must not convert (per-conjunct all-or-nothing)")
	}
	if _, ok := convertExpr(datafusion.Not{Expr: bad}); ok {
		t.Fatal("NOT of an unconvertible expression must not convert")
	}
}

func TestConvertFilters_DropsPerConjunct(t *testing.T) {
	sc := ib.NewSchema(0,
		ib.NestedField{ID: 1, Name: "id", Type: ib.PrimitiveTypes.Int64},
		ib.NestedField{ID: 2, Name: "name", Type: ib.PrimitiveTypes.String},
	)

	good := cmp("id", datafusion.CompareGt, i64(5))
	badLiteral := cmp("id", datafusion.CompareEq, datafusion.Literal{Type: datafusion.LiteralUint64, Value: uint64(math.MaxUint64)})
	badBind := cmp("no_such_column", datafusion.CompareEq, i64(1))
	badBindType := cmp("name", datafusion.CompareEq, datafusion.Literal{Type: datafusion.LiteralBool, Value: true})

	// Only the convertible, bindable conjunct survives.
	got := convertFilters([]datafusion.Expr{good, badLiteral, badBind, badBindType}, sc)
	if got == nil {
		t.Fatal("the good conjunct must survive")
	}
	assertEq(t, got, ib.GreaterThan(ib.Reference("id"), int64(5)))

	// Nothing convertible: no row filter at all, never an error.
	if got := convertFilters([]datafusion.Expr{badLiteral, badBind}, sc); got != nil {
		t.Fatalf("expected no filter, got %s", got)
	}

	// Two good conjuncts AND together.
	good2 := cmp("name", datafusion.CompareEq, utf8("x"))
	got = convertFilters([]datafusion.Expr{good, good2}, sc)
	want := ib.NewAnd(
		ib.GreaterThan(ib.Reference("id"), int64(5)),
		ib.EqualTo(ib.Reference("name"), "x"),
	)
	assertEq(t, got, want)
}
