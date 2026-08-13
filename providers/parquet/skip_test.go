package parquet

import (
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// Helpers building hand-constructed stats (no Parquet files involved).

func intStats(min, max int64) colStats {
	return colStats{
		dtype:     arrow.PrimitiveTypes.Int64,
		hasMinMax: true,
		min:       intVal(min),
		max:       intVal(max),
	}
}

func intStatsN(min, max, nullCount int64) colStats {
	cs := intStats(min, max)
	cs.hasNullCount = true
	cs.nullCount = nullCount
	return cs
}

func uintStats(min, max uint64) colStats {
	return colStats{
		dtype:     arrow.PrimitiveTypes.Uint64,
		hasMinMax: true,
		min:       uintVal(min),
		max:       uintVal(max),
	}
}

func floatStats(min, max float64) colStats {
	return colStats{
		dtype:     arrow.PrimitiveTypes.Float64,
		hasMinMax: true,
		min:       floatVal(min),
		max:       floatVal(max),
	}
}

func strStats(min, max string) colStats {
	return colStats{
		dtype:     arrow.BinaryTypes.String,
		hasMinMax: true,
		min:       bytesVal([]byte(min)),
		max:       bytesVal([]byte(max)),
	}
}

func tsStats(unit arrow.TimeUnit, min, max int64) colStats {
	return colStats{
		dtype:     &arrow.TimestampType{Unit: unit, TimeZone: "UTC"},
		hasMinMax: true,
		min:       intVal(min),
		max:       intVal(max),
	}
}

func stats1(numRows int64, cs colStats) rowGroupStats {
	return rowGroupStats{numRows: numRows, cols: map[int]colStats{0: cs}}
}

func col0() datafusion.Column { return datafusion.Column{Name: "c", Index: 0} }

func i64(v int64) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralInt64, Value: v}
}

func u64(v uint64) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralUint64, Value: v}
}

func f64(v float64) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralFloat64, Value: v}
}

func utf8(v string) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralUtf8, Value: v}
}

func ts(unit datafusion.TimeUnit, v int64) datafusion.Literal {
	return datafusion.Literal{Type: datafusion.LiteralTimestamp, Value: v, Unit: unit}
}

func cmpE(op datafusion.CompareOp, lit datafusion.Literal) datafusion.Expr {
	return datafusion.Compare{Column: col0(), Op: op, Literal: lit}
}

// TestCanSkip_CompareBoundaries: every operator against stats [10, 20],
// at literals below/at-min/contained/at-max/above.
func TestCanSkip_CompareBoundaries(t *testing.T) {
	st := stats1(100, intStats(10, 20))
	cases := []struct {
		name string
		op   datafusion.CompareOp
		lit  int64
		skip bool
	}{
		// eq: skip iff lit < min || lit > max
		{"eq below", datafusion.CompareEq, 5, true},
		{"eq at min", datafusion.CompareEq, 10, false},
		{"eq contained", datafusion.CompareEq, 15, false},
		{"eq at max", datafusion.CompareEq, 20, false},
		{"eq above", datafusion.CompareEq, 25, true},
		// lt: skip iff min >= lit
		{"lt below", datafusion.CompareLt, 5, true},
		{"lt at min", datafusion.CompareLt, 10, true},
		{"lt contained", datafusion.CompareLt, 15, false},
		{"lt at max", datafusion.CompareLt, 20, false},
		{"lt above", datafusion.CompareLt, 25, false},
		// lteq: skip iff min > lit
		{"lteq below", datafusion.CompareLtEq, 5, true},
		{"lteq at min", datafusion.CompareLtEq, 10, false},
		{"lteq contained", datafusion.CompareLtEq, 15, false},
		{"lteq at max", datafusion.CompareLtEq, 20, false},
		{"lteq above", datafusion.CompareLtEq, 25, false},
		// gt: skip iff max <= lit
		{"gt below", datafusion.CompareGt, 5, false},
		{"gt at min", datafusion.CompareGt, 10, false},
		{"gt contained", datafusion.CompareGt, 15, false},
		{"gt at max", datafusion.CompareGt, 20, true},
		{"gt above", datafusion.CompareGt, 25, true},
		// gteq: skip iff max < lit
		{"gteq below", datafusion.CompareGtEq, 5, false},
		{"gteq at min", datafusion.CompareGtEq, 10, false},
		{"gteq contained", datafusion.CompareGtEq, 15, false},
		{"gteq at max", datafusion.CompareGtEq, 20, false},
		{"gteq above", datafusion.CompareGtEq, 25, true},
		// neq: skip only when min == max == lit; range stats never prove it
		{"neq below", datafusion.CompareNeq, 5, false},
		{"neq at min", datafusion.CompareNeq, 10, false},
		{"neq contained", datafusion.CompareNeq, 15, false},
		{"neq at max", datafusion.CompareNeq, 20, false},
		{"neq above", datafusion.CompareNeq, 25, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canSkip(cmpE(tc.op, i64(tc.lit)), st, nil); got != tc.skip {
				t.Fatalf("canSkip(%s %d) over [10,20]: got %v, want %v", tc.op, tc.lit, got, tc.skip)
			}
		})
	}
}

func TestCanSkip_MinEqualsMax(t *testing.T) {
	st := stats1(100, intStats(7, 7))
	if !canSkip(cmpE(datafusion.CompareNeq, i64(7)), st, nil) {
		t.Fatal("neq 7 over [7,7]: every non-null row equals 7, must skip")
	}
	if canSkip(cmpE(datafusion.CompareNeq, i64(8)), st, nil) {
		t.Fatal("neq 8 over [7,7]: rows satisfy, must keep")
	}
	if canSkip(cmpE(datafusion.CompareEq, i64(7)), st, nil) {
		t.Fatal("eq 7 over [7,7]: must keep")
	}
}

func TestCanSkip_MissingStats(t *testing.T) {
	// No entry at all for the column.
	stNoCol := rowGroupStats{numRows: 100, cols: map[int]colStats{}}
	// Entry present but no min/max and no null count.
	stNoMinMax := stats1(100, colStats{dtype: arrow.PrimitiveTypes.Int64})

	exprs := []datafusion.Expr{
		cmpE(datafusion.CompareEq, i64(5)),
		cmpE(datafusion.CompareLt, i64(5)),
		cmpE(datafusion.CompareNeq, i64(5)),
		datafusion.IsNull{Column: col0()},
		datafusion.IsNull{Column: col0(), Negated: true},
		datafusion.Between{Column: col0(), Low: i64(1), High: i64(2)},
		datafusion.InList{Column: col0(), List: []datafusion.Literal{i64(1)}},
		datafusion.InList{Column: col0(), Negated: true, List: []datafusion.Literal{i64(1)}},
	}
	for _, e := range exprs {
		if canSkip(e, stNoCol, nil) {
			t.Fatalf("no stats entry: %T must keep", e)
		}
		if canSkip(e, stNoMinMax, nil) {
			t.Fatalf("entry without min/max or null count: %T must keep", e)
		}
	}
}

func TestCanSkip_AllNull(t *testing.T) {
	st := stats1(100, colStats{
		dtype:        arrow.PrimitiveTypes.Int64,
		hasNullCount: true,
		nullCount:    100,
	})
	// No non-null row exists; comparisons and IS NOT NULL can never be satisfied.
	skips := []datafusion.Expr{
		cmpE(datafusion.CompareEq, i64(5)),
		cmpE(datafusion.CompareNeq, i64(5)),
		cmpE(datafusion.CompareLt, i64(5)),
		cmpE(datafusion.CompareGtEq, i64(5)),
		datafusion.Between{Column: col0(), Low: i64(1), High: i64(2)},
		datafusion.Between{Column: col0(), Negated: true, Low: i64(1), High: i64(2)},
		datafusion.InList{Column: col0(), List: []datafusion.Literal{i64(1)}},
		datafusion.InList{Column: col0(), Negated: true, List: []datafusion.Literal{i64(1)}},
		datafusion.IsNull{Column: col0(), Negated: true},
	}
	for _, e := range skips {
		if !canSkip(e, st, nil) {
			t.Fatalf("all-null column: %#v must skip", e)
		}
	}
	if canSkip(datafusion.IsNull{Column: col0()}, st, nil) {
		t.Fatal("all-null column: IS NULL is satisfied by every row, must keep")
	}
}

func TestCanSkip_NullCounts(t *testing.T) {
	// nullCount == 0: IS NULL can never be satisfied.
	noNulls := stats1(100, intStatsN(10, 20, 0))
	if !canSkip(datafusion.IsNull{Column: col0()}, noNulls, nil) {
		t.Fatal("nullCount=0: IS NULL must skip")
	}
	if canSkip(datafusion.IsNull{Column: col0(), Negated: true}, noNulls, nil) {
		t.Fatal("nullCount=0: IS NOT NULL must keep")
	}

	// Some nulls: neither decision provable.
	someNulls := stats1(100, intStatsN(10, 20, 5))
	if canSkip(datafusion.IsNull{Column: col0()}, someNulls, nil) {
		t.Fatal("some nulls: IS NULL must keep")
	}
	if canSkip(datafusion.IsNull{Column: col0(), Negated: true}, someNulls, nil) {
		t.Fatal("some nulls: IS NOT NULL must keep")
	}

	// Missing null count: IS NULL / IS NOT NULL undecidable, and the
	// all-null shortcut for comparisons must not fire.
	noNC := stats1(100, intStats(10, 20))
	if canSkip(datafusion.IsNull{Column: col0()}, noNC, nil) {
		t.Fatal("missing null count: IS NULL must keep")
	}
	if canSkip(datafusion.IsNull{Column: col0(), Negated: true}, noNC, nil) {
		t.Fatal("missing null count: IS NOT NULL must keep")
	}
}

func TestCanSkip_Between(t *testing.T) {
	st := stats1(100, intStats(10, 20))
	between := func(neg bool, lo, hi int64) datafusion.Expr {
		return datafusion.Between{Column: col0(), Negated: neg, Low: i64(lo), High: i64(hi)}
	}
	cases := []struct {
		name string
		e    datafusion.Expr
		skip bool
	}{
		{"disjoint below", between(false, 1, 5), true},
		{"touching min", between(false, 5, 10), false},
		{"contained", between(false, 12, 18), false},
		{"touching max", between(false, 20, 25), false},
		{"disjoint above", between(false, 25, 30), true},
		{"covering", between(false, 5, 25), false},
		{"inverted bounds", between(false, 20, 10), true}, // empty range, no row satisfies
		// negated: satisfied iff col < low || col > high;
		// skip iff min >= low && max <= high
		{"negated stats contained in range", between(true, 5, 25), true},
		{"negated stats equal range", between(true, 10, 20), true},
		{"negated range partial low", between(true, 15, 25), false},
		{"negated range partial high", between(true, 5, 15), false},
		{"negated disjoint", between(true, 30, 40), false},
		{"negated inverted bounds", between(true, 20, 10), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canSkip(tc.e, st, nil); got != tc.skip {
				t.Fatalf("got %v, want %v", got, tc.skip)
			}
		})
	}
}

func TestCanSkip_InList(t *testing.T) {
	st := stats1(100, intStats(10, 20))
	in := func(neg bool, vals ...int64) datafusion.Expr {
		lits := make([]datafusion.Literal, len(vals))
		for i, v := range vals {
			lits[i] = i64(v)
		}
		return datafusion.InList{Column: col0(), Negated: neg, List: lits}
	}
	cases := []struct {
		name string
		e    datafusion.Expr
		skip bool
	}{
		{"all outside", in(false, 1, 5, 25), true},
		{"one at min", in(false, 1, 10), false},
		{"one contained", in(false, 1, 15, 25), false},
		{"empty list", in(false), true}, // IN () satisfies no row
		{"negated wide stats", in(true, 10, 20), false},
		{"negated empty list", in(true), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := canSkip(tc.e, st, nil); got != tc.skip {
				t.Fatalf("got %v, want %v", got, tc.skip)
			}
		})
	}

	// Negated IN with min==max: skip iff that single value is in the list.
	single := stats1(100, intStats(7, 7))
	if !canSkip(in(true, 5, 7), single, nil) {
		t.Fatal("NOT IN (5,7) over [7,7]: every non-null row is 7 which is in the list, must skip")
	}
	if canSkip(in(true, 5, 6), single, nil) {
		t.Fatal("NOT IN (5,6) over [7,7]: rows satisfy, must keep")
	}
}

func TestCanSkip_FloatNaN(t *testing.T) {
	nan := math.NaN()
	// NaN literal never skips.
	stGood := stats1(100, floatStats(1.0, 2.0))
	if canSkip(cmpE(datafusion.CompareEq, f64(nan)), stGood, nil) {
		t.Fatal("NaN literal must keep")
	}
	if canSkip(cmpE(datafusion.CompareLt, f64(nan)), stGood, nil) {
		t.Fatal("NaN literal must keep")
	}
	// NaN in stats never skips.
	for _, cs := range []colStats{floatStats(nan, 2.0), floatStats(1.0, nan), floatStats(nan, nan)} {
		st := stats1(100, cs)
		if canSkip(cmpE(datafusion.CompareEq, f64(10.0)), st, nil) {
			t.Fatal("NaN in min/max stats must keep")
		}
		if canSkip(datafusion.Between{Column: col0(), Low: f64(5), High: f64(6)}, st, nil) {
			t.Fatal("NaN in min/max stats must keep for BETWEEN")
		}
		if canSkip(datafusion.InList{Column: col0(), List: []datafusion.Literal{f64(9)}}, st, nil) {
			t.Fatal("NaN in min/max stats must keep for IN")
		}
	}
	// Sane float pruning still works.
	if !canSkip(cmpE(datafusion.CompareEq, f64(10.0)), stGood, nil) {
		t.Fatal("eq 10.0 over [1.0,2.0] must skip")
	}
	// Negative zero equals positive zero; [−0.0, −0.0] must not skip eq 0.0.
	negZero := stats1(100, floatStats(math.Copysign(0, -1), math.Copysign(0, -1)))
	if canSkip(cmpE(datafusion.CompareEq, f64(0.0)), negZero, nil) {
		t.Fatal("eq 0.0 over [-0.0,-0.0] must keep (SQL equality)")
	}
}

func TestCanSkip_UnsignedExtremes(t *testing.T) {
	// Stats span the upper half of uint64: as raw signed bits these would
	// look negative; the unsigned domain must order them correctly.
	hi := stats1(100, uintStats(math.MaxInt64+1, math.MaxUint64))
	if !canSkip(cmpE(datafusion.CompareEq, u64(5)), hi, nil) {
		t.Fatal("eq 5 over uint64 [2^63, 2^64-1] must skip")
	}
	if !canSkip(cmpE(datafusion.CompareLt, u64(math.MaxInt64+1)), hi, nil) {
		t.Fatal("lt 2^63 over uint64 [2^63, 2^64-1] must skip")
	}
	if canSkip(cmpE(datafusion.CompareEq, u64(math.MaxUint64)), hi, nil) {
		t.Fatal("eq 2^64-1 over uint64 [2^63, 2^64-1] must keep")
	}
	if canSkip(cmpE(datafusion.CompareGt, u64(math.MaxInt64)), hi, nil) {
		t.Fatal("gt 2^63-1 over uint64 [2^63, 2^64-1] must keep")
	}
	// Signed literal against an unsigned column: type family mismatch, keep.
	if canSkip(cmpE(datafusion.CompareEq, i64(5)), hi, nil) {
		t.Fatal("signed literal on unsigned column must keep")
	}
}

func TestCanSkip_Utf8Bytewise(t *testing.T) {
	st := stats1(100, strStats("bbb", "ddd"))
	if !canSkip(cmpE(datafusion.CompareEq, utf8("aaa")), st, nil) {
		t.Fatal(`eq "aaa" over ["bbb","ddd"] must skip`)
	}
	if canSkip(cmpE(datafusion.CompareEq, utf8("ccc")), st, nil) {
		t.Fatal(`eq "ccc" over ["bbb","ddd"] must keep`)
	}
	if !canSkip(cmpE(datafusion.CompareGt, utf8("ddd")), st, nil) {
		t.Fatal(`gt "ddd" over ["bbb","ddd"] must skip`)
	}
	// Prefix ordering: "b" < "bbb" bytewise.
	if !canSkip(cmpE(datafusion.CompareLt, utf8("b")), st, nil) {
		t.Fatal(`lt "b" over ["bbb","ddd"] must skip`)
	}
}

func TestCanSkip_TimestampUnits(t *testing.T) {
	st := stats1(100, tsStats(arrow.Microsecond, 1_000_000, 2_000_000))
	// Unit matches: prune.
	if !canSkip(cmpE(datafusion.CompareEq, ts(datafusion.TimeUnitMicrosecond, 5)), st, nil) {
		t.Fatal("eq 5us over [1e6,2e6]us must skip")
	}
	// Unit mismatch: same numeric value would wrongly skip; must keep.
	if canSkip(cmpE(datafusion.CompareEq, ts(datafusion.TimeUnitSecond, 5)), st, nil) {
		t.Fatal("timestamp literal with mismatched unit must keep")
	}
	if canSkip(cmpE(datafusion.CompareEq, ts(datafusion.TimeUnitNanosecond, 5)), st, nil) {
		t.Fatal("timestamp literal with mismatched unit must keep")
	}
}

func TestCanSkip_TypeMismatchKeeps(t *testing.T) {
	st := stats1(100, intStats(10, 20))
	mismatches := []datafusion.Literal{
		utf8("10"),
		f64(15),
		u64(15),
		{Type: datafusion.LiteralBool, Value: true},
		{Type: datafusion.LiteralInt64, Value: "not an int64"}, // wrong Go type inside
	}
	for _, lit := range mismatches {
		if canSkip(cmpE(datafusion.CompareEq, lit), st, nil) {
			t.Fatalf("literal %v against int64 column must keep", lit)
		}
	}
	// Unknown comparison operator: keep.
	if canSkip(cmpE(datafusion.CompareOp("like"), i64(5)), st, nil) {
		t.Fatal("unknown operator must keep")
	}
}

func TestCanSkip_NotRewrites(t *testing.T) {
	st := stats1(100, intStatsN(10, 20, 0))
	not := func(e datafusion.Expr) datafusion.Expr { return datafusion.Not{Expr: e} }

	// Not(eq 15) == neq 15: not provable over [10,20].
	if canSkip(not(cmpE(datafusion.CompareEq, i64(15))), st, nil) {
		t.Fatal("NOT (= 15) over [10,20] must keep")
	}
	// Not(eq lit) over min==max==lit skips.
	single := stats1(100, intStats(7, 7))
	if !canSkip(not(cmpE(datafusion.CompareEq, i64(7))), single, nil) {
		t.Fatal("NOT (= 7) over [7,7] must skip")
	}
	// Not(lt 5): col >= 5, always true over [10,20]; but Not(gteq 5) == lt 5 skips.
	if canSkip(not(cmpE(datafusion.CompareLt, i64(5))), st, nil) {
		t.Fatal("NOT (< 5) == >= 5 over [10,20] must keep")
	}
	if !canSkip(not(cmpE(datafusion.CompareGtEq, i64(5))), st, nil) {
		t.Fatal("NOT (>= 5) == < 5 over [10,20] must skip")
	}
	// Operator flips must be exact: NOT(> 20) == <= 20 keeps (boundary).
	if canSkip(not(cmpE(datafusion.CompareGt, i64(20))), st, nil) {
		t.Fatal("NOT (> 20) == <= 20 over [10,20] must keep")
	}
	// NOT(>= 21) == < 21 keeps; NOT(<= 9) == > 9 keeps.
	if canSkip(not(cmpE(datafusion.CompareGtEq, i64(21))), st, nil) {
		t.Fatal("NOT (>= 21) == < 21 over [10,20] must keep")
	}
	if canSkip(not(cmpE(datafusion.CompareLtEq, i64(9))), st, nil) {
		t.Fatal("NOT (<= 9) == > 9 over [10,20] must keep")
	}

	// Double negation cancels.
	if !canSkip(not(not(cmpE(datafusion.CompareEq, i64(5)))), st, nil) {
		t.Fatal("NOT NOT (= 5) over [10,20] must skip")
	}

	// Not(IsNull): nullCount=0 means IS NOT NULL is satisfied everywhere; keep.
	if canSkip(not(datafusion.IsNull{Column: col0()}), st, nil) {
		t.Fatal("NOT IS NULL with nullCount=0 must keep")
	}
	// Not(IsNotNull) == IS NULL: nullCount=0 skips.
	if !canSkip(not(datafusion.IsNull{Column: col0(), Negated: true}), st, nil) {
		t.Fatal("NOT IS NOT NULL == IS NULL with nullCount=0 must skip")
	}

	// Not(Between) toggles Negated.
	if !canSkip(not(datafusion.Between{Column: col0(), Low: i64(5), High: i64(25)}), st, nil) {
		t.Fatal("NOT BETWEEN 5 AND 25 over [10,20] must skip")
	}
	if canSkip(not(datafusion.Between{Column: col0(), Low: i64(1), High: i64(5)}), st, nil) {
		t.Fatal("NOT BETWEEN 1 AND 5 over [10,20] must keep")
	}

	// Not(InList) toggles Negated.
	if canSkip(not(datafusion.InList{Column: col0(), List: []datafusion.Literal{i64(1)}}), st, nil) {
		t.Fatal("NOT IN (1) over [10,20] must keep")
	}
	single7 := stats1(100, intStats(7, 7))
	if !canSkip(not(datafusion.InList{Column: col0(), List: []datafusion.Literal{i64(7)}}), single7, nil) {
		t.Fatal("NOT IN (7) over [7,7] must skip")
	}
	if !canSkip(not(datafusion.InList{Column: col0(), Negated: true, List: []datafusion.Literal{i64(1)}}), st, nil) {
		t.Fatal("NOT (NOT IN (1)) == IN (1) over [10,20] must skip")
	}

	// De Morgan: NOT(a AND b) == NOT a OR NOT b — skips only if both sides skip.
	disjointEq := cmpE(datafusion.CompareEq, i64(5))     // skips
	containedEq := cmpE(datafusion.CompareEq, i64(15))   // keeps
	skipsNegated := cmpE(datafusion.CompareGtEq, i64(5)) // NOT of it (< 5) skips
	if canSkip(not(datafusion.And{Left: containedEq, Right: disjointEq}), st, nil) {
		t.Fatal("NOT(keep AND skip): NOT(contained) keeps, so OR must keep")
	}
	if !canSkip(not(datafusion.And{Left: skipsNegated, Right: skipsNegated}), st, nil) {
		t.Fatal("NOT(a AND a) where NOT a skips must skip")
	}
	// De Morgan: NOT(a OR b) == NOT a AND NOT b — skips if either negation skips.
	if !canSkip(not(datafusion.Or{Left: skipsNegated, Right: containedEq}), st, nil) {
		t.Fatal("NOT(a OR b) where NOT a skips must skip")
	}
	if canSkip(not(datafusion.Or{Left: containedEq, Right: containedEq}), st, nil) {
		t.Fatal("NOT(keep OR keep) must keep when negations keep")
	}
}

func TestCanSkip_AndOrComposition(t *testing.T) {
	st := stats1(100, intStats(10, 20))
	skip := cmpE(datafusion.CompareEq, i64(5))  // provably empty
	keep := cmpE(datafusion.CompareEq, i64(15)) // possible

	if !canSkip(datafusion.And{Left: skip, Right: keep}, st, nil) {
		t.Fatal("AND with one skip side must skip")
	}
	if !canSkip(datafusion.And{Left: keep, Right: skip}, st, nil) {
		t.Fatal("AND with one skip side must skip")
	}
	if canSkip(datafusion.And{Left: keep, Right: keep}, st, nil) {
		t.Fatal("AND with no skip side must keep")
	}
	if canSkip(datafusion.Or{Left: skip, Right: keep}, st, nil) {
		t.Fatal("OR with a keep side must keep")
	}
	if !canSkip(datafusion.Or{Left: skip, Right: skip}, st, nil) {
		t.Fatal("OR with both sides skipping must skip")
	}
	// Filters over multiple columns: unreferenced second column keeps.
	multi := rowGroupStats{numRows: 100, cols: map[int]colStats{0: intStats(10, 20)}}
	other := datafusion.Compare{Column: datafusion.Column{Name: "d", Index: 1}, Op: datafusion.CompareEq, Literal: i64(5)}
	if canSkip(other, multi, nil) {
		t.Fatal("predicate on column without stats must keep")
	}
	if !canSkip(datafusion.And{Left: other, Right: skip}, multi, nil) {
		t.Fatal("AND(no-stats keep, skip) must skip")
	}
}

// fakeBloom reports a fixed set of absent values for column index 0.
type fakeBloom struct {
	absentVals map[int64]bool
	calls      int
}

func (f *fakeBloom) absent(fieldIdx int, lit datafusion.Literal) bool {
	f.calls++
	if fieldIdx != 0 {
		return false
	}
	v, ok := lit.Value.(int64)
	return ok && f.absentVals[v]
}

func TestCanSkip_BloomIntegration(t *testing.T) {
	st := stats1(100, intStats(10, 20))
	bloom := &fakeBloom{absentVals: map[int64]bool{15: true}}

	// eq within [min,max] but bloom-absent: skip.
	if !canSkip(cmpE(datafusion.CompareEq, i64(15)), st, bloom) {
		t.Fatal("eq 15 bloom-absent must skip")
	}
	// eq within [min,max], bloom maybe-present: keep.
	if canSkip(cmpE(datafusion.CompareEq, i64(16)), st, bloom) {
		t.Fatal("eq 16 bloom-maybe must keep")
	}
	// IN list where every element is min/max-absent or bloom-absent: skip.
	inList := datafusion.InList{Column: col0(), List: []datafusion.Literal{i64(5), i64(15)}}
	if !canSkip(inList, st, bloom) {
		t.Fatal("IN (5,15): 5 outside range, 15 bloom-absent, must skip")
	}
	// neq must never consult the bloom filter (absence of one value proves nothing).
	before := bloom.calls
	if canSkip(cmpE(datafusion.CompareNeq, i64(15)), st, bloom) {
		t.Fatal("neq must keep")
	}
	if bloom.calls != before {
		t.Fatal("neq must not probe the bloom filter")
	}
	// min/max skip must short-circuit before bloom I/O.
	before = bloom.calls
	if !canSkip(cmpE(datafusion.CompareEq, i64(5)), st, bloom) {
		t.Fatal("eq 5 outside range must skip")
	}
	if bloom.calls != before {
		t.Fatal("min/max skip must not probe the bloom filter")
	}
	// Negated IN must never consult the bloom filter.
	before = bloom.calls
	_ = canSkip(datafusion.InList{Column: col0(), Negated: true, List: []datafusion.Literal{i64(15)}}, st, bloom)
	if bloom.calls != before {
		t.Fatal("NOT IN must not probe the bloom filter")
	}
}

func TestCanSkip_EmptyRowGroup(t *testing.T) {
	// A zero-row group with a trustworthy null count of zero proves emptiness.
	st := stats1(0, colStats{dtype: arrow.PrimitiveTypes.Int64, hasNullCount: true, nullCount: 0})
	if !canSkip(cmpE(datafusion.CompareEq, i64(5)), st, nil) {
		t.Fatal("empty row group: comparison can skip")
	}
}
