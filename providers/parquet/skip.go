package parquet

import (
	"bytes"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// This file implements the pure row-group skip evaluator (design D2/D3):
// canSkip answers exactly one question — can this row group be *proven* to
// contain no row satisfying the predicate? Returning false (keep) is always
// correct; returning true requires proof from the statistics. Every
// uncertain input (missing stats, type mismatches, NaN, unknown shapes)
// keeps the row group.

// domain identifies the totally ordered comparison domain a column's
// statistics live in (the column's Parquet sort-order domain).
type domain int

const (
	domainBool domain = iota
	domainInt
	domainUint
	domainFloat
	domainBytes
)

// statValue is a single value (a min/max endpoint or a converted literal)
// in a column's comparison domain.
type statValue struct {
	dom domain
	b   bool
	i   int64
	u   uint64
	f   float64
	by  []byte
}

func boolVal(v bool) statValue     { return statValue{dom: domainBool, b: v} }
func intVal(v int64) statValue     { return statValue{dom: domainInt, i: v} }
func uintVal(v uint64) statValue   { return statValue{dom: domainUint, u: v} }
func floatVal(v float64) statValue { return statValue{dom: domainFloat, f: v} }
func bytesVal(v []byte) statValue  { return statValue{dom: domainBytes, by: v} }

// cmpVal compares two statValues of the same domain. Callers guarantee the
// domains match (both derive from the same column type) and that neither
// value is NaN.
func cmpVal(a, b statValue) int {
	switch a.dom {
	case domainBool:
		switch {
		case a.b == b.b:
			return 0
		case !a.b:
			return -1
		default:
			return 1
		}
	case domainInt:
		switch {
		case a.i < b.i:
			return -1
		case a.i > b.i:
			return 1
		default:
			return 0
		}
	case domainUint:
		switch {
		case a.u < b.u:
			return -1
		case a.u > b.u:
			return 1
		default:
			return 0
		}
	case domainFloat:
		switch {
		case a.f < b.f:
			return -1
		case a.f > b.f:
			return 1
		default:
			return 0
		}
	default:
		return bytes.Compare(a.by, b.by)
	}
}

func isNaN(v statValue) bool { return v.dom == domainFloat && math.IsNaN(v.f) }

// colStats is one column's per-row-group statistics view. All parts are
// optional; absent parts simply make fewer skip decisions provable.
type colStats struct {
	dtype        arrow.DataType // the column's registered Arrow type
	hasMinMax    bool
	min, max     statValue
	hasNullCount bool
	nullCount    int64
}

// rowGroupStats is the hand-constructible statistics view of one row
// group: per-top-level-column stats keyed by Arrow field index, plus the
// group's row count.
type rowGroupStats struct {
	numRows int64
	cols    map[int]colStats
}

// bloomProber proves values absent from a row group's column via its bloom
// filter. absent must return true only on a definite absence; any doubt
// (no filter, unsupported type, I/O error) returns false.
type bloomProber interface {
	absent(fieldIndex int, lit datafusion.Literal) bool
}

// canSkip reports whether stats prove that no row in the row group can
// satisfy expr (evaluate to SQL TRUE). bloom may be nil.
func canSkip(expr datafusion.Expr, st rowGroupStats, bloom bloomProber) bool {
	switch e := expr.(type) {
	case datafusion.And:
		return canSkip(e.Left, st, bloom) || canSkip(e.Right, st, bloom)
	case datafusion.Or:
		return canSkip(e.Left, st, bloom) && canSkip(e.Right, st, bloom)
	case datafusion.Not:
		inner, ok := negate(e.Expr)
		if !ok {
			return false
		}
		return canSkip(inner, st, bloom)
	case datafusion.Compare:
		return canSkipCompare(e, st, bloom)
	case datafusion.IsNull:
		return canSkipIsNull(e, st)
	case datafusion.Between:
		return canSkipBetween(e, st)
	case datafusion.InList:
		return canSkipInList(e, st, bloom)
	default:
		return false
	}
}

// negate rewrites NOT(e) into an equivalent positive form. Sound for
// satisfaction under SQL three-valued logic: a NULL operand satisfies
// neither a predicate nor its complement, so pushing the negation into the
// leaves only ever errs toward keeping.
func negate(expr datafusion.Expr) (datafusion.Expr, bool) {
	switch e := expr.(type) {
	case datafusion.Compare:
		op, ok := negateOp(e.Op)
		if !ok {
			return nil, false
		}
		return datafusion.Compare{Column: e.Column, Op: op, Literal: e.Literal}, true
	case datafusion.IsNull:
		return datafusion.IsNull{Column: e.Column, Negated: !e.Negated}, true
	case datafusion.Between:
		return datafusion.Between{Column: e.Column, Negated: !e.Negated, Low: e.Low, High: e.High}, true
	case datafusion.InList:
		return datafusion.InList{Column: e.Column, Negated: !e.Negated, List: e.List}, true
	case datafusion.And:
		return datafusion.Or{Left: datafusion.Not{Expr: e.Left}, Right: datafusion.Not{Expr: e.Right}}, true
	case datafusion.Or:
		return datafusion.And{Left: datafusion.Not{Expr: e.Left}, Right: datafusion.Not{Expr: e.Right}}, true
	case datafusion.Not:
		return e.Expr, true
	default:
		return nil, false
	}
}

func negateOp(op datafusion.CompareOp) (datafusion.CompareOp, bool) {
	switch op {
	case datafusion.CompareEq:
		return datafusion.CompareNeq, true
	case datafusion.CompareNeq:
		return datafusion.CompareEq, true
	case datafusion.CompareLt:
		return datafusion.CompareGtEq, true
	case datafusion.CompareLtEq:
		return datafusion.CompareGt, true
	case datafusion.CompareGt:
		return datafusion.CompareLtEq, true
	case datafusion.CompareGtEq:
		return datafusion.CompareLt, true
	default:
		return "", false
	}
}

// provablyAllNull reports whether the column provably has no non-null
// value in the row group (which means no comparison-style predicate can be
// satisfied there).
func provablyAllNull(cs colStats, numRows int64) bool {
	return cs.hasNullCount && cs.nullCount == numRows
}

// usableRange reports whether min/max are present and safe to compare
// against (floats containing NaN are not).
func usableRange(cs colStats) bool {
	return cs.hasMinMax && !isNaN(cs.min) && !isNaN(cs.max)
}

func canSkipCompare(c datafusion.Compare, st rowGroupStats, bloom bloomProber) bool {
	cs, ok := st.cols[c.Column.Index]
	if !ok {
		return false
	}
	if provablyAllNull(cs, st.numRows) {
		return true
	}
	if usableRange(cs) {
		if lit, ok := literalValue(c.Literal, cs.dtype); ok && !isNaN(lit) {
			switch c.Op {
			case datafusion.CompareEq:
				if cmpVal(lit, cs.min) < 0 || cmpVal(lit, cs.max) > 0 {
					return true
				}
			case datafusion.CompareLt:
				if cmpVal(cs.min, lit) >= 0 {
					return true
				}
			case datafusion.CompareLtEq:
				if cmpVal(cs.min, lit) > 0 {
					return true
				}
			case datafusion.CompareGt:
				if cmpVal(cs.max, lit) <= 0 {
					return true
				}
			case datafusion.CompareGtEq:
				if cmpVal(cs.max, lit) < 0 {
					return true
				}
			case datafusion.CompareNeq:
				if cmpVal(cs.min, cs.max) == 0 && cmpVal(cs.min, lit) == 0 {
					return true
				}
			}
		}
	}
	// Bloom probes prove absence only for equality, and only after the
	// min/max range failed to decide (avoids bloom I/O when unnecessary).
	if c.Op == datafusion.CompareEq && bloom != nil {
		return bloom.absent(c.Column.Index, c.Literal)
	}
	return false
}

func canSkipIsNull(e datafusion.IsNull, st rowGroupStats) bool {
	cs, ok := st.cols[e.Column.Index]
	if !ok || !cs.hasNullCount {
		return false
	}
	if e.Negated {
		// IS NOT NULL: skip only when provably all-null.
		return cs.nullCount == st.numRows
	}
	// IS NULL: skip only when provably null-free.
	return cs.nullCount == 0
}

func canSkipBetween(e datafusion.Between, st rowGroupStats) bool {
	cs, ok := st.cols[e.Column.Index]
	if !ok {
		return false
	}
	if provablyAllNull(cs, st.numRows) {
		return true
	}
	low, okL := literalValue(e.Low, cs.dtype)
	high, okH := literalValue(e.High, cs.dtype)
	if !okL || !okH || isNaN(low) || isNaN(high) {
		return false
	}
	if !e.Negated && cmpVal(low, high) > 0 {
		// Inverted bounds: BETWEEN over an empty range satisfies no row.
		return true
	}
	if !usableRange(cs) {
		return false
	}
	if e.Negated {
		// Satisfied iff col < low || col > high; unprovable unless the
		// whole range sits inside [low, high].
		return cmpVal(cs.min, low) >= 0 && cmpVal(cs.max, high) <= 0
	}
	return cmpVal(high, cs.min) < 0 || cmpVal(low, cs.max) > 0
}

func canSkipInList(e datafusion.InList, st rowGroupStats, bloom bloomProber) bool {
	cs, ok := st.cols[e.Column.Index]
	if !ok {
		return false
	}
	if provablyAllNull(cs, st.numRows) {
		return true
	}
	if e.Negated {
		// NOT IN: provable only when every non-null value equals one
		// single value (min == max) and that value is in the list.
		if !usableRange(cs) || cmpVal(cs.min, cs.max) != 0 {
			return false
		}
		for _, l := range e.List {
			if v, ok := literalValue(l, cs.dtype); ok && !isNaN(v) && cmpVal(v, cs.min) == 0 {
				return true
			}
		}
		return false
	}
	// IN: skip iff every element is provably absent. An empty list
	// satisfies no row, so it vacuously skips.
	for _, l := range e.List {
		if !inElementAbsent(e.Column.Index, l, cs, bloom) {
			return false
		}
	}
	return true
}

// inElementAbsent reports whether one IN-list element is provably absent
// from the row group, by min/max range or by bloom filter.
func inElementAbsent(fieldIdx int, l datafusion.Literal, cs colStats, bloom bloomProber) bool {
	if usableRange(cs) {
		if v, ok := literalValue(l, cs.dtype); ok && !isNaN(v) {
			if cmpVal(v, cs.min) < 0 || cmpVal(v, cs.max) > 0 {
				return true
			}
		}
	}
	return bloom != nil && bloom.absent(fieldIdx, l)
}

// literalValue converts a pushed literal into the comparison domain of a
// column with the given Arrow type. It succeeds only for the exact,
// lossless combinations the skip rules rely on; anything else reports
// false (keep).
func literalValue(l datafusion.Literal, dt arrow.DataType) (statValue, bool) {
	if dt == nil {
		return statValue{}, false
	}
	switch dt.ID() {
	case arrow.BOOL:
		if v, ok := l.Value.(bool); ok && l.Type == datafusion.LiteralBool {
			return boolVal(v), true
		}
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		if v, ok := signedLiteral(l); ok {
			return intVal(v), true
		}
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		if v, ok := unsignedLiteral(l); ok {
			return uintVal(v), true
		}
	case arrow.FLOAT32, arrow.FLOAT64:
		if v, ok := floatLiteral(l); ok {
			return floatVal(v), true
		}
	case arrow.STRING, arrow.LARGE_STRING:
		if v, ok := l.Value.(string); ok && l.Type == datafusion.LiteralUtf8 {
			return bytesVal([]byte(v)), true
		}
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		if v, ok := l.Value.([]byte); ok && l.Type == datafusion.LiteralBinary {
			return bytesVal(v), true
		}
	case arrow.DATE32:
		if v, ok := l.Value.(int32); ok && l.Type == datafusion.LiteralDate32 {
			return intVal(int64(v)), true
		}
	case arrow.TIMESTAMP:
		if v, ok := l.Value.(int64); ok && l.Type == datafusion.LiteralTimestamp {
			if ts, tok := dt.(*arrow.TimestampType); tok && unitMatches(l.Unit, ts.Unit) {
				return intVal(v), true
			}
		}
	}
	return statValue{}, false
}

func signedLiteral(l datafusion.Literal) (int64, bool) {
	switch l.Type {
	case datafusion.LiteralInt8:
		v, ok := l.Value.(int8)
		return int64(v), ok
	case datafusion.LiteralInt16:
		v, ok := l.Value.(int16)
		return int64(v), ok
	case datafusion.LiteralInt32:
		v, ok := l.Value.(int32)
		return int64(v), ok
	case datafusion.LiteralInt64:
		v, ok := l.Value.(int64)
		return v, ok
	}
	return 0, false
}

func unsignedLiteral(l datafusion.Literal) (uint64, bool) {
	switch l.Type {
	case datafusion.LiteralUint8:
		v, ok := l.Value.(uint8)
		return uint64(v), ok
	case datafusion.LiteralUint16:
		v, ok := l.Value.(uint16)
		return uint64(v), ok
	case datafusion.LiteralUint32:
		v, ok := l.Value.(uint32)
		return uint64(v), ok
	case datafusion.LiteralUint64:
		v, ok := l.Value.(uint64)
		return v, ok
	}
	return 0, false
}

func floatLiteral(l datafusion.Literal) (float64, bool) {
	switch l.Type {
	case datafusion.LiteralFloat32:
		v, ok := l.Value.(float32)
		return float64(v), ok
	case datafusion.LiteralFloat64:
		v, ok := l.Value.(float64)
		return v, ok
	}
	return 0, false
}

func unitMatches(u datafusion.TimeUnit, au arrow.TimeUnit) bool {
	switch u {
	case datafusion.TimeUnitSecond:
		return au == arrow.Second
	case datafusion.TimeUnitMillisecond:
		return au == arrow.Millisecond
	case datafusion.TimeUnitMicrosecond:
		return au == arrow.Microsecond
	case datafusion.TimeUnitNanosecond:
		return au == arrow.Nanosecond
	}
	return false
}
