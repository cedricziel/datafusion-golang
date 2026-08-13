package iceberg

import (
	"math"

	ib "github.com/apache/iceberg-go"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// This file converts the engine's pushed predicate AST into iceberg-go
// BooleanExpressions (design D6). Unlike the Parquet side's skip
// evaluator, faithfulness here is correctness-critical: iceberg-go filters
// rows *exactly* with the converted expression, so a semantically-off
// conversion would drop matching rows the engine can never recover.
//
// Two guards enforce that a pushed filter can only ever widen the scan:
//
//   - Conversion is all-or-nothing per top-level conjunct: if any node
//     inside a conjunct fails to convert (an unrepresentable literal, an
//     unknown shape), the entire conjunct is dropped. Dropping a whole
//     conjunct only reads more (the engine re-filters); dropping a
//     sub-expression — say one side of an OR — would narrow the scan and
//     lose rows, so it is never done.
//   - Every converted conjunct is validated with iceberg.BindExpr against
//     the table schema; a bind error drops the conjunct instead of
//     surfacing, because a pushed filter must never be able to fail a
//     query, only fail to optimize it.
//
// Literal mappings are lossless-only widenings; anything else fails the
// conversion.

// convertFilters converts the pushed filters (implicit AND) into a single
// row-filter expression, dropping every conjunct that cannot be converted
// faithfully or fails to bind. Returns nil when nothing survives.
func convertFilters(filters []datafusion.Expr, schema *ib.Schema) ib.BooleanExpression {
	var result ib.BooleanExpression
	for _, f := range filters {
		e, ok := convertExpr(f)
		if !ok {
			continue
		}
		if _, err := ib.BindExpr(schema, e, true); err != nil {
			continue
		}
		if result == nil {
			result = e
		} else {
			result = ib.NewAnd(result, e)
		}
	}
	return result
}

func convertExpr(expr datafusion.Expr) (ib.BooleanExpression, bool) {
	switch e := expr.(type) {
	case datafusion.Compare:
		v, ok := convertLiteral(e.Literal)
		if !ok {
			return nil, false
		}
		return comparePred(e.Op, ib.Reference(e.Column.Name), v)
	case datafusion.IsNull:
		ref := ib.Reference(e.Column.Name)
		if e.Negated {
			return ib.NotNull(ref), true
		}
		return ib.IsNull(ref), true
	case datafusion.Between:
		return convertBetween(e)
	case datafusion.InList:
		return convertInList(e)
	case datafusion.And:
		l, ok := convertExpr(e.Left)
		if !ok {
			return nil, false
		}
		r, ok := convertExpr(e.Right)
		if !ok {
			return nil, false
		}
		return ib.NewAnd(l, r), true
	case datafusion.Or:
		l, ok := convertExpr(e.Left)
		if !ok {
			return nil, false
		}
		r, ok := convertExpr(e.Right)
		if !ok {
			return nil, false
		}
		return ib.NewOr(l, r), true
	case datafusion.Not:
		inner, ok := convertExpr(e.Expr)
		if !ok {
			return nil, false
		}
		return ib.NewNot(inner), true
	default:
		return nil, false
	}
}

// convertBetween rewrites BETWEEN into range comparisons. Sound under SQL
// three-valued logic: a NULL operand satisfies neither the conjunction
// nor the negated disjunction, matching BETWEEN's NULL behavior.
func convertBetween(e datafusion.Between) (ib.BooleanExpression, bool) {
	low, okL := convertLiteral(e.Low)
	high, okH := convertLiteral(e.High)
	if !okL || !okH {
		return nil, false
	}
	ref := ib.Reference(e.Column.Name)
	if e.Negated {
		lt, ok := comparePred(datafusion.CompareLt, ref, low)
		if !ok {
			return nil, false
		}
		gt, ok := comparePred(datafusion.CompareGt, ref, high)
		if !ok {
			return nil, false
		}
		return ib.NewOr(lt, gt), true
	}
	ge, ok := comparePred(datafusion.CompareGtEq, ref, low)
	if !ok {
		return nil, false
	}
	le, ok := comparePred(datafusion.CompareLtEq, ref, high)
	if !ok {
		return nil, false
	}
	return ib.NewAnd(ge, le), true
}

func convertInList(e datafusion.InList) (ib.BooleanExpression, bool) {
	if len(e.List) == 0 {
		// The engine never pushes empty lists; engines disagree on the
		// exact NULL semantics of IN (), so don't guess.
		return nil, false
	}
	vals := make([]any, len(e.List))
	for i, l := range e.List {
		v, ok := convertLiteral(l)
		if !ok {
			return nil, false
		}
		vals[i] = v
	}
	ref := ib.Reference(e.Column.Name)
	switch vals[0].(type) {
	case bool:
		return inPred[bool](ref, e.Negated, vals)
	case int32:
		return inPred[int32](ref, e.Negated, vals)
	case int64:
		return inPred[int64](ref, e.Negated, vals)
	case float32:
		return inPred[float32](ref, e.Negated, vals)
	case float64:
		return inPred[float64](ref, e.Negated, vals)
	case string:
		return inPred[string](ref, e.Negated, vals)
	case []byte:
		return inPred[[]byte](ref, e.Negated, vals)
	case ib.Date:
		return inPred[ib.Date](ref, e.Negated, vals)
	case ib.Timestamp:
		return inPred[ib.Timestamp](ref, e.Negated, vals)
	case ib.TimestampNano:
		return inPred[ib.TimestampNano](ref, e.Negated, vals)
	default:
		return nil, false
	}
}

func inPred[T ib.LiteralType](ref ib.Reference, negated bool, vals []any) (ib.BooleanExpression, bool) {
	typed := make([]T, len(vals))
	for i, v := range vals {
		t, ok := v.(T)
		if !ok {
			// Mixed-type list after conversion: not faithfully
			// representable as one set predicate.
			return nil, false
		}
		typed[i] = t
	}
	if negated {
		return ib.NotIn(ref, typed...), true
	}
	return ib.IsIn(ref, typed...), true
}

func comparePred(op datafusion.CompareOp, ref ib.Reference, v any) (ib.BooleanExpression, bool) {
	switch v := v.(type) {
	case bool:
		return typedComparePred(op, ref, v)
	case int32:
		return typedComparePred(op, ref, v)
	case int64:
		return typedComparePred(op, ref, v)
	case float32:
		return typedComparePred(op, ref, v)
	case float64:
		return typedComparePred(op, ref, v)
	case string:
		return typedComparePred(op, ref, v)
	case []byte:
		return typedComparePred(op, ref, v)
	case ib.Date:
		return typedComparePred(op, ref, v)
	case ib.Timestamp:
		return typedComparePred(op, ref, v)
	case ib.TimestampNano:
		return typedComparePred(op, ref, v)
	default:
		return nil, false
	}
}

func typedComparePred[T ib.LiteralType](op datafusion.CompareOp, ref ib.Reference, v T) (ib.BooleanExpression, bool) {
	switch op {
	case datafusion.CompareEq:
		return ib.EqualTo(ref, v), true
	case datafusion.CompareNeq:
		return ib.NotEqualTo(ref, v), true
	case datafusion.CompareLt:
		return ib.LessThan(ref, v), true
	case datafusion.CompareLtEq:
		return ib.LessThanEqual(ref, v), true
	case datafusion.CompareGt:
		return ib.GreaterThan(ref, v), true
	case datafusion.CompareGtEq:
		return ib.GreaterThanEqual(ref, v), true
	default:
		return nil, false
	}
}

// convertLiteral maps a pushed literal onto one of iceberg-go's literal
// Go types using lossless widenings only: int8/16 → int32, uint8/16/32 →
// int64, uint64 → int64 when representable, timestamps → microseconds
// (nanoseconds stay nanoseconds). date64 and anything unrepresentable
// report false.
func convertLiteral(l datafusion.Literal) (any, bool) {
	switch l.Type {
	case datafusion.LiteralBool:
		v, ok := l.Value.(bool)
		return v, ok
	case datafusion.LiteralInt8:
		v, ok := l.Value.(int8)
		return int32(v), ok
	case datafusion.LiteralInt16:
		v, ok := l.Value.(int16)
		return int32(v), ok
	case datafusion.LiteralInt32:
		v, ok := l.Value.(int32)
		return v, ok
	case datafusion.LiteralInt64:
		v, ok := l.Value.(int64)
		return v, ok
	case datafusion.LiteralUint8:
		v, ok := l.Value.(uint8)
		return int64(v), ok
	case datafusion.LiteralUint16:
		v, ok := l.Value.(uint16)
		return int64(v), ok
	case datafusion.LiteralUint32:
		v, ok := l.Value.(uint32)
		return int64(v), ok
	case datafusion.LiteralUint64:
		v, ok := l.Value.(uint64)
		if !ok || v > math.MaxInt64 {
			return nil, false
		}
		return int64(v), true
	case datafusion.LiteralFloat32:
		v, ok := l.Value.(float32)
		return v, ok
	case datafusion.LiteralFloat64:
		v, ok := l.Value.(float64)
		return v, ok
	case datafusion.LiteralUtf8:
		v, ok := l.Value.(string)
		return v, ok
	case datafusion.LiteralBinary:
		v, ok := l.Value.([]byte)
		return v, ok
	case datafusion.LiteralDate32:
		v, ok := l.Value.(int32)
		return ib.Date(v), ok
	case datafusion.LiteralTimestamp:
		v, ok := l.Value.(int64)
		if !ok {
			return nil, false
		}
		switch l.Unit {
		case datafusion.TimeUnitSecond:
			return scaleTimestamp(v, 1_000_000)
		case datafusion.TimeUnitMillisecond:
			return scaleTimestamp(v, 1_000)
		case datafusion.TimeUnitMicrosecond:
			return ib.Timestamp(v), true
		case datafusion.TimeUnitNanosecond:
			return ib.TimestampNano(v), true
		}
		return nil, false
	default:
		return nil, false
	}
}

func scaleTimestamp(v, factor int64) (any, bool) {
	if v > math.MaxInt64/factor || v < math.MinInt64/factor {
		return nil, false
	}
	return ib.Timestamp(v * factor), true
}
