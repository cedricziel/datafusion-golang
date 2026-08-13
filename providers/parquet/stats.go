package parquet

import (
	"bytes"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// buildRowGroupStats fills the skip evaluator's statistics view for one
// row group, restricted to the given top-level Arrow field indices. Any
// column whose statistics are absent, untrustworthy (Statistics() returns
// nil for unknown sort orders and known-bad writers), or not convertible
// simply gets no entry, which the evaluator treats as "keep".
func buildRowGroupStats(md *metadata.FileMetaData, manifest *pqarrow.SchemaManifest, sc *arrow.Schema, rgIdx int, fields map[int]struct{}) rowGroupStats {
	rg := md.RowGroup(rgIdx)
	st := rowGroupStats{numRows: rg.NumRows(), cols: make(map[int]colStats, len(fields))}
	for idx := range fields {
		if idx < 0 || idx >= len(manifest.Fields) || idx >= sc.NumFields() {
			continue
		}
		sf := &manifest.Fields[idx]
		if !sf.IsLeaf() {
			// Nested column: no stats-based decision (design D3).
			continue
		}
		cc, err := rg.ColumnChunk(sf.ColIndex)
		if err != nil {
			continue
		}
		typed, err := cc.Statistics()
		if err != nil || typed == nil {
			continue
		}
		cs, ok := colStatsFromTyped(typed, sc.Field(idx).Type)
		if !ok {
			continue
		}
		st.cols[idx] = cs
	}
	return st
}

// colStatsFromTyped converts one column chunk's TypedStatistics into the
// evaluator's view for a column registered with Arrow type dt. Min/max are
// taken only when the concrete statistics type matches the physical type
// the Arrow type implies (anything else is untrustworthy).
//
// Null-count trust: arrow-go's decoded statistics report HasNullCount()
// unconditionally — a file whose writer omitted the optional null_count
// field decodes as null count 0. A spurious zero must never prove "no
// nulls here" (it would let IS NULL skip a group that has nulls), so only
// a positive null count — which can only come from a genuinely written
// field — is passed through. This deliberately forgoes the IS NULL skip
// optimization; the all-null skip (nullCount == numRows > 0) survives.
//
// Min/max trust: decoded HasMinMax() is true when *either* bound was
// written, with the missing bound decoding to the type's zero value, so a
// min > max pair is provably garbage and dropped.
func colStatsFromTyped(stats metadata.TypedStatistics, dt arrow.DataType) (colStats, bool) {
	cs := colStats{dtype: dt}
	if stats.HasNullCount() && stats.NullCount() > 0 {
		cs.hasNullCount = true
		cs.nullCount = stats.NullCount()
	}
	if stats.HasMinMax() {
		if minV, maxV, ok := statsMinMax(stats, dt); ok && !isNaN(minV) && !isNaN(maxV) && cmpVal(minV, maxV) <= 0 {
			cs.hasMinMax = true
			cs.min, cs.max = minV, maxV
		}
	}
	return cs, cs.hasMinMax || cs.hasNullCount
}

// statsMinMax reinterprets the raw physical min/max into the column's
// comparison domain: unsigned logical types bit-reinterpret their signed
// physical storage, byte arrays compare bytewise, temporal types compare
// as their integer representation.
func statsMinMax(stats metadata.TypedStatistics, dt arrow.DataType) (minV, maxV statValue, ok bool) {
	switch dt.ID() {
	case arrow.BOOL:
		if s, k := stats.(*metadata.BooleanStatistics); k {
			return boolVal(s.Min()), boolVal(s.Max()), true
		}
	case arrow.INT8, arrow.INT16, arrow.INT32:
		if s, k := stats.(*metadata.Int32Statistics); k {
			return intVal(int64(s.Min())), intVal(int64(s.Max())), true
		}
	case arrow.INT64:
		if s, k := stats.(*metadata.Int64Statistics); k {
			return intVal(s.Min()), intVal(s.Max()), true
		}
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		if s, k := stats.(*metadata.Int32Statistics); k {
			return uintVal(uint64(uint32(s.Min()))), uintVal(uint64(uint32(s.Max()))), true
		}
	case arrow.UINT64:
		if s, k := stats.(*metadata.Int64Statistics); k {
			return uintVal(uint64(s.Min())), uintVal(uint64(s.Max())), true
		}
	case arrow.FLOAT32:
		if s, k := stats.(*metadata.Float32Statistics); k {
			return floatVal(float64(s.Min())), floatVal(float64(s.Max())), true
		}
	case arrow.FLOAT64:
		if s, k := stats.(*metadata.Float64Statistics); k {
			return floatVal(s.Min()), floatVal(s.Max()), true
		}
	case arrow.STRING, arrow.LARGE_STRING, arrow.BINARY, arrow.LARGE_BINARY:
		if s, k := stats.(*metadata.ByteArrayStatistics); k {
			return bytesVal(bytes.Clone(s.Min())), bytesVal(bytes.Clone(s.Max())), true
		}
	case arrow.FIXED_SIZE_BINARY:
		if s, k := stats.(*metadata.FixedLenByteArrayStatistics); k {
			return bytesVal(bytes.Clone(s.Min())), bytesVal(bytes.Clone(s.Max())), true
		}
	case arrow.DATE32:
		if s, k := stats.(*metadata.Int32Statistics); k {
			return intVal(int64(s.Min())), intVal(int64(s.Max())), true
		}
	case arrow.TIMESTAMP:
		if s, k := stats.(*metadata.Int64Statistics); k {
			return intVal(s.Min()), intVal(s.Max()), true
		}
	}
	return statValue{}, statValue{}, false
}
