package parquet

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	pqschema "github.com/apache/arrow-go/v18/parquet/schema"
)

func testDescr(t *testing.T, typ pq.Type) *pqschema.Column {
	t.Helper()
	n, err := pqschema.NewPrimitiveNode("c", pq.Repetitions.Optional, typ, -1, -1)
	if err != nil {
		t.Fatalf("NewPrimitiveNode: %v", err)
	}
	return pqschema.NewColumn(n, 1, 0)
}

func TestColStatsFromTyped_Int64(t *testing.T) {
	s := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	s.SetMinMax(10, 20)
	s.IncNulls(3)

	cs, ok := colStatsFromTyped(s, arrow.PrimitiveTypes.Int64)
	if !ok {
		t.Fatal("expected usable stats")
	}
	if !cs.hasMinMax || cmpVal(cs.min, intVal(10)) != 0 || cmpVal(cs.max, intVal(20)) != 0 {
		t.Fatalf("unexpected min/max: %+v", cs)
	}
	if !cs.hasNullCount || cs.nullCount != 3 {
		t.Fatalf("unexpected null count: %+v", cs)
	}
}

func TestColStatsFromTyped_NoMinMaxStillCarriesNullCount(t *testing.T) {
	s := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	s.IncNulls(7)

	cs, ok := colStatsFromTyped(s, arrow.PrimitiveTypes.Int64)
	if !ok {
		t.Fatal("null count alone is usable")
	}
	if cs.hasMinMax {
		t.Fatal("must not invent min/max")
	}
	if !cs.hasNullCount || cs.nullCount != 7 {
		t.Fatalf("unexpected null count: %+v", cs)
	}
}

// emptyStatProvider mimics decoded file statistics with no fields set,
// the shape the adapter sees for chunks whose writer recorded nothing.
type emptyStatProvider struct{}

func (emptyStatProvider) GetMin() []byte           { return nil }
func (emptyStatProvider) GetMax() []byte           { return nil }
func (emptyStatProvider) GetNullCount() int64      { return 0 }
func (emptyStatProvider) GetDistinctCount() int64  { return 0 }
func (emptyStatProvider) IsSetMax() bool           { return false }
func (emptyStatProvider) IsSetMin() bool           { return false }
func (emptyStatProvider) IsSetNullCount() bool     { return false }
func (emptyStatProvider) IsSetDistinctCount() bool { return false }

func TestColStatsFromTyped_NothingSet(t *testing.T) {
	s := metadata.NewStatisticsFromEncoded(testDescr(t, pq.Types.Int64), nil, 10, emptyStatProvider{})
	if _, ok := colStatsFromTyped(s, arrow.PrimitiveTypes.Int64); ok {
		t.Fatal("stats without min/max or null count are unusable")
	}
}

func TestColStatsFromTyped_ZeroNullCountIsNotTrusted(t *testing.T) {
	// arrow-go decodes an *omitted* null_count field as 0 and still
	// reports HasNullCount() (writer-side constructor default), so a zero
	// can never prove "no nulls here".
	s := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	s.SetMinMax(10, 20)
	if !s.HasNullCount() || s.NullCount() != 0 {
		t.Fatalf("premise: fresh stats claim null count 0, got has=%v count=%d", s.HasNullCount(), s.NullCount())
	}
	cs, ok := colStatsFromTyped(s, arrow.PrimitiveTypes.Int64)
	if !ok || !cs.hasMinMax {
		t.Fatalf("min/max must still be usable: %+v", cs)
	}
	if cs.hasNullCount {
		t.Fatal("a decoded null count of 0 is ambiguous and must not be trusted")
	}
}

func TestColStatsFromTyped_InvertedMinMaxDropped(t *testing.T) {
	// A one-sided min/max (spec-legal) decodes the missing bound as the
	// type's zero value; min > max is provably garbage.
	s := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	s.SetMinMax(10, -5)
	cs, _ := colStatsFromTyped(s, arrow.PrimitiveTypes.Int64)
	if cs.hasMinMax {
		t.Fatal("min > max must drop the range")
	}
}

func TestColStatsFromTyped_UnsignedBitReinterpretation(t *testing.T) {
	// uint32 column: physical int32 stats store the unsigned bit patterns.
	// 0xFFFFFFFF as int32 is -1; the adapter must recover 4294967295.
	s32 := metadata.NewStatistics(testDescr(t, pq.Types.Int32), nil).(*metadata.Int32Statistics)
	s32.SetMinMax(100, -1)
	cs, ok := colStatsFromTyped(s32, arrow.PrimitiveTypes.Uint32)
	if !ok || !cs.hasMinMax {
		t.Fatalf("expected usable min/max, got %+v", cs)
	}
	if cs.min.dom != domainUint || cs.min.u != 100 || cs.max.u != 4294967295 {
		t.Fatalf("expected uint domain [100, 4294967295], got %+v", cs)
	}

	// uint64 column: physical int64, high-half values are negative as bits.
	s64 := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	s64.SetMinMax(5, -1) // -1 bits == MaxUint64
	cs, ok = colStatsFromTyped(s64, arrow.PrimitiveTypes.Uint64)
	if !ok || !cs.hasMinMax {
		t.Fatalf("expected usable min/max, got %+v", cs)
	}
	if cs.min.dom != domainUint || cs.min.u != 5 || cs.max.u != 18446744073709551615 {
		t.Fatalf("expected uint domain [5, MaxUint64], got %+v", cs)
	}
}

func TestColStatsFromTyped_PhysicalTypeMismatchKeepsOnlyNullCount(t *testing.T) {
	// An int32-typed stats object for a column registered as int64: the
	// min/max bytes cannot be trusted, but the null count still can.
	s := metadata.NewStatistics(testDescr(t, pq.Types.Int32), nil).(*metadata.Int32Statistics)
	s.SetMinMax(1, 2)
	s.IncNulls(4)

	cs, ok := colStatsFromTyped(s, arrow.PrimitiveTypes.Int64)
	if !ok {
		t.Fatal("null count alone is usable")
	}
	if cs.hasMinMax {
		t.Fatal("physical/Arrow type mismatch must drop min/max")
	}
	if !cs.hasNullCount || cs.nullCount != 4 {
		t.Fatalf("unexpected null count: %+v", cs)
	}
}

func TestColStatsFromTyped_ByteArray(t *testing.T) {
	s := metadata.NewStatistics(testDescr(t, pq.Types.ByteArray), nil).(*metadata.ByteArrayStatistics)
	s.SetMinMax(pq.ByteArray("alpha"), pq.ByteArray("omega"))

	cs, ok := colStatsFromTyped(s, arrow.BinaryTypes.String)
	if !ok || !cs.hasMinMax {
		t.Fatalf("expected usable min/max, got %+v", cs)
	}
	if cs.min.dom != domainBytes || string(cs.min.by) != "alpha" || string(cs.max.by) != "omega" {
		t.Fatalf("unexpected byte stats: %+v", cs)
	}
}

func TestColStatsFromTyped_FloatAndBool(t *testing.T) {
	f := metadata.NewStatistics(testDescr(t, pq.Types.Float), nil).(*metadata.Float32Statistics)
	f.SetMinMax(1.5, 2.5)
	cs, ok := colStatsFromTyped(f, arrow.PrimitiveTypes.Float32)
	if !ok || !cs.hasMinMax || cs.min.dom != domainFloat || cs.min.f != 1.5 || cs.max.f != 2.5 {
		t.Fatalf("unexpected float32 stats: %+v", cs)
	}

	b := metadata.NewStatistics(testDescr(t, pq.Types.Boolean), nil).(*metadata.BooleanStatistics)
	b.SetMinMax(false, true)
	cs, ok = colStatsFromTyped(b, arrow.FixedWidthTypes.Boolean)
	if !ok || !cs.hasMinMax || cs.min.dom != domainBool || cs.min.b != false || cs.max.b != true {
		t.Fatalf("unexpected bool stats: %+v", cs)
	}
}

func TestColStatsFromTyped_TimestampAndDate(t *testing.T) {
	tsStats := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	tsStats.SetMinMax(1_000, 2_000)
	dt := &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	cs, ok := colStatsFromTyped(tsStats, dt)
	if !ok || !cs.hasMinMax || cs.min.dom != domainInt || cs.min.i != 1_000 {
		t.Fatalf("unexpected timestamp stats: %+v", cs)
	}

	dStats := metadata.NewStatistics(testDescr(t, pq.Types.Int32), nil).(*metadata.Int32Statistics)
	dStats.SetMinMax(19_000, 19_100)
	cs, ok = colStatsFromTyped(dStats, arrow.FixedWidthTypes.Date32)
	if !ok || !cs.hasMinMax || cs.min.dom != domainInt || cs.min.i != 19_000 {
		t.Fatalf("unexpected date32 stats: %+v", cs)
	}
}

func TestColStatsFromTyped_UnsupportedArrowType(t *testing.T) {
	// Int96 stats, or an Arrow type outside the mapping (e.g. date64),
	// must not produce min/max.
	s := metadata.NewStatistics(testDescr(t, pq.Types.Int64), nil).(*metadata.Int64Statistics)
	s.SetMinMax(1, 2)
	cs, ok := colStatsFromTyped(s, arrow.FixedWidthTypes.Date64)
	if ok && cs.hasMinMax {
		t.Fatalf("date64 must not get min/max from int64 stats: %+v", cs)
	}
}
