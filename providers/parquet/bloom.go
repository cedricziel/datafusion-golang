package parquet

import (
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// rowGroupBloomProber implements bloomProber over one row group's bloom
// filters (design D5). All I/O is lazy: nothing is read until the
// evaluator actually probes an equality value, so scans without eq/IN
// predicates never touch bloom filter pages. A probe returns true only on
// a definite absence; every uncertain path (no filter, unsupported or
// mismatched types, I/O errors) returns false (keep).
type rowGroupBloomProber struct {
	reader   *file.Reader
	manifest *pqarrow.SchemaManifest
	schema   *arrow.Schema
	rgIdx    int

	rgReader    *metadata.RowGroupBloomFilterReader
	rgReaderErr bool
	cache       map[int]metadata.BloomFilter // leaf index -> filter (nil = none)
}

func (p *rowGroupBloomProber) absent(fieldIdx int, lit datafusion.Literal) bool {
	if fieldIdx < 0 || fieldIdx >= len(p.manifest.Fields) || fieldIdx >= p.schema.NumFields() {
		return false
	}
	sf := &p.manifest.Fields[fieldIdx]
	if !sf.IsLeaf() {
		return false
	}
	probe, ok := bloomProbe(lit, p.schema.Field(fieldIdx).Type)
	if !ok {
		return false
	}
	bf := p.filterFor(sf.ColIndex)
	if bf == nil {
		return false
	}
	// Check == false is a definite absence (bloom filters have false
	// positives, never false negatives), so skipping is safe.
	return !probe(bf)
}

func (p *rowGroupBloomProber) filterFor(leafIdx int) metadata.BloomFilter {
	if bf, ok := p.cache[leafIdx]; ok {
		return bf
	}
	if p.rgReader == nil && !p.rgReaderErr {
		rg, err := p.reader.GetBloomFilterReader().RowGroup(p.rgIdx)
		if err != nil || rg == nil {
			p.rgReaderErr = true
		} else {
			p.rgReader = rg
		}
	}
	if p.rgReader == nil {
		return nil
	}
	bf, err := p.rgReader.GetColumnBloomFilter(leafIdx)
	if err != nil {
		bf = nil
	}
	if p.cache == nil {
		p.cache = make(map[int]metadata.BloomFilter)
	}
	p.cache[leafIdx] = bf
	return bf
}

// bloomProbe converts a pushed literal into the column's physical
// representation — exactly the bytes the writer hashed — and returns a
// function running the typed membership check. The allowlist is strict
// (design D5): floats are excluded because 0.0 and -0.0 are equal in SQL
// but hash differently, and bools are excluded as pointless. Any type or
// unit mismatch yields no probe.
func bloomProbe(l datafusion.Literal, dt arrow.DataType) (func(metadata.BloomFilter) bool, bool) {
	switch dt.ID() {
	case arrow.STRING, arrow.LARGE_STRING:
		if v, ok := l.Value.(string); ok && l.Type == datafusion.LiteralUtf8 {
			return checkFn(pq.ByteArray(v)), true
		}
	case arrow.BINARY, arrow.LARGE_BINARY:
		if v, ok := l.Value.([]byte); ok && l.Type == datafusion.LiteralBinary {
			return checkFn(pq.ByteArray(v)), true
		}
	case arrow.FIXED_SIZE_BINARY:
		if v, ok := l.Value.([]byte); ok && l.Type == datafusion.LiteralBinary {
			return checkFn(pq.FixedLenByteArray(v)), true
		}
	case arrow.INT8, arrow.INT16, arrow.INT32:
		if v, ok := signedLiteral(l); ok && v >= math.MinInt32 && v <= math.MaxInt32 {
			return checkFn(int32(v)), true
		}
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		// Physical int32 stores the unsigned bit pattern (zero-extended
		// for the narrow widths).
		if v, ok := unsignedLiteral(l); ok && v <= math.MaxUint32 {
			return checkFn(int32(uint32(v))), true
		}
	case arrow.INT64:
		if v, ok := signedLiteral(l); ok {
			return checkFn(v), true
		}
	case arrow.UINT64:
		if v, ok := unsignedLiteral(l); ok {
			return checkFn(int64(v)), true
		}
	case arrow.DATE32:
		if v, ok := l.Value.(int32); ok && l.Type == datafusion.LiteralDate32 {
			return checkFn(v), true
		}
	case arrow.TIMESTAMP:
		if v, ok := l.Value.(int64); ok && l.Type == datafusion.LiteralTimestamp {
			if ts, tok := dt.(*arrow.TimestampType); tok && unitMatches(l.Unit, ts.Unit) {
				return checkFn(v), true
			}
		}
	}
	return nil, false
}

func checkFn[T pq.ColumnTypes](v T) func(metadata.BloomFilter) bool {
	return func(bf metadata.BloomFilter) bool {
		return (&metadata.TypedBloomFilter[T]{BloomFilter: bf}).Check(v)
	}
}
