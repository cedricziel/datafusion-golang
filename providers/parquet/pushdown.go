package parquet

import (
	"context"
	"fmt"
	"math"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
)

// ScanWithOptions starts a scan that honors the projection exactly, uses
// the pushed filters to skip row groups whose statistics or bloom filters
// prove they contain no matching row (never skipping on uncertainty — the
// filters are advisory, and the engine re-applies every one), and stops
// early once the limit hint is satisfied.
func (t *tableProvider) ScanWithOptions(ctx context.Context, opts *datafusion.ScanOptions) (array.RecordReader, error) {
	if opts == nil {
		return t.Scan(ctx)
	}

	rdr, err := file.OpenParquetFile(t.path, false)
	if err != nil {
		return nil, fmt.Errorf("parquet: open %s: %w", t.path, err)
	}

	fr, err := pqarrow.NewFileReader(rdr, readProps, memory.DefaultAllocator)
	if err != nil {
		_ = rdr.Close()
		return nil, fmt.Errorf("parquet: read %s: %w", t.path, err)
	}

	rowGroups := selectRowGroups(rdr, fr, t.schema, opts.Filters)
	if rowGroups != nil && len(rowGroups) == 0 {
		// Every row group is provably free of matches: zero rows is a
		// valid result, and skipping the reader avoids relying on what an
		// explicitly empty row-group list would mean to GetRecordReader.
		_ = rdr.Close()
		return emptyProjectedReader(t.schema, opts.Projection)
	}

	reader, err := t.openProjectedReader(ctx, rdr, fr, opts.Projection, rowGroups)
	if err != nil {
		_ = rdr.Close()
		return nil, err
	}
	if opts.Limit >= 0 {
		reader = &limitReader{RecordReader: reader, remaining: opts.Limit}
	}
	return reader, nil
}

// openProjectedReader opens the record reader over the surviving row
// groups with exactly the projected columns, in projection order
// (design D1). rowGroups nil means all.
func (t *tableProvider) openProjectedReader(ctx context.Context, rdr *file.Reader, fr *pqarrow.FileReader, projection []int, rowGroups []int) (array.RecordReader, error) {
	if projection == nil {
		rr, err := fr.GetRecordReader(ctx, nil, rowGroups)
		if err != nil {
			return nil, fmt.Errorf("parquet: scan %s: %w", t.path, err)
		}
		return &closingRecordReader{RecordReader: rr, file: rdr}, nil
	}

	if len(projection) == 0 {
		// Zero-column projection: the engine only needs row counts, but
		// GetRecordReader with no indices reads everything, so read the
		// single cheapest column and project it away.
		rr, err := fr.GetRecordReader(ctx, []int{cheapestLeaf(rdr, rowGroups)}, rowGroups)
		if err != nil {
			return nil, fmt.Errorf("parquet: scan %s: %w", t.path, err)
		}
		return datafusion.ProjectReader(&closingRecordReader{RecordReader: rr, file: rdr}, []int{}), nil
	}

	// Deduplicate projected fields in first-appearance order — that is
	// the column order GetRecordReader yields (GetFieldIndices dedups
	// leaf roots the same way).
	readOrder := make([]int, 0, len(projection))
	posInRead := make(map[int]int, len(projection))
	var leaves []int
	for _, fieldIdx := range projection {
		if fieldIdx < 0 || fieldIdx >= len(fr.Manifest.Fields) {
			return nil, fmt.Errorf("parquet: projection index %d out of range for %s", fieldIdx, t.path)
		}
		if _, seen := posInRead[fieldIdx]; seen {
			continue
		}
		posInRead[fieldIdx] = len(readOrder)
		readOrder = append(readOrder, fieldIdx)
		leaves = append(leaves, fieldLeaves(&fr.Manifest.Fields[fieldIdx])...)
	}

	rr, err := fr.GetRecordReader(ctx, leaves, rowGroups)
	if err != nil {
		return nil, fmt.Errorf("parquet: scan %s: %w", t.path, err)
	}
	var reader array.RecordReader = &closingRecordReader{RecordReader: rr, file: rdr}

	if len(readOrder) != len(projection) {
		// Duplicated projection entries: re-expand the deduplicated read.
		perm := make([]int, len(projection))
		for i, fieldIdx := range projection {
			perm[i] = posInRead[fieldIdx]
		}
		reader = datafusion.ProjectReader(reader, perm)
	}
	return reader, nil
}

// selectRowGroups returns the row groups a scan must read: every group
// except those the filters provably rule out. nil means "all" (no filters,
// or nothing skipped).
func selectRowGroups(rdr *file.Reader, fr *pqarrow.FileReader, sc *arrow.Schema, filters []datafusion.Expr) []int {
	if len(filters) == 0 {
		return nil
	}
	fields := referencedFields(filters, sc, fr.Manifest)
	if len(fields) == 0 {
		return nil
	}
	md := rdr.MetaData()
	n := rdr.NumRowGroups()
	survivors := make([]int, 0, n)
	for i := 0; i < n; i++ {
		st := buildRowGroupStats(md, fr.Manifest, sc, i, fields)
		bloom := &rowGroupBloomProber{reader: rdr, manifest: fr.Manifest, schema: sc, rgIdx: i}
		skip := false
		for _, f := range filters {
			// Filters combine with AND: one provably-empty conjunct
			// empties the whole row group.
			if canSkip(f, st, bloom) {
				skip = true
				break
			}
		}
		if !skip {
			survivors = append(survivors, i)
		}
	}
	if len(survivors) == n {
		return nil
	}
	return survivors
}

// referencedFields collects the top-level field indices the filters refer
// to, keeping only references whose index and name agree with the
// registered schema — anything inconsistent gets no statistics and is
// therefore never used to skip.
func referencedFields(filters []datafusion.Expr, sc *arrow.Schema, manifest *pqarrow.SchemaManifest) map[int]struct{} {
	fields := make(map[int]struct{})
	var visit func(e datafusion.Expr)
	addCol := func(c datafusion.Column) {
		if c.Index >= 0 && c.Index < sc.NumFields() && c.Index < len(manifest.Fields) && sc.Field(c.Index).Name == c.Name {
			fields[c.Index] = struct{}{}
		}
	}
	visit = func(e datafusion.Expr) {
		switch e := e.(type) {
		case datafusion.Compare:
			addCol(e.Column)
		case datafusion.IsNull:
			addCol(e.Column)
		case datafusion.Between:
			addCol(e.Column)
		case datafusion.InList:
			addCol(e.Column)
		case datafusion.And:
			visit(e.Left)
			visit(e.Right)
		case datafusion.Or:
			visit(e.Left)
			visit(e.Right)
		case datafusion.Not:
			visit(e.Expr)
		}
	}
	for _, f := range filters {
		visit(f)
	}
	return fields
}

// fieldLeaves returns the Parquet leaf column indices under one top-level
// field, in schema order: a primitive field is its own single leaf, a
// nested field contributes all its leaves.
func fieldLeaves(sf *pqarrow.SchemaField) []int {
	if sf.IsLeaf() {
		return []int{sf.ColIndex}
	}
	var out []int
	for i := range sf.Children {
		out = append(out, fieldLeaves(&sf.Children[i])...)
	}
	return out
}

// cheapestLeaf picks the leaf column with the fewest total compressed
// bytes across the row groups to be read, for the zero-column projection
// case. rowGroups nil means all.
func cheapestLeaf(rdr *file.Reader, rowGroups []int) int {
	md := rdr.MetaData()
	if rowGroups == nil {
		rowGroups = make([]int, rdr.NumRowGroups())
		for i := range rowGroups {
			rowGroups[i] = i
		}
	}
	best, bestSize := 0, int64(math.MaxInt64)
	for leaf := 0; leaf < md.Schema.NumColumns(); leaf++ {
		var total int64
		for _, g := range rowGroups {
			cc, err := md.RowGroup(g).ColumnChunk(leaf)
			if err != nil {
				total = math.MaxInt64
				break
			}
			total += cc.TotalCompressedSize()
		}
		if total < bestSize {
			best, bestSize = leaf, total
		}
	}
	return best
}

// emptyProjectedReader returns a zero-row reader carrying exactly the
// projected schema.
func emptyProjectedReader(sc *arrow.Schema, projection []int) (array.RecordReader, error) {
	fields := sc.Fields()
	if projection != nil {
		fields = make([]arrow.Field, len(projection))
		for i, idx := range projection {
			if idx < 0 || idx >= sc.NumFields() {
				return nil, fmt.Errorf("parquet: projection index %d out of range", idx)
			}
			fields[i] = sc.Field(idx)
		}
	}
	return array.NewRecordReader(arrow.NewSchema(fields, nil), nil)
}

// limitReader stops producing batches once at least the hinted number of
// rows has been produced; the engine trims any excess in the final batch.
type limitReader struct {
	array.RecordReader
	remaining int64
}

func (r *limitReader) Next() bool {
	if r.remaining <= 0 {
		return false
	}
	if !r.RecordReader.Next() {
		return false
	}
	r.remaining -= r.RecordReader.RecordBatch().NumRows()
	return true
}

// Record implements the deprecated accessor of array.RecordReader.
func (r *limitReader) Record() arrow.RecordBatch { return r.RecordBatch() }
