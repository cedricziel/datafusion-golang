package parquet

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pq "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
)

// writeFile writes one row group per batch with the given writer properties.
func writeFile(t *testing.T, path string, sc *arrow.Schema, props *pq.WriterProperties, batches ...arrow.RecordBatch) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	fw, err := pqarrow.NewFileWriter(sc, f, props, pqarrow.DefaultWriterProps())
	if err != nil {
		t.Fatalf("NewFileWriter: %v", err)
	}
	for i, rec := range batches {
		if err := fw.Write(rec); err != nil {
			t.Fatalf("Write batch %d: %v", i, err)
		}
	}
	if err := fw.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
}

func idBatch(t *testing.T, sc *arrow.Schema, ids []int64) arrow.RecordBatch {
	t.Helper()
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	for i, id := range ids {
		_ = i
		b.Field(1).(*array.StringBuilder).Append(string(rune('a' + id%26)))
	}
	return b.NewRecordBatch()
}

func openReaders(t *testing.T, path string) (*file.Reader, *pqarrow.FileReader, *arrow.Schema) {
	t.Helper()
	rdr, err := file.OpenParquetFile(path, false)
	if err != nil {
		t.Fatalf("OpenParquetFile: %v", err)
	}
	t.Cleanup(func() { _ = rdr.Close() })
	fr, err := pqarrow.NewFileReader(rdr, readProps, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("NewFileReader: %v", err)
	}
	sc, err := fr.Schema()
	if err != nil {
		t.Fatalf("Schema: %v", err)
	}
	return rdr, fr, sc
}

func TestFieldLeaves_FlatAndNested(t *testing.T) {
	sc := arrow.NewSchema([]arrow.Field{
		{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.StructOf(
			arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			arrow.Field{Name: "y", Type: arrow.BinaryTypes.String, Nullable: true},
		), Nullable: true},
		{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	dir := t.TempDir()
	path := filepath.Join(dir, "nested.parquet")
	b := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	b.Field(0).(*array.Int64Builder).Append(1)
	sb := b.Field(1).(*array.StructBuilder)
	sb.Append(true)
	sb.FieldBuilder(0).(*array.Int64Builder).Append(2)
	sb.FieldBuilder(1).(*array.StringBuilder).Append("z")
	b.Field(2).(*array.StringBuilder).Append("w")
	rec := b.NewRecordBatch()
	b.Release()
	defer rec.Release()
	writeFile(t, path, sc, pq.NewWriterProperties(), rec)

	_, fr, _ := openReaders(t, path)
	if got := fieldLeaves(&fr.Manifest.Fields[0]); len(got) != 1 || got[0] != 0 {
		t.Fatalf("field a leaves: %v", got)
	}
	if got := fieldLeaves(&fr.Manifest.Fields[1]); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("field s leaves: %v", got)
	}
	if got := fieldLeaves(&fr.Manifest.Fields[2]); len(got) != 1 || got[0] != 3 {
		t.Fatalf("field b leaves: %v", got)
	}
}

func idNameSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
}

// threeGroupFile writes row groups with id ranges [1,10], [11,20], [21,30].
func threeGroupFile(t *testing.T, props *pq.WriterProperties) string {
	t.Helper()
	sc := idNameSchema()
	dir := t.TempDir()
	path := filepath.Join(dir, "groups.parquet")
	var batches []arrow.RecordBatch
	for g := 0; g < 3; g++ {
		ids := make([]int64, 10)
		for i := range ids {
			ids[i] = int64(g*10 + i + 1)
		}
		rec := idBatch(t, sc, ids)
		defer rec.Release()
		batches = append(batches, rec)
	}
	writeFile(t, path, sc, props, batches...)
	return path
}

func idCol() datafusion.Column { return datafusion.Column{Name: "id", Index: 0} }

func TestSelectRowGroups_PrunesDisjointGroups(t *testing.T) {
	path := threeGroupFile(t, pq.NewWriterProperties())
	rdr, fr, sc := openReaders(t, path)

	gt15 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareGt, Literal: i64(15)}
	got := selectRowGroups(rdr, fr, sc, []datafusion.Expr{gt15})
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("id > 15 must keep row groups 1 and 2, got %v", got)
	}

	eq5 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareEq, Literal: i64(5)}
	got = selectRowGroups(rdr, fr, sc, []datafusion.Expr{eq5})
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("id = 5 must keep only row group 0, got %v", got)
	}

	gt100 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareGt, Literal: i64(100)}
	got = selectRowGroups(rdr, fr, sc, []datafusion.Expr{gt100})
	if got == nil || len(got) != 0 {
		t.Fatalf("id > 100 must skip every row group, got %v", got)
	}

	// No filters: nil (read all).
	if got := selectRowGroups(rdr, fr, sc, nil); got != nil {
		t.Fatalf("no filters must select all, got %v", got)
	}
}

func TestSelectRowGroups_MissingStatsKeepsEverything(t *testing.T) {
	path := threeGroupFile(t, pq.NewWriterProperties(pq.WithStats(false)))
	rdr, fr, sc := openReaders(t, path)

	gt100 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareGt, Literal: i64(100)}
	if got := selectRowGroups(rdr, fr, sc, []datafusion.Expr{gt100}); got != nil {
		t.Fatalf("without statistics nothing may be skipped, got %v", got)
	}
}

func TestSelectRowGroups_MismatchedColumnReferenceKeepsEverything(t *testing.T) {
	path := threeGroupFile(t, pq.NewWriterProperties())
	rdr, fr, sc := openReaders(t, path)

	// Index 0 is "id", but the name says otherwise: never trust it.
	bad := datafusion.Compare{
		Column:  datafusion.Column{Name: "name", Index: 0},
		Op:      datafusion.CompareGt,
		Literal: i64(100),
	}
	if got := selectRowGroups(rdr, fr, sc, []datafusion.Expr{bad}); got != nil {
		t.Fatalf("mismatched column name/index must not skip, got %v", got)
	}
}

func TestSelectRowGroups_BloomFilterSkipsAbsentEquality(t *testing.T) {
	// Row group 0 has even ids 2..20, row group 1 odd ids 21..39; min/max
	// alone cannot rule out eq 5 in group 0, the bloom filter can.
	sc := idNameSchema()
	dir := t.TempDir()
	path := filepath.Join(dir, "bloom.parquet")
	even := make([]int64, 10)
	for i := range even {
		even[i] = int64(2*i + 2)
	}
	odd := make([]int64, 10)
	for i := range odd {
		odd[i] = int64(21 + 2*i)
	}
	b1 := idBatch(t, sc, even)
	defer b1.Release()
	b2 := idBatch(t, sc, odd)
	defer b2.Release()
	writeFile(t, path, sc, pq.NewWriterProperties(pq.WithBloomFilterEnabled(true)), b1, b2)

	rdr, fr, arrsc := openReaders(t, path)

	eq5 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareEq, Literal: i64(5)}
	got := selectRowGroups(rdr, fr, arrsc, []datafusion.Expr{eq5})
	if got == nil || len(got) != 0 {
		t.Fatalf("eq 5: absent from both bloom filters, want no survivors, got %v", got)
	}

	eq4 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareEq, Literal: i64(4)}
	got = selectRowGroups(rdr, fr, arrsc, []datafusion.Expr{eq4})
	if len(got) != 1 || got[0] != 0 {
		t.Fatalf("eq 4: present in group 0, absent from group 1, got %v", got)
	}

	// IN list: every element bloom-absent or out of range.
	in := datafusion.InList{Column: idCol(), List: []datafusion.Literal{i64(5), i64(41)}}
	got = selectRowGroups(rdr, fr, arrsc, []datafusion.Expr{in})
	if got == nil || len(got) != 0 {
		t.Fatalf("IN (5, 41): want no survivors, got %v", got)
	}
}

func TestSelectRowGroups_BloomFalsePositiveKeeps(t *testing.T) {
	// Stats disabled so min/max cannot decide; only the bloom filter can
	// skip. Hunt for a value the filter wrongly reports as "maybe
	// present": that row group must be kept (and the engine's re-applied
	// filter then removes the non-matching rows).
	sc := idNameSchema()
	dir := t.TempDir()
	path := filepath.Join(dir, "bloom-fp.parquet")
	ids := make([]int64, 20)
	for i := range ids {
		ids[i] = int64(i)
	}
	rec := idBatch(t, sc, ids)
	defer rec.Release()
	// A deliberately tiny, high-FPP filter makes a false positive near
	// certain within the probed range.
	writeFile(t, path, sc, pq.NewWriterProperties(
		pq.WithStats(false),
		pq.WithBloomFilterEnabled(true),
		pq.WithBloomFilterNDV(20),
		pq.WithBloomFilterFPP(0.4),
	), rec)

	rdr, fr, arrsc := openReaders(t, path)
	prober := &rowGroupBloomProber{reader: rdr, manifest: fr.Manifest, schema: arrsc, rgIdx: 0}

	falsePositive := int64(-1)
	for v := int64(1_000); v < 200_000; v++ {
		if !prober.absent(0, i64(v)) {
			falsePositive = v
			break
		}
	}
	if falsePositive < 0 {
		t.Skip("no bloom false positive found in the probed range")
	}

	eqFP := datafusion.Compare{Column: idCol(), Op: datafusion.CompareEq, Literal: i64(falsePositive)}
	got := selectRowGroups(rdr, fr, arrsc, []datafusion.Expr{eqFP})
	if got != nil {
		t.Fatalf("bloom maybe-present must keep the row group, got %v", got)
	}
}

func TestCheapestLeaf(t *testing.T) {
	// Column "id" (int64, 10 distinct values) compresses larger than
	// "name" (single repeated letter per group), but what matters is the
	// function picks the provably smallest chunk without error.
	path := threeGroupFile(t, pq.NewWriterProperties())
	rdr, _, _ := openReaders(t, path)
	leaf := cheapestLeaf(rdr, nil)
	if leaf < 0 || leaf >= rdr.MetaData().Schema.NumColumns() {
		t.Fatalf("cheapestLeaf out of range: %d", leaf)
	}
	md := rdr.MetaData()
	sizeOf := func(l int) int64 {
		var total int64
		for g := 0; g < rdr.NumRowGroups(); g++ {
			cc, err := md.RowGroup(g).ColumnChunk(l)
			if err != nil {
				t.Fatalf("ColumnChunk: %v", err)
			}
			total += cc.TotalCompressedSize()
		}
		return total
	}
	for l := 0; l < md.Schema.NumColumns(); l++ {
		if sizeOf(leaf) > sizeOf(l) {
			t.Fatalf("leaf %d (size %d) is not the cheapest; %d is smaller (%d)", leaf, sizeOf(leaf), l, sizeOf(l))
		}
	}
}

// byteRange is a half-open [off, off+len) span, used by
// TestScanWithOptions_PrunedRowGroupBytesNeverRequested to check that a
// pruned row group's on-disk bytes are never requested from the backend
// (spec: object-store's "Reads are ranged, not whole-object" and
// parquet-table-provider's "Scans read selectively from the storage
// backend").
type byteRange struct{ off, len int64 }

func (r byteRange) overlaps(o byteRange) bool {
	return r.off < o.off+o.len && o.off < r.off+r.len
}

// recordingStore wraps a Store and records every ReadAt range performed
// through it, so a test can assert which byte ranges a scan actually
// requested from the backend.
type recordingStore struct {
	objectstore.Store
	mu     sync.Mutex
	ranges []byteRange
}

func (s *recordingStore) Open(ctx context.Context, path string) (objectstore.Object, error) {
	obj, err := s.Store.Open(ctx, path)
	if err != nil {
		return nil, err
	}
	return &recordingObject{Object: obj, store: s}, nil
}

type recordingObject struct {
	objectstore.Object
	store *recordingStore
}

func (o *recordingObject) ReadAt(p []byte, off int64) (int, error) {
	n, err := o.Object.ReadAt(p, off)
	o.store.mu.Lock()
	o.store.ranges = append(o.store.ranges, byteRange{off: off, len: int64(n)})
	o.store.mu.Unlock()
	return n, err
}

// columnChunkRange returns the on-disk byte span of column chunk col in
// row group rg — from its dictionary page (if any, else its first data
// page) through its total compressed size.
func columnChunkRange(t *testing.T, rdr *file.Reader, rg, col int) byteRange {
	t.Helper()
	cc, err := rdr.MetaData().RowGroup(rg).ColumnChunk(col)
	if err != nil {
		t.Fatalf("ColumnChunk(%d, %d): %v", rg, col, err)
	}
	start := cc.DataPageOffset()
	if cc.HasDictionaryPage() && cc.DictionaryPageOffset() < start {
		start = cc.DictionaryPageOffset()
	}
	return byteRange{off: start, len: cc.TotalCompressedSize()}
}

func TestScanWithOptions_PrunedRowGroupBytesNeverRequested(t *testing.T) {
	path := threeGroupFile(t, pq.NewWriterProperties())

	rdr, _, _ := openReaders(t, path)
	var excluded []byteRange
	for col := 0; col < rdr.MetaData().Schema.NumColumns(); col++ {
		excluded = append(excluded, columnChunkRange(t, rdr, 0, col))
	}

	rec := &recordingStore{Store: objectstore.NewLocalStore()}
	tp, err := NewTableProvider(context.Background(), path, WithStore(rec))
	if err != nil {
		t.Fatalf("NewTableProvider: %v", err)
	}
	pd := tp.(datafusion.PushdownTableProvider)

	// id > 15 provably excludes row group 0 (ids 1..10).
	gt15 := datafusion.Compare{Column: idCol(), Op: datafusion.CompareGt, Literal: i64(15)}
	reader, err := pd.ScanWithOptions(context.Background(), &datafusion.ScanOptions{
		Projection: nil,
		Filters:    []datafusion.Expr{gt15},
		Limit:      -1,
	})
	if err != nil {
		t.Fatalf("ScanWithOptions: %v", err)
	}
	for reader.Next() {
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reading scan: %v", err)
	}
	reader.Release()

	for _, got := range rec.ranges {
		for _, ex := range excluded {
			if got.overlaps(ex) {
				t.Fatalf("scan requested range [%d,%d) which overlaps pruned row group 0's range [%d,%d)",
					got.off, got.off+got.len, ex.off, ex.off+ex.len)
			}
		}
	}
}
