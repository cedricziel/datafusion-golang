package parquet

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	pqparquet "github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/cedricziel/datafusion-golang/datafusion"
	"github.com/cedricziel/datafusion-golang/objectstore"
)

var (
	writeProps      = pqparquet.NewWriterProperties()
	arrowWriteProps = pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
)

// keepOpenWriter wraps an objectstore.Writer so passing it to
// pqarrow.NewFileWriter does not hand the writer authority to commit:
// FileWriter.Close closes its underlying io.Writer when that writer
// implements io.Closer, which would otherwise commit the object as a
// side effect of the parquet writer finalizing its footer, before
// rewrite has decided the insert succeeded (design D1's durability
// requirement — commit is the last, explicit step).
type keepOpenWriter struct{ objectstore.Writer }

func (keepOpenWriter) Close() error { return nil }

// rebindSchema returns a zero-copy view of rec under schema, sharing rec's
// column arrays. pqarrow.FileWriter.Write requires the record's schema to
// be exactly equal to the writer's — including per-field metadata such as
// the PARQUET:field_id that pqarrow.FileReader.Schema stamps on t.schema
// when it was derived from an existing file. Batches arriving from the
// insert stream (engine-delivered or freshly built) are only logically
// equivalent to t.schema, not byte-identical, so they must be rebound
// before Write. The caller must Release the result.
func rebindSchema(rec arrow.RecordBatch, schema *arrow.Schema) arrow.RecordBatch {
	return array.NewRecordBatch(schema, rec.Columns(), rec.NumRows())
}

// InsertInto implements datafusion.WritableTableProvider. Append and
// Overwrite are both delivered as "write a complete new object, then
// commit it over the original via the store's Writer" — Parquet's
// footer-at-end format has no API to append to a closed file. Append
// additionally streams the existing object's rows ahead of the insert's
// rows; Overwrite writes only the insert's rows. Any failure — reading
// the input, writing, or finalizing — aborts the write and leaves the
// original object untouched; only a successful commit (Writer.Close)
// publishes the new one. Commit atomicity is per-backend: see
// objectstore's Writer contract and design D5 for what each backend
// family guarantees.
//
// The reported count is the number of rows consumed from rows, not the
// table's resulting size, matching Overwrite's "replaced size" ambiguity
// and the engine's "provider's word, forwarded verbatim" contract. A
// zero-row Append short-circuits without touching the file at all; a
// zero-row Overwrite still writes a valid, empty (schema-only) file.
func (t *tableProvider) InsertInto(ctx context.Context, op datafusion.InsertOp, rows array.RecordReader) (uint64, error) {
	defer rows.Release()

	if op != datafusion.InsertAppend && op != datafusion.InsertOverwrite {
		return 0, fmt.Errorf("parquet: insert op %d not supported (only Append and Overwrite)", op)
	}

	t.insertMu.Lock()
	defer t.insertMu.Unlock()

	hasFirst := rows.Next()
	if !hasFirst {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("parquet: reading insert input: %w", err)
		}
		if op == datafusion.InsertAppend {
			return 0, nil
		}
	}

	return t.rewrite(ctx, op, rows, hasFirst)
}

// rewrite performs the commit-on-Close write described on InsertInto.
// hasFirst indicates rows.Next() already returned true once (its current
// batch is the first one to write) before rewrite was called; the
// zero-row-Overwrite case calls this with hasFirst false.
func (t *tableProvider) rewrite(ctx context.Context, op datafusion.InsertOp, rows array.RecordReader, hasFirst bool) (rowCount uint64, err error) {
	w, err := t.store.Create(ctx, t.path)
	if err != nil {
		return 0, fmt.Errorf("parquet: creating writer for insert: %w", err)
	}
	// Cleanup on any path that doesn't reach the final commit: abort the
	// write, leaving the original object intact.
	committed := false
	defer func() {
		if !committed {
			_ = w.Abort()
		}
	}()

	fw, err := pqarrow.NewFileWriter(t.schema, keepOpenWriter{w}, writeProps, arrowWriteProps)
	if err != nil {
		return 0, fmt.Errorf("parquet: opening writer for insert: %w", err)
	}

	if op == datafusion.InsertAppend {
		if err := t.streamExistingRows(ctx, fw); err != nil {
			return 0, err
		}
	}

	writeBatch := func(rec arrow.RecordBatch) error {
		bound := rebindSchema(rec, t.schema)
		defer bound.Release()
		if err := fw.Write(bound); err != nil {
			return fmt.Errorf("parquet: writing insert batch: %w", err)
		}
		rowCount += uint64(bound.NumRows())
		return nil
	}

	if hasFirst {
		if err := writeBatch(rows.RecordBatch()); err != nil {
			return 0, err
		}
	}
	for rows.Next() {
		if err := writeBatch(rows.RecordBatch()); err != nil {
			return 0, err
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("parquet: reading insert input: %w", err)
	}

	if err := fw.Close(); err != nil {
		return 0, fmt.Errorf("parquet: finalizing insert: %w", err)
	}
	if err := w.Close(); err != nil {
		return 0, fmt.Errorf("parquet: committing insert: %w", err)
	}
	committed = true

	return rowCount, nil
}

// streamExistingRows writes every row currently in the table (via the
// provider's own scan machinery) to fw, ahead of the insert's rows —
// the "old rows first" half of Append.
func (t *tableProvider) streamExistingRows(ctx context.Context, fw *pqarrow.FileWriter) error {
	existing, err := t.Scan(ctx)
	if err != nil {
		return fmt.Errorf("parquet: reading existing rows for append: %w", err)
	}
	defer existing.Release()
	for existing.Next() {
		if err := fw.Write(existing.RecordBatch()); err != nil {
			return fmt.Errorf("parquet: writing existing row for append: %w", err)
		}
	}
	if err := existing.Err(); err != nil {
		return fmt.Errorf("parquet: reading existing rows for append: %w", err)
	}
	return nil
}
