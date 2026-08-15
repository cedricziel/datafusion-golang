package closingreader_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/cedricziel/datafusion-golang/providers/internal/closingreader"
)

type countingCloser struct{ closes int }

func (c *countingCloser) Close() error {
	c.closes++
	return nil
}

func emptyReader(t *testing.T) array.RecordReader {
	t.Helper()
	sc := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	rr, err := array.NewRecordReader(sc, nil)
	if err != nil {
		t.Fatalf("NewRecordReader: %v", err)
	}
	return rr
}

func TestReader_ClosesOnExhaustion(t *testing.T) {
	closer := &countingCloser{}
	r := closingreader.New(emptyReader(t), closer)
	defer r.Release()

	if r.Next() {
		t.Fatal("expected an empty reader to yield no batches")
	}
	if closer.closes != 1 {
		t.Fatalf("closes = %d, want 1 after exhaustion", closer.closes)
	}
}

func TestReader_ReleaseIsIdempotent(t *testing.T) {
	closer := &countingCloser{}
	r := closingreader.New(emptyReader(t), closer)

	r.Release()
	r.Release()
	if closer.closes != 1 {
		t.Fatalf("closes = %d, want exactly 1 across two Release calls", closer.closes)
	}
}
