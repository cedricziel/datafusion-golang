// Package closingreader wraps an array.RecordReader so that Release, or
// running the underlying stream to exhaustion, also closes the object or
// file the scan opened — the same idempotent-close shape providers/csv,
// providers/jsonl, and providers/parquet each need for their Scan.
package closingreader

import (
	"io"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// Reader wraps rr so that closer is closed exactly once, when rr is
// exhausted or Released, whichever comes first.
type Reader struct {
	array.RecordReader
	closer io.Closer
	closed bool
}

// New returns a Reader that closes closer when rr is exhausted or Released.
func New(rr array.RecordReader, closer io.Closer) *Reader {
	return &Reader{RecordReader: rr, closer: closer}
}

func (r *Reader) Next() bool {
	ok := r.RecordReader.Next()
	if !ok {
		r.close()
	}
	return ok
}

func (r *Reader) Release() {
	r.RecordReader.Release()
	r.close()
}

func (r *Reader) close() {
	if r.closed {
		return
	}
	r.closed = true
	_ = r.closer.Close()
}
