package iceberg

import (
	"iter"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// readerFromSeq adapts an iceberg-go record iterator into an
// array.RecordReader by pumping it on a dedicated goroutine and handing
// batches over a channel.
//
// array.ReaderFromIter is not usable here: it drives the iterator with
// iter.Pull2, whose coroutine is created inside the engine's scan
// callback — a cgo callback goroutine locked to its OS thread — and later
// resumed from other engine threads, which the Go runtime forbids
// ("coro: OS thread locking must match locking at coroutine creation").
// A plain goroutine plus channel has no thread affinity.
type seqReader struct {
	schema   *arrow.Schema
	ch       <-chan seqItem
	cancel   chan struct{}
	cur      arrow.RecordBatch
	err      error
	refCount atomic.Int64
}

type seqItem struct {
	rec arrow.RecordBatch
	err error
}

func readerFromSeq(schema *arrow.Schema, itr iter.Seq2[arrow.RecordBatch, error]) array.RecordReader {
	ch := make(chan seqItem)
	cancel := make(chan struct{})
	go func() {
		defer close(ch)
		for rec, err := range itr {
			select {
			case ch <- seqItem{rec: rec, err: err}:
				if err != nil {
					return
				}
			case <-cancel:
				if rec != nil {
					rec.Release()
				}
				return
			}
		}
	}()
	r := &seqReader{schema: schema, ch: ch, cancel: cancel}
	r.refCount.Add(1)
	return r
}

func (r *seqReader) Schema() *arrow.Schema { return r.schema }

func (r *seqReader) Next() bool {
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
	if r.err != nil {
		return false
	}
	item, ok := <-r.ch
	if !ok {
		return false
	}
	if item.err != nil {
		r.err = item.err
		if item.rec != nil {
			item.rec.Release()
		}
		return false
	}
	r.cur = item.rec
	return true
}

func (r *seqReader) RecordBatch() arrow.RecordBatch { return r.cur }

// Record implements the deprecated accessor of array.RecordReader.
func (r *seqReader) Record() arrow.RecordBatch { return r.cur }

func (r *seqReader) Err() error { return r.err }

func (r *seqReader) Retain() { r.refCount.Add(1) }

func (r *seqReader) Release() {
	if r.refCount.Add(-1) != 0 {
		return
	}
	close(r.cancel)
	// Drain so the producer goroutine exits and in-flight batches are
	// released.
	for item := range r.ch {
		if item.rec != nil {
			item.rec.Release()
		}
	}
	if r.cur != nil {
		r.cur.Release()
		r.cur = nil
	}
}
