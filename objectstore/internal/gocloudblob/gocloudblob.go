// Package gocloudblob adapts a gocloud.dev/blob Bucket to the
// objectstore.Store interface. It is the one implementation shared by the
// S3, GCS, and Azure Blob backends (design D1) — each cloud subpackage is
// just URL-scheme registration over this adapter, tested here against
// gocloud's in-memory memblob driver so the adapter itself needs no
// cloud credentials to verify.
package gocloudblob

import (
	"context"
	"fmt"
	"io"
	"iter"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/cedricziel/datafusion-golang/objectstore"
)

// New wraps bucket as an objectstore.Store.
func New(bucket *blob.Bucket) objectstore.Store {
	return &store{bucket: bucket}
}

type store struct {
	bucket *blob.Bucket
}

// Open looks up the object's size via one Attributes call (metadata only,
// no data read) and returns a handle whose Read/Seek/ReadAt are all
// derived from ReadAt via io.SectionReader — each ReadAt issues one
// ranged NewRangeReader call, so a pushdown-pruned scan costs one
// Attributes call plus one range per surviving byte range (design D4).
func (s *store) Open(ctx context.Context, key string) (objectstore.Object, error) {
	attrs, err := s.bucket.Attributes(ctx, key)
	if err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return nil, fmt.Errorf("objectstore: open %q: %w", key, objectstore.ErrNotExist)
		}
		return nil, fmt.Errorf("objectstore: open %q: %w", key, err)
	}
	ra := &rangeReaderAt{ctx: ctx, bucket: s.bucket, key: key}
	return &object{SectionReader: io.NewSectionReader(ra, 0, attrs.Size)}, nil
}

// rangeReaderAt issues one ranged read per ReadAt call; it does no
// buffering or coalescing of its own (design Non-Goals).
type rangeReaderAt struct {
	ctx    context.Context
	bucket *blob.Bucket
	key    string
}

func (r *rangeReaderAt) ReadAt(p []byte, off int64) (int, error) {
	rdr, err := r.bucket.NewRangeReader(r.ctx, r.key, off, int64(len(p)), nil)
	if err != nil {
		return 0, err
	}
	defer rdr.Close()
	return io.ReadFull(rdr, p)
}

// object composes Read/Seek/ReadAt/Size from io.SectionReader (which
// already implements all four correctly given a ReaderAt and a known
// size); Close is a no-op since each ReadAt opens and closes its own
// short-lived range reader rather than holding a connection open.
type object struct {
	*io.SectionReader
}

func (o *object) Close() error { return nil }

// Create returns a Writer whose Close commits the object (blob.Writer's
// own Close) and whose Abort cancels the writer's context — the pattern
// gocloud's NewWriter doc prescribes ("To abort a write, cancel ctx");
// Close must still be called after an abort, so Abort does that too and
// discards the resulting (expected) error.
func (s *store) Create(ctx context.Context, key string) (objectstore.Writer, error) {
	cctx, cancel := context.WithCancel(ctx)
	w, err := s.bucket.NewWriter(cctx, key, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("objectstore: creating writer for %q: %w", key, err)
	}
	return &writer{w: w, cancel: cancel}, nil
}

type writer struct {
	w      *blob.Writer
	cancel context.CancelFunc
	done   bool
}

func (w *writer) Write(p []byte) (int, error) { return w.w.Write(p) }

func (w *writer) Close() error {
	if w.done {
		return nil
	}
	w.done = true
	defer w.cancel()
	return w.w.Close()
}

func (w *writer) Abort() error {
	if w.done {
		return nil
	}
	w.done = true
	w.cancel()
	_ = w.w.Close()
	return nil
}

func (s *store) Remove(ctx context.Context, key string) error {
	if err := s.bucket.Delete(ctx, key); err != nil {
		if gcerrors.Code(err) == gcerrors.NotFound {
			return fmt.Errorf("objectstore: remove %q: %w", key, objectstore.ErrNotExist)
		}
		return fmt.Errorf("objectstore: remove %q: %w", key, err)
	}
	return nil
}

func (s *store) List(ctx context.Context, prefix string) iter.Seq2[objectstore.ObjectInfo, error] {
	return func(yield func(objectstore.ObjectInfo, error) bool) {
		it := s.bucket.List(&blob.ListOptions{Prefix: prefix})
		for {
			obj, err := it.Next(ctx)
			if err == io.EOF {
				return
			}
			if err != nil {
				yield(objectstore.ObjectInfo{}, err)
				return
			}
			if !yield(objectstore.ObjectInfo{Path: obj.Key, Size: obj.Size}, nil) {
				return
			}
		}
	}
}
