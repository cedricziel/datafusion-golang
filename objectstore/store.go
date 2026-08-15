// Package objectstore resolves a table location — a bare local path or a
// URL such as file://, mem://, s3://, gs://, or azblob:// — to a pluggable
// storage backend, so file-based table providers can read and write
// objects without knowing which backend holds them.
//
// The local and in-memory (mem://) backends are built in. Cloud backends
// register their scheme as an import side effect: importing
// objectstore/s3, objectstore/gcs, or objectstore/azure enables s3://,
// gs://, or azblob:// respectively.
package objectstore

import (
	"context"
	"errors"
	"io"
	"iter"
)

// ErrNotExist is wrapped by the error a backend returns when the
// requested object does not exist. Use errors.Is(err, ErrNotExist) to
// detect it.
var ErrNotExist = errors.New("objectstore: object does not exist")

// ObjectInfo describes one object returned by Store.List.
type ObjectInfo struct {
	Path string
	Size int64
}

// Store is a storage backend bound to one location (a local root, or a
// cloud bucket/container). All methods are safe for concurrent use.
type Store interface {
	// Open returns a handle for reading the object at path. It returns an
	// error wrapping ErrNotExist if no object exists at path. Opening the
	// handle does not itself read any of the object's data.
	Open(ctx context.Context, path string) (Object, error)

	// Create returns a writer for the object at path. The written bytes
	// become visible at path only when the writer's Close returns nil;
	// until then, and if the write is aborted or fails, readers observe
	// the prior object (or its absence) unchanged.
	Create(ctx context.Context, path string) (Writer, error)

	// Remove deletes the object at path. It returns an error wrapping
	// ErrNotExist if no object exists at path.
	Remove(ctx context.Context, path string) error

	// List yields every object whose path has the given prefix, in no
	// particular order. A prefix matching no objects yields nothing and
	// no error.
	List(ctx context.Context, prefix string) iter.Seq2[ObjectInfo, error]
}

// Object is a handle for reading one object. It satisfies
// parquet.ReaderAtSeeker (github.com/apache/arrow-go/v18/parquet) so
// arrow-go's parquet reader can perform selective, ranged reads directly
// against any backend.
type Object interface {
	io.ReadSeekCloser
	io.ReaderAt

	// Size returns the object's total size in bytes.
	Size() int64
}

// Writer is a handle for writing one object. No partial write is ever
// observable at the target path: the object is published atomically on a
// successful Close, and Abort (or a failed Close) leaves the prior
// object, or its absence, unchanged.
type Writer interface {
	io.Writer

	// Close commits the written bytes, publishing them at the target
	// path. It is an error to call Write after Close.
	Close() error

	// Abort discards the written bytes without publishing them, leaving
	// the prior object (or its absence) unchanged. It is a no-op if Close
	// already succeeded.
	Abort() error
}
