package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"iter"
	"os"
	"path/filepath"
	"strings"
)

// Local is the local-filesystem backend: bare paths and file:// locations
// resolve to it. It requires no configuration and behaves exactly like
// the same operations performed directly with the os package.
var Local Store = localStore{}

type localStore struct{}

// NewLocalStore returns the local-filesystem backend, for callers that
// want to pass it explicitly (e.g. a provider's WithStore option) instead
// of relying on Resolve's bare-path/file:// fallback.
func NewLocalStore() Store { return Local }

func (localStore) Open(_ context.Context, path string) (Object, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("objectstore: open %q: %w", path, ErrNotExist)
		}
		return nil, fmt.Errorf("objectstore: open %q: %w", path, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("objectstore: stat %q: %w", path, err)
	}
	if info.IsDir() {
		_ = f.Close()
		return nil, fmt.Errorf("objectstore: open %q: is a directory", path)
	}
	return &localObject{File: f, size: info.Size()}, nil
}

type localObject struct {
	*os.File
	size int64
}

func (o *localObject) Size() int64 { return o.size }

// localWriter commits by writing to a temp file in the target's directory,
// fsyncing it, and renaming it over the target on Close — the same
// pattern providers/parquet/insert.go used directly before this package
// existed. Rename within one directory is atomic on every platform this
// project targets: a concurrent reader observes the complete old bytes or
// the complete new bytes, never a mixture.
func (localStore) Create(_ context.Context, path string) (Writer, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("objectstore: preparing directory for %q: %w", path, err)
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return nil, fmt.Errorf("objectstore: creating temp file for %q: %w", path, err)
	}
	return &localWriter{file: tmp, path: path}, nil
}

type localWriter struct {
	file  *os.File
	path  string
	guard onceGuard
}

func (w *localWriter) Write(p []byte) (int, error) { return w.file.Write(p) }

func (w *localWriter) Close() error {
	if !w.guard.begin() {
		return nil
	}
	committed := false
	defer func() {
		if !committed {
			_ = os.Remove(w.file.Name())
		}
	}()

	if err := w.file.Sync(); err != nil {
		_ = w.file.Close()
		return fmt.Errorf("objectstore: syncing %q: %w", w.path, err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("objectstore: closing %q: %w", w.path, err)
	}
	if err := os.Rename(w.file.Name(), w.path); err != nil {
		return fmt.Errorf("objectstore: committing %q: %w", w.path, err)
	}
	committed = true
	return nil
}

func (w *localWriter) Abort() error {
	if !w.guard.begin() {
		return nil
	}
	_ = w.file.Close()
	return os.Remove(w.file.Name())
}

func (localStore) Remove(_ context.Context, path string) error {
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("objectstore: remove %q: %w", path, ErrNotExist)
		}
		return fmt.Errorf("objectstore: remove %q: %w", path, err)
	}
	return nil
}

func (localStore) List(_ context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		base := prefix
		if info, err := os.Stat(base); err != nil || !info.IsDir() {
			base = filepath.Dir(prefix)
			if _, err := os.Stat(base); err != nil {
				return
			}
		}
		_ = filepath.WalkDir(base, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasPrefix(path, prefix) {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				if !yield(ObjectInfo{}, err) {
					return fs.SkipAll
				}
				return nil
			}
			if !yield(ObjectInfo{Path: path, Size: info.Size()}, nil) {
				return fs.SkipAll
			}
			return nil
		})
	}
}
