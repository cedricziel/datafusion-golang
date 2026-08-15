package objectstore

import (
	"bytes"
	"context"
	"fmt"
	"iter"
	"net/url"
	"sort"
	"strings"
	"sync"
)

func init() {
	if err := Register("mem", func(_ context.Context, _ *url.URL) (Store, error) {
		return newMemStore(), nil
	}); err != nil {
		panic(err)
	}
}

// memStore is a process-local, dependency-free backend: one instance per
// distinct mem:// host (bucket), created fresh by the registered Opener
// and cached by Resolve like any other backend. Objects live only for the
// process's lifetime.
type memStore struct {
	mu   sync.RWMutex
	objs map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{objs: make(map[string][]byte)}
}

func (s *memStore) Open(_ context.Context, path string) (Object, error) {
	s.mu.RLock()
	data, ok := s.objs[path]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("objectstore: open %q: %w", path, ErrNotExist)
	}
	return &memObject{Reader: bytes.NewReader(data)}, nil
}

// memObject embeds *bytes.Reader for Read/ReadAt/Seek/Size and adds only
// the Close a stored []byte has no use for.
type memObject struct {
	*bytes.Reader
}

func (o *memObject) Close() error { return nil }

func (s *memStore) Create(_ context.Context, path string) (Writer, error) {
	return &memWriter{store: s, path: path}, nil
}

type memWriter struct {
	store *memStore
	path  string
	buf   bytes.Buffer
	guard onceGuard
}

func (w *memWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }

func (w *memWriter) Close() error {
	if !w.guard.begin() {
		return nil
	}
	// Copy into an exactly-sized slice rather than storing w.buf.Bytes()
	// directly: the buffer's backing array can have much more capacity
	// than the written length, and objs entries live for the store's
	// lifetime — retaining that excess capacity would be a standing
	// memory cost for every write, not a one-time one.
	data := make([]byte, w.buf.Len())
	copy(data, w.buf.Bytes())
	w.store.mu.Lock()
	w.store.objs[w.path] = data
	w.store.mu.Unlock()
	return nil
}

func (w *memWriter) Abort() error {
	w.guard.begin()
	return nil
}

func (s *memStore) Remove(_ context.Context, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objs[path]; !ok {
		return fmt.Errorf("objectstore: remove %q: %w", path, ErrNotExist)
	}
	delete(s.objs, path)
	return nil
}

func (s *memStore) List(_ context.Context, prefix string) iter.Seq2[ObjectInfo, error] {
	return func(yield func(ObjectInfo, error) bool) {
		s.mu.RLock()
		matches := make([]ObjectInfo, 0, len(s.objs))
		for k, v := range s.objs {
			if strings.HasPrefix(k, prefix) {
				matches = append(matches, ObjectInfo{Path: k, Size: int64(len(v))})
			}
		}
		s.mu.RUnlock()

		sort.Slice(matches, func(i, j int) bool { return matches[i].Path < matches[j].Path })
		for _, info := range matches {
			if !yield(info, nil) {
				return
			}
		}
	}
}
