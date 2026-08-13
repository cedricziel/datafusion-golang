package datafusion

import (
	"errors"
	"log"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/apache/arrow-go/v18/arrow/array"
)

// ErrSessionClosed is returned when a SessionContext is used after Close.
var ErrSessionClosed = errors.New("datafusion: session is closed")

// SessionContext is a Go handle to a DataFusion session context. Create one
// with NewSessionContext, execute SQL with SQL, and release engine
// resources deterministically with Close.
//
// A SessionContext is safe for concurrent use by multiple goroutines.
type SessionContext struct {
	// mu guards handle: SQL takes it for reading so multiple queries can
	// run concurrently, Close takes it for writing so it can drain any
	// in-flight calls before freeing the engine-side handle.
	mu     sync.RWMutex
	handle sessionHandle
	closed atomic.Bool
}

// NewSessionContext creates a new session context bound to a DataFusion
// engine instance. It does not require any prior global initialization.
func NewSessionContext() (*SessionContext, error) {
	handle, err := sessionNew()
	if err != nil {
		return nil, err
	}

	s := &SessionContext{handle: handle}
	runtime.SetFinalizer(s, finalizeSessionContext)
	return s, nil
}

// SQL executes a SQL statement on the session and returns the result as an
// Arrow record reader. The result schema is available before the first
// batch is read. Data crosses the engine boundary via the Arrow C Stream
// interface without row-level copying.
func (s *SessionContext) SQL(query string) (array.RecordReader, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed.Load() {
		return nil, ErrSessionClosed
	}
	return sessionSQL(s.handle, query)
}

// Close releases all engine resources associated with the session. It waits
// for any in-flight SQL calls to complete first. Close is idempotent: it is
// safe to call multiple times.
func (s *SessionContext) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sessionFree(s.handle)
	s.handle = nil
	runtime.SetFinalizer(s, nil)
	return nil
}

func finalizeSessionContext(s *SessionContext) {
	if !s.closed.CompareAndSwap(false, true) {
		return
	}
	if debugFinalizerWarnings {
		log.Printf("datafusion: SessionContext garbage collected without Close(); freeing as a leak backstop")
	}
	sessionFree(s.handle)
}
