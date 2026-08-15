package objectstore

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"sync"
)

// Opener constructs a Store for the bucket/container named by u (its Host,
// with any credentials/config carried in u.RawQuery). Openers are called
// at most once per distinct scheme+host+query; the resulting Store is
// cached and reused for every location that resolves to the same
// scheme+host+query.
type Opener func(ctx context.Context, u *url.URL) (Store, error)

// UnregisteredSchemeError is returned by Resolve when a location's scheme
// has no registered Opener.
type UnregisteredSchemeError struct {
	Scheme string
}

func (e *UnregisteredSchemeError) Error() string {
	if hint, ok := schemeImportHints[e.Scheme]; ok {
		return fmt.Sprintf("objectstore: scheme %q not registered (import %s)", e.Scheme, hint)
	}
	return fmt.Sprintf("objectstore: scheme %q not registered", e.Scheme)
}

// schemeImportHints names the subpackage that registers each scheme this
// module ships a backend for, so an unregistered-scheme error can tell the
// caller exactly what to import.
var schemeImportHints = map[string]string{
	"s3":     "github.com/cedricziel/datafusion-golang/objectstore/s3",
	"gs":     "github.com/cedricziel/datafusion-golang/objectstore/gcs",
	"azblob": "github.com/cedricziel/datafusion-golang/objectstore/azure",
}

// inflightResolve tracks one in-progress Opener call for a cache key, so
// concurrent Resolve calls for the same key wait for the single call in
// flight instead of each invoking the Opener themselves.
type inflightResolve struct {
	done  chan struct{}
	store Store
	err   error
}

var registry = struct {
	mu       sync.Mutex
	openers  map[string]Opener
	cache    map[string]Store
	inflight map[string]*inflightResolve
}{
	openers:  make(map[string]Opener),
	cache:    make(map[string]Store),
	inflight: make(map[string]*inflightResolve),
}

// Register associates scheme with opener, enabling locations of the form
// "<scheme>://..." to resolve through it. Backend subpackages call
// Register from an init() so that a blank import enables their scheme.
// Registering the same scheme twice replaces the previous opener and
// clears any stores already cached for it. Register rejects a scheme
// Resolve always treats as local (empty or "file"); a single-letter
// scheme is otherwise a perfectly registrable scheme — whether a given
// location is a drive-letter path or a registered scheme is decided in
// Resolve by whether the location has a "://" authority, not by the
// scheme's length.
func Register(scheme string, opener Opener) error {
	if scheme == "" || scheme == "file" {
		return fmt.Errorf("objectstore: cannot register reserved scheme %q", scheme)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.openers[scheme] = opener
	prefix := scheme + "://"
	for key := range registry.cache {
		if strings.HasPrefix(key, prefix) {
			delete(registry.cache, key)
		}
	}
	return nil
}

// Resolve splits location into a Store and the path within it.
//
// A location with no scheme, or with the "file" scheme, resolves to the
// local backend. A Windows-style drive-letter path (e.g. `C:\data\x` or
// `C:/data/x`) parses with a scheme but no "://" authority — url.Parse
// leaves its Host empty — and is treated as a local path on that basis,
// not by scheme length: `x://host/key` is a valid URI with a genuine
// one-character scheme (Host "host"), and resolves through the registry
// like any other scheme. Any other scheme is looked up in the registry;
// if no Opener is registered for it, Resolve returns an
// *UnregisteredSchemeError.
func Resolve(ctx context.Context, location string) (Store, string, error) {
	u, err := url.Parse(location)
	if err != nil || u.Scheme == "" {
		return Local, location, nil
	}
	if u.Scheme == "file" {
		return Local, u.Path, nil
	}
	if u.Host == "" && isDriveLetter(u.Scheme) {
		return Local, location, nil
	}

	cacheKey := u.Scheme + "://" + u.Host + "?" + u.RawQuery
	path := strings.TrimPrefix(u.Path, "/")

	registry.mu.Lock()
	if store, cached := registry.cache[cacheKey]; cached {
		registry.mu.Unlock()
		return store, path, nil
	}
	if call, inFlight := registry.inflight[cacheKey]; inFlight {
		registry.mu.Unlock()
		<-call.done
		if call.err != nil {
			return nil, "", call.err
		}
		return call.store, path, nil
	}
	opener, registered := registry.openers[u.Scheme]
	if !registered {
		registry.mu.Unlock()
		return nil, "", &UnregisteredSchemeError{Scheme: u.Scheme}
	}
	call := &inflightResolve{done: make(chan struct{})}
	registry.inflight[cacheKey] = call
	registry.mu.Unlock()

	// The Opener runs with no lock held: it may itself call Resolve or
	// Register (e.g. to compose backends), which would deadlock against
	// a non-reentrant lock held here. Concurrent Resolve calls for this
	// same key block on call.done above instead of each invoking the
	// Opener, so it still runs at most once per key.
	store, openErr := opener(ctx, u)

	registry.mu.Lock()
	delete(registry.inflight, cacheKey)
	if openErr == nil {
		registry.cache[cacheKey] = store
	}
	registry.mu.Unlock()

	if openErr != nil {
		call.err = fmt.Errorf("objectstore: opening %q: %w", u.Scheme, openErr)
		close(call.done)
		return nil, "", call.err
	}
	call.store = store
	close(call.done)
	return store, path, nil
}

// Option configures ResolveWithOptions.
type Option func(*Options)

// Options is Option's target; exported so callers building their own
// Option values (rather than using WithStore) can populate it directly.
type Options struct {
	Store Store
}

// WithStore overrides the backend a location resolves against, bypassing
// the registry entirely: location is then used as-is as the in-store
// path, not parsed as a URL. Intended for tests and explicitly configured
// buckets — the programmatic escape hatch described in design D3. Every
// provider package (parquet, csv, json, jsonl) re-exports this as its own
// WithStore so callers don't import objectstore directly for the common
// case, but they share this one implementation.
func WithStore(store Store) Option {
	return func(o *Options) { o.Store = store }
}

// ResolveWithOptions is Resolve, except a Store supplied via WithStore
// bypasses resolution entirely instead of being looked up by scheme.
func ResolveWithOptions(ctx context.Context, location string, opts ...Option) (Store, string, error) {
	var o Options
	for _, opt := range opts {
		opt(&o)
	}
	if o.Store != nil {
		return o.Store, location, nil
	}
	return Resolve(ctx, location)
}

// isDriveLetter reports whether scheme is a single ASCII letter — the
// scheme url.Parse assigns to a Windows drive-letter path like
// `C:\data\x` or `C:/data/x`. It is not on its own a signal to treat a
// location as local: combined with an empty Host (see Resolve), it is —
// a registered single-letter scheme used with a real "://" authority
// (e.g. `x://host/key`) has a non-empty Host and is not affected.
func isDriveLetter(scheme string) bool {
	if len(scheme) != 1 {
		return false
	}
	c := scheme[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
