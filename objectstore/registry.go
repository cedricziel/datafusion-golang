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

var registry = struct {
	mu      sync.RWMutex
	openers map[string]Opener
	cache   map[string]Store
}{
	openers: make(map[string]Opener),
	cache:   make(map[string]Store),
}

// Register associates scheme with opener, enabling locations of the form
// "<scheme>://..." to resolve through it. Backend subpackages call
// Register from an init() so that a blank import enables their scheme.
// Registering the same scheme twice replaces the previous opener and
// clears any stores already cached for it. Register rejects a scheme
// Resolve always treats as local (empty, "file", or a drive letter).
func Register(scheme string, opener Opener) error {
	if isReservedScheme(scheme) {
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
// local backend. A Windows-style drive-letter path (e.g. `C:\data\x`) is
// treated as a local path, not a URL scheme. Any other scheme is looked
// up in the registry; if no Opener is registered for it, Resolve returns
// an *UnregisteredSchemeError.
func Resolve(ctx context.Context, location string) (Store, string, error) {
	u, err := url.Parse(location)
	if err != nil || u.Scheme == "" || isDriveLetter(u.Scheme) {
		return Local, location, nil
	}
	if u.Scheme == "file" {
		return Local, u.Path, nil
	}

	cacheKey := u.Scheme + "://" + u.Host + "?" + u.RawQuery
	path := strings.TrimPrefix(u.Path, "/")

	registry.mu.RLock()
	store, cached := registry.cache[cacheKey]
	opener, registered := registry.openers[u.Scheme]
	registry.mu.RUnlock()
	if cached {
		return store, path, nil
	}
	if !registered {
		return nil, "", &UnregisteredSchemeError{Scheme: u.Scheme}
	}

	// Hold the write lock across the opener call so two concurrent
	// first-resolutions of the same scheme+host+query can't both open a
	// backend and race to cache it — the loser's Store (and whatever
	// connection/client it holds) would otherwise leak. Openers run once
	// per distinct key and are cached forever after, so this cost is
	// paid at most once per bucket/container, not per query.
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if store, cached := registry.cache[cacheKey]; cached {
		return store, path, nil
	}
	store, err = opener(ctx, u)
	if err != nil {
		return nil, "", fmt.Errorf("objectstore: opening %q: %w", u.Scheme, err)
	}
	registry.cache[cacheKey] = store
	return store, path, nil
}

// isReservedScheme reports whether scheme is one Resolve always treats as
// local, and so can never be registered: empty (bare paths), "file", or a
// single-letter drive letter.
func isReservedScheme(scheme string) bool {
	return scheme == "" || scheme == "file" || isDriveLetter(scheme)
}

// isDriveLetter reports whether scheme is a single ASCII letter, as
// produced by parsing a Windows drive-letter path like `C:\data\x` as a
// URL: no registered scheme is ever one character, so this is an
// unambiguous signal to treat the location as local instead.
func isDriveLetter(scheme string) bool {
	if len(scheme) != 1 {
		return false
	}
	c := scheme[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}
