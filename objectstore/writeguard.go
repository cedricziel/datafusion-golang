package objectstore

// onceGuard makes a Writer's Close/Abort idempotent. Every backend's
// Writer must deliver the same commit-on-Close contract (design D5:
// nothing observable until Close returns nil; Abort or a failed Close
// leaves prior state untouched; either method no-ops once the other has
// already run) — this bookkeeping is shared so that contract is defined
// once instead of re-derived per backend.
type onceGuard struct{ done bool }

// begin reports whether this call is the first to reach Close or Abort.
// The caller should no-op if it returns false.
func (g *onceGuard) begin() bool {
	if g.done {
		return false
	}
	g.done = true
	return true
}
