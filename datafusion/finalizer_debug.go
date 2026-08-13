//go:build datafusion_debug

package datafusion

// debugFinalizerWarnings enables a log warning when a SessionContext is
// garbage collected without an explicit Close(). Explicit Close is the
// contract; the finalizer is only a leak backstop, so this is opt-in via
// the datafusion_debug build tag rather than always-on noise.
const debugFinalizerWarnings = true
