#ifndef DATAFUSION_GO_H
#define DATAFUSION_GO_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * ABI-compatible with the Arrow C Data Interface:
 * https://arrow.apache.org/docs/format/CDataInterface.html
 * Guarded by the canonical macro so this header can coexist with other
 * copies of the interface definitions (e.g. arrow-go's abi.h).
 */
#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
    const char *format;
    const char *name;
    const char *metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema **children;
    struct ArrowSchema *dictionary;
    void (*release)(struct ArrowSchema *);
    void *private_data;
};

struct ArrowArray {
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void **buffers;
    struct ArrowArray **children;
    struct ArrowArray *dictionary;
    void (*release)(struct ArrowArray *);
    void *private_data;
};

#endif /* ARROW_C_DATA_INTERFACE */

/*
 * ABI-compatible with the Arrow C Stream Interface:
 * https://arrow.apache.org/docs/format/CStreamInterface.html
 */
#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *self, struct ArrowSchema *out);
    int (*get_next)(struct ArrowArrayStream *self, struct ArrowArray *out);
    const char *(*get_last_error)(struct ArrowArrayStream *self);
    void (*release)(struct ArrowArrayStream *self);
    void *private_data;
};

#endif /* ARROW_C_STREAM_INTERFACE */

/* Opaque handle to a Rust-side DataFusion SessionContext. */
typedef void *df_session_t;

/*
 * Create a new session context. Returns NULL and sets *error_out (an
 * error string owned by the caller, to be freed via df_string_free) on
 * failure. Does not require any prior global initialization.
 */
df_session_t df_session_new(char **error_out);

/*
 * Execute a SQL statement on the session, writing the result into
 * *out_stream as an Arrow C Stream. Returns NULL on success, or a
 * heap-allocated error string (freed via df_string_free) on failure.
 * *out_stream is left untouched on failure.
 */
char *df_session_sql(df_session_t session, const char *sql, struct ArrowArrayStream *out_stream);

/*
 * Register a Go-implemented table under the given name. `handle` is an
 * opaque Go-side token (a runtime/cgo Handle) identifying the table
 * implementation; ownership of the handle passes to the engine on entry.
 * On success the engine retains the handle until the session is freed, at
 * which point it calls go_table_release. On failure (duplicate name,
 * schema fetch error, ...) the engine calls go_table_release before
 * returning, and the caller must NOT release the handle itself.
 *
 * The table's schema is fetched exactly once, via go_table_schema, during
 * this call. Returns NULL on success, or an error string (freed via
 * df_string_free) on failure.
 */
char *df_session_register_table(df_session_t session, const char *name, uintptr_t handle);

/*
 * Register a Go-implemented scalar function under the given name.
 * `arg_types` is a struct-typed ArrowSchema whose fields declare the
 * function's argument types, in order; `return_type` is a struct-typed
 * ArrowSchema with exactly one field declaring the return type. Both
 * schemas are consumed by this call regardless of outcome (moved on
 * success, released on failure); the caller must not touch them again.
 *
 * `handle` follows the same ownership contract as
 * df_session_register_table: it passes to the engine on entry, and on
 * failure the engine calls go_scalar_udf_release before returning.
 * Returns NULL on success, or an error string (freed via df_string_free)
 * on failure.
 */
char *df_session_register_scalar_udf(df_session_t session, const char *name,
                                     struct ArrowSchema *arg_types,
                                     struct ArrowSchema *return_type,
                                     uintptr_t handle);

/* Release a session context and all engine resources it owns. */
void df_session_free(df_session_t session);

/* Free a string previously returned by this library. */
void df_string_free(char *s);

/*
 * Callback trampolines exported by the Go side (via cgo //export) and
 * resolved at final link time, since the Rust staticlib is always linked
 * into the same Go binary. Error strings written to *error_out are
 * allocated with malloc (Go's C.CString) and freed by the Rust side with
 * free.
 *
 * go_table_schema: export the table's schema into *out_schema (which the
 * caller passes zero-initialized) or set *error_out.
 *
 * go_table_scan: start a full-table scan, exporting an Arrow C Stream
 * into *out_stream (zero-initialized by the caller) or set *error_out.
 *
 * go_scalar_udf_invoke: evaluate one batch. args_array/args_schema carry
 * a struct array whose N children are the N argument columns; ownership
 * of both transfers to the callee. On success the callee writes the
 * result array of the declared return type into *out_array and
 * *out_schema (zero-initialized by the caller); on failure it sets
 * *error_out and leaves the out params untouched.
 *
 * go_table_release / go_scalar_udf_release: drop the Go-side handle.
 */
extern void go_table_schema(uintptr_t handle, struct ArrowSchema *out_schema, char **error_out);
extern void go_table_scan(uintptr_t handle, struct ArrowArrayStream *out_stream, char **error_out);
extern void go_table_release(uintptr_t handle);
extern void go_scalar_udf_invoke(uintptr_t handle, struct ArrowArray *args_array,
                                 struct ArrowSchema *args_schema, struct ArrowArray *out_array,
                                 struct ArrowSchema *out_schema, char **error_out);
extern void go_scalar_udf_release(uintptr_t handle);

#ifdef __cplusplus
}
#endif

#endif /* DATAFUSION_GO_H */
