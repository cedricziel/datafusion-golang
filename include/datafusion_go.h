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
 * `supports_pushdown` (0 or 1) declares whether the provider accepts scan
 * pushdown: when set, the engine classifies the query's filters during
 * planning and passes projection/filters/limit to go_table_scan; when
 * clear, every go_table_scan call carries the no-pushdown sentinels and
 * the engine projects and filters after the scan, exactly as before.
 *
 * The table's schema is fetched exactly once, via go_table_schema, during
 * this call. Returns NULL on success, or an error string (freed via
 * df_string_free) on failure.
 */
char *df_session_register_table(df_session_t session, const char *name, uintptr_t handle,
                                uint8_t supports_pushdown);

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

/*
 * Register a Go-implemented catalog under the given name, making
 * `name.schema.table` queryable via SQL. `handle` follows the same
 * ownership contract as df_session_register_table: it passes to the
 * engine on entry, and on a rejected duplicate-name registration the
 * engine calls go_catalog_release before returning.
 *
 * The engine's default catalog name is not special-cased: registering
 * under it replaces the default catalog outright (this call still
 * succeeds) rather than erroring, and any tables/functions previously
 * registered via df_session_register_table/df_session_register_scalar_udf
 * become unreachable through SQL as a result. Registering under any other
 * name already in use returns an error and leaves the existing catalog
 * unchanged.
 *
 * Nothing is fetched from Go during this call: schemas and tables are
 * discovered lazily, per query, by calling back into Go through the
 * catalog/schema trampolines below. Returns NULL on success, or an error
 * string (freed via df_string_free) on failure.
 */
char *df_session_register_catalog(df_session_t session, const char *name, uintptr_t handle);

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
 * go_table_scan: start a table scan, exporting an Arrow C Stream into
 * *out_stream (zero-initialized by the caller) or set *error_out. The
 * pushdown parameters are borrowed by the callee for the duration of the
 * call only (the caller owns and frees them):
 *   - projection/projection_len: the ordered column indices (into the
 *     registered schema) the scan must return. projection_len == -1 means
 *     no projection (full schema); 0 means zero columns. For tables
 *     registered without supports_pushdown, always NULL/-1, and the
 *     stream must carry the full registered schema.
 *   - filters_json: the pushed filter predicates as one JSON document
 *     (see datafusion/expr.go for the format), or NULL when none.
 *   - limit: advisory fetch hint; -1 means none.
 *
 * go_scalar_udf_invoke: evaluate one batch. args_array/args_schema carry
 * a struct array whose N children are the N argument columns; ownership
 * of both transfers to the callee. On success the callee writes the
 * result array of the declared return type into *out_array and
 * *out_schema (zero-initialized by the caller); on failure it sets
 * *error_out and leaves the out params untouched.
 *
 * go_table_release / go_scalar_udf_release: drop the Go-side handle.
 *
 * go_catalog_schema_names: export the catalog's current schema names as
 * one JSON array-of-strings document (e.g. ["a","b"]) into a
 * malloc-allocated *out_names_json (freed by the Rust side with free), or
 * set *error_out. Called fresh for every query that needs it (no caching).
 *
 * go_catalog_schema_lookup: resolve `name` to a schema. On success sets
 * *out_found (0 or 1); when 1, *out_schema_handle is a fresh opaque
 * handle identifying the resolved schema (later passed to the
 * go_schema_* trampolines and released via go_schema_release), and when
 * 0 the schema does not exist (not an error). Sets *error_out only on a
 * genuine failure (I/O, timeout, ...), leaving the other out-params
 * untouched.
 *
 * go_catalog_release: drop the Go-side catalog handle.
 *
 * go_schema_table_names: export the schema's current table names, same
 * shape and calling convention as go_catalog_schema_names.
 *
 * go_schema_table_lookup: resolve `name` to a table. On success sets
 * *out_found (0 or 1); when 1, *out_table_handle and
 * *out_supports_pushdown carry the same shape df_session_register_table's
 * `handle`/`supports_pushdown` do — the returned handle is fed directly
 * into the same table-provider machinery a directly-registered table
 * uses (go_table_schema/go_table_scan/go_table_release), no separate
 * lookup-table trampoline exists. When 0 the table does not exist (not an
 * error). Sets *error_out only on a genuine failure, leaving the other
 * out-params untouched.
 *
 * go_schema_release: drop the Go-side schema handle.
 */
extern void go_table_schema(uintptr_t handle, struct ArrowSchema *out_schema, char **error_out);
/*
 * (The pointer parameters are logically const — the callee only reads
 * them — but cgo cannot express const in exported prototypes.)
 */
extern void go_table_scan(uintptr_t handle, int32_t *projection, intptr_t projection_len,
                          char *filters_json, int64_t limit,
                          struct ArrowArrayStream *out_stream, char **error_out);
extern void go_table_release(uintptr_t handle);
extern void go_scalar_udf_invoke(uintptr_t handle, struct ArrowArray *args_array,
                                 struct ArrowSchema *args_schema, struct ArrowArray *out_array,
                                 struct ArrowSchema *out_schema, char **error_out);
extern void go_scalar_udf_release(uintptr_t handle);

/*
 * (name is logically const — the callee only reads it — but cgo cannot
 * express const in exported prototypes, matching go_table_scan above.)
 */
extern void go_catalog_schema_names(uintptr_t handle, char **out_names_json, char **error_out);
extern void go_catalog_schema_lookup(uintptr_t handle, char *name, uintptr_t *out_schema_handle,
                                     uint8_t *out_found, char **error_out);
extern void go_catalog_release(uintptr_t handle);
extern void go_schema_table_names(uintptr_t handle, char **out_names_json, char **error_out);
extern void go_schema_table_lookup(uintptr_t handle, char *name, uintptr_t *out_table_handle,
                                   uint8_t *out_supports_pushdown, uint8_t *out_found,
                                   char **error_out);
extern void go_schema_release(uintptr_t handle);

#ifdef __cplusplus
}
#endif

#endif /* DATAFUSION_GO_H */
