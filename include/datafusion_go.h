#ifndef DATAFUSION_GO_H
#define DATAFUSION_GO_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * ABI-compatible with the Arrow C Stream Interface:
 * https://arrow.apache.org/docs/format/CStreamInterface.html
 *
 * ArrowSchema and ArrowArray are only ever referenced by pointer here; their
 * full definitions live on both sides of the FFI boundary (arrow-go's cdata
 * package in Go, arrow-rs's ffi module in Rust) and are not needed to
 * declare this struct.
 */
struct ArrowSchema;
struct ArrowArray;

struct ArrowArrayStream {
    int (*get_schema)(struct ArrowArrayStream *self, struct ArrowSchema *out);
    int (*get_next)(struct ArrowArrayStream *self, struct ArrowArray *out);
    const char *(*get_last_error)(struct ArrowArrayStream *self);
    void (*release)(struct ArrowArrayStream *self);
    void *private_data;
};

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

/* Release a session context and all engine resources it owns. */
void df_session_free(df_session_t session);

/* Free a string previously returned by this library. */
void df_string_free(char *s);

#ifdef __cplusplus
}
#endif

#endif /* DATAFUSION_GO_H */
