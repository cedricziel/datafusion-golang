//! Thin C ABI over DataFusion, consumed by the `datafusion` Go package via cgo.
//!
//! Every entry point is `extern "C"`, wraps its body in `catch_unwind` so
//! panics never unwind across the FFI boundary, and follows the error
//! convention: a fallible function returns a heap-allocated UTF-8 error
//! string (owned by the caller, freed via `df_string_free`) or NULL/0 on
//! success.

use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::OnceLock;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchIterator;
use datafusion::execution::context::SessionContext;
use tokio::runtime::Runtime;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("failed to build shared tokio runtime")
    })
}

/// Converts a displayable error into a heap-allocated C string, owned by
/// the caller and freed via [`df_string_free`]. NUL bytes in the message
/// are stripped since a C string cannot represent them.
fn error_to_cstring(msg: impl std::fmt::Display) -> *mut c_char {
    let cleaned = msg.to_string().replace('\0', "");
    // `cleaned` no longer contains NUL bytes, so this cannot fail.
    CString::new(cleaned).unwrap().into_raw()
}

unsafe fn write_error_out(error_out: *mut *mut c_char, msg: impl std::fmt::Display) {
    if !error_out.is_null() {
        unsafe {
            *error_out = error_to_cstring(msg);
        }
    }
}

/// Creates a new session context. Returns NULL and sets `*error_out` on
/// failure (e.g. a panic while constructing the shared runtime).
///
/// # Safety
/// `error_out` must either be NULL or point to valid, writable memory for a
/// `char*`.
#[no_mangle]
pub unsafe extern "C" fn df_session_new(error_out: *mut *mut c_char) -> *mut c_void {
    if !error_out.is_null() {
        unsafe {
            *error_out = std::ptr::null_mut();
        }
    }

    let result = catch_unwind(AssertUnwindSafe(|| {
        // Force the shared runtime to initialize now, surfacing failures at
        // session-creation time rather than on first query.
        runtime();
        let ctx = SessionContext::new();
        Box::into_raw(Box::new(ctx)) as *mut c_void
    }));

    match result {
        Ok(handle) => handle,
        Err(payload) => {
            unsafe { write_error_out(error_out, panic_message(&payload)) };
            std::ptr::null_mut()
        }
    }
}

/// Executes `sql` on `session`, writing the result into `*out_stream` as an
/// Arrow C Stream. Returns NULL on success; on failure returns an error
/// string and leaves `*out_stream` untouched.
///
/// # Safety
/// `session` must be a live handle returned by [`df_session_new`] and not
/// yet passed to [`df_session_free`]. `sql` must be a valid NUL-terminated
/// UTF-8 C string. `out_stream` must point to valid, writable memory for a
/// `struct ArrowArrayStream`.
#[no_mangle]
pub unsafe extern "C" fn df_session_sql(
    session: *mut c_void,
    sql: *const c_char,
    out_stream: *mut FFI_ArrowArrayStream,
) -> *mut c_char {
    if session.is_null() {
        return error_to_cstring("session handle is null");
    }
    if sql.is_null() {
        return error_to_cstring("sql is null");
    }
    if out_stream.is_null() {
        return error_to_cstring("out_stream is null");
    }

    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let ctx = unsafe { &*(session as *const SessionContext) };
        let sql_str = unsafe { CStr::from_ptr(sql) }
            .to_str()
            .map_err(|e| format!("sql is not valid UTF-8: {e}"))?;

        runtime().block_on(async {
            let df = ctx.sql(sql_str).await.map_err(|e| e.to_string())?;
            let schema: SchemaRef = df.schema().inner().clone();
            let batches: Vec<RecordBatch> = df.collect().await.map_err(|e| e.to_string())?;
            let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
            let stream = FFI_ArrowArrayStream::new(Box::new(iter));
            // out_stream points to caller-allocated (possibly uninitialized)
            // memory sized for FFI_ArrowArrayStream; ptr::write moves the
            // value in without dropping whatever garbage was there.
            unsafe { std::ptr::write(out_stream, stream) };
            Ok(())
        })
    }));

    match result {
        Ok(Ok(())) => std::ptr::null_mut(),
        Ok(Err(msg)) => error_to_cstring(msg),
        Err(payload) => error_to_cstring(panic_message(&payload)),
    }
}

/// Releases a session context and all engine resources it owns.
///
/// # Safety
/// `session` must be a live handle returned by [`df_session_new`], not
/// already freed, and not used again after this call.
#[no_mangle]
pub unsafe extern "C" fn df_session_free(session: *mut c_void) {
    if session.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(Box::from_raw(session as *mut SessionContext));
    }));
}

/// Frees a string previously returned by this library.
///
/// # Safety
/// `s` must either be NULL or a pointer previously returned by a function
/// in this library, not already freed.
#[no_mangle]
pub unsafe extern "C" fn df_string_free(s: *mut c_char) {
    if s.is_null() {
        return;
    }
    let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
        drop(CString::from_raw(s));
    }));
}

fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        format!("panic: {s}")
    } else if let Some(s) = payload.downcast_ref::<String>() {
        format!("panic: {s}")
    } else {
        "panic: <non-string payload>".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    #[test]
    fn select_1_through_c_abi() {
        unsafe {
            let mut error_out: *mut c_char = std::ptr::null_mut();
            let session = df_session_new(&mut error_out as *mut *mut c_char);
            assert!(error_out.is_null(), "df_session_new should not error");
            assert!(!session.is_null());

            let sql = CString::new("SELECT 1 AS one").unwrap();
            let mut stream = MaybeUninit::<FFI_ArrowArrayStream>::uninit();
            let err = df_session_sql(session, sql.as_ptr(), stream.as_mut_ptr());
            assert!(err.is_null(), "query should succeed");

            let mut stream = stream.assume_init();
            let reader = arrow::ffi_stream::ArrowArrayStreamReader::from_raw(&mut stream)
                .expect("failed to import stream");

            use arrow::record_batch::RecordBatchReader;
            let schema = reader.schema();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "one");

            let batches: Vec<RecordBatch> = reader.collect::<Result<_, _>>().unwrap();
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].num_rows(), 1);

            df_session_free(session);
        }
    }

    #[test]
    fn invalid_sql_returns_error() {
        unsafe {
            let mut error_out: *mut c_char = std::ptr::null_mut();
            let session = df_session_new(&mut error_out as *mut *mut c_char);
            assert!(!session.is_null());

            let sql = CString::new("SELEKT 1").unwrap();
            let mut stream = MaybeUninit::<FFI_ArrowArrayStream>::uninit();
            let err = df_session_sql(session, sql.as_ptr(), stream.as_mut_ptr());
            assert!(!err.is_null(), "invalid SQL should return an error");

            let msg = CStr::from_ptr(err).to_string_lossy().to_string();
            assert!(!msg.is_empty());
            df_string_free(err);

            // session should remain usable after an error
            let sql_ok = CString::new("SELECT 1 AS one").unwrap();
            let mut stream_ok = MaybeUninit::<FFI_ArrowArrayStream>::uninit();
            let err_ok = df_session_sql(session, sql_ok.as_ptr(), stream_ok.as_mut_ptr());
            assert!(err_ok.is_null());
            let mut s = stream_ok.assume_init();
            if let Some(release) = s.release {
                release(&mut s);
            }

            df_session_free(session);
        }
    }
}
