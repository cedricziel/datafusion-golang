//! Thin C ABI over DataFusion, consumed by the `datafusion` Go package via cgo.
//!
//! Every entry point is `extern "C"`, wraps its body in `catch_unwind` so
//! panics never unwind across the FFI boundary, and follows the error
//! convention: a fallible function returns a heap-allocated UTF-8 error
//! string (owned by the caller, freed via `df_string_free`) or NULL/0 on
//! success.

mod catalog;
mod ffi;
mod pushdown;
mod table;
mod udf;

use std::ffi::{c_char, c_void, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::{Arc, OnceLock};

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::FFI_ArrowArrayStream;
use arrow::record_batch::RecordBatchIterator;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::ScalarUDF;
use tokio::runtime::Runtime;

use crate::catalog::GoCatalogProvider;
use crate::ffi::{
    go_catalog_release, go_scalar_udf_release, go_table_release, struct_fields_from_ffi,
};
use crate::table::GoTableProvider;
use crate::udf::GoScalarUdf;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub(crate) fn runtime() -> &'static Runtime {
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
            unsafe { write_error_out(error_out, panic_message(payload.as_ref())) };
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
        Err(payload) => error_to_cstring(panic_message(payload.as_ref())),
    }
}

/// Registers a Go-implemented table under `name`. `handle` is the opaque
/// Go-side token; ownership passes to this function on entry. On success
/// the engine retains it until the session is freed; on failure it is
/// released via `go_table_release` before returning (see the header
/// contract). `supports_pushdown` (0/1) declares whether the provider
/// accepts scan pushdown (design D4 of add-table-provider-pushdown).
/// `supports_insert` (0/1) declares whether the provider accepts
/// INSERT INTO / INSERT OVERWRITE / REPLACE INTO (design D3 of
/// add-table-provider-insert): when set, `INSERT` delivers rows to
/// `go_table_insert`; when clear, `INSERT` fails with a "does not support
/// INSERT" error and no FFI call is made. Returns NULL on success or an
/// error string on failure.
///
/// # Safety
/// `session` must be a live handle returned by [`df_session_new`]. `name`
/// must be a valid NUL-terminated UTF-8 C string. `handle` must be a live
/// Go `cgo.Handle` value not previously passed to any registration call.
#[no_mangle]
pub unsafe extern "C" fn df_session_register_table(
    session: *mut c_void,
    name: *const c_char,
    handle: usize,
    supports_pushdown: u8,
    supports_insert: u8,
) -> *mut c_char {
    if session.is_null() {
        return error_to_cstring("session handle is null");
    }
    if name.is_null() {
        return error_to_cstring("table name is null");
    }

    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let ctx = unsafe { &*(session as *const SessionContext) };
        let name = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| format!("table name is not valid UTF-8: {e}"))?;

        let exists = ctx
            .table_exist(name)
            .map_err(|e| format!("checking for existing table '{name}': {e}"))?;
        if exists {
            unsafe { go_table_release(handle) };
            return Err(format!("table '{name}' is already registered"));
        }

        // try_new releases the handle itself on failure; after this point
        // the provider's Drop impl owns the release.
        let provider =
            GoTableProvider::try_new(handle, supports_pushdown != 0, supports_insert != 0)?;
        ctx.register_table(name, Arc::new(provider))
            .map_err(|e| format!("registering table '{name}': {e}"))?;
        Ok(())
    }));

    match result {
        Ok(Ok(())) => std::ptr::null_mut(),
        Ok(Err(msg)) => error_to_cstring(msg),
        Err(payload) => error_to_cstring(panic_message(payload.as_ref())),
    }
}

/// Registers a Go-implemented scalar function under `name`. `arg_types`
/// is a struct-typed Arrow C Schema whose fields declare the argument
/// types; `return_type` is a struct-typed Arrow C Schema with exactly one
/// field. Both schemas are consumed regardless of outcome. `handle`
/// follows the same ownership contract as [`df_session_register_table`].
///
/// # Safety
/// `session` must be a live handle returned by [`df_session_new`]. `name`
/// must be a valid NUL-terminated UTF-8 C string. `arg_types` and
/// `return_type` must point to live, exported Arrow C Schemas; they are
/// moved out of (left released) by this call. `handle` must be a live Go
/// `cgo.Handle` value not previously passed to any registration call.
#[no_mangle]
pub unsafe extern "C" fn df_session_register_scalar_udf(
    session: *mut c_void,
    name: *const c_char,
    arg_types: *mut FFI_ArrowSchema,
    return_type: *mut FFI_ArrowSchema,
    handle: usize,
) -> *mut c_char {
    if session.is_null() || name.is_null() || arg_types.is_null() || return_type.is_null() {
        return error_to_cstring("df_session_register_scalar_udf: null argument");
    }

    // Take ownership of the schemas immediately (the Arrow C ABI permits
    // moving the structs) so every path below — success or failure —
    // releases them exactly once.
    let arg_schema = unsafe {
        let s = std::ptr::read(arg_types);
        std::ptr::write(arg_types, FFI_ArrowSchema::empty());
        s
    };
    let ret_schema = unsafe {
        let s = std::ptr::read(return_type);
        std::ptr::write(return_type, FFI_ArrowSchema::empty());
        s
    };

    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let release_and_err = |msg: String| -> Result<(), String> {
            unsafe { go_scalar_udf_release(handle) };
            Err(msg)
        };

        let ctx = unsafe { &*(session as *const SessionContext) };
        let name = match unsafe { CStr::from_ptr(name) }.to_str() {
            Ok(name) => name,
            Err(e) => return release_and_err(format!("function name is not valid UTF-8: {e}")),
        };

        if ctx.state().scalar_functions().contains_key(name) {
            return release_and_err(format!("scalar function '{name}' is already registered"));
        }

        let arg_fields = match struct_fields_from_ffi(&arg_schema) {
            Ok(fields) => fields,
            Err(e) => return release_and_err(format!("argument types for '{name}': {e}")),
        };
        let ret_fields = match struct_fields_from_ffi(&ret_schema) {
            Ok(fields) => fields,
            Err(e) => return release_and_err(format!("return type for '{name}': {e}")),
        };
        if ret_fields.len() != 1 {
            return release_and_err(format!(
                "return type schema for '{name}' must have exactly one field, got {}",
                ret_fields.len()
            ));
        }
        let return_type = ret_fields[0].data_type().clone();

        // From here on the GoScalarUdf's Drop impl owns the handle release.
        let udf = GoScalarUdf::new(name.to_string(), handle, arg_fields, return_type);
        ctx.register_udf(ScalarUDF::new_from_impl(udf));
        Ok(())
    }));

    match result {
        Ok(Ok(())) => std::ptr::null_mut(),
        Ok(Err(msg)) => error_to_cstring(msg),
        Err(payload) => error_to_cstring(panic_message(payload.as_ref())),
    }
}

/// Registers a Go-implemented catalog under `name`, making
/// `name.schema.table` queryable via SQL. `handle` follows the same
/// ownership contract as [`df_session_register_table`]: it passes to the
/// engine on entry, and on a rejected duplicate-name registration the
/// engine calls `go_catalog_release` before returning.
///
/// The engine's default catalog name is not special-cased (design D6): if
/// `name` equals it, this call replaces the default catalog outright
/// rather than erroring, and any tables/functions previously registered
/// via `df_session_register_table`/`df_session_register_scalar_udf`
/// become unreachable through SQL. Registering under any other name
/// already in use returns an error and leaves the existing catalog
/// unchanged. Nothing is fetched from Go during this call (design D3);
/// schemas and tables are discovered lazily, per query.
///
/// # Safety
/// `session` must be a live handle returned by [`df_session_new`]. `name`
/// must be a valid NUL-terminated UTF-8 C string. `handle` must be a live
/// Go `cgo.Handle` value not previously passed to any registration call.
#[no_mangle]
pub unsafe extern "C" fn df_session_register_catalog(
    session: *mut c_void,
    name: *const c_char,
    handle: usize,
) -> *mut c_char {
    if session.is_null() {
        return error_to_cstring("session handle is null");
    }
    if name.is_null() {
        return error_to_cstring("catalog name is null");
    }

    let result = catch_unwind(AssertUnwindSafe(|| -> Result<(), String> {
        let ctx = unsafe { &*(session as *const SessionContext) };
        let name = unsafe { CStr::from_ptr(name) }
            .to_str()
            .map_err(|e| format!("catalog name is not valid UTF-8: {e}"))?;

        let default_catalog = ctx.state().config_options().catalog.default_catalog.clone();
        if name != default_catalog && ctx.catalog(name).is_some() {
            unsafe { go_catalog_release(handle) };
            return Err(format!("catalog '{name}' is already registered"));
        }

        let provider = GoCatalogProvider::new(handle);
        ctx.register_catalog(name, Arc::new(provider));
        Ok(())
    }));

    match result {
        Ok(Ok(())) => std::ptr::null_mut(),
        Ok(Err(msg)) => error_to_cstring(msg),
        Err(payload) => error_to_cstring(panic_message(payload.as_ref())),
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
