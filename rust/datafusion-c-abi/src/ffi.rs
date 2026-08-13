//! Shared FFI plumbing for calling back from Rust into Go: the extern
//! declarations for the Go-exported trampoline symbols, error-string
//! handling for errors produced by Go callbacks, and standalone Arrow C
//! Schema import helpers.

use std::ffi::{c_char, c_void, CStr};

use arrow::datatypes::{Fields, Schema};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

// Fixed symbols exported by the Go side via cgo //export, resolved at
// final link time (design D1). What varies per registration is the opaque
// cgo.Handle-derived `handle` argument (design D2).
extern "C" {
    pub(crate) fn go_table_schema(
        handle: usize,
        out_schema: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_table_scan(
        handle: usize,
        projection: *const i32,
        projection_len: isize,
        filters_json: *const c_char,
        limit: i64,
        out_stream: *mut FFI_ArrowArrayStream,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_table_release(handle: usize);
    pub(crate) fn go_scalar_udf_invoke(
        handle: usize,
        args_array: *mut FFI_ArrowArray,
        args_schema: *mut FFI_ArrowSchema,
        out_array: *mut FFI_ArrowArray,
        out_schema: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_scalar_udf_release(handle: usize);

    // Catalog kind (add-catalog-provider design D2).
    pub(crate) fn go_catalog_schema_names(
        handle: usize,
        out_names_json: *mut *mut c_char,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_catalog_schema_lookup(
        handle: usize,
        name: *const c_char,
        out_schema_handle: *mut usize,
        out_found: *mut u8,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_catalog_release(handle: usize);

    // Schema kind (add-catalog-provider design D2).
    pub(crate) fn go_schema_table_names(
        handle: usize,
        out_names_json: *mut *mut c_char,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_schema_table_lookup(
        handle: usize,
        name: *const c_char,
        out_table_handle: *mut usize,
        out_supports_pushdown: *mut u8,
        out_found: *mut u8,
        error_out: *mut *mut c_char,
    );
    pub(crate) fn go_schema_release(handle: usize);

    // Go callbacks allocate error strings with malloc (C.CString), so the
    // matching deallocator is libc free, not Rust's allocator.
    fn free(ptr: *mut c_void);
}

/// Takes ownership of a malloc-allocated error string written by a Go
/// callback, copies it into a Rust `String`, and frees the C allocation.
/// Returns `None` if the callback left the out-param NULL (success).
///
/// # Safety
/// `err` must be NULL or a live malloc-allocated NUL-terminated string.
pub(crate) unsafe fn take_go_error(err: *mut c_char) -> Option<String> {
    if err.is_null() {
        return None;
    }
    Some(unsafe { take_go_string(err) })
}

/// Imports a standalone Arrow C Schema describing a struct type and
/// returns its fields. Used for a table's schema and for the
/// struct-of-argument-types shape of a scalar UDF signature (design D5).
pub(crate) fn struct_fields_from_ffi(ffi: &FFI_ArrowSchema) -> Result<Fields, String> {
    let schema =
        Schema::try_from(ffi).map_err(|e| format!("importing Arrow schema over FFI: {e}"))?;
    Ok(schema.fields().clone())
}

/// Runs a blocking Go callback on the shared runtime's blocking thread
/// pool (design D6), so a slow or blocking Go implementation cannot
/// starve the async worker threads every concurrent query depends on.
pub(crate) async fn spawn_go_blocking<T, F>(f: F) -> Result<T, String>
where
    F: FnOnce() -> Result<T, String> + Send + 'static,
    T: Send + 'static,
{
    crate::runtime()
        .spawn_blocking(f)
        .await
        .map_err(|e| format!("Go callback task failed: {e}"))?
}

/// Runs a blocking Go callback via `tokio::task::block_in_place`
/// (add-catalog-provider design D4), for synchronous DataFusion catalog
/// trait methods (`CatalogProvider::schema_names`/`schema`,
/// `SchemaProvider::table_names`) that cannot `.await` `spawn_go_blocking`
/// because there is no `async fn` to await inside. Tells the multi-thread
/// runtime's scheduler that the current worker thread is about to block,
/// so it can hand off other ready tasks first — valid only when called
/// from within the shared multi-thread runtime (walking-skeleton D5),
/// which every call site here is: query planning always runs inside
/// `runtime().block_on(...)`.
pub(crate) fn block_go<T>(f: impl FnOnce() -> Result<T, String>) -> Result<T, String> {
    tokio::task::block_in_place(f)
}

/// Takes ownership of a malloc-allocated, NUL-terminated string a Go
/// callback wrote on success (the out-param is guaranteed non-NULL
/// whenever the callback reports no error), copying it into a Rust
/// `String` and freeing the C allocation.
///
/// # Safety
/// `s` must be a live malloc-allocated NUL-terminated UTF-8 string.
pub(crate) unsafe fn take_go_string(s: *mut c_char) -> String {
    let msg = unsafe { CStr::from_ptr(s) }.to_string_lossy().into_owned();
    unsafe { free(s as *mut c_void) };
    msg
}

/// Decodes a JSON array-of-strings document written by a Go callback
/// (add-catalog-provider design D5).
pub(crate) fn decode_name_list(json: &str) -> Result<Vec<String>, String> {
    serde_json::from_str(json).map_err(|e| format!("decoding name list {json:?}: {e}"))
}

/// Test stand-ins for the Go-exported trampoline symbols. A pure
/// `cargo test` binary has no Go side, so these `#[no_mangle]`
/// definitions satisfy the extern declarations above at link time.
///
/// Table- and UDF-kind stub behavior is keyed on `handle % 10` (each kind
/// dispatches through its own trampoline, so the digit is reused across
/// kinds with no collision); each test picks a unique handle value (e.g.
/// `TABLE_OK + 10`) to also get deterministic per-handle release/invoke
/// tracking under parallel test execution. Catalog- and schema-kind stub
/// behavior additionally packs a second field into the handle — see the
/// `CATALOG_*`/`SCHEMA_*` constants and `catalog::tests::handle` below.
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::ffi::c_char;
    use std::sync::{Mutex, OnceLock};

    use arrow::array::{Array, Int64Array, RecordBatch, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::error::ArrowError;
    use arrow::ffi::{from_ffi, to_ffi};
    use arrow::record_batch::RecordBatchIterator;
    use std::sync::Arc;

    pub const TABLE_OK: usize = 1;
    pub const TABLE_SCAN_MIDSTREAM_ERR: usize = 2;
    pub const TABLE_SCHEMA_ERR: usize = 3;
    pub const TABLE_SCAN_OPEN_ERR: usize = 4;
    pub const UDF_DOUBLE: usize = 5;
    pub const UDF_INVOKE_ERR: usize = 6;
    /// Honors the projection it receives (a well-behaved pushdown table).
    pub const TABLE_PUSHDOWN_OK: usize = 7;
    /// Ignores the projection and always returns the full schema (a
    /// pushdown table violating the projection contract).
    pub const TABLE_PUSHDOWN_WRONG_SCHEMA: usize = 8;

    // Catalog- and schema-kind stub behavior (add-catalog-provider).
    // `catalog::tests::handle(catalog_kind, schema_kind, unique)` packs all
    // three fields into one outer-test handle value, since
    // go_catalog_schema_lookup below passes the catalog handle through
    // unchanged as the schema handle — decoded here by catalog_kind_of/
    // schema_kind_of/uniqueness_base_of so the packing/unpacking logic has
    // exactly one implementation on each side.
    pub const CATALOG_OK: usize = 1;
    pub const CATALOG_SCHEMA_NAMES_ERR: usize = 2;
    pub const CATALOG_SCHEMA_LOOKUP_ERR: usize = 3;

    pub const SCHEMA_OK: usize = 1;
    pub const SCHEMA_OK_PUSHDOWN: usize = 2;
    pub const SCHEMA_TABLE_NAMES_ERR: usize = 3;
    pub const SCHEMA_TABLE_LOOKUP_ERR: usize = 4;

    pub const STUB_SCHEMA_NAME: &str = "sch";
    pub const STUB_TABLE_NAME: &str = "t";

    /// Decodes the CATALOG_* field packed into a test handle by
    /// `catalog::tests::handle`.
    pub fn catalog_kind_of(handle: usize) -> usize {
        handle % 10
    }

    /// Decodes the SCHEMA_* field packed into a test handle.
    pub fn schema_kind_of(handle: usize) -> usize {
        (handle / 10) % 10
    }

    /// Decodes the uniqueness offset packed into a test handle — the same
    /// base a derived table handle (`uniqueness_base_of(h) + TABLE_OK`,
    /// etc.) is built from, so a test can compute the exact table handle
    /// go_table_scan received without re-deriving the packing scheme.
    pub fn uniqueness_base_of(handle: usize) -> usize {
        (handle / 100) * 100
    }

    /// What one `go_table_scan` call received, recorded by the stub.
    #[derive(Debug, Clone)]
    pub struct ScanRecord {
        pub handle: usize,
        pub projection: Option<Vec<i32>>,
        pub filters_json: Option<String>,
        pub limit: i64,
    }

    fn scan_records_lock() -> &'static Mutex<Vec<ScanRecord>> {
        static V: OnceLock<Mutex<Vec<ScanRecord>>> = OnceLock::new();
        V.get_or_init(|| Mutex::new(vec![]))
    }

    pub fn scan_records_for(handle: usize) -> Vec<ScanRecord> {
        scan_records_lock()
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.handle == handle)
            .cloned()
            .collect()
    }

    fn released_tables_lock() -> &'static Mutex<Vec<usize>> {
        static V: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
        V.get_or_init(|| Mutex::new(vec![]))
    }

    fn released_udfs_lock() -> &'static Mutex<Vec<usize>> {
        static V: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
        V.get_or_init(|| Mutex::new(vec![]))
    }

    fn released_catalogs_lock() -> &'static Mutex<Vec<usize>> {
        static V: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
        V.get_or_init(|| Mutex::new(vec![]))
    }

    fn released_schemas_lock() -> &'static Mutex<Vec<usize>> {
        static V: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
        V.get_or_init(|| Mutex::new(vec![]))
    }

    pub fn released_catalogs() -> Vec<usize> {
        released_catalogs_lock().lock().unwrap().clone()
    }

    pub fn released_schemas() -> Vec<usize> {
        released_schemas_lock().lock().unwrap().clone()
    }

    /// Creates a session through the real C ABI entry point, shared by
    /// every module's tests.
    pub unsafe fn new_session() -> *mut std::ffi::c_void {
        let mut err: *mut c_char = std::ptr::null_mut();
        let session = crate::df_session_new(&mut err);
        assert!(err.is_null());
        assert!(!session.is_null());
        session
    }

    /// Runs `sql` through the real C ABI entry point and collects the
    /// result, shared by every module's tests that don't need the result
    /// schema separately (table.rs keeps its own richer variant for that).
    pub unsafe fn query(
        session: *mut std::ffi::c_void,
        sql: &str,
    ) -> Result<Vec<RecordBatch>, String> {
        use arrow::ffi_stream::ArrowArrayStreamReader;
        use std::ffi::CString;
        use std::mem::MaybeUninit;

        let csql = CString::new(sql).unwrap();
        let mut stream = MaybeUninit::<FFI_ArrowArrayStream>::uninit();
        let err = crate::df_session_sql(session, csql.as_ptr(), stream.as_mut_ptr());
        if !err.is_null() {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            crate::df_string_free(err);
            return Err(msg);
        }
        let mut stream = stream.assume_init();
        let reader = ArrowArrayStreamReader::from_raw(&mut stream).map_err(|e| e.to_string())?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }

    fn udf_invocations_lock() -> &'static Mutex<Vec<usize>> {
        static V: OnceLock<Mutex<Vec<usize>>> = OnceLock::new();
        V.get_or_init(|| Mutex::new(vec![]))
    }

    pub fn released_tables() -> Vec<usize> {
        released_tables_lock().lock().unwrap().clone()
    }

    pub fn released_udfs() -> Vec<usize> {
        released_udfs_lock().lock().unwrap().clone()
    }

    pub fn udf_invocations() -> Vec<usize> {
        udf_invocations_lock().lock().unwrap().clone()
    }

    extern "C" {
        fn malloc(size: usize) -> *mut std::ffi::c_void;
    }

    /// Emulates a Go-written malloc-allocated NUL-terminated string, so
    /// the Rust side's `free`-based cleanup matches. Shared by the error
    /// out-param convention and by the catalog/schema name-list convention
    /// (design D5 of add-catalog-provider), which are the same shape.
    unsafe fn write_malloc_string(out: *mut *mut c_char, msg: &str) {
        let bytes = msg.as_bytes();
        let p = unsafe { malloc(bytes.len() + 1) } as *mut u8;
        assert!(!p.is_null());
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
            *p.add(bytes.len()) = 0;
            *out = p as *mut c_char;
        }
    }

    unsafe fn write_malloc_error(error_out: *mut *mut c_char, msg: &str) {
        unsafe { write_malloc_string(error_out, msg) };
    }

    fn stub_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn stub_batches() -> Vec<RecordBatch> {
        let schema = stub_schema();
        let b1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
            ],
        )
        .unwrap();
        let b2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["carol", "dave"])),
            ],
        )
        .unwrap();
        vec![b1, b2]
    }

    #[no_mangle]
    extern "C" fn go_table_schema(
        handle: usize,
        out_schema: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    ) {
        unsafe {
            if handle % 10 == TABLE_SCHEMA_ERR {
                write_malloc_error(error_out, "schema fetch failed (stub)");
                return;
            }
            let ffi = FFI_ArrowSchema::try_from(stub_schema().as_ref()).unwrap();
            std::ptr::write(out_schema, ffi);
        }
    }

    #[no_mangle]
    extern "C" fn go_table_scan(
        handle: usize,
        projection: *const i32,
        projection_len: isize,
        filters_json: *const c_char,
        limit: i64,
        out_stream: *mut FFI_ArrowArrayStream,
        error_out: *mut *mut c_char,
    ) {
        let projection = if projection_len < 0 {
            None
        } else {
            Some(if projection_len == 0 {
                vec![]
            } else {
                unsafe { std::slice::from_raw_parts(projection, projection_len as usize) }.to_vec()
            })
        };
        let filters = if filters_json.is_null() {
            None
        } else {
            Some(
                unsafe { CStr::from_ptr(filters_json) }
                    .to_string_lossy()
                    .into_owned(),
            )
        };
        scan_records_lock().lock().unwrap().push(ScanRecord {
            handle,
            projection: projection.clone(),
            filters_json: filters,
            limit,
        });

        let project = |batches: Vec<RecordBatch>| -> (SchemaRef, Vec<RecordBatch>) {
            match &projection {
                Some(indices) => {
                    let indices: Vec<usize> = indices.iter().map(|&i| i as usize).collect();
                    let schema = Arc::new(stub_schema().project(&indices).unwrap());
                    let batches = batches
                        .into_iter()
                        .map(|b| b.project(&indices).unwrap())
                        .collect();
                    (schema, batches)
                }
                None => (stub_schema(), batches),
            }
        };

        unsafe {
            match handle % 10 {
                TABLE_SCAN_OPEN_ERR => {
                    write_malloc_error(error_out, "scan open failed (stub)");
                }
                TABLE_SCAN_MIDSTREAM_ERR => {
                    let batches = stub_batches();
                    let schema = batches[0].schema();
                    let items = vec![
                        Ok(batches[0].clone()),
                        Err(ArrowError::ComputeError("mid-stream failure (stub)".into())),
                    ];
                    let iter = RecordBatchIterator::new(items, schema);
                    std::ptr::write(out_stream, FFI_ArrowArrayStream::new(Box::new(iter)));
                }
                TABLE_PUSHDOWN_WRONG_SCHEMA => {
                    // Deliberately ignores the projection: full schema back.
                    let batches = stub_batches();
                    let schema = batches[0].schema();
                    let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
                    std::ptr::write(out_stream, FFI_ArrowArrayStream::new(Box::new(iter)));
                }
                _ => {
                    // TABLE_OK ignores the (always absent) pushdown params;
                    // TABLE_PUSHDOWN_OK honors the projection exactly.
                    let (schema, batches) = project(stub_batches());
                    let iter = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
                    std::ptr::write(out_stream, FFI_ArrowArrayStream::new(Box::new(iter)));
                }
            }
        }
    }

    #[no_mangle]
    extern "C" fn go_table_release(handle: usize) {
        released_tables_lock().lock().unwrap().push(handle);
    }

    #[no_mangle]
    extern "C" fn go_scalar_udf_invoke(
        handle: usize,
        args_array: *mut FFI_ArrowArray,
        args_schema: *mut FFI_ArrowSchema,
        out_array: *mut FFI_ArrowArray,
        out_schema: *mut FFI_ArrowSchema,
        error_out: *mut *mut c_char,
    ) {
        udf_invocations_lock().lock().unwrap().push(handle);
        unsafe {
            // Take ownership of the inputs like the real Go side does.
            let ffi_array = std::ptr::read(args_array);
            std::ptr::write(args_array, FFI_ArrowArray::empty());
            let ffi_schema = std::ptr::read(args_schema);
            std::ptr::write(args_schema, FFI_ArrowSchema::empty());

            if handle % 10 == UDF_INVOKE_ERR {
                write_malloc_error(error_out, "evaluation failed (stub)");
                return;
            }

            let data = from_ffi(ffi_array, &ffi_schema).unwrap();
            let bundled = StructArray::from(data);
            let col = bundled
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let doubled: Int64Array = col.iter().map(|v| v.map(|v| v * 2)).collect();
            let (arr, schema) = to_ffi(&doubled.into_data()).unwrap();
            std::ptr::write(out_array, arr);
            std::ptr::write(out_schema, schema);
        }
    }

    #[no_mangle]
    extern "C" fn go_scalar_udf_release(handle: usize) {
        released_udfs_lock().lock().unwrap().push(handle);
    }

    #[no_mangle]
    extern "C" fn go_catalog_schema_names(
        handle: usize,
        out_names_json: *mut *mut c_char,
        error_out: *mut *mut c_char,
    ) {
        unsafe {
            if catalog_kind_of(handle) == CATALOG_SCHEMA_NAMES_ERR {
                write_malloc_error(error_out, "schema listing failed (stub)");
                return;
            }
            write_malloc_string(out_names_json, &format!("[{STUB_SCHEMA_NAME:?}]"));
        }
    }

    #[no_mangle]
    extern "C" fn go_catalog_schema_lookup(
        handle: usize,
        name: *const c_char,
        out_schema_handle: *mut usize,
        out_found: *mut u8,
        error_out: *mut *mut c_char,
    ) {
        let name = unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned();
        unsafe {
            if catalog_kind_of(handle) == CATALOG_SCHEMA_LOOKUP_ERR {
                write_malloc_error(error_out, "schema lookup failed (stub)");
                return;
            }
            if name != STUB_SCHEMA_NAME {
                *out_found = 0;
                return;
            }
            *out_found = 1;
            // The schema handle reuses the catalog handle's numeric value:
            // its tens digit selects the SCHEMA_* stub behavior below, and
            // its hundreds-and-up digits carry the per-test uniqueness
            // offset through to the derived table handle.
            *out_schema_handle = handle;
        }
    }

    #[no_mangle]
    extern "C" fn go_catalog_release(handle: usize) {
        released_catalogs_lock().lock().unwrap().push(handle);
    }

    #[no_mangle]
    extern "C" fn go_schema_table_names(
        handle: usize,
        out_names_json: *mut *mut c_char,
        error_out: *mut *mut c_char,
    ) {
        unsafe {
            if schema_kind_of(handle) == SCHEMA_TABLE_NAMES_ERR {
                write_malloc_error(error_out, "table listing failed (stub)");
                return;
            }
            write_malloc_string(out_names_json, &format!("[{STUB_TABLE_NAME:?}]"));
        }
    }

    #[no_mangle]
    extern "C" fn go_schema_table_lookup(
        handle: usize,
        name: *const c_char,
        out_table_handle: *mut usize,
        out_supports_pushdown: *mut u8,
        out_found: *mut u8,
        error_out: *mut *mut c_char,
    ) {
        let name = unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned();
        let kind = schema_kind_of(handle);
        unsafe {
            if kind == SCHEMA_TABLE_LOOKUP_ERR {
                write_malloc_error(error_out, "table lookup failed (stub)");
                return;
            }
            if name != STUB_TABLE_NAME {
                *out_found = 0;
                return;
            }
            *out_found = 1;
            let base = uniqueness_base_of(handle);
            if kind == SCHEMA_OK_PUSHDOWN {
                *out_table_handle = base + TABLE_PUSHDOWN_OK;
                *out_supports_pushdown = 1;
            } else {
                *out_table_handle = base + TABLE_OK;
                *out_supports_pushdown = 0;
            }
        }
    }

    #[no_mangle]
    extern "C" fn go_schema_release(handle: usize) {
        released_schemas_lock().lock().unwrap().push(handle);
    }
}
