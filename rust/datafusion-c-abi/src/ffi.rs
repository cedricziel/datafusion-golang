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
    let msg = unsafe { CStr::from_ptr(err) }
        .to_string_lossy()
        .into_owned();
    unsafe { free(err as *mut c_void) };
    Some(msg)
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

/// Test stand-ins for the Go-exported trampoline symbols. A pure
/// `cargo test` binary has no Go side, so these `#[no_mangle]`
/// definitions satisfy the extern declarations above at link time.
///
/// Behavior is keyed on `handle % 10` so each test can use a unique
/// handle value and still get deterministic per-handle release/invoke
/// tracking under parallel test execution.
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

    /// Emulates a Go-written error string: allocated with malloc so the
    /// Rust side's `free`-based cleanup matches.
    unsafe fn write_malloc_error(error_out: *mut *mut c_char, msg: &str) {
        extern "C" {
            fn malloc(size: usize) -> *mut std::ffi::c_void;
        }
        let bytes = msg.as_bytes();
        let p = unsafe { malloc(bytes.len() + 1) } as *mut u8;
        assert!(!p.is_null());
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), p, bytes.len());
            *p.add(bytes.len()) = 0;
            *error_out = p as *mut c_char;
        }
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
}
