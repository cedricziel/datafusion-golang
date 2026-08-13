//! A DataFusion scalar UDF backed by a Go implementation, dispatching
//! through the fixed Go-exported trampoline symbols in [`crate::ffi`].

use std::hash::{Hash, Hasher};
use std::ptr;

use arrow::array::{make_array, Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Fields};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

use crate::ffi::{go_scalar_udf_invoke, go_scalar_udf_release, take_go_error};

/// Wraps a Go-side scalar function identified by an opaque
/// `cgo.Handle`-derived token. The argument and return types are supplied
/// once at registration and cached here (design D3); the handle is
/// released on drop (design D2).
#[derive(Debug)]
pub(crate) struct GoScalarUdf {
    name: String,
    handle: usize,
    signature: Signature,
    arg_fields: Fields,
    return_type: DataType,
}

impl GoScalarUdf {
    /// Takes ownership of `handle`; the caller must have validated the
    /// argument/return schemas before constructing.
    pub(crate) fn new(
        name: String,
        handle: usize,
        arg_fields: Fields,
        return_type: DataType,
    ) -> Self {
        let arg_types: Vec<DataType> = arg_fields.iter().map(|f| f.data_type().clone()).collect();
        // Volatile is the safe default: the engine will never constant-fold
        // the call away, so evaluation always reaches the Go implementation.
        let signature = Signature::exact(arg_types, Volatility::Volatile);
        Self {
            name,
            handle,
            signature,
            arg_fields,
            return_type,
        }
    }
}

impl Drop for GoScalarUdf {
    fn drop(&mut self) {
        unsafe { go_scalar_udf_release(self.handle) };
    }
}

// Handles are unique per registration, so identity-by-handle satisfies the
// DynEq/DynHash supertraits of ScalarUDFImpl.
impl PartialEq for GoScalarUdf {
    fn eq(&self, other: &Self) -> bool {
        self.handle == other.handle
    }
}

impl Eq for GoScalarUdf {}

impl Hash for GoScalarUdf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.handle.hash(state);
    }
}

impl ScalarUDFImpl for GoScalarUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let num_rows = args.number_rows;
        let arrays: Vec<ArrayRef> = args
            .args
            .into_iter()
            .map(|cv| cv.into_array(num_rows))
            .collect::<Result<_>>()?;

        // Bundle the batch's argument columns into one struct array so the
        // FFI signature stays fixed-arity (design D5).
        let bundled = if arrays.is_empty() {
            StructArray::new_empty_fields(num_rows, None)
        } else {
            match StructArray::try_new(self.arg_fields.clone(), arrays, None) {
                Ok(s) => s,
                Err(e) => return exec_err!("bundling arguments for Go UDF '{}': {e}", self.name),
            }
        };
        let (mut ffi_array, mut ffi_schema) = match to_ffi(&bundled.into_data()) {
            Ok(pair) => pair,
            Err(e) => return exec_err!("exporting arguments for Go UDF '{}': {e}", self.name),
        };

        // invoke_with_args is synchronous and may run directly on the
        // thread driving the query (see D6 deviation note in the design
        // report): the calling thread must wait for the result either way,
        // so the callback runs inline. Go consumes the argument array
        // (move) and schema (release) in all cases, so dropping the
        // now-empty ffi structs afterwards is a no-op; on a Go panic
        // before consumption, dropping them releases the export instead.
        let mut out_array = FFI_ArrowArray::empty();
        let mut out_schema = FFI_ArrowSchema::empty();
        let mut err: *mut std::ffi::c_char = ptr::null_mut();
        unsafe {
            go_scalar_udf_invoke(
                self.handle,
                &mut ffi_array,
                &mut ffi_schema,
                &mut out_array,
                &mut out_schema,
                &mut err,
            );
        }
        if let Some(msg) = unsafe { take_go_error(err) } {
            return exec_err!("Go scalar UDF '{}' failed: {msg}", self.name);
        }

        let data = match unsafe { from_ffi(out_array, &out_schema) } {
            Ok(data) => data,
            Err(e) => return exec_err!("importing result of Go UDF '{}': {e}", self.name),
        };
        let array = make_array(data);
        if array.data_type() != &self.return_type {
            return exec_err!(
                "Go scalar UDF '{}' returned type {} but declared {}",
                self.name,
                array.data_type(),
                self.return_type
            );
        }
        if array.len() != num_rows {
            return exec_err!(
                "Go scalar UDF '{}' returned {} rows for a batch of {num_rows}",
                self.name,
                array.len()
            );
        }
        Ok(ColumnarValue::Array(array))
    }
}

#[cfg(test)]
mod tests {
    use crate::ffi::tests::{released_udfs, udf_invocations, UDF_DOUBLE, UDF_INVOKE_ERR};
    use crate::{
        df_session_free, df_session_new, df_session_register_scalar_udf, df_session_sql,
        df_string_free,
    };

    use std::ffi::{c_char, CStr, CString};
    use std::mem::MaybeUninit;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ffi::FFI_ArrowSchema;
    use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

    unsafe fn new_session() -> *mut std::ffi::c_void {
        let mut err: *mut c_char = std::ptr::null_mut();
        let session = df_session_new(&mut err);
        assert!(err.is_null());
        session
    }

    fn int64_args_schema() -> FFI_ArrowSchema {
        let schema = Schema::new(vec![Field::new("x", DataType::Int64, true)]);
        FFI_ArrowSchema::try_from(&schema).unwrap()
    }

    fn int64_return_schema() -> FFI_ArrowSchema {
        let schema = Schema::new(vec![Field::new("out", DataType::Int64, true)]);
        FFI_ArrowSchema::try_from(&schema).unwrap()
    }

    unsafe fn register(
        session: *mut std::ffi::c_void,
        name: &str,
        handle: usize,
    ) -> Option<String> {
        let cname = CString::new(name).unwrap();
        let mut args = int64_args_schema();
        let mut ret = int64_return_schema();
        let err =
            df_session_register_scalar_udf(session, cname.as_ptr(), &mut args, &mut ret, handle);
        if err.is_null() {
            None
        } else {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            df_string_free(err);
            Some(msg)
        }
    }

    unsafe fn query(session: *mut std::ffi::c_void, sql: &str) -> Result<Vec<RecordBatch>, String> {
        let csql = CString::new(sql).unwrap();
        let mut stream = MaybeUninit::<FFI_ArrowArrayStream>::uninit();
        let err = df_session_sql(session, csql.as_ptr(), stream.as_mut_ptr());
        if !err.is_null() {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            df_string_free(err);
            return Err(msg);
        }
        let mut stream = stream.assume_init();
        let reader = ArrowArrayStreamReader::from_raw(&mut stream).map_err(|e| e.to_string())?;
        reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())
    }

    fn int64_col(batches: &[RecordBatch], col: usize) -> Vec<i64> {
        let mut out = vec![];
        for b in batches {
            let a = b.column(col).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..a.len() {
                out.push(a.value(i));
            }
        }
        out
    }

    #[test]
    fn register_and_call_double() {
        unsafe {
            let session = new_session();
            let handle = UDF_DOUBLE + 10;
            assert_eq!(register(session, "double", handle), None);

            let batches = query(session, "SELECT double(21) AS answer").unwrap();
            assert_eq!(int64_col(&batches, 0), vec![42]);

            df_session_free(session);
            assert!(
                released_udfs().contains(&handle),
                "session free must release the Go handle"
            );
        }
    }

    #[test]
    fn vectorized_over_a_column() {
        unsafe {
            let session = new_session();
            assert_eq!(register(session, "double2", UDF_DOUBLE + 20), None);
            let batches = query(
                session,
                "SELECT double2(v) FROM (VALUES (1), (2), (3)) AS t(v) ORDER BY 1",
            )
            .unwrap();
            assert_eq!(int64_col(&batches, 0), vec![2, 4, 6]);
            df_session_free(session);
        }
    }

    #[test]
    fn duplicate_registration_is_rejected_and_releases_handle() {
        unsafe {
            let session = new_session();
            let first = UDF_DOUBLE + 30;
            let second = UDF_DOUBLE + 40;
            assert_eq!(register(session, "dupfn", first), None);
            let msg = register(session, "dupfn", second).expect("duplicate must error");
            assert!(msg.contains("already"), "got: {msg}");
            assert!(released_udfs().contains(&second));
            assert!(!released_udfs().contains(&first));

            // original function remains callable
            let batches = query(session, "SELECT dupfn(5)").unwrap();
            assert_eq!(int64_col(&batches, 0), vec![10]);
            df_session_free(session);
        }
    }

    #[test]
    fn registering_over_a_builtin_is_rejected() {
        unsafe {
            let session = new_session();
            let handle = UDF_DOUBLE + 50;
            let msg = register(session, "abs", handle).expect("builtin collision must error");
            assert!(msg.contains("already"), "got: {msg}");
            assert!(released_udfs().contains(&handle));
            df_session_free(session);
        }
    }

    #[test]
    fn wrong_argument_type_is_rejected_at_planning() {
        unsafe {
            let session = new_session();
            let handle = UDF_DOUBLE + 60;
            assert_eq!(register(session, "typedfn", handle), None);
            let err = query(session, "SELECT typedfn('not a number')").unwrap_err();
            assert!(
                err.contains("coerce") || err.contains("No function matches"),
                "got: {err}"
            );
            assert!(
                !udf_invocations().contains(&handle),
                "planning rejection must not invoke the function"
            );
            df_session_free(session);
        }
    }

    #[test]
    fn evaluation_error_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            assert_eq!(register(session, "failfn", UDF_INVOKE_ERR + 70), None);
            let err = query(session, "SELECT failfn(1)").unwrap_err();
            assert!(err.contains("evaluation failed"), "got: {err}");
            let batches = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(int64_col(&batches, 0), vec![1]);
            df_session_free(session);
        }
    }

    #[test]
    fn arity_mismatch_is_rejected_at_planning() {
        unsafe {
            let session = new_session();
            let handle = UDF_DOUBLE + 80;
            assert_eq!(register(session, "unary", handle), None);
            let err = query(session, "SELECT unary(1, 2)").unwrap_err();
            assert!(
                err.contains("expected") || err.contains("coerce"),
                "got: {err}"
            );
            assert!(!udf_invocations().contains(&handle));
            df_session_free(session);
        }
    }

    #[test]
    fn udf_applied_to_go_table_scan() {
        // integration: a registered UDF applied over a registered table
        unsafe {
            use crate::ffi::tests::TABLE_OK;
            let session = new_session();
            let cname = CString::new("combo_t").unwrap();
            let err = crate::df_session_register_table(session, cname.as_ptr(), TABLE_OK + 90, 0);
            assert!(err.is_null());
            assert_eq!(register(session, "combofn", UDF_DOUBLE + 90), None);

            let batches = query(session, "SELECT combofn(id) FROM combo_t ORDER BY 1").unwrap();
            assert_eq!(int64_col(&batches, 0), vec![2, 4, 6, 8]);
            df_session_free(session);
        }
    }
}
