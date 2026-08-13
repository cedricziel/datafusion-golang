//! A DataFusion `TableProvider` backed by a Go implementation, dispatching
//! through the fixed Go-exported trampoline symbols in [`crate::ffi`].

use std::fmt;
use std::ptr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{exec_err, internal_err, DataFusionError, Result};
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

use crate::ffi::{
    go_table_release, go_table_scan, go_table_schema, spawn_go_blocking, take_go_error,
};

/// Wraps a Go-side table implementation identified by an opaque
/// `cgo.Handle`-derived token. The schema is fetched exactly once at
/// construction (design D3); the handle is released on drop (design D2).
#[derive(Debug)]
pub(crate) struct GoTableProvider {
    handle: usize,
    schema: SchemaRef,
}

impl GoTableProvider {
    /// Takes ownership of `handle`. On error the handle is released via
    /// `go_table_release` before returning, so the caller never needs to
    /// clean up.
    pub(crate) fn try_new(handle: usize) -> Result<Self, String> {
        // Registration runs on the Go caller's own thread (a direct FFI
        // call, not a tokio worker), so calling back into Go inline here
        // cannot starve the shared runtime.
        let mut ffi_schema = FFI_ArrowSchema::empty();
        let mut err: *mut std::ffi::c_char = ptr::null_mut();
        unsafe { go_table_schema(handle, &mut ffi_schema, &mut err) };
        if let Some(msg) = unsafe { take_go_error(err) } {
            unsafe { go_table_release(handle) };
            return Err(format!("fetching table schema from Go: {msg}"));
        }
        match Schema::try_from(&ffi_schema) {
            Ok(schema) => Ok(Self {
                handle,
                schema: Arc::new(schema),
            }),
            Err(e) => {
                unsafe { go_table_release(handle) };
                Err(format!("importing table schema over FFI: {e}"))
            }
        }
    }
}

impl Drop for GoTableProvider {
    fn drop(&mut self) {
        unsafe { go_table_release(self.handle) };
    }
}

#[async_trait]
impl TableProvider for GoTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // No pushdown in this phase: DataFusion filters after the scan.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The exec node outlives `self` only within a query, and queries
        // are fully drained before the session (and thus the provider)
        // is dropped (design D7), so copying the raw handle is safe.
        Ok(Arc::new(GoTableExec::new(
            self.handle,
            Arc::clone(&self.schema),
            projection.cloned(),
        )))
    }
}

/// Leaf execution plan that pulls record batches from the Arrow C Stream
/// produced by `go_table_scan`, batch by batch as the query consumes them
/// (design D4). Every call into Go happens on the blocking pool (design
/// D6).
#[derive(Debug)]
struct GoTableExec {
    handle: usize,
    projection: Option<Vec<usize>>,
    projected_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl GoTableExec {
    fn new(handle: usize, table_schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(
                table_schema
                    .project(indices)
                    .expect("projection indices validated by planner"),
            ),
            None => table_schema,
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&projected_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            handle,
            projection,
            projected_schema,
            properties,
        }
    }
}

impl DisplayAs for GoTableExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GoTableExec")
    }
}

/// The stream reader owns an `FFI_ArrowArrayStream` (which is `Send`) and
/// a `SchemaRef`; batches are pulled strictly sequentially, so moving it
/// between blocking-pool threads between pulls is sound.
struct GoScanReader(ArrowArrayStreamReader);

fn open_go_scan(handle: usize) -> Result<GoScanReader, String> {
    let mut ffi_stream = FFI_ArrowArrayStream::empty();
    let mut err: *mut std::ffi::c_char = ptr::null_mut();
    unsafe { go_table_scan(handle, &mut ffi_stream, &mut err) };
    if let Some(msg) = unsafe { take_go_error(err) } {
        return Err(format!("Go table scan failed: {msg}"));
    }
    // from_raw also fetches the stream schema, i.e. one more call into Go;
    // callers must therefore invoke this on the blocking pool too.
    unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }
        .map(GoScanReader)
        .map_err(|e| format!("importing Go scan stream: {e}"))
}

enum ScanState {
    Unopened(usize),
    Open(GoScanReader),
}

impl ExecutionPlan for GoTableExec {
    fn name(&self) -> &str {
        "GoTableExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("GoTableExec does not accept children")
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("GoTableExec has a single partition, got {partition}");
        }
        let projection = self.projection.clone();
        let stream = futures::stream::try_unfold(ScanState::Unopened(self.handle), move |state| {
            let projection = projection.clone();
            async move {
                let mut reader = match state {
                    ScanState::Unopened(handle) => spawn_go_blocking(move || open_go_scan(handle))
                        .await
                        .map_err(DataFusionError::Execution)?,
                    ScanState::Open(reader) => reader,
                };
                let (item, reader) = spawn_go_blocking(move || {
                    let item = reader.0.next();
                    Ok((item, reader))
                })
                .await
                .map_err(DataFusionError::Execution)?;
                match item {
                    Some(Ok(batch)) => {
                        let batch = match &projection {
                            Some(indices) => batch.project(indices)?,
                            None => batch,
                        };
                        Ok(Some((batch, ScanState::Open(reader))))
                    }
                    Some(Err(e)) => exec_err!("Go table scan failed: {e}"),
                    None => Ok::<Option<(RecordBatch, ScanState)>, DataFusionError>(None),
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.projected_schema),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use crate::ffi::tests::{
        released_tables, TABLE_OK, TABLE_SCAN_MIDSTREAM_ERR, TABLE_SCAN_OPEN_ERR, TABLE_SCHEMA_ERR,
    };
    use crate::{df_session_free, df_session_new, df_session_register_table, df_session_sql};

    use std::ffi::{c_char, CStr, CString};
    use std::mem::MaybeUninit;

    use arrow::array::{Int64Array, RecordBatch};
    use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow::record_batch::RecordBatchReader;

    unsafe fn new_session() -> *mut std::ffi::c_void {
        let mut err: *mut c_char = std::ptr::null_mut();
        let session = df_session_new(&mut err);
        assert!(err.is_null());
        assert!(!session.is_null());
        session
    }

    unsafe fn register(
        session: *mut std::ffi::c_void,
        name: &str,
        handle: usize,
    ) -> Option<String> {
        let cname = CString::new(name).unwrap();
        let err = df_session_register_table(session, cname.as_ptr(), handle);
        if err.is_null() {
            None
        } else {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            crate::df_string_free(err);
            Some(msg)
        }
    }

    unsafe fn query(
        session: *mut std::ffi::c_void,
        sql: &str,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<RecordBatch>), String> {
        let csql = CString::new(sql).unwrap();
        let mut stream = MaybeUninit::<FFI_ArrowArrayStream>::uninit();
        let err = df_session_sql(session, csql.as_ptr(), stream.as_mut_ptr());
        if !err.is_null() {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            crate::df_string_free(err);
            return Err(msg);
        }
        let mut stream = stream.assume_init();
        let reader = ArrowArrayStreamReader::from_raw(&mut stream).map_err(|e| e.to_string())?;
        let schema = reader.schema();
        let batches = reader
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        Ok((schema, batches))
    }

    #[test]
    fn register_and_select_star() {
        unsafe {
            let session = new_session();
            let handle = TABLE_OK + 10; // unique per test for release tracking
            assert_eq!(register(session, "people", handle), None);

            let (schema, batches) = query(session, "SELECT id, name FROM people ORDER BY id")
                .expect("query should succeed");
            assert_eq!(schema.field(0).name(), "id");
            assert_eq!(schema.field(1).name(), "name");
            let total: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(total, 4, "stub table produces 2 batches x 2 rows");
            let first = batches
                .iter()
                .find(|b| b.num_rows() > 0)
                .expect("at least one non-empty batch");
            let ids = first
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(ids.value(0), 1);

            df_session_free(session);
            assert!(
                released_tables().contains(&handle),
                "session free must release the Go handle"
            );
        }
    }

    #[test]
    fn duplicate_registration_is_rejected_and_releases_handle() {
        unsafe {
            let session = new_session();
            let first = TABLE_OK + 20;
            let second = TABLE_OK + 30;
            assert_eq!(register(session, "dup", first), None);
            let msg = register(session, "dup", second).expect("duplicate must error");
            assert!(msg.contains("already"), "unexpected message: {msg}");
            assert!(
                released_tables().contains(&second),
                "rejected registration must release the new handle"
            );
            assert!(
                !released_tables().contains(&first),
                "original registration must stay alive"
            );

            // original table remains queryable
            let (_, batches) = query(session, "SELECT * FROM dup").unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
            df_session_free(session);
        }
    }

    #[test]
    fn schema_fetch_error_fails_registration() {
        unsafe {
            let session = new_session();
            let handle = TABLE_SCHEMA_ERR + 40;
            let msg = register(session, "bad", handle).expect("schema error must fail");
            assert!(msg.contains("schema fetch failed"), "got: {msg}");
            assert!(released_tables().contains(&handle));
            df_session_free(session);
        }
    }

    #[test]
    fn unknown_column_fails_at_planning() {
        unsafe {
            let session = new_session();
            assert_eq!(register(session, "plan_t", TABLE_OK + 50), None);
            let err = query(session, "SELECT missing_col FROM plan_t").unwrap_err();
            assert!(err.contains("missing_col"), "got: {err}");
            df_session_free(session);
        }
    }

    #[test]
    fn scan_open_error_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            assert_eq!(register(session, "openerr", TABLE_SCAN_OPEN_ERR + 60), None);
            let err = query(session, "SELECT * FROM openerr").unwrap_err();
            assert!(err.contains("scan open failed"), "got: {err}");
            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1);
            df_session_free(session);
        }
    }

    #[test]
    fn scan_midstream_error_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            assert_eq!(
                register(session, "miderr", TABLE_SCAN_MIDSTREAM_ERR + 70),
                None
            );
            let err = query(session, "SELECT * FROM miderr").unwrap_err();
            assert!(err.contains("mid-stream failure"), "got: {err}");
            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1);
            df_session_free(session);
        }
    }

    #[test]
    fn filtered_and_projected_query() {
        unsafe {
            let session = new_session();
            assert_eq!(register(session, "proj", TABLE_OK + 80), None);
            // filter is applied post-scan (no pushdown); projection is
            // honored by the exec node
            let (schema, batches) = query(session, "SELECT name FROM proj WHERE id > 2").unwrap();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "name");
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
            df_session_free(session);
        }
    }
}
