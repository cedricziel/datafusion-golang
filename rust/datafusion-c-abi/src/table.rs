//! A DataFusion `TableProvider` backed by a Go implementation, dispatching
//! through the fixed Go-exported trampoline symbols in [`crate::ffi`].

use std::ffi::CString;
use std::fmt;
use std::ptr;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::ffi::FFI_ArrowSchema;
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow::record_batch::RecordBatchReader;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::{
    exec_err, internal_err, not_impl_err, DataFusionError, Result, SchemaExt,
};
use datafusion::datasource::sink::{DataSink, DataSinkExec};
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::StreamExt;

use crate::ffi::{
    go_table_insert, go_table_release, go_table_scan, go_table_schema, spawn_go_blocking,
    take_go_error,
};
use crate::pushdown;

/// Wraps a Go-side table implementation identified by an opaque
/// `cgo.Handle`-derived token. The schema is fetched exactly once at
/// construction (design D3); the handle is released on drop (design D2).
#[derive(Debug)]
pub(crate) struct GoTableProvider {
    handle: usize,
    schema: SchemaRef,
    supports_pushdown: bool,
    supports_insert: bool,
}

impl GoTableProvider {
    /// Takes ownership of `handle`. On error the handle is released via
    /// `go_table_release` before returning, so the caller never needs to
    /// clean up.
    pub(crate) fn try_new(
        handle: usize,
        supports_pushdown: bool,
        supports_insert: bool,
    ) -> Result<Self, String> {
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
                supports_pushdown,
                supports_insert,
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
        if !self.supports_pushdown {
            // No pushdown: DataFusion filters after the scan.
            return Ok(vec![
                TableProviderFilterPushDown::Unsupported;
                filters.len()
            ]);
        }
        // Pure-Rust classification, no FFI during planning (design D4).
        // Never `Exact`: DataFusion re-applies every `Inexact` conjunct
        // after the scan, so correctness cannot depend on what the Go
        // provider does with a pushed filter (design D1).
        Ok(filters
            .iter()
            .map(|f| {
                if pushdown::classify(f, &self.schema).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The exec node outlives `self` only within a query, and queries
        // are fully drained before the session (and thus the provider)
        // is dropped (design D7), so copying the raw handle is safe.
        let pushdown = if self.supports_pushdown {
            Some(PushdownArgs {
                filters_json: pushdown::filters_to_json(filters, &self.schema),
                limit: limit.map_or(-1, |l| l as i64),
            })
        } else {
            None
        };
        Ok(Arc::new(GoTableExec::new(
            self.handle,
            Arc::clone(&self.schema),
            projection.cloned(),
            pushdown,
        )))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !self.supports_insert {
            return not_impl_err!("table does not support INSERT");
        }
        // The SQL planner already projects/casts the input to the
        // registered schema (design Context); this is defense-in-depth,
        // the same check MemTable's own insert_into makes.
        self.schema
            .logically_equivalent_names_and_types(&input.schema())?;

        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(GoDataSink {
                handle: self.handle,
                schema: Arc::clone(&self.schema),
                insert_op,
            }),
            None,
        )))
    }
}

/// Number of in-flight record batches the write_all bridge (design D4)
/// buffers between the async pump task and the synchronous Go-facing
/// reader. Small and fixed: this bounds memory, not throughput — Go pulls
/// as fast as it commits.
const INSERT_CHANNEL_CAPACITY: usize = 8;

fn insert_op_to_i32(op: InsertOp) -> i32 {
    match op {
        InsertOp::Append => 0,
        InsertOp::Overwrite => 1,
        InsertOp::Replace => 2,
    }
}

/// `DataSink` that delivers an INSERT statement's input rows to Go via
/// `go_table_insert`, bridging the async input stream to Go's synchronous
/// pull through a bounded channel (design D4).
#[derive(Debug)]
struct GoDataSink {
    handle: usize,
    schema: SchemaRef,
    insert_op: InsertOp,
}

impl DisplayAs for GoDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "GoDataSink")
    }
}

/// Synchronous `RecordBatchReader` over the receiving end of the bridge
/// channel. `blocking_recv` is only valid off the async runtime's worker
/// threads; every use of this type runs inside `spawn_go_blocking`
/// (design D4), i.e. on the blocking pool, never a worker thread.
struct ChannelReader {
    schema: SchemaRef,
    rx: tokio::sync::mpsc::Receiver<Result<RecordBatch>>,
}

impl Iterator for ChannelReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rx
            .blocking_recv()
            .map(|r| r.map_err(|e| ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for ChannelReader {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[async_trait]
impl DataSink for GoDataSink {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let (tx, rx) = tokio::sync::mpsc::channel(INSERT_CHANNEL_CAPACITY);

        // Pulls the input plan and forwards each batch (or its error) into
        // the bridge channel. Must be joined before this function returns
        // on every path — success, Go-side error, or early Go return — so
        // an in-flight pull (e.g. a scan of another Go table sourcing an
        // `INSERT ... SELECT`) never outlives the statement (design D6).
        let pump = crate::runtime().spawn(async move {
            while let Some(item) = data.next().await {
                if tx.send(item).await.is_err() {
                    // Receiver dropped: the Go call returned (successfully
                    // or not) without draining further. Stop pulling.
                    break;
                }
            }
        });

        let handle = self.handle;
        let schema = Arc::clone(&self.schema);
        let insert_op = insert_op_to_i32(self.insert_op);
        let result = spawn_go_blocking(move || {
            let reader = ChannelReader { schema, rx };
            let mut ffi_stream = FFI_ArrowArrayStream::new(Box::new(reader));
            let mut rows: u64 = 0;
            let mut err: *mut std::ffi::c_char = ptr::null_mut();
            unsafe {
                go_table_insert(handle, &mut ffi_stream, insert_op, &mut rows, &mut err);
            }
            if let Some(msg) = unsafe { take_go_error(err) } {
                return Err(msg);
            }
            Ok(rows)
        })
        .await;

        // Always await the pump, on every path (design D6) — a detached
        // pull from the input plan must not survive this function. A
        // panic in the pump (as opposed to a normal Err polled from the
        // input stream, already forwarded through the channel above)
        // drops `tx` the same way a clean finish does, so Go would
        // otherwise see an ordinary end-of-stream and could report a
        // truncated insert as a success; surface it as a failure instead,
        // even when the Go call itself reported success.
        match (result, pump.await) {
            (Ok(rows), Ok(())) => Ok(rows),
            (Ok(_), Err(join_err)) => Err(DataFusionError::Execution(format!(
                "insert input pump failed: {join_err}"
            ))),
            (Err(msg), _) => Err(DataFusionError::Execution(msg)),
        }
    }
}

/// Scan-time pushdown payload, present only for providers registered with
/// `supports_pushdown` (design D5).
#[derive(Debug)]
struct PushdownArgs {
    filters_json: Option<CString>,
    limit: i64,
}

/// Leaf execution plan that pulls record batches from the Arrow C Stream
/// produced by `go_table_scan`, batch by batch as the query consumes them
/// (design D4). Every call into Go happens on the blocking pool (design
/// D6).
#[derive(Debug)]
struct GoTableExec {
    handle: usize,
    projection: Option<Vec<usize>>,
    pushdown: Option<PushdownArgs>,
    projected_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl GoTableExec {
    fn new(
        handle: usize,
        table_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        pushdown: Option<PushdownArgs>,
    ) -> Self {
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
            pushdown,
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

/// Everything one `go_table_scan` call needs, bundled so it can move onto
/// the blocking pool. For a non-pushdown scan the sentinels (NULL/-1) are
/// passed and the returned stream carries the full registered schema; for
/// a pushdown scan the projection/filters/limit cross and the returned
/// stream's schema is validated against `expected_schema` (design D5/D6).
struct ScanRequest {
    handle: usize,
    pushdown: bool,
    projection: Option<Vec<i32>>,
    filters_json: Option<CString>,
    limit: i64,
    expected_schema: SchemaRef,
}

/// The stream reader owns an `FFI_ArrowArrayStream` (which is `Send`) and
/// a `SchemaRef`; batches are pulled strictly sequentially, so moving it
/// between blocking-pool threads between pulls is sound.
struct GoScanReader(ArrowArrayStreamReader);

fn open_go_scan(request: &ScanRequest) -> Result<GoScanReader, String> {
    let (projection_ptr, projection_len): (*const i32, isize) = match &request.projection {
        Some(indices) => (indices.as_ptr(), indices.len() as isize),
        None => (ptr::null(), -1),
    };
    let filters_ptr = request
        .filters_json
        .as_ref()
        .map_or(ptr::null(), |f| f.as_ptr());

    let mut ffi_stream = FFI_ArrowArrayStream::empty();
    let mut err: *mut std::ffi::c_char = ptr::null_mut();
    unsafe {
        go_table_scan(
            request.handle,
            projection_ptr,
            projection_len,
            filters_ptr,
            request.limit,
            &mut ffi_stream,
            &mut err,
        )
    };
    if let Some(msg) = unsafe { take_go_error(err) } {
        return Err(format!("Go table scan failed: {msg}"));
    }
    // from_raw also fetches the stream schema, i.e. one more call into Go;
    // callers must therefore invoke this on the blocking pool too.
    let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut ffi_stream) }
        .map_err(|e| format!("importing Go scan stream: {e}"))?;
    if request.pushdown {
        // Projection is a hard contract for pushdown providers: fail fast
        // on a wrong schema instead of returning wrong columns (design D6).
        validate_projected_schema(&request.expected_schema, reader.schema().as_ref())?;
    }
    Ok(GoScanReader(reader))
}

/// Compares names, types, and order; metadata and nullability are ignored
/// (a nullability mismatch degrades to an Arrow-level error downstream,
/// which beats rejecting a working provider).
fn validate_projected_schema(expected: &Schema, actual: &Schema) -> Result<(), String> {
    let mismatch = |detail: String| {
        Err(format!(
            "Go pushdown table scan violated the projection contract: {detail} \
             (expected schema: {expected}, returned schema: {actual})"
        ))
    };
    if actual.fields().len() != expected.fields().len() {
        return mismatch(format!(
            "expected {} column(s), got {}",
            expected.fields().len(),
            actual.fields().len()
        ));
    }
    for (i, (want, got)) in expected
        .fields()
        .iter()
        .zip(actual.fields().iter())
        .enumerate()
    {
        if want.name() != got.name() {
            return mismatch(format!(
                "column {i} should be '{}', got '{}'",
                want.name(),
                got.name()
            ));
        }
        if want.data_type() != got.data_type() {
            return mismatch(format!(
                "column {i} ('{}') should have type {}, got {}",
                want.name(),
                want.data_type(),
                got.data_type()
            ));
        }
    }
    Ok(())
}

enum ScanState {
    Unopened,
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
        // Pushdown providers return the projected schema themselves; the
        // legacy path scans the full schema and re-projects per batch in
        // Rust (design D5).
        let legacy_projection = match self.pushdown {
            Some(_) => None,
            None => self.projection.clone(),
        };
        let request = Arc::new(ScanRequest {
            handle: self.handle,
            pushdown: self.pushdown.is_some(),
            projection: match &self.pushdown {
                Some(_) => self
                    .projection
                    .as_ref()
                    .map(|indices| indices.iter().map(|&i| i as i32).collect()),
                None => None,
            },
            filters_json: self.pushdown.as_ref().and_then(|p| p.filters_json.clone()),
            limit: self.pushdown.as_ref().map_or(-1, |p| p.limit),
            expected_schema: Arc::clone(&self.projected_schema),
        });
        let stream = futures::stream::try_unfold(ScanState::Unopened, move |state| {
            let request = Arc::clone(&request);
            let legacy_projection = legacy_projection.clone();
            async move {
                let mut reader = match state {
                    ScanState::Unopened => spawn_go_blocking(move || open_go_scan(&request))
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
                        let batch = match &legacy_projection {
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
    use super::*;
    use crate::ffi::tests::{
        insert_records_for, released_tables, scan_records_for, INSERT_APPEND_ONLY,
        INSERT_ERR_IMMEDIATE, INSERT_ERR_MIDSTREAM, INSERT_OK, TABLE_OK, TABLE_PUSHDOWN_OK,
        TABLE_PUSHDOWN_WRONG_SCHEMA, TABLE_SCAN_MIDSTREAM_ERR, TABLE_SCAN_OPEN_ERR,
        TABLE_SCHEMA_ERR,
    };
    use crate::{df_session_free, df_session_new, df_session_register_table, df_session_sql};

    use std::ffi::{c_char, CStr, CString};
    use std::mem::MaybeUninit;

    use arrow::array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field};
    use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
    use arrow::record_batch::RecordBatchReader;
    use futures::TryStreamExt;

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
        register_with_pushdown(session, name, handle, false)
    }

    unsafe fn register_with_pushdown(
        session: *mut std::ffi::c_void,
        name: &str,
        handle: usize,
        supports_pushdown: bool,
    ) -> Option<String> {
        let cname = CString::new(name).unwrap();
        let err =
            df_session_register_table(session, cname.as_ptr(), handle, supports_pushdown as u8, 0);
        if err.is_null() {
            None
        } else {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            crate::df_string_free(err);
            Some(msg)
        }
    }

    unsafe fn register_with_insert(
        session: *mut std::ffi::c_void,
        name: &str,
        handle: usize,
        supports_insert: bool,
    ) -> Option<String> {
        let cname = CString::new(name).unwrap();
        let err =
            df_session_register_table(session, cname.as_ptr(), handle, 0, supports_insert as u8);
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
            let handle = TABLE_OK + 80;
            assert_eq!(register(session, "proj", handle), None);
            // filter is applied post-scan (no pushdown); projection is
            // honored by the exec node
            let (schema, batches) = query(session, "SELECT name FROM proj WHERE id > 2").unwrap();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "name");
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

            // a non-pushdown provider never receives pushdown parameters
            let records = scan_records_for(handle);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].projection, None);
            assert_eq!(records[0].filters_json, None);
            assert_eq!(records[0].limit, -1);
            df_session_free(session);
        }
    }

    #[test]
    fn pushdown_classifies_supported_and_unsupported_conjuncts() {
        unsafe {
            let session = new_session();
            let handle = TABLE_PUSHDOWN_OK + 90;
            assert_eq!(
                register_with_pushdown(session, "pd_cls", handle, true),
                None
            );

            let (_, batches) = query(
                session,
                "SELECT id, name FROM pd_cls WHERE id > 2 AND length(name) = 5",
            )
            .unwrap();
            // rows: (1,alice)(2,bob)(3,carol)(4,dave); id>2 && len==5 => carol
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

            let records = scan_records_for(handle);
            assert_eq!(records.len(), 1);
            let filters = records[0]
                .filters_json
                .as_deref()
                .expect("supported conjunct must be pushed");
            assert!(filters.contains("\"op\":\"gt\""), "got: {filters}");
            assert!(filters.contains("\"name\":\"id\""), "got: {filters}");
            assert!(
                !filters.contains("length"),
                "function conjunct must be withheld: {filters}"
            );
            df_session_free(session);
        }
    }

    #[test]
    fn pushdown_projection_is_delivered_and_not_reprojected() {
        unsafe {
            let session = new_session();
            let handle = TABLE_PUSHDOWN_OK + 100;
            assert_eq!(
                register_with_pushdown(session, "pd_proj", handle, true),
                None
            );

            let (schema, batches) = query(session, "SELECT name FROM pd_proj").unwrap();
            assert_eq!(schema.fields().len(), 1);
            assert_eq!(schema.field(0).name(), "name");
            let names: Vec<String> = batches
                .iter()
                .flat_map(|b| {
                    let col = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
                    (0..col.len())
                        .map(|i| col.value(i).to_string())
                        .collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(names, ["alice", "bob", "carol", "dave"]);

            let records = scan_records_for(handle);
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].projection,
                Some(vec![1]),
                "scan must receive the name-column projection"
            );
            assert_eq!(records[0].filters_json, None);
            df_session_free(session);
        }
    }

    #[test]
    fn pushdown_wrong_schema_fails_query_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            let handle = TABLE_PUSHDOWN_WRONG_SCHEMA + 110;
            assert_eq!(
                register_with_pushdown(session, "pd_bad", handle, true),
                None
            );

            let err = query(session, "SELECT name FROM pd_bad").unwrap_err();
            assert!(
                err.contains("projection contract"),
                "expected schema-mismatch error, got: {err}"
            );
            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1);
            df_session_free(session);
        }
    }

    #[test]
    fn pushdown_limit_hint_arrives_for_filterless_limit() {
        unsafe {
            let session = new_session();
            let handle = TABLE_PUSHDOWN_OK + 120;
            assert_eq!(
                register_with_pushdown(session, "pd_limit", handle, true),
                None
            );

            let (_, batches) = query(session, "SELECT * FROM pd_limit LIMIT 3").unwrap();
            assert_eq!(
                batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                3,
                "engine must enforce the limit even though the stub ignores it"
            );
            let records = scan_records_for(handle);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].limit, 3);

            // with a pushed (Inexact) filter, the limit never reaches the scan
            let (_, batches) =
                query(session, "SELECT * FROM pd_limit WHERE id > 0 LIMIT 3").unwrap();
            assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
            let records = scan_records_for(handle);
            assert_eq!(records.len(), 2);
            assert_eq!(records[1].limit, -1);
            assert!(records[1].filters_json.is_some());
            df_session_free(session);
        }
    }

    #[test]
    fn pushdown_filter_only_query_returns_correct_rows() {
        unsafe {
            let session = new_session();
            let handle = TABLE_PUSHDOWN_OK + 130;
            assert_eq!(register_with_pushdown(session, "pd_f", handle, true), None);

            // The stub ignores the pushed filter entirely; DataFusion's
            // re-filter (Inexact) must still produce the right answer.
            let (_, batches) = query(session, "SELECT id FROM pd_f WHERE id > 2").unwrap();
            let ids: Vec<i64> = batches
                .iter()
                .flat_map(|b| {
                    let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(ids, [3, 4]);
            df_session_free(session);
        }
    }

    #[test]
    fn pushdown_projection_none_expects_full_schema() {
        // SQL almost always plans an explicit projection, so exercise the
        // `projection == None` path directly on the exec node.
        let handle = TABLE_PUSHDOWN_OK + 140;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        let exec = GoTableExec::new(
            handle,
            Arc::clone(&schema),
            None,
            Some(PushdownArgs {
                filters_json: None,
                limit: -1,
            }),
        );
        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches: Vec<RecordBatch> = crate::runtime().block_on(stream.try_collect()).unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);
        assert_eq!(batches[0].schema().fields().len(), 2);
        let records = scan_records_for(handle);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].projection, None, "None projection crosses as -1");
    }

    fn total_rows(batches: &[RecordBatch]) -> i64 {
        batches.iter().map(|b| b.num_rows() as i64).sum()
    }

    fn count_column(batches: &[RecordBatch]) -> u64 {
        assert_eq!(batches.len(), 1, "DML result is a single row");
        let col = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .expect("count column must be UInt64");
        assert_eq!(col.len(), 1);
        col.value(0)
    }

    #[test]
    fn insert_reaches_go_table_insert_with_append_and_full_schema() {
        unsafe {
            let session = new_session();
            let handle = INSERT_OK + 200;
            assert_eq!(register_with_insert(session, "t", handle, true), None);

            let (schema, batches) =
                query(session, "INSERT INTO t VALUES (1, 'alice'), (2, 'bob')").unwrap();
            assert_eq!(schema.field(0).name(), "count");
            assert_eq!(count_column(&batches), 2);

            let records = insert_records_for(handle);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].insert_op, 0, "INSERT INTO must deliver Append");
            assert_eq!(total_rows(&records[0].rows), 2);
            assert_eq!(records[0].rows[0].schema().fields().len(), 2);

            df_session_free(session);
        }
    }

    #[test]
    fn insert_against_non_writable_provider_fails_without_invoking_it() {
        unsafe {
            let session = new_session();
            let handle = TABLE_OK + 210;
            assert_eq!(register(session, "ro", handle), None);

            let err = query(session, "INSERT INTO ro VALUES (1, 'alice')").unwrap_err();
            assert!(
                err.contains("does not support INSERT"),
                "unexpected message: {err}"
            );
            assert!(insert_records_for(handle).is_empty());

            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn insert_select_from_another_registered_table() {
        unsafe {
            let session = new_session();
            let src = TABLE_OK + 220;
            let dst = INSERT_OK + 230;
            assert_eq!(register(session, "src", src), None);
            assert_eq!(register_with_insert(session, "dst", dst, true), None);

            let (_, batches) = query(session, "INSERT INTO dst SELECT * FROM src").unwrap();
            assert_eq!(
                count_column(&batches),
                4,
                "stub source table produces 2 batches x 2 rows"
            );

            let records = insert_records_for(dst);
            assert_eq!(records.len(), 1);
            assert_eq!(total_rows(&records[0].rows), 4);
            df_session_free(session);
        }
    }

    #[test]
    fn insert_midstream_input_error_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            let src = TABLE_SCAN_MIDSTREAM_ERR + 240;
            let dst = INSERT_OK + 250;
            assert_eq!(register(session, "src2", src), None);
            assert_eq!(register_with_insert(session, "dst2", dst, true), None);

            let err = query(session, "INSERT INTO dst2 SELECT * FROM src2").unwrap_err();
            assert!(!err.is_empty());

            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn insert_stub_error_immediate_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            let handle = INSERT_ERR_IMMEDIATE + 260;
            assert_eq!(register_with_insert(session, "t3", handle, true), None);

            let err = query(session, "INSERT INTO t3 VALUES (1, 'alice')").unwrap_err();
            assert!(
                err.contains("insert failed immediately"),
                "unexpected message: {err}"
            );

            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn insert_stub_error_after_partial_consumption_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            let src = TABLE_OK + 270;
            let dst = INSERT_ERR_MIDSTREAM + 280;
            assert_eq!(register(session, "src3", src), None);
            assert_eq!(register_with_insert(session, "dst3", dst, true), None);

            let err = query(session, "INSERT INTO dst3 SELECT * FROM src3").unwrap_err();
            assert!(
                err.contains("insert failed mid-stream"),
                "unexpected message: {err}"
            );

            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn insert_overwrite_delivers_overwrite_mode() {
        unsafe {
            let session = new_session();
            let handle = INSERT_OK + 290;
            assert_eq!(register_with_insert(session, "t4", handle, true), None);

            let (_, batches) = query(session, "INSERT OVERWRITE t4 VALUES (1, 'alice')").unwrap();
            assert_eq!(count_column(&batches), 1);

            let records = insert_records_for(handle);
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].insert_op, 1,
                "INSERT OVERWRITE must deliver Overwrite"
            );
            df_session_free(session);
        }
    }

    #[test]
    fn insert_provider_rejecting_non_append_fails_overwrite() {
        unsafe {
            let session = new_session();
            let handle = INSERT_APPEND_ONLY + 300;
            assert_eq!(register_with_insert(session, "t5", handle, true), None);

            let err = query(session, "INSERT OVERWRITE t5 VALUES (1, 'alice')").unwrap_err();
            assert!(
                err.contains("only Append is supported"),
                "unexpected message: {err}"
            );

            let (_, batches) = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(batches[0].num_rows(), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn replace_into_delivers_replace_mode() {
        unsafe {
            let session = new_session();
            let handle = INSERT_OK + 310;
            assert_eq!(register_with_insert(session, "t6", handle, true), None);

            let (_, batches) = query(session, "REPLACE INTO t6 VALUES (1, 'alice')").unwrap();
            assert_eq!(count_column(&batches), 1);

            let records = insert_records_for(handle);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].insert_op, 2, "REPLACE INTO must deliver Replace");
            df_session_free(session);
        }
    }
}
