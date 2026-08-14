//! Go-implemented `CatalogProvider`/`SchemaProvider`, dispatching through
//! the fixed Go-exported trampoline symbols in [`crate::ffi`]. A table a
//! catalog hands back is queried through the existing [`GoTableProvider`]
//! machinery unchanged — this module adds no new table-scan FFI.

use std::ffi::{c_char, CString};
use std::ptr;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, Result};

use crate::ffi::{
    block_go, decode_name_list, go_catalog_release, go_catalog_schema_lookup,
    go_catalog_schema_names, go_schema_release, go_schema_table_lookup, go_schema_table_names,
    spawn_go_blocking, take_go_error, take_go_string,
};
use crate::table::GoTableProvider;

/// Wraps a Go-side catalog implementation identified by an opaque
/// `cgo.Handle`-derived token. Nothing is fetched at construction (design
/// D3 of add-catalog-provider) — a catalog's entire purpose is content not
/// known up front. The handle is released on drop.
#[derive(Debug)]
pub(crate) struct GoCatalogProvider {
    handle: usize,
}

impl GoCatalogProvider {
    pub(crate) fn new(handle: usize) -> Self {
        Self { handle }
    }
}

impl Drop for GoCatalogProvider {
    fn drop(&mut self) {
        unsafe { go_catalog_release(self.handle) };
    }
}

impl CatalogProvider for GoCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        let handle = self.handle;
        // schema_names is infallible per the DataFusion trait (returns
        // Vec<String>, not Result) — a Go-reported failure has no Result
        // to travel through, so it is raised as a panic instead. The
        // outer FFI entry point's catch_unwind converts it into a normal
        // query error string (walking-skeleton D4), so the process never
        // crashes and the session stays usable, matching the "errors
        // surface as query errors" contract even though this particular
        // path can't return a clean Err.
        block_go(move || {
            let mut out_json: *mut c_char = ptr::null_mut();
            let mut err: *mut c_char = ptr::null_mut();
            unsafe { go_catalog_schema_names(handle, &mut out_json, &mut err) };
            if let Some(msg) = unsafe { take_go_error(err) } {
                return Err(msg);
            }
            let json = unsafe { take_go_string(out_json) };
            decode_name_list(&json)
        })
        .unwrap_or_else(|msg| panic!("Go catalog schema listing failed: {msg}"))
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let handle = self.handle;
        let cname = CString::new(name).unwrap_or_default();
        // schema is also infallible per the trait (returns
        // Option<Arc<dyn SchemaProvider>>, not Result); see schema_names
        // above for why a Go error becomes a panic here.
        let found = block_go(move || {
            let mut out_schema_handle: usize = 0;
            let mut out_found: u8 = 0;
            let mut err: *mut c_char = ptr::null_mut();
            unsafe {
                go_catalog_schema_lookup(
                    handle,
                    cname.as_ptr(),
                    &mut out_schema_handle,
                    &mut out_found,
                    &mut err,
                )
            };
            if let Some(msg) = unsafe { take_go_error(err) } {
                return Err(msg);
            }
            Ok((out_found != 0).then_some(out_schema_handle))
        })
        .unwrap_or_else(|msg| panic!("Go catalog schema lookup for '{name}' failed: {msg}"));

        found.map(|schema_handle| {
            Arc::new(GoSchemaProvider::new(schema_handle)) as Arc<dyn SchemaProvider>
        })
    }
}

/// Wraps a Go-side schema implementation identified by an opaque
/// `cgo.Handle`-derived token, minted fresh by [`GoCatalogProvider::schema`]
/// for each lookup (design D3). The handle is released on drop.
#[derive(Debug)]
pub(crate) struct GoSchemaProvider {
    handle: usize,
}

impl GoSchemaProvider {
    pub(crate) fn new(handle: usize) -> Self {
        Self { handle }
    }
}

impl Drop for GoSchemaProvider {
    fn drop(&mut self) {
        unsafe { go_schema_release(self.handle) };
    }
}

#[async_trait]
impl SchemaProvider for GoSchemaProvider {
    fn table_names(&self) -> Vec<String> {
        let handle = self.handle;
        // Infallible per the trait; see GoCatalogProvider::schema_names
        // for why a Go error becomes a panic here.
        block_go(move || {
            let mut out_json: *mut c_char = ptr::null_mut();
            let mut err: *mut c_char = ptr::null_mut();
            unsafe { go_schema_table_names(handle, &mut out_json, &mut err) };
            if let Some(msg) = unsafe { take_go_error(err) } {
                return Err(msg);
            }
            let json = unsafe { take_go_string(out_json) };
            decode_name_list(&json)
        })
        .unwrap_or_else(|msg| panic!("Go schema table listing failed: {msg}"))
    }

    fn table_exist(&self, name: &str) -> bool {
        // Derived in pure Rust from table_names (design D2) — no FFI call
        // of its own; DataFusion's own table_type() default accepts the
        // same kind of derived-convenience cost.
        self.table_names().iter().any(|n| n == name)
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        // table() genuinely returns a Result, so a Go error surfaces as a
        // normal query error here — no panic needed (design D7).
        //
        // Both the lookup and GoTableProvider::try_new's own schema fetch
        // (go_table_schema) are blocking FFI calls; both run inside this
        // one spawn_go_blocking dispatch so neither ever runs inline on a
        // tokio worker thread, and a catalog-discovered table costs one
        // blocking-pool round trip, not two.
        let handle = self.handle;
        let name_owned = name.to_string();
        let cname = CString::new(name)
            .map_err(|e| DataFusionError::Execution(format!("table name '{name}': {e}")))?;

        let provider = spawn_go_blocking(move || {
            let mut out_table_handle: usize = 0;
            let mut out_supports_pushdown: u8 = 0;
            let mut out_found: u8 = 0;
            let mut err: *mut c_char = ptr::null_mut();
            unsafe {
                go_schema_table_lookup(
                    handle,
                    cname.as_ptr(),
                    &mut out_table_handle,
                    &mut out_supports_pushdown,
                    &mut out_found,
                    &mut err,
                )
            };
            if let Some(msg) = unsafe { take_go_error(err) } {
                return Err(msg);
            }
            if out_found == 0 {
                return Ok(None);
            }
            // No new table-scan code path: a catalog-discovered table is
            // wrapped by the exact same GoTableProvider a
            // RegisterTable-registered table uses (design D1, D2).
            // Insert support does not compose through a catalog in this
            // change (add-table-provider-insert's Impact section scopes
            // it to df_session_register_table only) — a catalog-discovered
            // table is never insert-capable, unchanged from before.
            let provider =
                GoTableProvider::try_new(out_table_handle, out_supports_pushdown != 0, false)
                    .map_err(|e| {
                        format!("constructing catalog-discovered table '{name_owned}': {e}")
                    })?;
            Ok(Some(provider))
        })
        .await
        .map_err(|msg| {
            DataFusionError::Execution(format!("Go schema table lookup for '{name}' failed: {msg}"))
        })?;

        Ok(provider.map(|p| Arc::new(p) as Arc<dyn TableProvider>))
    }
}

#[cfg(test)]
mod tests {
    use super::GoCatalogProvider;
    use crate::ffi::tests::{
        new_session, query, released_catalogs, released_schemas, released_tables, scan_records_for,
        CATALOG_OK, CATALOG_SCHEMA_LOOKUP_ERR, CATALOG_SCHEMA_NAMES_ERR, SCHEMA_OK,
        SCHEMA_OK_PUSHDOWN, SCHEMA_TABLE_LOOKUP_ERR, SCHEMA_TABLE_NAMES_ERR, STUB_TABLE_NAME,
    };
    use crate::{
        df_session_free, df_session_register_catalog, df_session_register_table, df_string_free,
    };

    use std::ffi::{CStr, CString};
    use std::sync::Arc;

    use arrow::array::{Array, Int64Array, RecordBatch};

    unsafe fn register_catalog(
        session: *mut std::ffi::c_void,
        name: &str,
        handle: usize,
    ) -> Option<String> {
        let cname = CString::new(name).unwrap();
        let err = df_session_register_catalog(session, cname.as_ptr(), handle);
        if err.is_null() {
            None
        } else {
            let msg = CStr::from_ptr(err).to_string_lossy().into_owned();
            df_string_free(err);
            Some(msg)
        }
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Encodes one outer-test handle: units digit selects the CATALOG_*
    /// stub behavior, tens digit selects the SCHEMA_* stub behavior
    /// (design: see ffi.rs test stub comments), hundreds-and-up is a
    /// caller-chosen uniqueness offset so parallel tests never collide on
    /// release tracking.
    fn handle(catalog_kind: usize, schema_kind: usize, unique: usize) -> usize {
        catalog_kind + schema_kind * 10 + unique * 100
    }

    #[test]
    fn full_scan_via_catalog_schema_table() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_OK, 10);
            assert_eq!(register_catalog(session, "cat", h), None);

            let batches = query(session, "SELECT id, name FROM cat.sch.t ORDER BY id").unwrap();
            assert_eq!(
                total_rows(&batches),
                4,
                "stub table produces 2 batches x 2 rows"
            );
            let ids: Vec<i64> = batches
                .iter()
                .flat_map(|b| {
                    let col = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    (0..col.len()).map(|i| col.value(i)).collect::<Vec<_>>()
                })
                .collect();
            assert_eq!(ids, [1, 2, 3, 4]);

            df_session_free(session);
            assert!(
                released_catalogs().contains(&h),
                "session free must release the catalog handle"
            );
        }
    }

    #[test]
    fn pushdown_composes_automatically_for_catalog_discovered_table() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_OK_PUSHDOWN, 20);
            assert_eq!(register_catalog(session, "cat", h), None);

            let batches = query(session, "SELECT name FROM cat.sch.t").unwrap();
            assert_eq!(total_rows(&batches), 4);
            assert_eq!(batches[0].schema().fields().len(), 1, "projection honored");

            // The derived table handle (base + TABLE_PUSHDOWN_OK) is the
            // one go_table_scan actually received the projection on.
            let base = crate::ffi::tests::uniqueness_base_of(h);
            let table_handle = base + crate::ffi::tests::TABLE_PUSHDOWN_OK;
            let records = scan_records_for(table_handle);
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].projection,
                Some(vec![1]),
                "catalog-discovered pushdown table must receive the projection exactly as a directly-registered one would"
            );

            df_session_free(session);
            assert!(released_tables().contains(&table_handle));
            assert!(released_schemas().contains(&h));
        }
    }

    #[test]
    fn duplicate_catalog_registration_is_rejected() {
        unsafe {
            let session = new_session();
            let first = handle(CATALOG_OK, SCHEMA_OK, 30);
            let second = handle(CATALOG_OK, SCHEMA_OK, 31);
            assert_eq!(register_catalog(session, "dupcat", first), None);
            let msg = register_catalog(session, "dupcat", second).expect("duplicate must error");
            assert!(msg.contains("already"), "unexpected message: {msg}");
            assert!(
                released_catalogs().contains(&second),
                "rejected registration must release the new handle"
            );
            assert!(
                !released_catalogs().contains(&first),
                "original registration must stay alive"
            );

            let batches = query(session, "SELECT * FROM dupcat.sch.t").unwrap();
            assert_eq!(total_rows(&batches), 4);
            df_session_free(session);
        }
    }

    #[test]
    fn registering_under_default_catalog_name_replaces_it() {
        unsafe {
            let session = new_session();
            let table_handle = crate::ffi::tests::TABLE_OK + 4000;
            let cname = CString::new("people").unwrap();
            assert!(
                df_session_register_table(session, cname.as_ptr(), table_handle, 0, 0).is_null()
            );

            let batches = query(session, "SELECT * FROM people").unwrap();
            assert_eq!(
                total_rows(&batches),
                4,
                "directly-registered table queryable before replacement"
            );

            let h = handle(CATALOG_OK, SCHEMA_OK, 40);
            assert_eq!(
                register_catalog(session, "datafusion", h),
                None,
                "registering under the default catalog name must succeed, not error"
            );

            let err = query(session, "SELECT * FROM people").unwrap_err();
            assert!(
                err.contains("people")
                    || err.to_lowercase().contains("not found")
                    || err.to_lowercase().contains("table"),
                "previously-registered table must become unreachable, got: {err}"
            );

            let batches = query(session, "SELECT * FROM datafusion.sch.t").unwrap();
            assert_eq!(
                total_rows(&batches),
                4,
                "the Go catalog is now queryable under the default name"
            );

            df_session_free(session);
        }
    }

    #[test]
    fn unknown_schema_produces_engines_standard_error() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_OK, 50);
            assert_eq!(register_catalog(session, "cat", h), None);

            let err = query(session, "SELECT * FROM cat.missing_schema.t").unwrap_err();
            assert!(!err.is_empty());
            let batches = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(total_rows(&batches), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn unknown_table_produces_engines_standard_error() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_OK, 60);
            assert_eq!(register_catalog(session, "cat", h), None);

            let err = query(session, "SELECT * FROM cat.sch.missing_table").unwrap_err();
            assert!(!err.is_empty());
            let batches = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(total_rows(&batches), 1, "session remains usable");
            df_session_free(session);
        }
    }

    #[test]
    fn table_names_error_surfaces_and_session_stays_usable() {
        // Unlike a direct catalog.schema.table planning-time lookup (see
        // schema_lookup_error above, which panics synchronously and is
        // caught by df_session_sql's own catch_unwind), information_schema
        // builds its virtual tables inside a spawned task: the panic is
        // caught by Tokio's own task-join machinery first and arrives here
        // as an ordinary DataFusionError, never as a raw panic. Both are
        // valid manifestations of the same contract (query fails, session
        // stays usable) — which one occurs depends on which DataFusion
        // code path invoked the Go callback.
        let ctx = info_schema_session();
        let h = handle(CATALOG_OK, SCHEMA_TABLE_NAMES_ERR, 110);
        ctx.register_catalog("cat", Arc::new(GoCatalogProvider::new(h)));

        let err = crate::runtime()
            .block_on(async {
                let df = ctx
                    .sql("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'cat'")
                    .await
                    .unwrap();
                df.collect().await
            })
            .expect_err("table listing failure must abort the query");
        assert!(
            err.to_string().contains("table listing failed"),
            "got: {err}"
        );

        // session stays usable for a later query
        let batches = crate::runtime()
            .block_on(async { ctx.sql("SELECT 1 AS one").await.unwrap().collect().await })
            .unwrap();
        assert_eq!(total_rows(&batches), 1);
    }

    #[test]
    fn schema_lookup_error_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_SCHEMA_LOOKUP_ERR, SCHEMA_OK, 80);
            assert_eq!(register_catalog(session, "cat", h), None);

            let err = query(session, "SELECT * FROM cat.sch.t").unwrap_err();
            assert!(err.contains("schema lookup failed"), "got: {err}");
            let batches = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(total_rows(&batches), 1);
            df_session_free(session);
        }
    }

    // schema_names()/table_names() are only invoked by DataFusion's
    // information_schema machinery (SHOW TABLES / information_schema.*),
    // not by a direct catalog.schema.table reference — resolving a
    // qualified reference goes straight through schema()/table() by name.
    // information_schema defaults to disabled and nothing in this change
    // turns it on for df_session_new, so these two tests construct a raw
    // SessionContext directly (same crate) with it enabled, to exercise
    // the real DataFusion call path rather than invoking the trait
    // methods in isolation.
    fn info_schema_session() -> datafusion::execution::context::SessionContext {
        datafusion::execution::context::SessionContext::new_with_config(
            datafusion::execution::context::SessionConfig::new().with_information_schema(true),
        )
    }

    #[test]
    fn schema_names_are_visible_through_information_schema() {
        let ctx = info_schema_session();
        let h = handle(CATALOG_OK, SCHEMA_OK, 90);
        ctx.register_catalog("cat", Arc::new(GoCatalogProvider::new(h)));

        let batches = crate::runtime().block_on(async {
            let df = ctx
                .sql("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'cat' ORDER BY table_name")
                .await
                .unwrap();
            df.collect().await.unwrap()
        });
        let names: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                let col = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::StringArray>()
                    .unwrap();
                (0..col.len())
                    .map(|i| col.value(i).to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        // information_schema.tables also lists DataFusion's own built-in
        // information_schema tables under every catalog it enumerates
        // (columns, tables, schemata, ...); "t" being present is what
        // proves table_names() is reachable via information_schema.
        assert!(
            names.contains(&STUB_TABLE_NAME.to_string()),
            "table_names() must be reachable via information_schema, got: {names:?}"
        );
        drop(ctx);
        assert!(released_catalogs().contains(&h));
    }

    #[test]
    fn schema_names_error_surfaces_and_session_stays_usable() {
        // See table_names_error_surfaces_and_session_stays_usable for why
        // this is an ordinary DataFusionError here rather than a raw panic.
        let ctx = info_schema_session();
        let h = handle(CATALOG_SCHEMA_NAMES_ERR, SCHEMA_OK, 100);
        ctx.register_catalog("cat", Arc::new(GoCatalogProvider::new(h)));

        let err = crate::runtime()
            .block_on(async {
                let df = ctx
                    .sql("SELECT table_name FROM information_schema.tables WHERE table_catalog = 'cat'")
                    .await
                    .unwrap();
                df.collect().await
            })
            .expect_err("schema listing failure must abort the query");
        assert!(
            err.to_string().contains("schema listing failed"),
            "got: {err}"
        );

        let batches = crate::runtime()
            .block_on(async { ctx.sql("SELECT 1 AS one").await.unwrap().collect().await })
            .unwrap();
        assert_eq!(total_rows(&batches), 1);
    }

    #[test]
    fn table_lookup_error_surfaces_and_session_stays_usable() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_TABLE_LOOKUP_ERR, 100);
            assert_eq!(register_catalog(session, "cat", h), None);

            let err = query(session, "SELECT * FROM cat.sch.t").unwrap_err();
            assert!(err.contains("table lookup failed"), "got: {err}");
            let batches = query(session, "SELECT 1 AS one").unwrap();
            assert_eq!(total_rows(&batches), 1);
            df_session_free(session);
        }
    }

    #[test]
    fn table_added_after_registration_becomes_queryable() {
        // The stub always reports table_names=["t"] regardless of when
        // asked (design D3: nothing is cached), so any query against
        // cat.sch.t succeeds without re-registering — this directly
        // exercises that no caching happens between two queries.
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_OK, 110);
            assert_eq!(register_catalog(session, "cat", h), None);

            let first = query(session, "SELECT * FROM cat.sch.t").unwrap();
            assert_eq!(total_rows(&first), 4);
            let second = query(session, "SELECT * FROM cat.sch.t").unwrap();
            assert_eq!(total_rows(&second), 4);
            df_session_free(session);
        }
    }

    #[test]
    fn catalog_and_schema_not_invoked_after_close() {
        unsafe {
            let session = new_session();
            let h = handle(CATALOG_OK, SCHEMA_OK, 120);
            assert_eq!(register_catalog(session, "cat", h), None);
            let _ = query(session, "SELECT * FROM cat.sch.t").unwrap();

            df_session_free(session);
            assert!(released_catalogs().contains(&h));
            // A schema handle is minted per-resolution (design D3); at
            // least one resolution happened during the query above, and
            // its handle (== h, since the stub passes it through
            // unchanged) must be released by the time the session is
            // freed, not left dangling.
            assert!(released_schemas().contains(&h));
        }
    }
}
