//! The `read_iceberg(location)` table function.
//!
//! Like `read_delta`, an Iceberg table is a single directory, so this takes one
//! location string (relative to the datasets store, optionally `datasets://`).

use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::BeaconTableFunctionImpl;
use beacon_object_storage::DatasetsStore;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::plan_err,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use super::definition::ExternalIcebergTableDefinition;
use super::loader::load_external_iceberg_table;
use super::provider::ExternalIcebergTable;

pub struct ReadIcebergFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    #[allow(dead_code)]
    data_object_store_url: ObjectStoreUrl,
    #[allow(dead_code)]
    datasets_object_store: Arc<DatasetsStore>,
}

impl ReadIcebergFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        datasets_object_store: Arc<DatasetsStore>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
            datasets_object_store,
        }
    }
}

impl std::fmt::Debug for ReadIcebergFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadIcebergFunc")
    }
}

impl BeaconTableFunctionImpl for ReadIcebergFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_iceberg".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads an existing Apache Iceberg table from a single location \
             (relative to the datasets store, optionally with a `datasets://` \
             scheme), e.g. read_iceberg('datasets://db/orders')."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![Field::new("location", DataType::Utf8, false)])
    }
}

/// Extract a single string literal from an expression argument.
fn string_literal(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Some(s.clone()),
        _ => None,
    }
}

impl TableFunctionImpl for ReadIcebergFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let Some(location) = args.first().and_then(string_literal) else {
            return plan_err!("read_iceberg requires a location string as the first argument");
        };

        // Config is threaded through the session as an extension (runtime-owned,
        // no process-global accessor).
        let storage = self
            .session_ctx
            .state()
            .config()
            .get_extension::<beacon_config::Config>()
            .context("beacon Config extension is not registered on the session")
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?
            .storage
            .clone();

        let location_for_def = location.clone();
        let table = tokio::task::block_in_place(|| {
            self.runtime_handle
                .block_on(async move { load_external_iceberg_table(&storage, &location).await })
        })
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let definition =
            ExternalIcebergTableDefinition::new(location_for_def.clone(), location_for_def);
        Ok(Arc::new(ExternalIcebergTable::new(definition, table)))
    }
}
