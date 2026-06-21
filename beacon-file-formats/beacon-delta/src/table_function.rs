//! The `read_delta(location [, version_or_timestamp])` table function.
//!
//! Unlike the glob-based `read_*` functions, a Delta table is a single directory,
//! so this takes one location string (not a list of globs). An optional second
//! string argument selects a snapshot for time travel: an integer is treated as a
//! version, anything else as an RFC-3339 timestamp.

use std::sync::Arc;

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

use crate::provider::{open_delta_provider, TimeTravel};

pub struct ReadDeltaFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    #[allow(dead_code)]
    data_object_store_url: ObjectStoreUrl,
    datasets_object_store: Arc<DatasetsStore>,
}

impl ReadDeltaFunc {
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

impl std::fmt::Debug for ReadDeltaFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadDeltaFunc")
    }
}

impl BeaconTableFunctionImpl for ReadDeltaFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_delta".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads a Delta Lake table from a single location. An optional second \
             argument selects a snapshot for time travel (integer = version, \
             otherwise an RFC-3339 timestamp)."
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

impl TableFunctionImpl for ReadDeltaFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let Some(location) = args.first().and_then(string_literal) else {
            return plan_err!("read_delta requires a location string as the first argument");
        };

        // Optional second argument: a version (integer) or an RFC-3339 timestamp.
        let time_travel = match args.get(1) {
            None => None,
            Some(Expr::Literal(ScalarValue::Int64(Some(v)), _)) => Some(TimeTravel::Version(*v)),
            Some(expr) => match string_literal(expr) {
                Some(s) => match s.parse::<i64>() {
                    Ok(v) => Some(TimeTravel::Version(v)),
                    Err(_) => Some(TimeTravel::Timestamp(s)),
                },
                None => {
                    return plan_err!(
                        "read_delta second argument must be a version (integer) or timestamp string"
                    )
                }
            },
        };

        let ctx = self.session_ctx.clone();
        let datasets_store = self.datasets_object_store.clone();
        let provider = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                open_delta_provider(ctx, datasets_store, &location, time_travel).await
            })
        })
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        Ok(provider)
    }
}
