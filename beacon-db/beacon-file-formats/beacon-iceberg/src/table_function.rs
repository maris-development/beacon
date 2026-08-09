//! The `read_iceberg(location [, snapshot_id])` table function.
//!
//! Unlike the glob-based `read_*` functions, an Iceberg table is a single
//! directory, so this takes one location string (not a list of globs). An
//! optional second argument selects an older snapshot for time travel.
//!
//! Each call opens the table afresh, so it always reads the current metadata.

use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::BeaconTableFunctionImpl;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::plan_err,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::provider::open_iceberg_table;

pub struct ReadIcebergFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadIcebergFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
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
            "Reads an Apache Iceberg table from a single location: the table \
             directory, which holds the `metadata` directory. An optional second \
             argument selects a snapshot id for time travel."
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

/// Decide the snapshot from the optional second `read_iceberg` argument. A
/// snapshot id is a 64-bit integer, accepted as a literal or as a string.
fn parse_snapshot_arg(arg: Option<&Expr>) -> datafusion::error::Result<Option<i64>> {
    match arg {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Int64(Some(id)), _)) => Ok(Some(*id)),
        Some(expr) => match string_literal(expr).and_then(|s| s.parse::<i64>().ok()) {
            Some(id) => Ok(Some(id)),
            None => plan_err!("read_iceberg second argument must be a snapshot id (integer)"),
        },
    }
}

impl TableFunctionImpl for ReadIcebergFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let Some(location) = args.first().and_then(string_literal) else {
            return plan_err!("read_iceberg requires a location string as the first argument");
        };
        let snapshot_id = parse_snapshot_arg(args.get(1))?;

        let ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = ctx.state();
        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                datafusion::common::plan_datafusion_err!(
                    "read_iceberg requires a ListingFactory extension"
                )
            })?;
        let store_url = listing_factory.parse_to_store(&state, &location).ok_or(
            datafusion::error::DataFusionError::External(
                "failed to parse location to store".into(),
            ),
        )?;
        let store = state
            .runtime_env()
            .object_store_registry
            .get_store(store_url.as_ref())
            .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        let opened = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                open_iceberg_table(store, &location, None, snapshot_id).await
            })
        })
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        Ok(opened.provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    #[test]
    fn string_literal_accepts_every_utf8_variant() {
        assert_eq!(string_literal(&utf8("db://t")).as_deref(), Some("db://t"));
        assert_eq!(
            string_literal(&Expr::Literal(
                ScalarValue::LargeUtf8(Some("x".into())),
                None
            ))
            .as_deref(),
            Some("x")
        );
        assert_eq!(
            string_literal(&Expr::Literal(
                ScalarValue::Utf8View(Some("y".into())),
                None
            ))
            .as_deref(),
            Some("y")
        );
        // Non-string literals are not locations.
        assert_eq!(
            string_literal(&Expr::Literal(ScalarValue::Int64(Some(1)), None)),
            None
        );
        assert_eq!(
            string_literal(&Expr::Literal(ScalarValue::Utf8(None), None)),
            None
        );
    }

    #[test]
    fn the_snapshot_argument_takes_an_integer_in_either_form() {
        assert_eq!(parse_snapshot_arg(None).unwrap(), None);
        assert_eq!(
            parse_snapshot_arg(Some(&Expr::Literal(ScalarValue::Int64(Some(9)), None))).unwrap(),
            Some(9)
        );
        // Snapshot ids are large, so they often arrive quoted.
        assert_eq!(
            parse_snapshot_arg(Some(&utf8("3821550127947089060"))).unwrap(),
            Some(3821550127947089060)
        );
        // A timestamp is not a snapshot id: Iceberg time travel is by id here.
        assert!(parse_snapshot_arg(Some(&utf8("2026-01-01T00:00:00Z"))).is_err());
        assert!(
            parse_snapshot_arg(Some(&Expr::Literal(ScalarValue::Float64(Some(1.5)), None)))
                .is_err()
        );
    }

    #[test]
    fn function_metadata_is_stable() {
        // Build without a live session; only the static metadata is inspected.
        let handle = tokio::runtime::Runtime::new().unwrap();
        let func = ReadIcebergFunc::new(handle.handle().clone(), Weak::new());
        assert_eq!(func.name(), "read_iceberg");
        assert!(func.description().unwrap().contains("Iceberg"));
        let args = func.arguments().unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].name(), "location");
        assert_eq!(args[0].data_type(), &DataType::Utf8);
        assert!(!args[0].is_nullable());
    }

    #[test]
    fn call_without_a_location_is_a_plan_error() {
        let handle = tokio::runtime::Runtime::new().unwrap();
        let func = ReadIcebergFunc::new(handle.handle().clone(), Weak::new());
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("location"), "{err}");
    }

    #[test]
    fn call_with_a_dropped_session_context_errors_cleanly() {
        // A location is present, but the weak session context can't be upgraded;
        // this must be a graceful plan error rather than a panic.
        let handle = tokio::runtime::Runtime::new().unwrap();
        let func = ReadIcebergFunc::new(handle.handle().clone(), Weak::new());
        let err = func.call(&[utf8("db://t")]).unwrap_err();
        assert!(err.to_string().contains("session context"), "{err}");
    }
}
