//! The `read_delta(location [, version_or_timestamp])` table function.
//!
//! Unlike the glob-based `read_*` functions, a Delta table is a single directory,
//! so this takes one location string (not a list of globs). An optional second
//! string argument selects a snapshot for time travel: an integer is treated as a
//! version, anything else as an RFC-3339 timestamp.

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

use crate::provider::{open_delta_provider, TimeTravel};

pub struct ReadDeltaFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadDeltaFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
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

        let ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;
        let state = ctx.state();
        let listing_factory = state.config().get_extension::<ListingFactory>().expect("");
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
        let provider = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                open_delta_provider(ctx, store, &location, time_travel).await
            })
        })
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        Ok(provider)
    }
}

/// Decide the time-travel selector from the optional second `read_delta`
/// argument, mirroring the logic in `TableFunctionImpl::call` so it can be
/// exercised without a live session. Returns `Ok(None)` when absent.
#[cfg(test)]
fn parse_time_travel_arg(arg: Option<&Expr>) -> datafusion::error::Result<Option<TimeTravel>> {
    match arg {
        None => Ok(None),
        Some(Expr::Literal(ScalarValue::Int64(Some(v)), _)) => Ok(Some(TimeTravel::Version(*v))),
        Some(expr) => match string_literal(expr) {
            Some(s) => match s.parse::<i64>() {
                Ok(v) => Ok(Some(TimeTravel::Version(v))),
                Err(_) => Ok(Some(TimeTravel::Timestamp(s))),
            },
            None => plan_err!(
                "read_delta second argument must be a version (integer) or timestamp string"
            ),
        },
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
            string_literal(&Expr::Literal(ScalarValue::LargeUtf8(Some("x".into())), None))
                .as_deref(),
            Some("x")
        );
        assert_eq!(
            string_literal(&Expr::Literal(ScalarValue::Utf8View(Some("y".into())), None))
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
    fn time_travel_arg_distinguishes_version_from_timestamp() {
        assert_eq!(parse_time_travel_arg(None).unwrap(), None);

        // An Int64 literal is a version.
        assert_eq!(
            parse_time_travel_arg(Some(&Expr::Literal(ScalarValue::Int64(Some(3)), None))).unwrap(),
            Some(TimeTravel::Version(3))
        );
        // A numeric *string* is also treated as a version.
        assert_eq!(
            parse_time_travel_arg(Some(&utf8("7"))).unwrap(),
            Some(TimeTravel::Version(7))
        );
        // A non-numeric string is a timestamp.
        assert_eq!(
            parse_time_travel_arg(Some(&utf8("2026-01-01T00:00:00Z"))).unwrap(),
            Some(TimeTravel::Timestamp("2026-01-01T00:00:00Z".to_string()))
        );
        // A second argument that is neither an integer nor a string is an error
        // (e.g. a float literal).
        assert!(
            parse_time_travel_arg(Some(&Expr::Literal(ScalarValue::Float64(Some(1.5)), None)))
                .is_err()
        );
    }

    #[test]
    fn function_metadata_is_stable() {
        // Build without a live session; only the static metadata is inspected.
        let handle = tokio::runtime::Runtime::new().unwrap();
        let func = ReadDeltaFunc::new(handle.handle().clone(), Weak::new());
        assert_eq!(func.name(), "read_delta");
        assert!(func.description().unwrap().contains("Delta"));
        let args = func.arguments().unwrap();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0].name(), "location");
        assert_eq!(args[0].data_type(), &DataType::Utf8);
        assert!(!args[0].is_nullable());
    }

    #[test]
    fn call_without_a_location_is_a_plan_error() {
        let handle = tokio::runtime::Runtime::new().unwrap();
        let func = ReadDeltaFunc::new(handle.handle().clone(), Weak::new());
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("location"), "{err}");
    }

    #[test]
    fn call_with_a_dropped_session_context_errors_cleanly() {
        // A location is present, but the weak session context can't be upgraded;
        // this must be a graceful plan error rather than a panic.
        let handle = tokio::runtime::Runtime::new().unwrap();
        let func = ReadDeltaFunc::new(handle.handle().clone(), Weak::new());
        let err = func.call(&[utf8("db://t")]).unwrap_err();
        assert!(err.to_string().contains("session context"), "{err}");
    }
}
