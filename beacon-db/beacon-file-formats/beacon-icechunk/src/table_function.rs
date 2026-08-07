//! The `read_icechunk(location [, branch [, snapshot [, dimensions]]])` table
//! function.
//!
//! Unlike the glob-based `read_*` functions, an Icechunk repository is a single
//! location, so this takes one location string rather than a list of globs.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::BeaconTableFunctionImpl;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::plan_err,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::definition::IcechunkTableDefinition;
use crate::provider::IcechunkTable;

pub struct ReadIcechunkFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadIcechunkFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl Debug for ReadIcechunkFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadIcechunkFunc")
    }
}

impl BeaconTableFunctionImpl for ReadIcechunkFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_icechunk".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads an Icechunk repository as a Zarr store. Optional arguments \
             select a branch or a snapshot (pass one of them; the default is the \
             tip of 'main') and an explicit list of dimensions to read."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![
            Field::new("location", DataType::Utf8, false),
            Field::new("branch", DataType::Utf8, true),
            Field::new("snapshot", DataType::Utf8, true),
            Field::new(
                "dimensions",
                DataType::List(Arc::new(Field::new("dimension", DataType::Utf8, false))),
                true,
            ),
        ])
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

/// Extract a `List<Utf8>` literal of dimension names.
fn string_list_literal(expr: &Expr) -> Option<Vec<String>> {
    let Expr::Literal(ScalarValue::List(values), _) = expr else {
        return None;
    };
    let strings = values
        .as_ref()
        .values()
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()?;
    Some(
        strings
            .iter()
            .filter_map(|value| value.map(str::to_string))
            .collect(),
    )
}

/// Turn the positional arguments into the OPTIONS map an
/// [`IcechunkTableDefinition`] carries, so the table function and
/// `CREATE EXTERNAL TABLE` build the same provider.
fn options_from_args(args: &[Expr]) -> datafusion::error::Result<HashMap<String, String>> {
    let mut options = HashMap::new();

    for (index, key) in [(1usize, "branch"), (2, "snapshot")] {
        let Some(arg) = args.get(index) else { continue };
        // A NULL placeholder lets a caller skip `branch` and still pass `snapshot`.
        if matches!(arg, Expr::Literal(value, _) if value.is_null()) {
            continue;
        }
        match string_literal(arg) {
            Some(value) if !value.trim().is_empty() => {
                options.insert(key.to_string(), value);
            }
            Some(_) => {}
            None => return plan_err!("read_icechunk `{key}` argument must be a string"),
        }
    }

    if let Some(arg) = args.get(3)
        && !matches!(arg, Expr::Literal(value, _) if value.is_null())
    {
        let Some(dimensions) = string_list_literal(arg) else {
            return plan_err!(
                "read_icechunk fourth argument must be a List<Utf8> of dimension names"
            );
        };
        if !dimensions.is_empty() {
            options.insert("read_dimensions".to_string(), dimensions.join(","));
        }
    }

    Ok(options)
}

impl TableFunctionImpl for ReadIcechunkFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let Some(location) = args.first().and_then(string_literal) else {
            return plan_err!("read_icechunk requires a location string as the first argument");
        };

        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            datafusion::common::plan_datafusion_err!("session context has been dropped")
        })?;

        let definition = IcechunkTableDefinition {
            // A table function has no registered name; the location identifies it.
            name: location.clone(),
            location,
            options: options_from_args(args)?,
            definition: None,
        };

        let table = tokio::task::block_in_place(|| {
            self.runtime_handle
                .block_on(async move { IcechunkTable::try_new(&session_ctx.state(), definition).await })
        })
        .map_err(|e| datafusion::error::DataFusionError::External(e.into()))?;

        Ok(Arc::new(table))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn utf8(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    fn null() -> Expr {
        Expr::Literal(ScalarValue::Utf8(None), None)
    }

    fn dimensions(values: &[&str]) -> Expr {
        let array = arrow::array::StringArray::from(values.to_vec());
        let list = ScalarValue::List(Arc::new(arrow::array::ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            arrow::buffer::OffsetBuffer::from_lengths([values.len()]),
            Arc::new(array),
            None,
        )));
        Expr::Literal(list, None)
    }

    #[test]
    fn location_only_leaves_the_defaults() {
        assert!(options_from_args(&[utf8("argo/repo")]).unwrap().is_empty());
    }

    #[test]
    fn a_branch_becomes_the_branch_option() {
        let options = options_from_args(&[utf8("argo/repo"), utf8("dev")]).unwrap();
        assert_eq!(options.get("branch").map(String::as_str), Some("dev"));
        assert!(!options.contains_key("snapshot"));
    }

    #[test]
    fn a_null_branch_lets_a_snapshot_be_positional() {
        let options = options_from_args(&[utf8("argo/repo"), null(), utf8("SNAP123")]).unwrap();
        assert!(!options.contains_key("branch"));
        assert_eq!(options.get("snapshot").map(String::as_str), Some("SNAP123"));
        // An empty string reads the same as NULL.
        let options = options_from_args(&[utf8("argo/repo"), utf8(""), utf8("SNAP123")]).unwrap();
        assert!(!options.contains_key("branch"));
    }

    #[test]
    fn dimensions_become_the_read_dimensions_option() {
        let options = options_from_args(&[
            utf8("argo/repo"),
            null(),
            null(),
            dimensions(&["time", "depth"]),
        ])
        .unwrap();
        assert_eq!(
            options.get("read_dimensions").map(String::as_str),
            Some("time,depth")
        );
    }

    #[test]
    fn non_string_version_arguments_are_plan_errors() {
        let err = options_from_args(&[
            utf8("argo/repo"),
            Expr::Literal(ScalarValue::Int64(Some(1)), None),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("branch"), "{err}");

        let err = options_from_args(&[
            utf8("argo/repo"),
            null(),
            null(),
            Expr::Literal(ScalarValue::Int64(Some(1)), None),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("List<Utf8>"), "{err}");
    }

    #[test]
    fn function_metadata_is_stable() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let func = ReadIcechunkFunc::new(runtime.handle().clone(), Weak::new());
        assert_eq!(func.name(), "read_icechunk");
        let args = func.arguments().unwrap();
        assert_eq!(args.len(), 4);
        assert_eq!(args[0].name(), "location");
        assert!(!args[0].is_nullable());
        assert!(args[1].is_nullable());
    }

    #[test]
    fn call_without_a_location_is_a_plan_error() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let func = ReadIcechunkFunc::new(runtime.handle().clone(), Weak::new());
        let err = func.call(&[]).unwrap_err();
        assert!(err.to_string().contains("location"), "{err}");
    }

    #[test]
    fn call_with_a_dropped_session_context_errors_cleanly() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let func = ReadIcechunkFunc::new(runtime.handle().clone(), Weak::new());
        let err = func.call(&[utf8("argo/repo")]).unwrap_err();
        assert!(err.to_string().contains("session context"), "{err}");
    }
}
