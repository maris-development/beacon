//! `read_atlas(paths)`, `read_atlas(paths, dimensions)` and
//! `read_atlas(paths, dimensions, skip_unbroadcastable)`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::{
    BeaconTableFunctionImpl, parse_bool_arg, parse_dimensions_arg,
};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    common::plan_datafusion_err,
    error::Result,
    prelude::{Expr, SessionContext},
};

use crate::format::ATLAS_FORMAT;

/// Reads the Atlas collections that match one or more glob patterns.
pub struct ReadAtlasFunc {
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Weak<SessionContext>,
}

impl ReadAtlasFunc {
    pub fn new(runtime_handle: tokio::runtime::Handle, session_ctx: Weak<SessionContext>) -> Self {
        Self {
            runtime_handle,
            session_ctx,
        }
    }
}

impl std::fmt::Debug for ReadAtlasFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadAtlasFunc")
    }
}

impl BeaconTableFunctionImpl for ReadAtlasFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads Atlas collections. Each path names a 'data.atlas' container file, exactly or \
             through a glob such as '**/data.atlas'. The optional second argument lists the \
             dimensions to read, and an array survives only when the list holds every one of its \
             own. The optional third argument, a boolean, skips a dataset whose columns fit no \
             one grid instead of failing the query."
                .to_string(),
        )
    }

    fn name(&self) -> String {
        "read_atlas".to_string()
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new(
                "dimensions",
                DataType::List(Arc::new(Field::new("dimension", DataType::Utf8, false))),
                false,
            ),
            Field::new("skip_unbroadcastable", DataType::Boolean, false),
        ])
    }
}

impl TableFunctionImpl for ReadAtlasFunc {
    fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
        let glob_paths = beacon_common::table_function::parse_glob_paths_arg(args, "read_atlas")?;
        let format_options = format_options_from_args(args)?;

        tracing::debug!("read_atlas glob paths: {glob_paths:?}");

        let session_ctx = self
            .session_ctx
            .upgrade()
            .ok_or_else(|| plan_datafusion_err!("session context has been dropped"))?;
        let state = session_ctx.state();

        let listing_factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| {
                plan_datafusion_err!("read_atlas: the listing factory is not registered")
            })?;
        let mut listing_urls = Vec::with_capacity(glob_paths.len());
        for path in &glob_paths {
            listing_urls.push(listing_factory.parse_listing_table_url(&state, path)?);
        }

        // Builds the format from the session's factory, so settings and cache are shared.
        let factory = state.get_file_format_factory(ATLAS_FORMAT).ok_or_else(|| {
            plan_datafusion_err!("read_atlas: the atlas file format is not registered")
        })?;
        let file_format = factory.create(&state, &format_options)?;

        let table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async {
                FastObjectTable::try_new(&session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        Ok(Arc::new(table))
    }
}

/// The positional arguments after the paths, as the `OPTIONS` the format
/// factory reads. The table function and `CREATE EXTERNAL TABLE` then build
/// the same format.
fn format_options_from_args(args: &[Expr]) -> Result<HashMap<String, String>> {
    let mut options = HashMap::new();

    let dimensions = parse_dimensions_arg(args, 1, "read_atlas", "second")?;
    if !dimensions.is_empty() {
        options.insert("read_dimensions".to_string(), dimensions.join(","));
    }

    if let Some(skip) = parse_bool_arg(
        args,
        2,
        "read_atlas",
        "third",
        "skip the datasets that cannot broadcast",
    )? {
        options.insert("skip_unbroadcastable".to_string(), skip.to_string());
    }

    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::scalar::ScalarValue;

    fn paths() -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some("obs/data.atlas".to_string())), None)
    }

    /// A one-row list of `names`, as the SQL planner builds `['a', 'b']`.
    fn dimensions(names: &[&str]) -> Expr {
        let values = arrow::array::StringArray::from(names.to_vec());
        let list = arrow::array::ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, false)),
            arrow::buffer::OffsetBuffer::from_lengths([names.len()]),
            Arc::new(values),
            None,
        );
        Expr::Literal(ScalarValue::List(Arc::new(list)), None)
    }

    fn boolean(value: bool) -> Expr {
        Expr::Literal(ScalarValue::Boolean(Some(value)), None)
    }

    #[test]
    fn paths_alone_leave_the_defaults() {
        assert!(format_options_from_args(&[paths()]).unwrap().is_empty());
    }

    #[test]
    fn dimensions_become_the_read_dimensions_option() {
        let options = format_options_from_args(&[paths(), dimensions(&["time", "depth"])]).unwrap();
        assert_eq!(
            options.get("read_dimensions").map(String::as_str),
            Some("time,depth")
        );
        assert!(!options.contains_key("skip_unbroadcastable"));
    }

    #[test]
    fn a_third_boolean_becomes_the_skip_option() {
        let options =
            format_options_from_args(&[paths(), dimensions(&["obs"]), boolean(true)]).unwrap();
        assert_eq!(
            options.get("skip_unbroadcastable").map(String::as_str),
            Some("true")
        );
        assert_eq!(options.get("read_dimensions").map(String::as_str), Some("obs"));
    }

    #[test]
    fn an_empty_list_skips_the_dimensions_and_keeps_the_flag() {
        let options = format_options_from_args(&[paths(), dimensions(&[]), boolean(true)]).unwrap();
        assert!(!options.contains_key("read_dimensions"));
        assert_eq!(
            options.get("skip_unbroadcastable").map(String::as_str),
            Some("true")
        );
    }

    /// A caller that wants the flag and no dimensions writes `[]` or `NULL`
    /// for that slot. SQL types an empty list by its own rules, so the
    /// element type is not `Utf8` there and must not be refused.
    #[test]
    fn an_empty_or_null_dimension_argument_still_takes_the_flag() {
        let empty_ints = Expr::Literal(
            ScalarValue::List(Arc::new(arrow::array::ListArray::new(
                Arc::new(Field::new("item", DataType::Int64, true)),
                arrow::buffer::OffsetBuffer::from_lengths([0usize]),
                Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new())),
                None,
            ))),
            None,
        );
        let options = format_options_from_args(&[paths(), empty_ints, boolean(true)]).unwrap();
        assert!(!options.contains_key("read_dimensions"));
        assert_eq!(
            options.get("skip_unbroadcastable").map(String::as_str),
            Some("true")
        );

        let null = Expr::Literal(ScalarValue::Null, None);
        let options = format_options_from_args(&[paths(), null, boolean(true)]).unwrap();
        assert!(!options.contains_key("read_dimensions"));
        assert_eq!(
            options.get("skip_unbroadcastable").map(String::as_str),
            Some("true")
        );
    }

    #[test]
    fn a_third_argument_that_is_not_a_boolean_is_a_plan_error() {
        let error = format_options_from_args(&[paths(), dimensions(&["obs"]), paths()])
            .expect_err("not a boolean")
            .to_string();
        assert!(error.contains("read_atlas third argument"), "{error}");
    }
}
