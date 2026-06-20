//! Shared support for Beacon's `read_*` file-format table functions.
//!
//! The [`BeaconTableFunctionImpl`] trait and the [`parse_glob_paths_arg`] helper
//! live here (rather than in `beacon-functions`) so each `beacon-arrow-*` format
//! crate can host its own `read_*` table function without depending on
//! `beacon-functions` (which depends on the format crates).

use arrow::datatypes::Field;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    logical_expr::{Documentation, Signature},
    prelude::Expr,
    scalar::ScalarValue,
};

/// Beacon's extension of DataFusion's [`TableFunctionImpl`] that also carries the
/// metadata Beacon needs to register the function and render its documentation.
pub trait BeaconTableFunctionImpl: TableFunctionImpl + Send + Sync {
    fn name(&self) -> String;
    fn as_any(&self) -> &dyn std::any::Any;
    fn arguments(&self) -> Option<Vec<Field>> {
        None
    }
    fn description(&self) -> Option<String> {
        None
    }
    fn signature(&self) -> Signature {
        // Default field that accepts glob paths
        let mut all_datatypes = vec![];
        let options = self.arguments().unwrap_or_default();
        for option in options {
            all_datatypes.push(option.data_type().clone());
        }
        Signature::exact(all_datatypes, datafusion::logical_expr::Volatility::Immutable)
    }
    fn documentation(&self) -> Option<Documentation> {
        None
    }
}

/// Parse the first table-function argument into a list of glob/path strings.
///
/// Accepts either a single string scalar (`Utf8`/`LargeUtf8`/`Utf8View`) or a
/// `List<Utf8>` of strings. `fn_name` is used only to build clear error messages.
pub fn parse_glob_paths_arg(args: &[Expr], fn_name: &str) -> datafusion::error::Result<Vec<String>> {
    let Some(first) = args.first() else {
        return plan_err!("{fn_name} requires at least 1 argument: glob_paths : Utf8 | List<Utf8>");
    };

    match first {
        // Single string -> one-element list
        Expr::Literal(ScalarValue::Utf8(Some(s)), _)
        | Expr::Literal(ScalarValue::LargeUtf8(Some(s)), _)
        | Expr::Literal(ScalarValue::Utf8View(Some(s)), _) => Ok(vec![s.clone()]),

        // List of strings
        Expr::Literal(ScalarValue::List(values), _) => {
            let string_array = values.as_ref().values();
            match string_array
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
            {
                Some(str_arr) => Ok(str_arr.iter().flatten().map(|s| s.to_string()).collect()),
                None => plan_err!(
                    "{fn_name} first argument must be a string or a List<Utf8> of glob paths"
                ),
            }
        }
        _ => plan_err!("{fn_name} first argument must be a string or a List<Utf8> of glob paths"),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_glob_paths_arg;
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;

    fn list_expr(items: &[&str]) -> Expr {
        let scalars: Vec<ScalarValue> = items
            .iter()
            .map(|s| ScalarValue::Utf8(Some(s.to_string())))
            .collect();
        Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_nullable(
                &scalars,
                &arrow::datatypes::DataType::Utf8,
            )),
            None,
        )
    }

    #[test]
    fn single_utf8_becomes_one_element_list() {
        let expr = Expr::Literal(ScalarValue::Utf8(Some("data/*.parquet".to_string())), None);
        let paths = parse_glob_paths_arg(&[expr], "read_parquet").unwrap();
        assert_eq!(paths, vec!["data/*.parquet".to_string()]);
    }

    #[test]
    fn single_large_utf8_is_accepted() {
        let expr = Expr::Literal(ScalarValue::LargeUtf8(Some("a.csv".to_string())), None);
        let paths = parse_glob_paths_arg(&[expr], "read_csv").unwrap();
        assert_eq!(paths, vec!["a.csv".to_string()]);
    }

    #[test]
    fn single_utf8_view_is_accepted() {
        let expr = Expr::Literal(ScalarValue::Utf8View(Some("a.nc".to_string())), None);
        let paths = parse_glob_paths_arg(&[expr], "read_netcdf").unwrap();
        assert_eq!(paths, vec!["a.nc".to_string()]);
    }

    #[test]
    fn list_of_strings_is_accepted() {
        let expr = list_expr(&["a.parquet", "b.parquet"]);
        let paths = parse_glob_paths_arg(&[expr], "read_parquet").unwrap();
        assert_eq!(paths, vec!["a.parquet".to_string(), "b.parquet".to_string()]);
    }

    #[test]
    fn missing_argument_errors() {
        assert!(parse_glob_paths_arg(&[], "read_parquet").is_err());
    }

    #[test]
    fn wrong_type_errors() {
        let expr = Expr::Literal(ScalarValue::Int64(Some(42)), None);
        assert!(parse_glob_paths_arg(&[expr], "read_parquet").is_err());
    }
}
