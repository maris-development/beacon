//! Shared support for Beacon's `read_*` file-format table functions.
//!
//! The [`BeaconTableFunctionImpl`] trait and the [`parse_glob_paths_arg`] helper
//! live here (rather than in `beacon-functions`) so each `beacon-arrow-*` format
//! crate can host its own `read_*` table function without depending on
//! `beacon-functions` (which depends on the format crates).

use std::collections::HashMap;

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

/// Parse an optional `List<Utf8>` argument into dimension names.
///
/// `None`, a `NULL` and an empty list all mean "no dimensions named". SQL types
/// an empty list by its own rules, so `[]` does not arrive as `List<Utf8>` and
/// must not be refused: a caller that wants a later positional argument and no
/// dimensions writes exactly that.
pub fn parse_dimensions_arg(
    args: &[Expr],
    index: usize,
    fn_name: &str,
    ordinal: &str,
) -> datafusion::error::Result<Vec<String>> {
    let Some(argument) = args.get(index) else {
        return Ok(vec![]);
    };
    if matches!(argument, Expr::Literal(value, _) if value.is_null()) {
        return Ok(vec![]);
    }
    let Expr::Literal(ScalarValue::List(values), _) = argument else {
        return plan_err!("{fn_name} {ordinal} argument must be a List<Utf8> of dimension names");
    };
    let values = values.as_ref().values();
    match values.as_any().downcast_ref::<arrow::array::StringArray>() {
        Some(names) => Ok(names.iter().flatten().map(str::to_string).collect()),
        None if values.is_empty() => Ok(vec![]),
        None => plan_err!("{fn_name} {ordinal} argument must be a List<Utf8> of dimension names"),
    }
}

/// Split the arguments of a `read_*` call into the positional arguments and
/// the options.
///
/// A struct as the last argument holds the options, for example
/// `{'type_widening_cast': 'lenient'}`. Its keys are the keys of
/// `CREATE EXTERNAL TABLE ... OPTIONS`, and each value must be a string. Without
/// such a struct, all arguments are positional and the options are empty.
pub fn split_options_arg<'a>(
    args: &'a [Expr],
    fn_name: &str,
) -> datafusion::error::Result<(&'a [Expr], HashMap<String, String>)> {
    let Some((Expr::Literal(ScalarValue::Struct(options), _), positional)) = args.split_last()
    else {
        return Ok((args, HashMap::new()));
    };
    if arrow::array::Array::len(options.as_ref()) != 1 {
        return plan_err!("{fn_name} options must be one struct value");
    }
    let mut parsed = HashMap::with_capacity(options.num_columns());
    for (field, column) in options.fields().iter().zip(options.columns()) {
        let value = ScalarValue::try_from_array(column, 0)?;
        let text = match &value {
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)) => text.clone(),
            _ => {
                return plan_err!(
                    "{fn_name} option '{}' must be a string, got {value:?}",
                    field.name()
                );
            }
        };
        parsed.insert(field.name().to_ascii_lowercase(), text);
    }
    Ok((positional, parsed))
}

/// Parse an optional boolean argument. `None` and a `NULL` both mean "unset".
pub fn parse_bool_arg(
    args: &[Expr],
    index: usize,
    fn_name: &str,
    ordinal: &str,
    meaning: &str,
) -> datafusion::error::Result<Option<bool>> {
    match args.get(index) {
        None => Ok(None),
        Some(Expr::Literal(value, _)) if value.is_null() => Ok(None),
        Some(Expr::Literal(ScalarValue::Boolean(Some(value)), _)) => Ok(Some(*value)),
        Some(_) => plan_err!("{fn_name} {ordinal} argument must be a boolean: {meaning}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{parse_bool_arg, parse_dimensions_arg, parse_glob_paths_arg, split_options_arg};
    use datafusion::prelude::Expr;
    use datafusion::scalar::ScalarValue;

    fn typed_list(items: &[ScalarValue], data_type: &arrow::datatypes::DataType) -> Expr {
        Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_nullable(items, data_type)),
            None,
        )
    }

    fn dims(names: &[&str]) -> Expr {
        let scalars: Vec<ScalarValue> = names
            .iter()
            .map(|name| ScalarValue::Utf8(Some(name.to_string())))
            .collect();
        typed_list(&scalars, &arrow::datatypes::DataType::Utf8)
    }

    fn paths() -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some("a.nc".to_string())), None)
    }

    #[test]
    fn a_list_of_names_becomes_the_dimensions() {
        let args = [paths(), dims(&["time", "lat"])];
        assert_eq!(
            parse_dimensions_arg(&args, 1, "read_netcdf", "second").unwrap(),
            vec!["time".to_string(), "lat".to_string()]
        );
    }

    /// The slot can be skipped in three ways, and every one names no dimension.
    #[test]
    fn an_absent_null_or_empty_dimension_argument_names_nothing() {
        assert!(
            parse_dimensions_arg(&[paths()], 1, "read_netcdf", "second")
                .unwrap()
                .is_empty()
        );

        let null = [paths(), Expr::Literal(ScalarValue::Null, None)];
        assert!(
            parse_dimensions_arg(&null, 1, "read_netcdf", "second")
                .unwrap()
                .is_empty()
        );

        // SQL types `[]` by its own rules, so the element type is not `Utf8`.
        let empty = [
            paths(),
            typed_list(&[], &arrow::datatypes::DataType::Int64),
        ];
        assert!(
            parse_dimensions_arg(&empty, 1, "read_netcdf", "second")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn a_non_empty_list_of_non_strings_is_a_plan_error() {
        let args = [
            paths(),
            typed_list(
                &[ScalarValue::Int64(Some(1))],
                &arrow::datatypes::DataType::Int64,
            ),
        ];
        let error = parse_dimensions_arg(&args, 1, "read_netcdf", "second")
            .unwrap_err()
            .to_string();
        assert!(error.contains("read_netcdf second"), "{error}");
    }

    #[test]
    fn a_boolean_argument_is_read_and_a_null_is_unset() {
        let set = [
            paths(),
            Expr::Literal(ScalarValue::Boolean(Some(true)), None),
        ];
        assert_eq!(
            parse_bool_arg(&set, 1, "read_netcdf", "second", "skip").unwrap(),
            Some(true)
        );

        let null = [paths(), Expr::Literal(ScalarValue::Null, None)];
        assert_eq!(
            parse_bool_arg(&null, 1, "read_netcdf", "second", "skip").unwrap(),
            None
        );
        assert_eq!(
            parse_bool_arg(&[paths()], 1, "read_netcdf", "second", "skip").unwrap(),
            None
        );

        let wrong = [paths(), paths()];
        let error = parse_bool_arg(&wrong, 1, "read_netcdf", "second", "skip the files")
            .unwrap_err()
            .to_string();
        assert!(error.contains("skip the files"), "{error}");
    }

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
    fn nulls_inside_a_list_are_skipped() {
        // A `List` literal may carry nulls (e.g. from an array constructor);
        // they are dropped rather than turned into empty paths.
        let scalars = vec![
            ScalarValue::Utf8(Some("a.parquet".to_string())),
            ScalarValue::Utf8(None),
            ScalarValue::Utf8(Some("b.parquet".to_string())),
        ];
        let expr = Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_nullable(
                &scalars,
                &arrow::datatypes::DataType::Utf8,
            )),
            None,
        );
        let paths = parse_glob_paths_arg(&[expr], "read_parquet").unwrap();
        assert_eq!(paths, vec!["a.parquet".to_string(), "b.parquet".to_string()]);
    }

    #[test]
    fn an_empty_list_yields_no_paths() {
        let expr = list_expr(&[]);
        let paths = parse_glob_paths_arg(&[expr], "read_parquet").unwrap();
        assert!(paths.is_empty());
    }

    #[test]
    fn a_list_of_non_strings_errors() {
        // Only `List<Utf8>` is accepted; a numeric list is a plan error, not a
        // silently empty path list.
        let scalars = vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))];
        let expr = Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_nullable(
                &scalars,
                &arrow::datatypes::DataType::Int64,
            )),
            None,
        );
        assert!(parse_glob_paths_arg(&[expr], "read_parquet").is_err());
    }

    #[test]
    fn a_null_string_scalar_errors() {
        // `Utf8(None)` does not match the `Some(s)` arms and falls through to
        // the catch-all error rather than producing an empty path.
        let expr = Expr::Literal(ScalarValue::Utf8(None), None);
        assert!(parse_glob_paths_arg(&[expr], "read_parquet").is_err());
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

    fn options(pairs: &[(&str, ScalarValue)]) -> Expr {
        use arrow::datatypes::Field;
        use datafusion::common::scalar::ScalarStructBuilder;

        let mut builder = ScalarStructBuilder::new();
        for (name, value) in pairs {
            builder = builder.with_scalar(Field::new(*name, value.data_type(), true), value.clone());
        }
        Expr::Literal(builder.build().unwrap(), None)
    }

    #[test]
    fn a_trailing_struct_holds_the_options() {
        let args = [
            paths(),
            options(&[
                ("type_widening_cast", ScalarValue::Utf8(Some("lenient".into()))),
                ("Delimiter", ScalarValue::Utf8View(Some(";".into()))),
            ]),
        ];
        let (positional, parsed) = split_options_arg(&args, "read_csv").unwrap();
        assert_eq!(positional, &args[..1]);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed["type_widening_cast"], "lenient");
        assert_eq!(parsed["delimiter"], ";", "a key reads in lowercase");
    }

    #[test]
    fn no_struct_gives_no_options() {
        let args = [paths(), dims(&["time"])];
        let (positional, parsed) = split_options_arg(&args, "read_netcdf").unwrap();
        assert_eq!(positional.len(), 2);
        assert!(parsed.is_empty());
    }

    #[test]
    fn an_option_that_is_not_a_string_errors() {
        let args = [paths(), options(&[("type_widening_cast", ScalarValue::Int64(Some(1)))])];
        let error = split_options_arg(&args, "read_parquet").unwrap_err().to_string();
        assert!(error.contains("type_widening_cast") && error.contains("string"), "{error}");
    }
}
