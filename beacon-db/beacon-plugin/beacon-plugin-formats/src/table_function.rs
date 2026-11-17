//! `read_<name>(paths, {key: value, ...})` for a plugin format.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::{BeaconTableFunctionImpl, parse_glob_paths_arg};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_plugin_sdk::file_format::{ArgKind, ArgSpec};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DFSchema, exec_err, plan_err};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use tokio::runtime::Handle;

use crate::factory::PluginFileFormatFactory;

/// The options in the struct after the paths, as `{key: value, ...}`, by `specs`.
///
/// A string list joins with `,`. A missing or `NULL` struct or field gives no option.
pub fn options_from_args(
    args: &[Expr],
    specs: &[ArgSpec],
    fn_name: &str,
) -> Result<HashMap<String, String>> {
    let mut options = HashMap::new();
    if args.len() > 2 {
        return plan_err!("{fn_name} takes the paths and one struct of options");
    }
    let Some(arg) = args.get(1) else {
        return Ok(options);
    };
    // SQL plans `{key: value}` as a `named_struct` call, not as a literal.
    let simplifier = ExprSimplifier::new(SimplifyContext::default());
    let arg = simplifier.simplify(simplifier.coerce(arg.clone(), &DFSchema::empty())?)?;
    let fields = match arg {
        Expr::Literal(value, _) if value.is_null() => return Ok(options),
        _ if specs.is_empty() => return plan_err!("{fn_name} takes no options"),
        Expr::Literal(ScalarValue::Struct(fields), _) => fields,
        _ => {
            return plan_err!(
                "{fn_name} second argument must be a struct of options: {{key: value, ...}}"
            );
        }
    };
    for (field, column) in fields.fields().iter().zip(fields.columns()) {
        let key = field.name();
        let Some(spec) = specs.iter().find(|spec| &spec.option_key == key) else {
            let valid: Vec<&str> = specs.iter().map(|spec| spec.option_key.as_str()).collect();
            return plan_err!(
                "{fn_name} has no option `{key}`. Valid options: {}",
                valid.join(", ")
            );
        };
        let value = ScalarValue::try_from_array(column, 0)?;
        if let Some(value) = option_value(&value, spec.kind, fn_name, key)? {
            options.insert(key.clone(), value);
        }
    }
    Ok(options)
}

/// The option text of one struct field, or `None` for `NULL` or an empty list.
fn option_value(
    value: &ScalarValue,
    kind: ArgKind,
    fn_name: &str,
    key: &str,
) -> Result<Option<String>> {
    if value.is_null() {
        return Ok(None);
    }
    match (kind, value) {
        (
            ArgKind::String,
            ScalarValue::Utf8(Some(text))
            | ScalarValue::LargeUtf8(Some(text))
            | ScalarValue::Utf8View(Some(text)),
        ) => Ok(Some(text.clone())),
        (ArgKind::Bool, ScalarValue::Boolean(Some(value))) => Ok(Some(value.to_string())),
        (ArgKind::StringList, ScalarValue::List(list)) => {
            let items = list.values();
            // SQL types an empty list by its own rules, so `[]` is not `List<Utf8>`.
            if items.is_empty() {
                return Ok(None);
            }
            if !matches!(
                items.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return plan_err!("{fn_name} option `{key}` must be a list of strings");
            }
            let items = cast(items, &DataType::Utf8)?;
            let names: Vec<&str> = items.as_string::<i32>().iter().flatten().collect();
            Ok((!names.is_empty()).then(|| names.join(",")))
        }
        (ArgKind::String, _) => plan_err!("{fn_name} option `{key}` must be a string"),
        (ArgKind::Bool, _) => plan_err!("{fn_name} option `{key}` must be a boolean"),
        (ArgKind::StringList, _) => plan_err!("{fn_name} option `{key}` must be a list of strings"),
    }
}

/// `read_<name>(paths, {key: value, ...})`: a table over files of one plugin format.
pub struct PluginReadFunc {
    factory: PluginFileFormatFactory,
    runtime_handle: Handle,
    name: String,
}

impl PluginReadFunc {
    pub fn new(factory: PluginFileFormatFactory, runtime_handle: Handle) -> Self {
        let name = format!("read_{}", factory.name());
        Self {
            factory,
            runtime_handle,
            name,
        }
    }
}

impl std::fmt::Debug for PluginReadFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PluginReadFunc({})", self.name)
    }
}

impl BeaconTableFunctionImpl for PluginReadFunc {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn description(&self) -> Option<String> {
        Some(format!(
            "Reads files of the plugin file format `{}` from the given glob paths.",
            self.factory.name()
        ))
    }

    fn arguments(&self) -> Option<Vec<Field>> {
        let options: Vec<Field> = self
            .factory
            .plugin()
            .table_function_args()
            .into_iter()
            .map(|spec| {
                let data_type = match spec.kind {
                    ArgKind::String => DataType::Utf8,
                    ArgKind::Bool => DataType::Boolean,
                    ArgKind::StringList => {
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
                    }
                };
                Field::new(spec.option_key, data_type, true)
            })
            .collect();
        let mut fields = vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )];
        if !options.is_empty() {
            fields.push(Field::new(
                "options",
                DataType::Struct(options.into()),
                true,
            ));
        }
        Some(fields)
    }
}

impl TableFunctionImpl for PluginReadFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> Result<Arc<dyn TableProvider>> {
        let session = args.session();
        let args = args.exprs();
        let Some(state) = session.as_any().downcast_ref::<SessionState>() else {
            return exec_err!(
                "{}: the session is not a DataFusion SessionState",
                self.name
            );
        };
        let Some(listing) = state.config().get_extension::<ListingFactory>() else {
            return exec_err!(
                "{}: the ListingFactory is not registered on the session",
                self.name
            );
        };
        let paths = parse_glob_paths_arg(args, &self.name)?;
        let urls = paths
            .iter()
            .map(|path| listing.parse_listing_table_url(state, path))
            .collect::<Result<Vec<_>>>()?;
        if urls.is_empty() {
            return plan_err!("{}: no valid glob paths provided", self.name);
        }
        let specs = self.factory.plugin().table_function_args();
        let options = options_from_args(args, &specs, &self.name)?;
        let format = self.factory.create(state, &options)?;
        let table = tokio::task::block_in_place(|| {
            self.runtime_handle
                .block_on(FastObjectTable::try_new(state, format, urls))
        })?;
        Ok(Arc::new(table))
    }
}
