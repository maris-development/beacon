//! `read_<name>(paths, ...)` for a plugin format.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_common::table_function::{
    BeaconTableFunctionImpl, parse_bool_arg, parse_dimensions_arg, parse_glob_paths_arg,
};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_plugin_sdk::file_format::{ArgKind, ArgSpec};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{exec_err, plan_err};
use datafusion::datasource::file_format::FileFormatFactory;
use datafusion::error::Result;
use datafusion::execution::SessionState;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use tokio::runtime::Handle;

use crate::factory::PluginFileFormatFactory;

const ORDINALS: [&str; 9] = [
    "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "ninth", "tenth",
];

/// The options that the positional arguments after the paths give, by `specs`.
///
/// A string list joins with `,`. A missing or `NULL` argument gives no option.
pub fn options_from_args(
    args: &[Expr],
    specs: &[ArgSpec],
    fn_name: &str,
) -> Result<HashMap<String, String>> {
    let mut options = HashMap::new();
    for (i, spec) in specs.iter().enumerate() {
        let index = i + 1;
        let ordinal = ORDINALS.get(i).copied().unwrap_or("next");
        let value = match spec.kind {
            ArgKind::StringList => {
                let items = parse_dimensions_arg(args, index, fn_name, ordinal)?;
                (!items.is_empty()).then(|| items.join(","))
            }
            ArgKind::Bool => parse_bool_arg(args, index, fn_name, ordinal, &spec.option_key)?
                .map(|value| value.to_string()),
            ArgKind::String => match args.get(index) {
                None => None,
                Some(Expr::Literal(value, _)) if value.is_null() => None,
                Some(Expr::Literal(
                    ScalarValue::Utf8(Some(text))
                    | ScalarValue::LargeUtf8(Some(text))
                    | ScalarValue::Utf8View(Some(text)),
                    _,
                )) => Some(text.clone()),
                Some(_) => {
                    return plan_err!(
                        "{fn_name} {ordinal} argument must be a string: {}",
                        spec.option_key
                    );
                }
            },
        };
        if let Some(value) = value {
            options.insert(spec.option_key.clone(), value);
        }
    }
    Ok(options)
}

/// `read_<name>(paths, ...)`: a table over files of one plugin format.
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
        let mut fields = vec![Field::new(
            "glob_paths",
            DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
            false,
        )];
        for spec in self.factory.plugin().table_function_args() {
            let data_type = match spec.kind {
                ArgKind::String => DataType::Utf8,
                ArgKind::Bool => DataType::Boolean,
                ArgKind::StringList => {
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)))
                }
            };
            fields.push(Field::new(spec.option_key, data_type, true));
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
