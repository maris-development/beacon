use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_common::{listing_url::parse_listing_table_url, super_table::SuperListingTable};
use beacon_formats::csv::CsvFormat;
use datafusion::{
    catalog::TableFunctionImpl,
    common::plan_err,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::file_formats::BeaconTableFunctionImpl;

pub struct ReadCsvFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
}

impl ReadCsvFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
        }
    }
}

impl std::fmt::Debug for ReadCsvFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadCsvFunc")
    }
}

impl BeaconTableFunctionImpl for ReadCsvFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_csv".to_string()
    }

    fn description(&self) -> Option<String> {
        Some("Reads CSV files from specified glob paths. With optional delimiter and record inference settings.".to_string())
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new("delimiter", DataType::Utf8, true),
            Field::new("infer_records", DataType::UInt64, true),
        ])
    }
}

impl TableFunctionImpl for ReadCsvFunc {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let mut glob_paths: Vec<String> = vec![];
        if let Some(glob_path_arg) = args.first() {
            match glob_path_arg {
                Expr::Literal(ScalarValue::List(values), _) => {
                    let string_array = values.as_ref().values();
                    match string_array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                    {
                        Some(str_arr) => {
                            str_arr.iter().for_each(|opt_str| {
                                if let Some(s) = opt_str {
                                    glob_paths.push(s.to_string());
                                }
                            });
                        }
                        None => {
                            return plan_err!(
                                "read_csv first argument must be a List<Utf8> of glob paths"
                            );
                        }
                    }
                }
                _ => {
                    return plan_err!("read_csv first argument must be a List<Utf8> of glob paths");
                }
            }
        } else {
            return plan_err!("read_csv requires at least 1 argument: glob_paths : List<Utf8>");
        }

        let delimeter = if let Some(delimiter_arg) = args.get(1) {
            match delimiter_arg {
                Expr::Literal(ScalarValue::Utf8(opt_str), _) => {
                    if let Some(s) = opt_str {
                        let mut chars = s.chars();
                        if let Some(c) = chars.next() {
                            c as u8
                        } else {
                            b','
                        }
                    } else {
                        b','
                    }
                }
                _ => {
                    return plan_err!("read_csv second argument 'delimiter' must be a Utf8 string");
                }
            }
        } else {
            b','
        };

        let infer_records = args
            .get(2)
            .and_then(|arg| match arg {
                Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v as usize),
                _ => None,
            })
            .unwrap_or(128_000);

        tracing::debug!("read_csv glob paths: {:?}", glob_paths);

        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_csv processing path: {}", path);
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }

        let file_format = CsvFormat::new(delimeter, infer_records);
        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(
                    &self.session_ctx.state(),
                    Arc::new(file_format),
                    listing_urls,
                )
                .await
            })
        })?;

        Ok(Arc::new(super_listing_table))
    }
}
