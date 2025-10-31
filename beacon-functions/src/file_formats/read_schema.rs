use std::sync::Arc;

use arrow::datatypes::{DataType, Field};
use beacon_common::{
    listing_url::parse_listing_table_url, schema_table_provider::SchemaTableProvider,
    super_table::SuperListingTable,
};
use beacon_formats::{
    arrow::ArrowFormat,
    csv::CsvFormat,
    netcdf::{
        object_resolver::{NetCDFObjectResolver, NetCDFSinkResolver},
        NetcdfFormat,
    },
    parquet::ParquetFormat,
    zarr::ZarrFormat,
};
use datafusion::{
    catalog::TableFunctionImpl,
    common::{plan_datafusion_err, plan_err},
    datasource::file_format::FileFormat,
    execution::object_store::ObjectStoreUrl,
    prelude::{Expr, SessionContext},
    scalar::ScalarValue,
};

use crate::file_formats::BeaconTableFunctionImpl;

pub struct ReadSchemaFunc {
    // Session Reference
    runtime_handle: tokio::runtime::Handle,
    session_ctx: Arc<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    data_object_store_prefix: object_store::path::Path,
    netcdf_object_resolver: Arc<NetCDFObjectResolver>,
    netcdf_sink_resolver: Arc<NetCDFSinkResolver>,
}

impl ReadSchemaFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Arc<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        data_object_store_prefix: object_store::path::Path,
        netcdf_object_resolver: Arc<NetCDFObjectResolver>,
        netcdf_sink_resolver: Arc<NetCDFSinkResolver>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
            data_object_store_prefix,
            netcdf_object_resolver,
            netcdf_sink_resolver,
        }
    }
}

impl std::fmt::Debug for ReadSchemaFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadSchemaFunc")
    }
}

impl BeaconTableFunctionImpl for ReadSchemaFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "read_schema".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Reads the compiled table schema from specified glob paths and file format."
                .to_string(),
        )
    }

    fn arguments(&self) -> Option<Vec<arrow::datatypes::Field>> {
        Some(vec![
            Field::new(
                "glob_paths",
                DataType::List(Arc::new(Field::new("glob_path", DataType::Utf8, false))),
                false,
            ),
            Field::new("file_format", DataType::Utf8, false),
        ])
    }
}

impl TableFunctionImpl for ReadSchemaFunc {
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
                                "read_schema first argument must be a List<Utf8> of glob paths"
                            );
                        }
                    }
                }
                _ => {
                    return plan_err!(
                        "read_schema first argument must be a List<Utf8> of glob paths"
                    );
                }
            }
        } else {
            return plan_err!("read_schema requires at least 1 argument: glob_paths : List<Utf8>");
        }

        tracing::debug!("read_schema glob paths: {:?}", glob_paths);

        let file_format: Arc<dyn FileFormat> = if let Some(expr) = args.get(1) {
            match expr {
                Expr::Literal(ScalarValue::Utf8(value), _) => {
                    let file_format_str = value.as_ref().ok_or_else(|| {
                        plan_datafusion_err!(
                            "read_schema second argument file_format must be a Utf8 string"
                        )
                    })?;

                    match file_format_str.to_lowercase().as_str() {
                        "parquet" => Arc::new(ParquetFormat::default()),
                        "csv" => Arc::new(CsvFormat::new(b',', 128_000)),
                        "arrow" => Arc::new(ArrowFormat::default()),
                        "netcdf" | "nc" => Arc::new(NetcdfFormat::new(
                            Default::default(),
                            self.netcdf_object_resolver.clone(),
                            self.netcdf_sink_resolver.clone(),
                        )),
                        "zarr" => Arc::new(ZarrFormat::default()),
                        _ => {
                            return plan_err!(
                                "read_schema second argument file_format must be one of: parquet, netcdf, zarr"
                            );
                        }
                    }
                }
                _ => {
                    return plan_err!("read_schema second argument  must be a List<Utf8>");
                }
            }
        } else {
            return plan_err!("read_schema requires at least 2 arguments: glob_paths : List<Utf8>, file_format: Utf8");
        };
        let mut listing_urls = vec![];
        for path in &glob_paths {
            tracing::debug!("read_schema processing path: {}", path);
            listing_urls.push(parse_listing_table_url(
                &self.data_object_store_url,
                &self.data_object_store_prefix,
                path,
            )?);
        }
        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(&self.session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        let schema_table_provider = SchemaTableProvider::new(super_listing_table);

        Ok(Arc::new(schema_table_provider))
    }
}
