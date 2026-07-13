use std::sync::{Arc, Weak};

use arrow::datatypes::{DataType, Field};
use beacon_arrow_netcdf::datafusion::NetcdfFormat;
use beacon_arrow_tiff::datafusion::TiffFormat;
use beacon_common::{
    listing_url::parse_listing_table_url, schema_table_provider::SchemaTableProvider,
    super_table::SuperListingTable,
};
use beacon_arrow_bbf::datafusion::BBFFormat;
use beacon_arrow_csv::datafusion::CsvFormat;
use beacon_arrow_ipc::datafusion::ArrowFormat;
use beacon_arrow_parquet::datafusion::ParquetFormat;
use beacon_arrow_zarr::datafusion::ZarrFormat;
use beacon_object_storage::DatasetsStore;
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
    session_ctx: Weak<SessionContext>,
    data_object_store_url: ObjectStoreUrl,
    datasets_object_store: Arc<DatasetsStore>,
}

impl ReadSchemaFunc {
    pub fn new(
        runtime_handle: tokio::runtime::Handle,
        session_ctx: Weak<SessionContext>,
        data_object_store_url: ObjectStoreUrl,
        datasets_object_store: Arc<DatasetsStore>,
    ) -> Self {
        Self {
            runtime_handle,
            session_ctx,
            data_object_store_url,
            datasets_object_store,
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
        let glob_paths = crate::file_formats::parse_glob_paths_arg(args, "read_schema")?;

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
                            self.datasets_object_store.clone(),
                            Default::default(),
                        )),
                        "tiff" | "tif" => Arc::new(TiffFormat::new(Default::default())),
                        "zarr" => Arc::new(ZarrFormat::default()),
                        "bbf" => Arc::new(BBFFormat::default()),
                        _ => {
                            return plan_err!(
                                "read_schema second argument file_format must be one of: parquet, netcdf, tiff, zarr, csv, arrow, bbf"
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
            listing_urls.push(parse_listing_table_url(&self.data_object_store_url, path)?);
        }
        let session_ctx = self.session_ctx.upgrade().ok_or_else(|| {
            plan_datafusion_err!("session context has been dropped")
        })?;
        let super_listing_table = tokio::task::block_in_place(|| {
            self.runtime_handle.block_on(async move {
                SuperListingTable::new(&session_ctx.state(), file_format, listing_urls).await
            })
        })?;

        let schema_table_provider = SchemaTableProvider::new(super_listing_table);

        Ok(Arc::new(schema_table_provider))
    }
}
