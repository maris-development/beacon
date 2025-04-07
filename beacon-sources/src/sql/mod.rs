use std::sync::Arc;

use datafusion::{
    catalog::TableFunction,
    datasource::{
        file_format::{FileFormat, FileFormatFactory},
        listing::ListingTableUrl,
    },
    execution::SessionState,
    prelude::{Expr, SessionContext},
};
use parquet::ParquetTableFunction;

use crate::DataSource;

pub mod csv;
pub mod netcdf;
pub mod odv;
pub mod parquet;

pub fn table_functions(session_ctx: Arc<SessionContext>) {
    session_ctx.register_udtf(
        "read_parquet",
        Arc::new(ParquetTableFunction::new(session_ctx.clone())),
    );
    session_ctx.register_udtf(
        "read_csv",
        Arc::new(csv::CsvTableFunction::new(session_ctx.clone())),
    );
    session_ctx.register_udtf(
        "read_odv_ascii",
        Arc::new(odv::OdvTableFunction::new(session_ctx.clone())),
    );
    session_ctx.register_udtf(
        "read_netcdf",
        Arc::new(netcdf::NetCDFTableFunction::new(session_ctx.clone())),
    );
}

pub(crate) fn parse_exprs_to_urls(
    args: &[Expr],
) -> datafusion::error::Result<Vec<ListingTableUrl>> {
    let mut urls = Vec::new();

    //Read all the urls from the args
    for arg in args {
        if let Expr::Literal(datafusion::common::ScalarValue::Utf8(Some(url))) = arg {
            urls.push(url.clone());
        }
    }

    // Check if urls are empty
    if urls.is_empty() {
        return Err(datafusion::error::DataFusionError::Plan(
            "No URLs/datasets provided for table function".to_string(),
        ));
    }
    // Check if urls are valid
    let mut table_urls = Vec::new();
    for url in urls {
        let table_url = ListingTableUrl::parse(format!("/datasets/{}", url))
            .map_err(|e| datafusion::error::DataFusionError::Plan(format!("Invalid URL: {}", e)))?;
        table_urls.push(table_url);
    }

    Ok(table_urls)
}

pub(crate) fn create_datasource(
    session_state: &SessionState,
    table_urls: Vec<ListingTableUrl>,
    file_format: Arc<dyn FileFormat>,
) -> Result<DataSource, datafusion::error::DataFusionError> {
    tokio::task::block_in_place(|| {
        tokio::runtime::Handle::current()
            .block_on(async { DataSource::new(&session_state, file_format, table_urls).await })
    })
    .map_err(|e| {
        datafusion::error::DataFusionError::Plan(format!("Failed to create/read DataSource: {}", e))
    })
}
