use std::{path::Path, sync::Arc};

use datafusion::{
    datasource::{file_format::FileFormat, listing::ListingTableUrl, provider_as_source},
    execution::SessionState,
    logical_expr::LogicalPlanBuilder,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    arrow_format::SuperArrowFormat, csv_format::SuperCsvFormat, netcdf_format::NetCDFFormat,
    odv_format::OdvFormat, parquet_format::SuperParquetFormat, DataSource,
};
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum Formats {
    Arrow {
        paths: FileSystemPath,
    },
    Parquet {
        paths: FileSystemPath,
    },
    Csv {
        paths: FileSystemPath,
        delimiter: u8,
        infer_schema_records: usize,
    },
    Odv {
        paths: FileSystemPath,
    },
    NetCDF {
        paths: FileSystemPath,
    },
}

impl Formats {
    pub fn file_format(&self) -> Arc<dyn FileFormat> {
        match self {
            Formats::Arrow { .. } => Arc::new(SuperArrowFormat::new()),
            Formats::Parquet { .. } => Arc::new(SuperParquetFormat::new()),
            Formats::Csv {
                delimiter,
                infer_schema_records,
                ..
            } => Arc::new(SuperCsvFormat::new(*delimiter, *infer_schema_records)),
            Formats::Odv { .. } => Arc::new(OdvFormat),
            Formats::NetCDF { .. } => Arc::new(NetCDFFormat::new()),
        }
    }

    pub async fn create_plan_builder(
        &self,
        session_state: &SessionState,
    ) -> anyhow::Result<LogicalPlanBuilder> {
        let file_format = self.file_format();
        let table_urls: Vec<ListingTableUrl> = match &self {
            Formats::Arrow { paths } => paths.try_into().unwrap(),
            Formats::Parquet { paths } => paths.try_into().unwrap(),
            Formats::Csv { paths, .. } => paths.try_into().unwrap(),
            Formats::Odv { paths } => paths.try_into().unwrap(),
            Formats::NetCDF { paths } => paths.try_into().unwrap(),
        };

        let datasource = DataSource::new(session_state, file_format, table_urls).await?;

        let source = provider_as_source(Arc::new(datasource));

        Ok(LogicalPlanBuilder::scan("tmp", source, None)?)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, ToSchema)]
#[serde(untagged)]
#[serde(deny_unknown_fields)]
pub enum FileSystemPath {
    ManyPaths(Vec<String>),
    Path(String),
}

impl FileSystemPath {
    pub fn parse_to_url<P: AsRef<Path>>(path: P) -> anyhow::Result<ListingTableUrl> {
        let table_url =
            ListingTableUrl::parse(&format!("/datasets/{}", path.as_ref().to_string_lossy()))?;
        if table_url
            .prefix()
            .prefix_matches(&beacon_config::DATASETS_DIR_PREFIX)
        {
            Ok(table_url)
        } else {
            Err(anyhow::anyhow!(
                "Path {} is not within the datasets directory.",
                table_url.as_str()
            ))
        }
    }
}

impl TryInto<Vec<ListingTableUrl>> for &FileSystemPath {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<Vec<ListingTableUrl>, Self::Error> {
        match self {
            FileSystemPath::ManyPaths(items) => Ok(items
                .into_iter()
                .map(|path| FileSystemPath::parse_to_url(path))
                .collect::<anyhow::Result<_>>()?),
            FileSystemPath::Path(path) => Ok(vec![FileSystemPath::parse_to_url(path)
                .map_err(|e| anyhow::anyhow!("Failed to parse path: {}", e))?]),
        }
    }
}
