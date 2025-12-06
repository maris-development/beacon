use std::sync::Arc;

use crate::table::{
    delta::DeltaTable, empty::EmptyTable, error::TableError, geospatial::GeoSpatialTable,
    logical::LogicalTable, preset::PresetTable,
};
use beacon_common::listing_url::parse_listing_table_url;
use datafusion::{
    catalog::TableProvider, execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};

/// Enum representing different types of tables.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TableType {
    Logical(LogicalTable),
    Preset(PresetTable),
    GeoSpatial(GeoSpatialTable),
    Empty(EmptyTable),
    Delta(DeltaTable),
}

impl TableType {
    pub async fn table_provider(
        &self,
        session_ctx: Arc<SessionContext>,
        table_directory_store_url: ObjectStoreUrl,
        table_directory_prefix: object_store::path::Path,
        data_directory_store_url: ObjectStoreUrl,
        data_directory_prefix: object_store::path::Path,
    ) -> Result<Arc<dyn TableProvider>, TableError> {
        match self {
            TableType::Logical(logical_table) => {
                Box::pin(logical_table.table_provider(
                    &data_directory_store_url,
                    &data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
            TableType::Preset(preset_table) => {
                Box::pin(preset_table.table_provider(
                    table_directory_store_url,
                    table_directory_prefix,
                    data_directory_store_url,
                    data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
            TableType::GeoSpatial(geo_spatial_table) => {
                Box::pin(geo_spatial_table.table_provider(
                    table_directory_store_url,
                    table_directory_prefix,
                    data_directory_store_url,
                    data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
            TableType::Empty(default_table) => {
                Box::pin(default_table.table_provider(
                    table_directory_store_url,
                    table_directory_prefix,
                    data_directory_store_url,
                    data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
            TableType::Delta(table) => {
                Box::pin(table.table_provider(
                    data_directory_store_url,
                    data_directory_prefix,
                    session_ctx,
                ))
                .await
            }
        }
    }

    pub async fn apply_operation(
        &self,
        op: serde_json::Value,
        session_ctx: Arc<SessionContext>,
        data_directory_store_url: &ObjectStoreUrl,
        data_directory_prefix: &object_store::path::Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            TableType::Logical(logical_table) => {
                let listing_urls = logical_table
                    .glob_paths
                    .iter()
                    .map(|glob_path| {
                        parse_listing_table_url(
                            data_directory_store_url,
                            data_directory_prefix,
                            glob_path,
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                logical_table
                    .file_format
                    .apply_operation(op, listing_urls, session_ctx)
                    .await
            }
            table_type => {
                Err(format!("No operations supported for table type: {:?}", table_type).into())
            }
        }
    }
}
