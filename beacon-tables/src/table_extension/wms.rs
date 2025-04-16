use std::{any::Any, collections::HashMap, path::PathBuf, sync::Arc};

use arrow::datatypes::Schema;
use beacon_common::rename_table_provider::RenameTableProvider;
use datafusion::{catalog::TableProvider, prelude::SessionContext};

use crate::table_extension::TableExtensionError;

use super::{geo_temporal::GeoTemporal, TableExtension, TableExtensionResult};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Wms {
    pub geo_temporal: GeoTemporal,
    pub layers: HashMap<String, String>,
}

#[typetag::serde(name = "wms")]
impl TableExtension for Wms {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
        origin_table_provider: Arc<dyn TableProvider>,
    ) -> TableExtensionResult<Arc<dyn TableProvider>> {
        // ToDo: Implement the WMS table provider logic
        // For now, we will just call the geo_temporal provider
        let geo_spatial_provider = self.geo_temporal.table_provider(
            table_directory.clone(),
            session_ctx.clone(),
            origin_table_provider.clone(),
        )?;

        let mut schema = Schema::new(geo_spatial_provider.schema().fields.clone());

        // Add the layers to the schema
        for (layer_name, table_column) in &self.layers {
            schema = RenameTableProvider::rename_field(&schema, &table_column, &layer_name)
                .map_err(|e| TableExtensionError::from(format!("Failed to rename field: {}", e)))?;
        }

        // Combine the two providers into a single provider
        Ok(
            RenameTableProvider::new(geo_spatial_provider, Arc::new(schema))
                .map_err(|e| {
                    TableExtensionError::from(format!(
                        "Failed to create renamed table provider: {}",
                        e
                    ))
                })
                .map(Arc::new)
                .map_err(|e| {
                    TableExtensionError::from(format!("Failed to create WMS table provider: {}", e))
                })?,
        )
    }
}
