use std::{any::Any, sync::Arc};

use beacon_common::rename_table_provider::RenameTableProvider;
use datafusion::{
    catalog::{SchemaProvider, TableFunctionImpl, TableProvider},
    scalar::ScalarValue,
};

use crate::{schema_provider::BeaconSchemaProvider, table};

use super::TableExtension;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct SpatialTemporalExtension {
    pub longitude_column: String,
    pub latitude_column: String,
    pub depth_column: String,
    pub time_column: String,
}

impl SpatialTemporalExtension {
    const LATITUDE: &'static str = "latitude";
    const LONGITUDE: &'static str = "longitude";
    const DEPTH: &'static str = "depth";
    const TIME: &'static str = "time";
}

#[typetag::serde(name = "spatial_temporal")]
impl TableExtension for SpatialTemporalExtension {
    fn description(&self) -> crate::table_extension::description::TableExtensionDescription {
        crate::table_extension::description::TableExtensionDescription {
            extension_name: self.typetag_name().to_string(),
            extension_description: "Spatial and temporal extension for a data table. Allows for querying using a set of harmonized column names for longitude, latitude, depth & time.".to_string(),
            function_name: Self::table_ext_function_name().to_string(),
            function_args: vec!["table_name".to_string()],
            example_usage: format!(
                "SELECT * FROM {}(table_name) WHERE longitude > 10 AND latitude < 20 AND depth > 100 AND time < '2023-01-01T00:00:00Z'",
                Self::table_ext_function_name()
            ),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
    fn extension_table(
        &self,
        origin_table_provider: Arc<dyn TableProvider>,
    ) -> datafusion::error::Result<Arc<dyn TableProvider>>
    where
        Self: Sized,
    {
        let origin_schema = origin_table_provider.schema();

        let renames = vec![
            (self.latitude_column.clone(), Self::LATITUDE.to_string()),
            (self.longitude_column.clone(), Self::LONGITUDE.to_string()),
            (self.depth_column.clone(), Self::DEPTH.to_string()),
            (self.time_column.clone(), Self::TIME.to_string()),
        ];
        let renamed_schema = RenameTableProvider::rename_fields(&origin_schema, &renames)?;
        let renamed_table =
            RenameTableProvider::new(origin_table_provider, Arc::new(renamed_schema)).map_err(
                |e| {
                    datafusion::error::DataFusionError::Plan(format!(
                        "Failed to create spatial_temporal extension table provider: {}",
                        e
                    ))
                },
            )?;

        Ok(Arc::new(renamed_table))
    }

    fn table_ext_function_name() -> &'static str
    where
        Self: Sized,
    {
        "as_spatial_temporal"
    }
}
