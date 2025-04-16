use std::{any::Any, path::PathBuf, sync::Arc};

use datafusion::{catalog::TableProvider, prelude::SessionContext};

use super::{
    geo_spatial::GeoSpatialExtension, temporal::TemporalExtension, vertical_axis::VerticalAxis,
    TableExtension, TableExtensionResult,
};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct GeoVerticalTemporal {
    pub geo_spatial: GeoSpatialExtension,
    pub vertical_axis: VerticalAxis,
    pub temporal: TemporalExtension,
}

#[typetag::serde(name = "geo_vertical_temporal")]
impl TableExtension for GeoVerticalTemporal {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_provider(
        &self,
        table_directory: PathBuf,
        session_ctx: Arc<SessionContext>,
        origin_table_provider: Arc<dyn TableProvider>,
    ) -> TableExtensionResult<Arc<dyn TableProvider>> {
        let geo_spatial_provider = self.geo_spatial.table_provider(
            table_directory.clone(),
            session_ctx.clone(),
            origin_table_provider.clone(),
        )?;

        let geo_temporal_provider = self.temporal.table_provider(
            table_directory.clone(),
            session_ctx.clone(),
            geo_spatial_provider,
        )?;

        let geo_vertical_temporal_provider = self.vertical_axis.table_provider(
            table_directory.clone(),
            session_ctx.clone(),
            geo_temporal_provider.clone(),
        )?;

        Ok(geo_vertical_temporal_provider)
    }
}
