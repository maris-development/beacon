use std::{any::Any, path::PathBuf, sync::Arc};

use datafusion::{catalog::TableProvider, prelude::SessionContext};

use super::{
    geo_spatial::GeoSpatialExtension, temporal::TemporalExtension, TableExtension,
    TableExtensionResult,
};

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct GeoTemporal {
    pub geo_spatial: GeoSpatialExtension,
    pub temporal: TemporalExtension,
}

#[typetag::serde(name = "geo_temporal")]
impl TableExtension for GeoTemporal {
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

        let geo_temporal_provider =
            self.temporal
                .table_provider(table_directory, session_ctx, geo_spatial_provider)?;

        // Combine the two providers into a single provider
        Ok(geo_temporal_provider)
    }
}
