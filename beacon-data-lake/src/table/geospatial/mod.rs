use crate::table::preset::{PresetColumnMapping, PresetTable};

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct GeoSpatialTable {
    #[serde(flatten)]
    preset_table: PresetTable,
    longitude_column: PresetColumnMapping,
    latitude_column: PresetColumnMapping,
}

impl GeoSpatialTable {}
