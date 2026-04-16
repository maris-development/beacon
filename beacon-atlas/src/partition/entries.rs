use chrono::{DateTime, Utc};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PartitionEntry {
    pub name: String,
    pub dataset_index: u32,
    pub deleted: bool,
    pub insert_timestamp: DateTime<Utc>,
}
