pub mod feed;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct AtlasTable {
    feed: Box<dyn feed::AtlasFeed>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    resync_interval_seconds: Option<u64>,
    // Directory within the datasets store where the Atlas Collection will be stored. This is used to determine the path for the collection's metadata and partitions.
    atlas_directory: String,
}
