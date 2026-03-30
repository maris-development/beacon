use futures::stream::BoxStream;

#[typetag::serde(tag = "atlas_feed")]
#[async_trait::async_trait]
pub trait AtlasFeed: std::fmt::Debug + Send + Sync {
    async fn fetch_new(
        &self,
        modified_from: Option<chrono::DateTime<chrono::Utc>>,
    ) -> BoxStream<'static, anyhow::Result<beacon_atlas::prelude::Dataset>>;
    async fn fetch_removed(&self) -> BoxStream<'static, anyhow::Result<String>>;
}
