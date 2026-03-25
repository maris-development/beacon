use futures::stream::BoxStream;

#[typetag::serde(tag = "atlas_feed")]
#[async_trait::async_trait]
pub trait AtlasFeed: std::fmt::Debug {
    async fn fetch(&self) -> BoxStream<'static, anyhow::Result<beacon_atlas::prelude::Dataset>>;
}
