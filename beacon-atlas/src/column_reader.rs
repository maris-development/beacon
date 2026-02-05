use crate::column::Column;

#[async_trait::async_trait]
pub trait ColumnReader {
    async fn read(&self, dataset_index: u32) -> anyhow::Result<Option<Column>>;
}
