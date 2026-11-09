use arrow::datatypes::SchemaRef;
use datafusion::common::Statistics;

use crate::stamp::Stamp;

pub(crate) struct StatisticsStore {}

impl StatisticsStore {
    pub async fn write_statistics(
        &self,
        stamp: Stamp,
        schema: SchemaRef,
        statistics: Statistics,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
