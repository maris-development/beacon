use datafusion::execution::SendableRecordBatchStream;

use super::{Output, OutputFormat};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename = "csv")]
pub struct CsvOutput;

#[typetag::serde]
#[async_trait::async_trait]
impl OutputFormat for CsvOutput {
    async fn output(&self, stream: SendableRecordBatchStream) -> anyhow::Result<Output> {
        todo!()
    }
}
