use datafusion::{
    dataframe::DataFrameWriteOptions,
    execution::SendableRecordBatchStream,
    prelude::{DataFrame, SessionContext},
};

use super::{Output, OutputFormat};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename = "parquet")]
pub struct ParquetOutput;

#[typetag::serde]
#[async_trait::async_trait]
impl OutputFormat for ParquetOutput {
    async fn output(&self, _: SessionContext, df: DataFrame) -> anyhow::Result<Output> {
        //Create temp path
        let temp_f = tempfile::Builder::new()
            .prefix("beacon")
            .suffix(".parquet")
            .tempfile()?;

        df.write_parquet(
            temp_f.path().as_os_str().to_str().unwrap(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

        Ok(Output {
            output_method: super::OutputMethod::File(temp_f),
            content_type: "application/vnd.apache.parquet".to_string(),
            content_disposition: "attachment".to_string(),
        })
    }
}
