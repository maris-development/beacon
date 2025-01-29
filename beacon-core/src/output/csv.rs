use datafusion::{
    dataframe::DataFrameWriteOptions,
    execution::SendableRecordBatchStream,
    prelude::{DataFrame, SessionContext},
};

use super::{Output, OutputFormat};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename = "csv")]
pub struct CsvOutput;

#[typetag::serde]
#[async_trait::async_trait]
impl OutputFormat for CsvOutput {
    async fn output(&self, _: SessionContext, df: DataFrame) -> anyhow::Result<Output> {
        //Create temp path
        let temp_f = tempfile::Builder::new()
            .prefix("beacon")
            .suffix(".csv")
            .tempfile()?;

        df.write_csv(
            temp_f.path().as_os_str().to_str().unwrap(),
            DataFrameWriteOptions::new(),
            None,
        )
        .await?;

        Ok(Output {
            output_method: super::OutputMethod::File(temp_f),
            content_type: "text/csv".to_string(),
            content_disposition: "attachment".to_string(),
        })
    }
}
