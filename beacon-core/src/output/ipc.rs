use std::{collections::HashMap, sync::Arc};

use datafusion::{
    dataframe::DataFrameWriteOptions,
    datasource::file_format::{
        arrow::{ArrowFormat, ArrowFormatFactory},
        format_as_file_type,
    },
    execution::SendableRecordBatchStream,
    logical_expr::LogicalPlanBuilder,
    prelude::{DataFrame, SessionContext},
};

use super::{Output, OutputFormat};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename = "ipc")]
pub struct IpcOutput;

#[typetag::serde]
#[async_trait::async_trait]
impl OutputFormat for IpcOutput {
    async fn output(&self, ctx: SessionContext, df: DataFrame) -> anyhow::Result<Output> {
        //Create temp path
        let temp_f = tempfile::Builder::new()
            .prefix("beacon")
            .suffix(".arrow")
            .tempfile()?;

        let format_factory = Arc::new(ArrowFormatFactory::new());
        let file_type = format_as_file_type(format_factory);

        let plan = LogicalPlanBuilder::copy_to(
            df.into_unoptimized_plan(),
            temp_f.path().to_str().unwrap().into(),
            file_type,
            HashMap::new(),
            vec![],
        )?
        .build()?;

        let state = ctx.state();

        DataFrame::new(state, plan).collect().await?;

        Ok(Output {
            output_method: super::OutputMethod::File(temp_f),
            content_type: "application/vnd.apache.arrow.file".to_string(),
            content_disposition: "attachment".to_string(),
        })
    }
}
