use std::sync::Arc;

use arrow::datatypes::{DataType, Schema};
use beacon_arrow_odv::writer::{AsyncOdvWriter, OdvOptions};
use datafusion::{
    config::CsvOptions,
    dataframe::DataFrameWriteOptions,
    logical_expr::SortExpr,
    prelude::{col, lit, DataFrame, Expr, SessionContext},
};
use futures::{pin_mut, StreamExt};
use serde::{Deserialize, Serialize};
use tempfile::tempdir;
use tokio::io::AsyncWriteExt;
use utoipa::ToSchema;

use super::{Output, OutputMethod};

pub async fn output(ctx: Arc<SessionContext>, df: DataFrame) -> anyhow::Result<Output> {
    let temp_dir = tempfile::tempdir()?;
    let arrow_schema = Arc::new(df.schema().as_arrow().clone());
    let mut file = tempfile::Builder::new()
        .prefix("beacon")
        .suffix(".tar.zst")
        .tempfile()?;

    let mut odv_writer = AsyncOdvWriter::new(
        OdvOptions::try_from_arrow_schema(arrow_schema.clone())?,
        arrow_schema.clone(),
        temp_dir.path(),
    )
    .await?;
    let mut stream = df.execute_stream().await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        odv_writer.write(batch).await?;
    }

    odv_writer.finish_to_tar(file.as_file_mut())?;
    drop(temp_dir);
    Ok(Output {
        output_method: OutputMethod::File(file),
        content_type: "application/tar+zstd".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
