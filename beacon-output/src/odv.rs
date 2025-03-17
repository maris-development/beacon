use std::sync::Arc;

use beacon_arrow_odv::writer::{AsyncOdvWriter, OdvOptions};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::StreamExt;

use super::{OutputResponse, OutputMethod, TempOutputFile};

pub async fn output(
    _ctx: Arc<SessionContext>,
    df: DataFrame,
    odv_options: Option<OdvOptions>,
) -> anyhow::Result<OutputResponse> {
    let temp_dir = tempfile::tempdir()?;
    let arrow_schema = Arc::new(df.schema().as_arrow().clone());
    let mut file = TempOutputFile::new("beacon", ".zip")?;

    let odv_options =
        odv_options.unwrap_or(OdvOptions::try_from_arrow_schema(arrow_schema.clone())?);

    let mut odv_writer =
        AsyncOdvWriter::new(odv_options, arrow_schema.clone(), temp_dir.path()).await?;
    let mut stream = df.execute_stream().await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        odv_writer.write(batch).await?;
    }

    odv_writer.finish_to_archive(file.file.as_file_mut())?;
    drop(temp_dir);
    Ok(OutputResponse {
        output_method: OutputMethod::File(file.file),
        content_type: "application/zip".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
