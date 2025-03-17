use datafusion::{dataframe::DataFrameWriteOptions, prelude::DataFrame};

use super::{OutputResponse, TempOutputFile};

pub async fn output(df: DataFrame) -> anyhow::Result<OutputResponse> {
    //Create temp path
    let temp_f = TempOutputFile::new("beacon", ".json")?;

    df.write_json(
        &temp_f.object_store_path(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;

    Ok(OutputResponse {
        output_method: super::OutputMethod::File(temp_f.file),
        content_type: "application/json".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
