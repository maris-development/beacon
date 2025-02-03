use datafusion::{dataframe::DataFrameWriteOptions, prelude::DataFrame};

use super::Output;

pub async fn output(df: DataFrame) -> anyhow::Result<Output> {
    //Create temp path
    let temp_f = tempfile::Builder::new()
        .prefix("beacon")
        .suffix(".json")
        .tempfile()?;

    df.write_json(
        temp_f.path().as_os_str().to_str().unwrap(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;

    Ok(Output {
        output_method: super::OutputMethod::File(temp_f),
        content_type: "application/json".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
