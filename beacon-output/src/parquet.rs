use datafusion::{dataframe::DataFrameWriteOptions, prelude::DataFrame};

use super::{Output, TempOutputFile};

pub async fn output(df: DataFrame) -> anyhow::Result<Output> {
    //Create temp path
    let temp_f = TempOutputFile::new("beacon", ".parquet")?;

    df.write_parquet(
        &temp_f.object_store_path(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;

    Ok(Output {
        output_method: super::OutputMethod::File(temp_f.file),
        content_type: "application/vnd.apache.parquet".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
