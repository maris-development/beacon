use datafusion::{dataframe::DataFrameWriteOptions, prelude::DataFrame};
use object_store::local::LocalFileSystem;

use super::{Output, TempOutputFile};

pub async fn output(df: DataFrame) -> anyhow::Result<Output> {
    //Create temp path
    let temp_f = TempOutputFile::new("beacon", ".csv")?;
    df.write_csv(
        &temp_f.object_store_path().to_string(),
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;

    Ok(Output {
        output_method: super::OutputMethod::File(temp_f.file),
        content_type: "text/csv".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
