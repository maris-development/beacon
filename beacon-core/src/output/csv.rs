use datafusion::{dataframe::DataFrameWriteOptions, prelude::DataFrame};
use object_store::local::LocalFileSystem;

use super::{create_temp_file, Output};

pub async fn output(df: DataFrame) -> anyhow::Result<Output> {
    //Create temp path
    let temp_f = create_temp_file("beacon", ".csv").unwrap();
    println!("Temp file: {:?}", temp_f.path());
    let path = object_store::path::Path::from(temp_f.path().to_str().unwrap());
    println!("Path: {:?}", path);
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
