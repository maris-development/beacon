use std::path::PathBuf;

use beacon_arrow_netcdf::reader::NetCDFArrowReader;
use beacon_atlas::partition::ops::read::Dataset;
use beacon_nd_arrow::NdRecordBatch;
use futures::StreamExt;
use futures::stream::BoxStream;

pub async fn read_as_dataset_stream(
    files: BoxStream<'static, anyhow::Result<PathBuf>>,
) -> BoxStream<'static, anyhow::Result<Dataset>> {
    (files
        .map(|file_path_res| {
            let file_path = file_path_res?;
            let file = NetCDFArrowReader::new(file_path.clone())?;

            let arrays = file.read_columns::<Vec<_>>(None)?;
            let schema = file.schema();

            let batch = NdRecordBatch::new(
                file_path
                    .file_name()
                    .ok_or_else(|| anyhow::anyhow!("Invalid file name"))?
                    .to_string_lossy()
                    .to_string(),
                schema,
                arrays,
            )?;

            println!("Read dataset from file: {}", file_path.display());

            let dataset = Dataset(batch);

            Ok(dataset)
        })
        .boxed()) as _
}
