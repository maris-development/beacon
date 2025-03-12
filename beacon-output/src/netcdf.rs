use std::sync::Arc;

use beacon_arrow_netcdf::encoders::default::DefaultEncoder;
use datafusion::prelude::{DataFrame, SessionContext};
use futures::StreamExt;

use super::{Output, OutputMethod, TempOutputFile};

pub async fn output(_ctx: Arc<SessionContext>, df: DataFrame) -> anyhow::Result<Output> {
    let arrow_schema = Arc::new(df.schema().as_arrow().clone());
    let file = TempOutputFile::new("beacon", ".nc")?;
    let mut nc_writer = beacon_arrow_netcdf::writer::ArrowRecordBatchWriter::<DefaultEncoder>::new(
        file.path(),
        arrow_schema.clone(),
    )?;
    let mut stream = df.execute_stream().await?;

    while let Some(batch) = stream.next().await {
        let batch = batch?;
        nc_writer.write_record_batch(batch)?;
    }

    nc_writer.finish()?;

    Ok(Output {
        output_method: OutputMethod::File(file.file),
        content_type: "application/netcdf".to_string(),
        content_disposition: "attachment".to_string(),
    })
}
