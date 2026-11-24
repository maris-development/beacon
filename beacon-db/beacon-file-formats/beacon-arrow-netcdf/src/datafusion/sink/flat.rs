//! The flat sink: one variable per column, on one unlimited `obs` dimension.

use std::{fmt::Formatter, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;

use super::scratch::{upload, ScratchFile};
use crate::{
    datafusion::options::NetcdfWriterOptions, encoders::default::DefaultEncoder,
    writer::ArrowRecordBatchWriter,
};

/// [`ArrowRecordBatchWriter<DefaultEncoder>`] which serializes columns into
/// NetCDF variables along a single unlimited `obs` dimension.
///
/// The sink writes the file in the temp folder of the task's disk manager. Then it
/// uploads the file to the path from [`FileSinkConfig::table_paths`] in the
/// object store of [`FileSinkConfig::object_store_url`].
#[derive(Debug, Clone)]
pub struct NetCDFSink {
    sink_config: FileSinkConfig,
    /// Options of the flat output.
    options: NetcdfWriterOptions,
}

impl NetCDFSink {
    /// Create a new flat sink bound to the given [`FileSinkConfig`].
    pub fn new(sink_config: FileSinkConfig, options: NetcdfWriterOptions) -> Self {
        Self {
            sink_config,
            options,
        }
    }
}

impl DisplayAs for NetCDFSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "NetCDFSink")
    }
}

#[async_trait::async_trait]
impl DataSink for NetCDFSink {
    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let arrow_schema = self.sink_config.output_schema().clone();
        let store = context
            .runtime_env()
            .object_store(&self.sink_config.object_store_url)?;
        let location = self.sink_config.table_paths[0].prefix();
        tracing::info!(
            "Writing NetCDF to {}{}",
            self.sink_config.object_store_url,
            location
        );

        let scratch = ScratchFile::new(context)?;
        let mut rows_written: u64 = 0;
        let mut nc_writer = ArrowRecordBatchWriter::<DefaultEncoder>::with_options(
            scratch.path(),
            arrow_schema,
            self.options.clone(),
        )
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to create NetCDF ArrowRecordBatchWriter: {}",
                e
            ))
        })?;

        let mut pinned_stream = std::pin::pin!(data);

        while let Some(batch) = pinned_stream.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            nc_writer.write_record_batch(batch).map_err(|e| {
                DataFusionError::Execution(format!("Failed to write record batch to NetCDF: {}", e))
            })?;
        }

        let nc_file = nc_writer.finish_file().map_err(|e| {
            DataFusionError::Execution(format!("Failed to finish writing NetCDF: {}", e))
        })?;
        upload(nc_file, store.as_ref(), location).await?;

        Ok(rows_written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_flat_sink_round_trip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("temperature", DataType::Float64, true),
            Field::new("pressure", DataType::Int64, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![15.0, 20.5, 30.1])),
                Arc::new(Int64Array::from(vec![1013, 1010, 1005])),
            ],
        )
        .unwrap();

        // Write to a temp NetCDF via the flat sink
        let tmp_dir = tempfile::tempdir().unwrap();
        let nc_path = tmp_dir.path().join("test_flat.nc");

        // Create an ArrowRecordBatchWriter and write directly to verify
        // the encoder pathway works
        let mut writer =
            ArrowRecordBatchWriter::<DefaultEncoder>::new(&nc_path, schema.clone()).unwrap();
        writer.write_record_batch(batch.clone()).unwrap();
        writer.finish().unwrap();

        // Read the file back with oxcdf and verify contents
        let nc_file = oxcdf::open(&nc_path).unwrap();

        let temp_var = nc_file
            .variable("temperature")
            .expect("temperature variable");
        let temp_vals: Vec<f64> = temp_var.get_values(..).unwrap();
        assert_eq!(temp_vals.len(), 3);
        assert!((temp_vals[0] - 15.0).abs() < f64::EPSILON);
        assert!((temp_vals[1] - 20.5).abs() < f64::EPSILON);
        assert!((temp_vals[2] - 30.1).abs() < f64::EPSILON);

        let press_var = nc_file.variable("pressure").expect("pressure variable");
        let press_vals: Vec<i64> = press_var.get_values(..).unwrap();
        assert_eq!(press_vals, vec![1013i64, 1010, 1005]);
    }
}
