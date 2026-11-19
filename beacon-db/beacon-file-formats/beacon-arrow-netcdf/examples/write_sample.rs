//! Write sample NetCDF files for a check with netCDF4 for Python.
//!
//! ```text
//! cargo run -p beacon-arrow-netcdf --example write_sample -- <folder>
//! python beacon-db/beacon-file-formats/beacon-arrow-netcdf/examples/check_sample.py <folder>
//! ```
//!
//! The example writes `flat.nc`, `flat_compressed.nc` and `nd.nc` in `<folder>`.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array, RecordBatch, StringArray, TimestampSecondArray};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, TimeUnit};
use beacon_arrow_netcdf::attributes::AttributeTarget;
use beacon_arrow_netcdf::datafusion::options::Compression;
use beacon_arrow_netcdf::datafusion::sink::NetCDFNdSink;
use beacon_arrow_netcdf::encoders::default::DefaultEncoder;
use beacon_arrow_netcdf::encoders::EncoderOptions;
use beacon_arrow_netcdf::oxcdf::AttributeValue;
use beacon_arrow_netcdf::writer::Writer;
use beacon_datafusion_ext::unique_values::{
    UniqueColumnValues, UniqueValuesHandle, UniqueValuesHandleCollection,
};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode, FileSinkConfig};
use datafusion::datasource::sink::DataSink;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let folder = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: write_sample <folder>"))?;
    let folder = Path::new(&folder);
    std::fs::create_dir_all(folder)?;

    write_flat(&folder.join("flat.nc"), &EncoderOptions::default())?;
    let compressed = EncoderOptions {
        compression: Some(Compression {
            level: 4u8.try_into().map_err(|e: String| anyhow::anyhow!(e))?,
            shuffle: true,
        }),
        chunk_rows: Some(2),
    };
    write_flat(&folder.join("flat_compressed.nc"), &compressed)?;
    write_nd(folder).await?;

    println!(
        "wrote flat.nc, flat_compressed.nc and nd.nc in {}",
        folder.display()
    );
    Ok(())
}

/// Write the 3 sample rows and the sample attributes to `path`.
fn write_flat(path: &Path, options: &EncoderOptions) -> anyhow::Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("i32", DataType::Int32, true),
        Field::new("f64", DataType::Float64, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("time", DataType::Timestamp(TimeUnit::Second, None), true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            Arc::new(TimestampSecondArray::from(vec![0, 60, 120])),
        ],
    )?;

    let global = |name: &str| AttributeTarget::Global {
        name: name.to_string(),
    };
    let attributes = [
        (global("title"), AttributeValue::Str("sample".to_string())),
        (global("comment"), AttributeValue::Str(String::new())),
        (
            AttributeTarget::Variable {
                variable: "f64".to_string(),
                name: "units".to_string(),
            },
            AttributeValue::Str("m".to_string()),
        ),
    ];

    let mut writer = Writer::<DefaultEncoder>::with_options(path, schema, options)?;
    writer.put_attributes(&attributes)?;
    writer.write_record_batch(batch)?;
    writer.close()
}

/// Write `nd.nc` with `NetCDFNdSink`: `temperature` over lat = [10, 20], lon = [100, 200, 300].
async fn write_nd(folder: &Path) -> anyhow::Result<()> {
    let lat: FieldRef = Arc::new(Field::new("lat", DataType::Int32, false));
    let lon: FieldRef = Arc::new(Field::new("lon", DataType::Int32, false));
    let temperature = Field::new("temperature", DataType::Float64, true);
    let schema = Arc::new(Schema::new(vec![
        lat.as_ref().clone(),
        lon.as_ref().clone(),
        temperature,
    ]));

    // The sink takes the dimension values from the handles, as `UniqueValuesExec` fills them.
    let handle = UniqueValuesHandle::try_new(&[lat.clone(), lon.clone()])?;
    {
        let mut values = handle.unique_column_values.lock();
        values.insert(
            lat,
            Box::new(UniqueColumnValues::<i32> {
                values: vec![10, 20],
            }),
        );
        values.insert(
            lon,
            Box::new(UniqueColumnValues::<i32> {
                values: vec![100, 200, 300],
            }),
        );
    }
    let collection = UniqueValuesHandleCollection::new();
    collection.register_handle(Arc::new(handle));

    let config = FileSinkConfig {
        original_url: String::new(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::default(),
        // The sink uploads the file through the local file system store.
        table_paths: vec![ListingTableUrl::parse(
            std::path::absolute(folder.join("nd.nc"))?.to_string_lossy(),
        )?],
        output_schema: schema.clone(),
        table_partition_cols: vec![],
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: "nc".to_string(),
        file_output_mode: FileOutputMode::SingleFile,
    };
    let sink = NetCDFNdSink::new(config, 2, collection)?;

    // The cell lat = 20, lon = 300 gets no row.
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 10, 10, 20, 20])),
            Arc::new(Int32Array::from(vec![100, 200, 300, 100, 200])),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0])),
        ],
    )?;
    let stream = Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter(vec![Ok(batch)]),
    ));
    sink.write_all(stream, &Arc::new(TaskContext::default()))
        .await?;
    Ok(())
}
