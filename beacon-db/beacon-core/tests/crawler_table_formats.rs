//! A crawler registers a Delta table as a Delta table, not as the Parquet files in its directory.
//!
//! A Delta directory keeps the Parquet files of old versions until a vacuum. Read as plain
//! Parquet, the table returns the rows of every version.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{runtime, scalar_i64};
use datafusion::arrow::array::{Int32Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use deltalake::protocol::SaveMode;
use deltalake::DeltaTableBuilder;

fn batch(ids: &[i32]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(ids.to_vec()))]).unwrap()
}

/// A Delta table at `dir` whose current version holds one row. Version 0 held two rows, and
/// their Parquet file stays in the directory.
async fn write_overwritten_delta(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let url = url::Url::from_directory_path(std::fs::canonicalize(dir).unwrap()).unwrap();
    let table = DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    let table = table
        .write(vec![batch(&[1, 2])])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    table
        .write(vec![batch(&[3])])
        .with_save_mode(SaveMode::Overwrite)
        .await
        .unwrap();
}

fn write_parquet(path: &Path) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let batch = batch(&[7]);
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crawled_delta_table_reads_its_current_version() {
    let rt = runtime("crawl-delta").await;
    write_overwritten_delta(&rt.datasets_dir().join("crawl_fmt/lake")).await;
    write_parquet(&rt.datasets_dir().join("crawl_fmt/plain/p.parquet"));

    rt.sql("CREATE CRAWLER cf ON 'crawl_fmt/'").await;
    rt.sql("RUN CRAWLER cf").await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM lake").await),
        1,
        "the Delta table holds one row; the Parquet files of both versions hold three"
    );
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM plain").await), 1);

    // A second run updates the crawler's own tables instead of skipping them.
    rt.sql("RUN CRAWLER cf").await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM lake").await), 1);
}
