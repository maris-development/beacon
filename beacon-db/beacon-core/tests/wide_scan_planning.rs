//! Planning over a very wide table.
//!
//! An Atlas table can declare 100k+ columns. DataFusion's common-subexpression
//! rule copies every input column into an intermediate projection, one linear
//! schema lookup per column, so the scan must be narrowed before that rule
//! runs. The runtime puts `OptimizeProjections` first in its rule list for
//! that reason, and this test pins the order: the query below repeats `abs(c0)`
//! after simplification and has to plan in well under a minute.

mod common;

use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use common::{runtime, total_rows};
use datafusion::parquet::arrow::ArrowWriter;

/// Wide enough that a quadratic pass takes minutes, narrow enough to write fast.
const COLUMNS: usize = 80_000;

#[tokio::test(flavor = "multi_thread")]
async fn a_repeated_sub_expression_over_a_wide_scan_plans_quickly() {
    let rt = runtime("wide-scan").await;

    let fields: Vec<Field> = (0..COLUMNS)
        .map(|i| Field::new(format!("c{i}"), DataType::Float64, true))
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let path = rt.datasets_dir().join("wide/w.parquet");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer.write(&RecordBatch::new_empty(schema)).unwrap();
    writer.close().unwrap();

    rt.sql("CREATE EXTERNAL TABLE wide STORED AS PARQUET LOCATION 'wide/'")
        .await;

    // `coalesce(f(x), y)` simplifies to a CASE that names `f(x)` twice.
    let query = rt.try_sql("SELECT coalesce(abs(c0), c1) AS d FROM wide LIMIT 5");
    let batches = tokio::time::timeout(Duration::from_secs(60), query)
        .await
        .expect("planning must not be quadratic in the column count")
        .expect("the query runs");
    assert_eq!(total_rows(&batches), 0);
}
