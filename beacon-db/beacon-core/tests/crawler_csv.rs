//! End-to-end crawler test for the CSV format: a Hive-partitioned tree of `.csv`
//! files is discovered and registered as a queryable, partition-pruned table.

mod common;

use common::{runtime, scalar_i64, write_file};

/// Write a CSV file (header `v,name`, one data row) at `path`.
fn write_csv(path: &std::path::Path) {
    write_file(path, "v,name\n1,a\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn crawler_discovers_partitioned_csv() {
    let rt = runtime("crawl-csv").await;

    write_csv(&rt.datasets_dir().join("csv_src/year=2024/a.csv"));
    write_csv(&rt.datasets_dir().join("csv_src/year=2025/b.csv"));

    rt.sql("CREATE CRAWLER csvc ON 'csv_src/'").await;
    rt.sql("RUN CRAWLER csvc").await;

    let count = scalar_i64(&rt.sql("SELECT count(*) FROM csv_src").await);
    assert_eq!(count, 2, "both CSV partition files should be discovered");

    let pruned = scalar_i64(&rt.sql("SELECT count(*) FROM csv_src WHERE year = '2024'").await);
    assert_eq!(pruned, 1, "partition column 'year' should filter to one file");
}
