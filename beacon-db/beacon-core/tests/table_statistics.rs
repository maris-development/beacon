//! The file statistics of external tables, through a real runtime.
//!
//! A table with `use_statistics_pruning` registers with the engine. A refresh
//! fills its statistics, a scan prunes on them, `table_statistics()` shows them,
//! and `DROP TABLE` deletes them. `ANALYZE FILES` starts a refresh in the background.

mod common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{AsArray, Float64Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use beacon_common::FileStatsConfig;
use beacon_core::AuthIdentity;
use beacon_file_statistics::RefreshSummary;
use common::{
    column_strings, restartable_runtime, runtime, runtime_with, total_rows, write_file, TestRuntime,
};
use datafusion::parquet::arrow::ArrowWriter;

const CREATE_OBS: &str = "CREATE EXTERNAL TABLE obs STORED AS PARQUET LOCATION 'obs/' \
                          OPTIONS ('use_statistics_pruning' 'true')";

/// Writes a Parquet file whose `TEMP` column spans `[min, max]`.
fn write_parquet(path: &Path, min: f64, max: f64) {
    std::fs::create_dir_all(path.parent().expect("a parent")).expect("a folder");
    let schema = Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, false),
        Field::new("DEPTH", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Float64Array::from(vec![min, max])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("a batch");
    let file = std::fs::File::create(path).expect("a file");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("a writer");
    writer.write(&batch).expect("a write");
    writer.close().expect("a close");
}

/// Three files with disjoint `TEMP` ranges under `obs/`.
fn write_obs(datasets: &Path) {
    write_parquet(&datasets.join("obs/cold.parquet"), 0.0, 5.0);
    write_parquet(&datasets.join("obs/mild.parquet"), 20.0, 25.0);
    write_parquet(&datasets.join("obs/hot.parquet"), 90.0, 100.0);
}

/// Refreshes the statistics of `table`, and waits for the end.
///
/// A refresh waits for the refresh of `table` that runs now, so the background
/// refresh at registration cannot run after this one.
async fn refresh(rt: &TestRuntime, table: &str) -> RefreshSummary {
    rt.runtime
        .file_stats()
        .expect("file statistics are on")
        .refresh(table)
        .await
        .expect("a refresh")
}

/// The `ANALYZE FILES` report as `(table, status)` rows.
fn report(batches: &[RecordBatch]) -> Vec<(String, String)> {
    column_strings(batches, 0)
        .into_iter()
        .zip(column_strings(batches, 1))
        .collect()
}

/// The row count of `table_statistics('<table>')`.
async fn statistics_rows(rt: &TestRuntime, table: &str) -> usize {
    total_rows(
        &rt.sql(&format!("SELECT _path FROM table_statistics('{table}')"))
            .await,
    )
}

/// The sum of the metric `name` over the `EXPLAIN ANALYZE` output of `sql`.
async fn metric(rt: &TestRuntime, sql: &str, name: &str) -> usize {
    let batches = rt.sql(&format!("EXPLAIN ANALYZE {sql}")).await;
    let text = arrow::util::pretty::pretty_format_batches(&batches)
        .expect("a table")
        .to_string();
    let marker = format!("{name}=");
    text.split(&marker)
        .skip(1)
        .map(|rest| {
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            digits.parse::<usize>().expect("a count")
        })
        .sum()
}

/// The error of `sql`, run as `identity`.
async fn error_as(rt: &TestRuntime, sql: &str, identity: AuthIdentity) -> String {
    match rt.try_sql_as(sql, identity).await {
        Ok(_) => panic!("{sql} should fail"),
        Err(error) => format!("{error:#}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refresh_fills_the_statistics_and_a_scan_prunes() {
    let rt = runtime("statistics-prune").await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;

    assert_eq!(refresh(&rt, "obs").await.listed, 3);
    // Each file has a row now, so a second refresh has nothing to do.
    let second = refresh(&rt, "obs").await;
    assert_eq!((second.analyzed, second.removed), (0, 0));

    let sql = "SELECT * FROM obs WHERE \"TEMP\" > 80";
    assert_eq!(metric(&rt, sql, "statistics_files_considered").await, 3);
    assert_eq!(metric(&rt, sql, "statistics_files_pruned").await, 2);
    assert_eq!(
        total_rows(&rt.sql(sql).await),
        2,
        "hot.parquet holds both rows"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_returns_at_once_and_refreshes_in_the_background() {
    let rt = runtime("statistics-analyze").await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;
    refresh(&rt, "obs").await;
    write_parquet(&rt.datasets_dir().join("obs/new.parquet"), 40.0, 45.0);

    // The interval is 900 seconds, so only the request reads the new file.
    let rows = report(&rt.sql("ANALYZE FILES obs").await);
    assert_eq!(rows.len(), 1, "one row for one table: {rows:?}");
    assert_eq!(rows[0].0, "obs");
    assert!(
        ["started", "queued"].contains(&rows[0].1.as_str()),
        "{rows:?}"
    );

    tokio::time::timeout(Duration::from_secs(30), async {
        while statistics_rows(&rt, "obs").await < 4 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the background refresh reads the new file");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_covers_each_table_with_statistics() {
    let rt = runtime("statistics-each").await;
    write_obs(rt.datasets_dir());
    write_parquet(&rt.datasets_dir().join("argo/a.parquet"), 1.0, 2.0);
    rt.sql(CREATE_OBS).await;
    rt.sql(
        "CREATE EXTERNAL TABLE argo STORED AS PARQUET LOCATION 'argo/' \
         OPTIONS ('use_statistics_pruning' 'true')",
    )
    .await;
    // A table without the option has no statistics.
    rt.sql("CREATE EXTERNAL TABLE plain STORED AS PARQUET LOCATION 'obs/'")
        .await;

    let rows = report(&rt.sql("ANALYZE FILES").await);
    let tables: Vec<&str> = rows.iter().map(|row| row.0.as_str()).collect();
    assert_eq!(tables, vec!["argo", "obs"]);

    let plain = error_as(&rt, "ANALYZE FILES plain", rt.admin().await).await;
    assert!(plain.contains("use_statistics_pruning"), "{plain}");
    let unknown = error_as(&rt, "ANALYZE FILES nope", rt.admin().await).await;
    assert!(unknown.contains("nope"), "{unknown}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn table_statistics_shows_one_row_for_each_file_to_the_super_user() {
    let rt = runtime("statistics-function").await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;
    refresh(&rt, "obs").await;

    let batches = rt
        .sql("SELECT _path, \"TEMP\"['min'] FROM table_statistics('obs') ORDER BY _path")
        .await;
    assert_eq!(
        column_strings(&batches, 0),
        vec!["obs/cold.parquet", "obs/hot.parquet", "obs/mild.parquet"]
    );
    let minimums: Vec<f64> = batches
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(1)
                .as_primitive::<arrow::datatypes::Float64Type>();
            (0..column.len())
                .map(|row| column.value(row))
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(minimums, vec![0.0, 90.0, 20.0]);

    let reader = AuthIdentity {
        username: "reader".to_string(),
        roles: Vec::new(),
        is_super_user: false,
    };
    let refused = error_as(&rt, "SELECT * FROM table_statistics('obs')", reader).await;
    assert!(refused.contains("super-user"), "{refused}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_statistics_survive_a_restart() {
    let rt = restartable_runtime("statistics-restart", |builder| builder).await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;
    // The second refresh waits for a background refresh that runs, so none runs at the restart.
    refresh(&rt, "obs").await;
    refresh(&rt, "obs").await;

    let rt = rt.restart().await;
    let sql = "SELECT * FROM obs WHERE \"TEMP\" > 80";
    assert_eq!(metric(&rt, sql, "statistics_files_pruned").await, 2);
    assert_eq!(refresh(&rt, "obs").await.analyzed, 0, "no file changed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_table_deletes_the_statistics() {
    let rt = runtime("statistics-drop").await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;
    refresh(&rt, "obs").await;

    rt.sql("DROP TABLE obs").await;

    let error = error_as(
        &rt,
        "SELECT * FROM table_statistics('obs')",
        rt.admin().await,
    )
    .await;
    assert!(error.contains("no statistics"), "{error}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refresh_registers_the_table_again_and_reads_new_files() {
    let rt = runtime("statistics-refresh").await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;
    refresh(&rt, "obs").await;
    write_parquet(&rt.datasets_dir().join("obs/new.parquet"), 40.0, 45.0);

    // A new registration refreshes at once; the interval alone waits 900 seconds.
    rt.sql("REFRESH obs").await;

    tokio::time::timeout(Duration::from_secs(30), async {
        while statistics_rows(&rt, "obs").await < 4 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the registration after REFRESH reads the new file");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crawler_passes_the_option_to_its_tables() {
    let rt = runtime("statistics-crawler").await;
    write_obs(rt.datasets_dir());

    rt.sql("CREATE CRAWLER c ON 'obs/' WITH ('use_statistics_pruning' 'true')")
        .await;
    rt.sql("RUN CRAWLER c").await;

    assert_eq!(refresh(&rt, "obs").await.listed, 3);
    let sql = "SELECT * FROM obs WHERE \"TEMP\" > 80";
    assert_eq!(metric(&rt, sql, "statistics_files_pruned").await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn or_replace_keeps_or_deletes_the_statistics_with_the_option() {
    let rt = runtime("statistics-replace").await;
    write_obs(rt.datasets_dir());
    rt.sql(CREATE_OBS).await;
    refresh(&rt, "obs").await;

    // The new table keeps statistics, so its rows stay.
    rt.sql(&CREATE_OBS.replacen("CREATE", "CREATE OR REPLACE", 1))
        .await;
    assert_eq!(refresh(&rt, "obs").await.analyzed, 0, "the rows stay");

    // The new table keeps no statistics, so they go.
    rt.sql("CREATE OR REPLACE EXTERNAL TABLE obs STORED AS PARQUET LOCATION 'obs/'")
        .await;
    let error = error_as(
        &rt,
        "SELECT * FROM table_statistics('obs')",
        rt.admin().await,
    )
    .await;
    assert!(error.contains("no statistics"), "{error}");
    assert!(report(&rt.sql("ANALYZE FILES").await).is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn too_many_range_columns_need_statistics_columns() {
    let rt = runtime("statistics-columns").await;
    let header: Vec<String> = (0..1001).map(|column| format!("c{column}")).collect();
    let row: Vec<String> = (0..1001).map(|column| column.to_string()).collect();
    write_file(
        &rt.datasets_dir().join("wide/a.csv"),
        &format!("{}\n{}\n", header.join(","), row.join(",")),
    );

    let error = error_as(
        &rt,
        "CREATE EXTERNAL TABLE wide STORED AS CSV LOCATION 'wide/' \
         OPTIONS ('use_statistics_pruning' 'true')",
        rt.admin().await,
    )
    .await;
    assert!(error.contains("statistics_columns"), "{error}");

    rt.sql(
        "CREATE EXTERNAL TABLE wide STORED AS CSV LOCATION 'wide/' \
         OPTIONS ('use_statistics_pruning' 'true', 'statistics_columns' 'c0,c1')",
    )
    .await;
    assert_eq!(refresh(&rt, "wide").await.listed, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn analyze_files_names_the_setting_when_statistics_are_off() {
    let rt = runtime_with("statistics-off", |builder| {
        builder.with_file_stats(FileStatsConfig {
            enable: false,
            ..FileStatsConfig::default()
        })
    })
    .await;

    let error = error_as(&rt, "ANALYZE FILES", rt.admin().await).await;
    assert!(error.contains("BEACON_FILE_STATS_ENABLE"), "{error}");
}
