//! File format plugins through the real loader: the fixture library exports `exflat`
//! (flat) and `exgrid` (nd).

mod common;

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use bytes::Bytes;
use common::plugins::example_plugin;
use common::{restartable_runtime, scalar_i64, write_file, TestRuntime};

async fn runtime_with_plugin(tag: &str) -> TestRuntime {
    let rt = restartable_runtime(tag, |b| b).await;
    rt.runtime
        .install_plugin_archive("example", Bytes::from(example_plugin(false)), "test")
        .await
        .unwrap();
    write_file(&rt.datasets_dir().join("flat/a.exf"), "1,x\n2,y\n3,z\n");
    write_file(&rt.datasets_dir().join("flat/b.exf"), "10,w\n");
    write_file(&rt.datasets_dir().join("grid/g.exg"), "2,3\n1,2,3,4,5,6\n");
    rt
}

fn text(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).unwrap().to_string()
}

async fn sql_error(rt: &TestRuntime, sql: &str) -> String {
    match rt.try_sql(sql).await {
        Ok(_) => panic!("expected an error for `{sql}`"),
        Err(error) => format!("{error:#}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stored_as_by_name_and_extension() {
    let rt = runtime_with_plugin("formats-stored-as").await;
    for key in ["exflat", "exf"] {
        rt.sql(&format!(
            "CREATE EXTERNAL TABLE t_{key} STORED AS {key} LOCATION 'flat/'"
        ))
        .await;
        let sum = scalar_i64(&rt.sql(&format!("SELECT sum(a) FROM t_{key}")).await);
        assert_eq!(sum, 16);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn read_and_schema_functions() {
    let rt = runtime_with_plugin("formats-read").await;
    let rows = rt
        .sql("SELECT a, b FROM read_exflat(['flat/a.exf']) ORDER BY a")
        .await;
    let rendered = text(&rows);
    assert!(rendered.contains("| 3 | z |"), "{rendered}");
    let schema = text(
        &rt.sql("SELECT * FROM read_exflat_schema(['flat/a.exf'])")
            .await,
    );
    assert!(schema.contains("Int64"), "{schema}");
    assert!(schema.contains("Utf8"), "{schema}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_format_end_to_end() {
    let rt = runtime_with_plugin("formats-nd").await;
    let rows = rt
        .sql("SELECT row, col, v FROM read_exgrid(['grid/g.exg']) ORDER BY row, col")
        .await;
    let rendered = text(&rows);
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 6);
    assert!(rendered.contains("| 1   | 2   | 6.0 |"), "{rendered}");
    let count = scalar_i64(
        &rt.sql("SELECT count(*) FROM read_exgrid(['grid/g.exg'])")
            .await,
    );
    assert_eq!(count, 6);
}

#[tokio::test(flavor = "multi_thread")]
async fn uninstall_breaks_tables_cleanly() {
    let rt = runtime_with_plugin("formats-uninstall").await;
    rt.sql("CREATE EXTERNAL TABLE t STORED AS exflat LOCATION 'flat/'")
        .await;
    rt.sql("UNINSTALL example").await;
    let error = sql_error(&rt, "SELECT * FROM t").await;
    assert!(
        error.contains("file format `exflat` is not installed"),
        "{error}"
    );
    let error = sql_error(&rt, "SELECT * FROM read_exflat(['flat/a.exf'])").await;
    assert!(error.contains("read_exflat"), "{error}");

    // A new install brings the format back.
    rt.runtime
        .install_plugin_archive("example", Bytes::from(example_plugin(false)), "test")
        .await
        .unwrap();
    let sum = scalar_i64(&rt.sql("SELECT sum(a) FROM read_exflat(['flat/'])").await);
    assert_eq!(sum, 16);
}

#[tokio::test(flavor = "multi_thread")]
async fn formats_survive_a_restart() {
    let rt = runtime_with_plugin("formats-restart").await;
    rt.sql("CREATE EXTERNAL TABLE t STORED AS exflat LOCATION 'flat/'")
        .await;
    let rt = rt.restart().await;
    let sum = scalar_i64(&rt.sql("SELECT sum(a) FROM t").await);
    assert_eq!(sum, 16);
    let count = scalar_i64(
        &rt.sql("SELECT count(*) FROM read_exflat(['flat/b.exf'])")
            .await,
    );
    assert_eq!(count, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn nd_format_with_partition_columns() {
    let rt = runtime_with_plugin("formats-nd-partitions").await;
    write_file(
        &rt.datasets_dir().join("pgrid/run=a/g.exg"),
        "2,3\n1,2,3,4,5,6\n",
    );
    write_file(&rt.datasets_dir().join("pgrid/run=b/g.exg"), "1,2\n7,8\n");
    rt.sql("CREATE EXTERNAL TABLE pg STORED AS exgrid PARTITIONED BY (run) LOCATION 'pgrid/'")
        .await;
    let counts = text(
        &rt.sql("SELECT run, count(*) AS n FROM pg GROUP BY run ORDER BY run")
            .await,
    );
    assert!(counts.contains("| a   | 6 |"), "{counts}");
    assert!(counts.contains("| b   | 2 |"), "{counts}");
    let sum = rt
        .sql("SELECT sum(v) AS s FROM pg WHERE run = 'b' AND v > 7")
        .await;
    assert!(text(&sum).contains("8.0"), "{}", text(&sum));
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_to_plugin_format() {
    let rt = runtime_with_plugin("formats-copy").await;
    rt.sql(
        "COPY (SELECT a, b FROM read_exflat(['flat/a.exf']) ORDER BY a) \
         TO 'tmp:///copy.exf' STORED AS exflat",
    )
    .await;
    let written = std::fs::read_to_string(rt.tmp_dir().join("copy.exf")).unwrap();
    assert_eq!(written, "1,x\n2,y\n3,z\n");
}

#[tokio::test(flavor = "multi_thread")]
async fn copy_to_read_only_plugin_format_fails() {
    let rt = runtime_with_plugin("formats-copy-read-only").await;
    let error = sql_error(
        &rt,
        "COPY (SELECT 1 AS v) TO 'tmp:///copy.exg' STORED AS exgrid",
    )
    .await;
    assert!(error.contains("cannot write"), "{error}");
}
