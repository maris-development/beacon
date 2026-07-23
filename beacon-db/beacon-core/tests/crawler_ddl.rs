//! The crawler DDL surface itself — `CREATE`/`RUN`/`SHOW`/`DROP CRAWLER` as
//! statements — as opposed to the discovery behaviour the `crawler_*` tests
//! cover.
//!
//! `RUN CRAWLER` returns its crawl report as a result set, and `SHOW CRAWLERS`
//! renders the full definition (options as JSON), so this is API surface the
//! admin endpoints translate straight to JSON.

mod common;

use common::{column_strings, runtime, scalar_i64, total_rows, write_file};
use datafusion::arrow::array::UInt64Array;

#[tokio::test(flavor = "multi_thread")]
async fn run_crawler_returns_the_crawl_report() {
    let rt = runtime("crawler-report").await;
    write_file(&rt.datasets_dir().join("rpt_src/a.csv"), "v\n1\n");

    rt.sql("CREATE CRAWLER rptc ON 'rpt_src/'").await;
    let report = rt.sql("RUN CRAWLER rptc").await;

    assert_eq!(total_rows(&report), 1, "one report row per run");
    let schema = report[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "crawler",
            "discovered",
            "created",
            "updated",
            "skipped",
            "failed",
            "skipped_files"
        ]
    );

    assert_eq!(column_strings(&report, 0), vec!["rptc"]);
    let discovered = report[0]
        .column(1)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .expect("discovered is UInt64")
        .value(0);
    assert_eq!(discovered, 1, "the single CSV table should be discovered");

    let created: Vec<String> =
        serde_json::from_str(&column_strings(&report, 2)[0]).expect("created is a JSON list");
    assert_eq!(created, vec!["rpt_src"], "the discovered table is reported");

    // And the discovered table is really there.
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM rpt_src").await), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn show_crawlers_renders_the_definition() {
    let rt = runtime("crawler-show").await;

    rt.sql("CREATE CRAWLER showc ON 'show_src/'").await;
    let rows = rt.sql("SHOW CRAWLERS").await;

    assert_eq!(total_rows(&rows), 1);
    let schema = rows[0].schema();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "name",
            "target_prefix",
            "format_filter",
            "detect_partitions",
            "schedule_secs",
            "event_driven",
            "table_naming",
            "options"
        ]
    );
    assert_eq!(column_strings(&rows, 0), vec!["showc"]);
    assert_eq!(column_strings(&rows, 1), vec!["show_src/"]);

    // Options render as a JSON object (empty here), not a debug string.
    let options: serde_json::Value =
        serde_json::from_str(&column_strings(&rows, 7)[0]).expect("options should be JSON");
    assert!(options.is_object(), "options should be a JSON object");
}

/// A quoted crawler name must be stored (and looked up) by its bare value.
/// Regression: the parsed identifier used to be lowered via `Display`, which
/// re-adds the SQL quotes, so `SHOW CRAWLERS` reported `"qc1"` and the
/// unquoted name did not resolve.
#[tokio::test(flavor = "multi_thread")]
async fn quoted_crawler_name_round_trips_unquoted() {
    let rt = runtime("crawler-quoted-name").await;
    write_file(&rt.datasets_dir().join("q_src/a.csv"), "v\n1\n");

    rt.sql("CREATE CRAWLER \"qc1\" ON 'q_src/'").await;

    assert_eq!(
        column_strings(&rt.sql("SHOW CRAWLERS").await, 0),
        vec!["qc1"],
        "the stored name must be the bare identifier value, not `\"qc1\"`"
    );

    // Quoted and unquoted references resolve to the same crawler.
    rt.sql("RUN CRAWLER qc1").await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM q_src").await), 1);

    rt.sql("DROP CRAWLER \"qc1\"").await;
    assert_eq!(
        total_rows(&rt.sql("SHOW CRAWLERS").await),
        0,
        "the quoted DROP should remove the crawler"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_crawler_removes_it_and_unknown_names_error() {
    let rt = runtime("crawler-drop").await;

    rt.sql("CREATE CRAWLER dropc ON 'drop_src/'").await;
    assert_eq!(total_rows(&rt.sql("SHOW CRAWLERS").await), 1);

    rt.sql("DROP CRAWLER dropc").await;
    assert_eq!(total_rows(&rt.sql("SHOW CRAWLERS").await), 0);

    assert!(
        rt.try_sql("RUN CRAWLER dropc").await.is_err(),
        "running a dropped crawler should fail"
    );
    assert!(
        rt.try_sql("DROP CRAWLER never_existed").await.is_err(),
        "dropping an unknown crawler should fail"
    );
}
