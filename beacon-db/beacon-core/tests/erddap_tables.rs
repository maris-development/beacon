//! End-to-end tests for `CREATE EXTERNAL TABLE ... STORED AS ERDDAP` against a local
//! fixture server that replays recorded ERDDAP responses.

mod common;

use beacon_erddap::fixture::{FixtureServer, Route};
use common::{TestRuntime, scalar_i64};

const T: &str = "erdGlobecBottle";

async fn tabledap_server() -> FixtureServer {
    FixtureServer::start(vec![
        Route::file(
            &format!("/erddap/info/{T}/index.json"),
            "tabledap_info.json",
        ),
        Route::status(
            &format!("/erddap/tabledap/{T}.parquet"),
            404,
            beacon_erddap::fixture::test_file("no_results.txt"),
        )
        .with_query("cruise_id=\"none\""),
        Route::file(&format!("/erddap/tabledap/{T}.parquet"), "tabledap.parquet"),
    ])
    .await
}

async fn create(rt: &TestRuntime, server: &FixtureServer, table: &str) {
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ERDDAP LOCATION '{}/tabledap/{T}'",
        server.erddap_url()
    ))
    .await;
}

fn last_data_request(server: &FixtureServer) -> String {
    server
        .requests()
        .into_iter()
        .rev()
        .find(|r| r.contains(".parquet"))
        .expect("a data request")
}

#[tokio::test(flavor = "multi_thread")]
async fn tabledap_select_projects_and_counts() {
    let rt = common::runtime("erddap-tabledap").await;
    let server = tabledap_server().await;
    create(&rt, &server, "bottles").await;

    let rows = rt.sql("SELECT cruise_id, time FROM bottles").await;
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 62);
    let request = last_data_request(&server);
    assert!(request.ends_with(".parquet?cruise_id,time"), "{request}");

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM bottles").await),
        62
    );
    let request = last_data_request(&server);
    assert!(request.ends_with(".parquet?cruise_id"), "{request}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tabledap_pushes_filters_and_refilters_locally() {
    let rt = common::runtime("erddap-filters").await;
    let server = tabledap_server().await;
    create(&rt, &server, "bottles").await;

    let all = scalar_i64(
        &rt.sql("SELECT count(*) FROM bottles WHERE temperature0 IS NOT NULL")
            .await,
    );
    let warm = scalar_i64(
        &rt.sql(
            "SELECT count(*) FROM bottles WHERE temperature0 > 8 \
             AND time >= TIMESTAMP '2002-05-30T12:00:00Z' AND ship = 'Wecoma'",
        )
        .await,
    );
    let request = last_data_request(&server);
    // Float columns relax a strict operator, so the pushed filter stays a superset.
    assert!(request.contains("&temperature0>=8"), "{request}");
    assert!(request.contains("&time>=2002-05-30T12:00:00Z"), "{request}");
    assert!(request.contains("&ship=\"Wecoma\""), "{request}");
    // The fixture ignores constraints, so a smaller count proves the local re-filter.
    assert_eq!(all, 62);
    assert!(warm > 0 && warm < all, "warm={warm} all={all}");
}

#[tokio::test(flavor = "multi_thread")]
async fn tabledap_no_results_is_empty() {
    let rt = common::runtime("erddap-empty").await;
    let server = tabledap_server().await;
    create(&rt, &server, "bottles").await;
    assert_eq!(
        scalar_i64(
            &rt.sql("SELECT count(*) FROM bottles WHERE cruise_id = 'none'")
                .await
        ),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn create_errors() {
    let rt = common::runtime("erddap-errors").await;
    let server = tabledap_server().await;
    let bad = rt
        .try_sql("CREATE EXTERNAL TABLE a STORED AS ERDDAP LOCATION 'https://h/erddap/info/x'")
        .await;
    let bad = bad.unwrap_err().to_string();
    assert!(bad.contains("/tabledap/<datasetID>"), "{bad}");
    let unknown = rt
        .try_sql(&format!(
            "CREATE EXTERNAL TABLE b STORED AS ERDDAP LOCATION '{}/tabledap/missing'",
            server.erddap_url()
        ))
        .await;
    let unknown = unknown.unwrap_err().to_string();
    // The top-level message carries the cause, so the user sees why the create failed.
    assert!(
        unknown.contains("failed to read ERDDAP dataset") && unknown.contains("HTTP 404"),
        "{unknown}"
    );
    let columns = rt
        .try_sql(&format!(
            "CREATE EXTERNAL TABLE c (x INT) STORED AS ERDDAP LOCATION '{}/tabledap/{T}'",
            server.erddap_url()
        ))
        .await;
    let columns = columns.unwrap_err().to_string();
    assert!(columns.contains("columns from the dataset"), "{columns}");
    let option = rt
        .try_sql(&format!(
            "CREATE EXTERNAL TABLE d STORED AS ERDDAP LOCATION '{}/tabledap/{T}' OPTIONS ('tls' 'true')",
            server.erddap_url()
        ))
        .await;
    let option = option.unwrap_err().to_string();
    assert!(option.contains("unknown ERDDAP option"), "{option}");
}

#[tokio::test(flavor = "multi_thread")]
async fn griddap_is_rejected_before_any_request() {
    let rt = common::runtime("erddap-griddap").await;
    let server = tabledap_server().await;
    let griddap = rt
        .try_sql(&format!(
            "CREATE EXTERNAL TABLE g STORED AS ERDDAP LOCATION '{}/griddap/erdHadISST'",
            server.erddap_url()
        ))
        .await;
    let griddap = griddap.unwrap_err().to_string();
    assert!(
        griddap
            .contains("ERDDAP griddap datasets are not supported yet; use a tabledap dataset URL"),
        "{griddap}"
    );
    assert!(server.requests().is_empty(), "{:?}", server.requests());
}

#[tokio::test(flavor = "multi_thread")]
async fn tabledap_table_survives_a_restart_without_the_info_request() {
    let rt = common::restartable_runtime("erddap-restart", |b| b).await;
    let server = tabledap_server().await;
    create(&rt, &server, "bottles").await;
    let info_requests = |server: &FixtureServer| {
        server
            .requests()
            .iter()
            .filter(|r| r.contains("/info/"))
            .count()
    };
    let before = info_requests(&server);
    assert_eq!(before, 1);
    let rt = rt.restart().await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM bottles").await),
        62
    );
    assert_eq!(
        before,
        info_requests(&server),
        "the restart reads the pinned schema"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_shows_the_request_url() {
    let rt = common::runtime("erddap-explain").await;
    let server = tabledap_server().await;
    create(&rt, &server, "bottles").await;
    let plan = arrow::util::pretty::pretty_format_batches(
        &rt.sql("EXPLAIN SELECT cruise_id FROM bottles WHERE \"cast\" = 3")
            .await,
    )
    .unwrap()
    .to_string();
    assert!(plan.contains("ErddapExec"), "{plan}");
    assert!(plan.contains(&format!("{T}.parquet?cruise_id")), "{plan}");
}

/// Reads a real ERDDAP server. Run with `--ignored` when the network is up.
/// `BEACON_ERDDAP_URL` and `BEACON_ERDDAP_TABLEDAP_ID` point it at another server and dataset.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn live_coastwatch_tabledap() {
    let base = std::env::var("BEACON_ERDDAP_URL")
        .unwrap_or_else(|_| "https://coastwatch.pfeg.noaa.gov/erddap".to_string());
    let custom_id = std::env::var("BEACON_ERDDAP_TABLEDAP_ID").ok();
    let id = custom_id.as_deref().unwrap_or(T);
    let rt = common::runtime("erddap-live").await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE live_t STORED AS ERDDAP LOCATION '{base}/tabledap/{id}'"
    ))
    .await;
    // The time window fits the default dataset only.
    let filter = if custom_id.is_none() {
        " WHERE time >= TIMESTAMP '2002-05-30T00:00:00Z' AND time < TIMESTAMP '2002-05-31T00:00:00Z'"
    } else {
        ""
    };
    let n = scalar_i64(
        &rt.sql(&format!("SELECT count(*) FROM live_t{filter}"))
            .await,
    );
    assert!(n > 0, "no rows from {base}/tabledap/{id}");
}
