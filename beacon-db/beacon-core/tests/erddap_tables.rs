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

/// The request URLs of the ERDDAP scan in the plan of `sql`.
async fn request_urls(rt: &common::TestRuntime, sql: &str) -> String {
    let plan = arrow::util::pretty::pretty_format_batches(&rt.sql(&format!("EXPLAIN {sql}")).await)
        .unwrap()
        .to_string();
    let start = plan.find("urls=[").expect("an ErddapExec in the plan") + "urls=[".len();
    let end = start + plan[start..].find(']').unwrap();
    plan[start..end].to_string()
}

/// `s` with each `%XX` escape decoded.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            out.push(u8::from_str_radix(&s[i + 1..i + 3], 16).expect("a hex escape"));
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).expect("UTF-8")
}

/// Reads `beaconTest` from the local Docker ERDDAP in `beacon-erddap/docker`. Run with `--ignored`
/// and `BEACON_ERDDAP_URL=http://localhost:8089/erddap` after `docker compose up`.
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn local_erddap_pushdown_matches_expected() {
    let Ok(base) = std::env::var("BEACON_ERDDAP_URL") else {
        eprintln!("skipped: set BEACON_ERDDAP_URL to the local ERDDAP");
        return;
    };
    let rt = common::runtime("erddap-local").await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE t STORED AS ERDDAP LOCATION '{base}/tabledap/beaconTest'"
    ))
    .await;
    // Each case: the predicate, the expected count, and the decoded query after `?`.
    // A query with no `&` sends no constraint.
    let cases: &[(&str, i64, &str)] = &[
        // All 18 data rows of beacon_test.csv.
        ("TRUE", 18, "station"),
        // Only `St "A"`: f32 7.794 widens to 7.79400015, above the f64 literal.
        ("temp > 7.794", 1, "temp&temp>=7.794"),
        // Only `back\slash`: f32 0.7 widens to 0.69999999, below the f64 literal.
        ("temp < 0.7", 1, "temp&temp<=0.7"),
        // Only `St A` has no temp.
        ("temp IS NULL", 1, "temp&temp=NaN"),
        ("temp IS NOT NULL", 17, "temp&temp!=NaN"),
        // Three rows have depth 10, and `ab` has no depth.
        ("depth <> 10", 14, "depth&depth!=10"),
        // Depth 20 is on two rows; 40, 60 and 140 are on one row each.
        (
            "depth IN (20, 40, 60, 140)",
            5,
            "depth&depth>=20&depth<=140",
        ),
        // Only `ab`.
        ("depth IS NULL", 1, "depth&depth=NaN"),
        // A fractional bound on an integer column rounds outwards: depth 20 to 140.
        ("depth > 19.5", 14, "depth&depth>=19"),
        // Depth 10 on three rows and depth 20 on two rows.
        ("depth < 20.5", 5, "depth&depth<=21"),
        ("station = 'St \"A\"'", 1, r#"station&station="St \"A\"""#),
        (
            "station = 'back\\slash'",
            1,
            r#"station&station="back\\slash""#,
        ),
        // Metacharacters match only themselves, so `aXb|c` stays out.
        (
            "station IN ('a.b|c', 'St \"A\"', 'back\\slash', 'none')",
            3,
            r#"station&station=~"a\\.b\\|c|St \"A\"|back\\\\slash|none""#,
        ),
        // Only `a.b|c`: the `.` is literal in LIKE.
        ("station LIKE 'a.%'", 1, r#"station&station=~"(?s)a\\..*""#),
        // The empty station field is null in the parquet response. Text nulls stay local.
        ("station IS NULL", 1, "station"),
        // Only the first of the two rows 1 ms apart.
        (
            "time = TIMESTAMP '2020-01-01T00:00:00.123Z'",
            1,
            "time&time=2020-01-01T00:00:00.123Z",
        ),
        // No row is at this sub-millisecond instant.
        (
            "time = TIMESTAMP '2020-01-01T00:00:00.1235Z'",
            0,
            "time&time>=2020-01-01T00:00:00.123Z&time<=2020-01-01T00:00:00.124Z",
        ),
        // The row 1 ms earlier and the 1969 row.
        (
            "time < TIMESTAMP '2020-01-01T00:00:00.124Z'",
            2,
            "time&time<=2020-01-01T00:00:00.124Z",
        ),
        // The later 1 ms row, 2020-01-02 and 2020-01-03.
        (
            "time BETWEEN TIMESTAMP '2020-01-01T00:00:00.124Z' \
             AND TIMESTAMP '2020-01-03T00:00:00Z'",
            3,
            "time&time>=2020-01-01T00:00:00.124Z&time<=2020-01-03T00:00:00Z",
        ),
        // Only the 1969 row.
        (
            "time < TIMESTAMP '1970-01-01T00:00:00Z'",
            1,
            "time&time<=1970-01-01T00:00:00Z",
        ),
    ];
    for &(predicate, expected, query) in cases {
        let pushed_sql = format!("SELECT count(*) FROM t WHERE {predicate}");
        // A filter does not move below a limit, so ERDDAP returns all rows here.
        let local_sql =
            format!("SELECT count(*) FROM (SELECT * FROM t LIMIT 1000) AS l WHERE {predicate}");
        let pushed = scalar_i64(&rt.sql(&pushed_sql).await);
        let local = scalar_i64(&rt.sql(&local_sql).await);
        let url = request_urls(&rt, &pushed_sql).await;
        eprintln!("{predicate} | expected {expected} | pushed {pushed} | local {local} | {url}");
        let sent = percent_decode(url.split_once(".parquet?").expect("a query").1);
        assert_eq!(sent, query, "request of: {predicate}");
        assert!(
            !request_urls(&rt, &local_sql).await.contains('&'),
            "the local query sends no constraint"
        );
        assert_eq!(local, expected, "local filter: {predicate}");
        assert_eq!(pushed, expected, "pushdown: {predicate} -> {url}");
    }

    let limited = "SELECT count(*) FROM (SELECT * FROM t LIMIT 3) AS l";
    let url = request_urls(&rt, limited).await;
    eprintln!("LIMIT 3 | expected 3 | {url}");
    assert_eq!(scalar_i64(&rt.sql(limited).await), 3);
    assert!(!url.contains("orderBy"), "{url}");

    let rows = rt.sql("SELECT station, time FROM t ORDER BY time").await;
    assert_eq!(common::total_rows(&rows), 18);
    let stations = common::column_strings(&rows, 0);
    assert_eq!(stations[..3], ["back\\slash", "St \"A\"", "a.b|c"]);
    let times: Vec<i64> = rows
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<arrow::array::TimestampNanosecondArray>()
                .expect("a nanosecond time column")
                .values()
                .to_vec()
        })
        .collect();
    // 1969-07-20T20:17:40Z, then 2020-01-01T00:00:00.123Z and .124Z.
    assert_eq!(
        times[..3],
        [
            -14_182_940_000_000_000,
            1_577_836_800_123_000_000,
            1_577_836_800_124_000_000
        ]
    );
}
