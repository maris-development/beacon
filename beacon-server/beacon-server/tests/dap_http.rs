//! HTTP tests for the DAP2 endpoint (`/dap/{table}.dds|.das|.dods|.csv|.nc`), driven
//! through the real router with `tower::ServiceExt::oneshot`.

mod common;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, HeaderMap, Request, StatusCode},
    Router,
};
use common::{basic, config};
use serde_json::json;
use tower::ServiceExt;

use beacon_server::axum::setup_router;

async fn router_with(config: beacon_server_config::Config) -> (Router, common::TestServer, String) {
    let harness = common::server_with(config).await;
    let cfg = harness.server.config().clone();
    let admin = basic(&cfg.admin.username, &cfg.admin.password);
    let router = setup_router(harness.server.clone(), cfg).unwrap();
    (router, harness, admin)
}

async fn admin_sql(router: &Router, admin: &str, sql: &str) {
    let request = Request::builder()
        .method("POST")
        .uri("/api/query")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, admin)
        .body(Body::from(json!({ "sql": sql }).to_string()))
        .unwrap();
    let res = router.clone().oneshot(request).await.unwrap();
    let status = res.status();
    let body = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    assert_eq!(
        status,
        StatusCode::OK,
        "{sql}: {}",
        String::from_utf8_lossy(&body)
    );
}

/// GET `uri`. Returns the status, the headers and the body.
async fn get(router: &Router, uri: &str, auth: Option<&str>) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    let res = router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let headers = res.headers().clone();
    let body = to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, headers, body)
}

/// The table `obs`: time, numeric, boolean and text columns, comments and one null row.
async fn obs_table(router: &Router, admin: &str) {
    for sql in [
        "CREATE TABLE obs (id BIGINT, t TIMESTAMP, depth DOUBLE, temp REAL, ok BOOLEAN, platform VARCHAR)",
        "INSERT INTO obs VALUES \
         (1, TIMESTAMP '2024-01-01 00:00:00', 5.0, 10.5, true, 'SHIP'), \
         (2, TIMESTAMP '2024-01-02 00:00:00', 50.0, 4.25, false, 'BUOY'), \
         (3, NULL, NULL, NULL, NULL, NULL)",
        "COMMENT ON TABLE obs IS 'test observations'",
        "COMMENT ON COLUMN obs.depth IS 'depth in meters'",
    ] {
        admin_sql(router, admin, sql).await;
    }
}

fn text(body: Vec<u8>) -> String {
    String::from_utf8(body).expect("UTF-8 body")
}

#[tokio::test(flavor = "multi_thread")]
async fn dds_describes_the_table_as_one_sequence() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.dds", None).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-description"], "dods-dds");
    assert_eq!(headers["xdods-server"], "dods/3.7");
    assert_eq!(
        text(body),
        "Dataset {\n    Sequence {\n        Float64 id;\n        Float64 t;\n        Float64 depth;\n        Float32 temp;\n        Int32 ok;\n        String platform;\n    } s;\n} obs;\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dds_lists_only_the_projected_columns_and_ignores_selections() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(
        &router,
        "/dap/obs.dds?s.platform,s.depth&s.depth%3E10",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        text(body),
        "Dataset {\n    Sequence {\n        Float64 depth;\n        String platform;\n    } s;\n} obs;\n"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn das_gives_fill_values_comments_and_time_units() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.das", None).await;
    let body = text(body);

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-description"], "dods-das");
    for part in [
        "        depth {\n            Float64 _FillValue NaN;\n            String comment \"depth in meters\";\n        }\n",
        "        t {\n            Float64 _FillValue NaN;\n            String units \"seconds since 1970-01-01T00:00:00Z\";\n        }\n",
        "        ok {\n            Int32 _FillValue 2147483647;\n        }\n",
        "        String summary \"test observations\";\n",
    ] {
        assert!(body.contains(part), "missing {part:?} in:\n{body}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_table_is_a_dap2_404() {
    let (router, _harness, _admin) = router_with(config(false)).await;

    let (status, headers, body) = get(&router, "/dap/nope.dds", None).await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(headers["content-description"], "dods-error");
    assert!(text(body).starts_with("Error {\n    code = 404;"));
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_column_is_a_dap2_400() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(&router, "/dap/obs.dds?nope", None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text(body).contains("unknown column"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_without_a_grant_is_a_404() {
    let (router, _harness, admin) = router_with(config(true)).await;
    obs_table(&router, &admin).await;

    // The anonymous user has no grant on `obs`.
    assert_eq!(
        get(&router, "/dap/obs.dds", None).await.0,
        StatusCode::NOT_FOUND
    );

    for sql in [
        "CREATE ROLE reader",
        "GRANT SELECT ON TABLE obs TO ROLE reader",
        "CREATE USER alice WITH PASSWORD 'pw'",
        "GRANT ROLE reader TO USER alice",
    ] {
        admin_sql(&router, &admin, sql).await;
    }
    let alice = basic("alice", "pw");
    assert_eq!(
        get(&router, "/dap/obs.dds", Some(&alice)).await.0,
        StatusCode::OK
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn no_credentials_without_anonymous_access_is_a_basic_challenge() {
    let mut config = config(false);
    config.auth.anonymous_enabled = false;
    let (router, _harness, _admin) = router_with(config).await;

    let (status, headers, body) = get(&router, "/dap/obs.dds", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(headers[header::WWW_AUTHENTICATE], "Basic realm=\"beacon\"");
    assert_eq!(headers["content-description"], "dods-error");
    assert!(text(body).starts_with("Error {\n    code = 401;"));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_index_page_links_the_responses() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs", None).await;
    let body = text(body);

    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["xdods-server"], "dods/3.7");
    assert!(headers["xopendap-server"]
        .to_str()
        .unwrap()
        .starts_with("beacon/"));
    assert!(
        body.contains("obs.dds")
            && body.contains("obs.das")
            && body.contains("obs.dods")
            && body.contains("obs.csv")
            && body.contains("obs.nc"),
        "{body}"
    );
}

/// A decoded DAP2 value.
#[derive(Debug, Clone, PartialEq)]
enum V {
    I(i32),
    F32(f32),
    F64(f64),
    S(String),
}

/// Splits a `.dods` body at `Data:\n` and decodes the rows. `types` gives the DAP2 type of
/// each column: `i` Int32, `f` Float32, `d` Float64, `s` String.
fn decode_dods(body: &[u8], types: &str) -> (String, Vec<Vec<V>>) {
    let marker = b"\nData:\n";
    let at = body
        .windows(marker.len())
        .position(|window| window == marker)
        .expect("Data: marker");
    let dds = String::from_utf8(body[..at + 1].to_vec()).unwrap();
    let mut rest = &body[at + marker.len()..];
    let mut rows = Vec::new();
    loop {
        let (tag, tail) = rest.split_at(4);
        rest = tail;
        if tag == [0xA5, 0, 0, 0] {
            assert!(rest.is_empty(), "bytes after the end marker");
            break;
        }
        assert_eq!(tag, [0x5A, 0, 0, 0], "row marker");
        let mut row = Vec::new();
        for kind in types.chars() {
            let value = match kind {
                'i' => {
                    let value = i32::from_be_bytes(rest[..4].try_into().unwrap());
                    rest = &rest[4..];
                    V::I(value)
                }
                'f' => {
                    let value = f32::from_be_bytes(rest[..4].try_into().unwrap());
                    rest = &rest[4..];
                    V::F32(value)
                }
                'd' => {
                    let value = f64::from_be_bytes(rest[..8].try_into().unwrap());
                    rest = &rest[8..];
                    V::F64(value)
                }
                's' => {
                    let length = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
                    let padded = length.div_ceil(4) * 4;
                    let value = String::from_utf8(rest[4..4 + length].to_vec()).unwrap();
                    rest = &rest[4 + padded..];
                    V::S(value)
                }
                other => panic!("unknown type {other}"),
            };
            row.push(value);
        }
        rows.push(row);
    }
    (dds, rows)
}

#[tokio::test(flavor = "multi_thread")]
async fn dods_returns_the_rows_that_match() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.dods?id,platform&depth%3E10", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers["content-description"], "dods-data");
    assert_eq!(headers[header::CONTENT_TYPE], "application/octet-stream");
    let (dds, rows) = decode_dods(&body, "ds");
    assert!(
        dds.contains("Float64 id;") && dds.contains("String platform;"),
        "{dds}"
    );
    assert_eq!(rows, vec![vec![V::F64(2.0), V::S("BUOY".into())]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn dods_accepts_the_sequence_prefix_and_time_seconds() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    // 1704153600 is 2024-01-02T00:00:00Z. The projection keeps the schema order: id, t.
    let (status, _, body) = get(&router, "/dap/obs.dods?s.t,s.id&s.t%3E=1704153600", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let (_, rows) = decode_dods(&body, "dd");
    assert_eq!(rows, vec![vec![V::F64(2.0), V::F64(1_704_153_600.0)]]);
}

#[tokio::test(flavor = "multi_thread")]
async fn dods_regex_is_a_full_match() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let platforms = |uri: &'static str| {
        let router = router.clone();
        async move {
            let (status, _, body) = get(&router, uri, None).await;
            assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
            let mut names: Vec<String> = decode_dods(&body, "s")
                .1
                .into_iter()
                .map(|row| match &row[0] {
                    V::S(name) => name.clone(),
                    other => panic!("{other:?}"),
                })
                .collect();
            names.sort();
            names
        }
    };

    assert_eq!(
        platforms("/dap/obs.dods?platform&platform=~%22B.*%22").await,
        ["BUOY"]
    );
    assert!(platforms("/dap/obs.dods?platform&platform=~%22UO%22")
        .await
        .is_empty());
    assert_eq!(
        platforms("/dap/obs.dods?platform&platform=~%22SHIP%7CBUOY%22").await,
        ["BUOY", "SHIP"]
    );
    assert!(
        platforms("/dap/obs.dods?platform&platform=~%22HIP%7CBUO%22")
            .await
            .is_empty()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dods_sends_fill_values_for_nulls() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(
        &router,
        "/dap/obs.dods?id,depth,temp,ok,platform&id=3",
        None,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let (_, rows) = decode_dods(&body, "ddfis");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row[0], V::F64(3.0));
    assert!(matches!(row[1], V::F64(value) if value.is_nan()));
    assert!(matches!(row[2], V::F32(value) if value.is_nan()));
    assert_eq!(row[3], V::I(i32::MAX));
    assert_eq!(row[4], V::S(String::new()));
}

#[tokio::test(flavor = "multi_thread")]
async fn dods_with_no_match_is_only_the_end_marker() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(&router, "/dap/obs.dods?id&id%3E100", None).await;

    assert_eq!(status, StatusCode::OK);
    let (_, rows) = decode_dods(&body, "d");
    assert!(rows.is_empty());
    assert!(body.ends_with(&[0xA5, 0, 0, 0]));
}

#[tokio::test(flavor = "multi_thread")]
async fn dods_rejects_a_bad_constraint_before_any_data() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.dods?id&depth%3E%22x%22", None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(headers["content-description"], "dods-error");
    assert!(text(body).contains("must be a number"));
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_returns_the_subset_as_a_file() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.csv?id,platform&depth%3E10", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "text/csv");
    assert!(headers[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap()
        .contains("obs.csv"));
    let body = text(body);
    assert_eq!(body.lines().collect::<Vec<_>>(), ["id,platform", "2,BUOY"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn nc_returns_a_netcdf_file() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, headers, body) = get(&router, "/dap/obs.nc?id,depth", None).await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(headers[header::CONTENT_TYPE], "application/x-netcdf");
    assert!(
        body.starts_with(b"\x89HDF\r\n\x1a\n") || body.starts_with(b"CDF"),
        "not a NetCDF file"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_rejects_a_bad_constraint() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    let (status, _, body) = get(&router, "/dap/obs.csv?nope", None).await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(text(body).contains("unknown column"));
}

/// The ids of the rows in a `.dods` body whose first column is a Float64 id.
async fn dods_ids(router: &Router, uri: &str) -> Vec<f64> {
    let (status, _, body) = get(router, uri, None).await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    decode_dods(&body, "d")
        .1
        .into_iter()
        .map(|row| match row[0] {
            V::F64(id) => id,
            ref other => panic!("{other:?}"),
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dotted_column_is_served_and_filtered() {
    let (router, harness, admin) = router_with(config(false)).await;
    // A managed Lance table rejects a `.` in a column name, so the table is a CSV file.
    let folder = harness.server.config().data.datasets.join("dotted");
    std::fs::create_dir_all(&folder).unwrap();
    std::fs::write(
        folder.join("a.csv"),
        "id,temp.units
1,degC
2,K
",
    )
    .unwrap();
    admin_sql(
        &router,
        &admin,
        "CREATE EXTERNAL TABLE dotted STORED AS CSV LOCATION 'dotted/'",
    )
    .await;

    let (status, _, body) = get(&router, "/dap/dotted.dds", None).await;
    assert_eq!(status, StatusCode::OK);
    assert!(text(body).contains("String temp%2Eunits;"));

    // pydap sends a single-encoded name, the same as the DDS shows it.
    let (status, _, body) = get(
        &router,
        "/dap/dotted.dods?id,s%2Etemp%2Eunits&s%2Etemp%2Eunits=%22degC%22",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        decode_dods(&body, "ds").1,
        vec![vec![V::F64(1.0), V::S("degC".into())]]
    );

    // The plain name and the double-encoded name resolve to the same column.
    for uri in [
        "/dap/dotted.dods?id&temp.units=%22K%22",
        "/dap/dotted.dods?id&temp%252Eunits=%22K%22",
    ] {
        assert_eq!(dods_ids(&router, uri).await, [2.0], "{uri}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_nan_row_does_not_match_an_ordering_comparison() {
    let (router, _harness, admin) = router_with(config(false)).await;
    for sql in [
        "CREATE TABLE nan_obs (id BIGINT, v DOUBLE)",
        "INSERT INTO nan_obs VALUES (1, 5.0), (2, CAST('NaN' AS DOUBLE))",
    ] {
        admin_sql(&router, &admin, sql).await;
    }

    for uri in [
        "/dap/nan_obs.dods?id&v%3E3",
        "/dap/nan_obs.dods?id&v%3E=3",
        "/dap/nan_obs.dods?id&v%3C=10",
        "/dap/nan_obs.dods?id&v%3C10",
    ] {
        assert_eq!(dods_ids(&router, uri).await, [1.0], "{uri}");
    }
    assert_eq!(dods_ids(&router, "/dap/nan_obs.dods?id&v=NaN").await, [2.0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mixed_case_table_name_is_found() {
    let (router, _harness, admin) = router_with(config(false)).await;
    for sql in [
        "CREATE TABLE \"MixedCase\" (id BIGINT)",
        "INSERT INTO \"MixedCase\" VALUES (7)",
    ] {
        admin_sql(&router, &admin, sql).await;
    }

    assert_eq!(
        get(&router, "/dap/MixedCase.dds", None).await.0,
        StatusCode::OK
    );
    assert_eq!(dods_ids(&router, "/dap/MixedCase.dods?id").await, [7.0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn boolean_and_float32_columns_can_be_filtered() {
    let (router, _harness, admin) = router_with(config(false)).await;
    obs_table(&router, &admin).await;

    assert_eq!(dods_ids(&router, "/dap/obs.dods?id&ok=1").await, [1.0]);
    assert_eq!(dods_ids(&router, "/dap/obs.dods?id&ok=0").await, [2.0]);
    assert_eq!(dods_ids(&router, "/dap/obs.dods?id&temp=10.5").await, [1.0]);
    assert_eq!(dods_ids(&router, "/dap/obs.dods?id&temp%3E5").await, [1.0]);
}
