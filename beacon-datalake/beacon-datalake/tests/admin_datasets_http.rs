//! HTTP-transport integration tests for the admin dataset file-management
//! endpoints (upload / download / delete), driven through the real router with
//! `tower::ServiceExt::oneshot`. Storage is rooted at a temp directory so the
//! tests never touch the developer's `./data`.

mod common;

use std::sync::Arc;

use ::axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
    Router,
};
use common::basic;
use tower::ServiceExt;

use beacon_datalake::axum::setup_router;

/// Test config with a deliberately small upload cap so the `413` path is
/// exercisable. The datasets/tmp/tables paths come from the ephemeral lake, so
/// only the upload cap (and the shared auth/sql defaults) need setting here.
fn temp_config(max_upload_bytes: u64) -> beacon_datalake_config::Config {
    let mut config = common::config(false);
    config.server.max_upload_bytes = max_upload_bytes;
    config
}

/// Opens an ephemeral lake from `config` and returns it with the `Arc<Config>`
/// the router should use — the lake's own config, which carries the upload cap
/// and the ephemeral temp paths.
async fn lake(
    config: beacon_datalake_config::Config,
) -> (common::TestLake, Arc<beacon_datalake_config::Config>) {
    let lake = common::lake_with(config).await;
    let cfg = lake.lake.config().clone();
    (lake, cfg)
}

fn request(method: &str, uri: &str, auth: Option<&str>, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(auth) = auth {
        builder = builder.header(header::AUTHORIZATION, auth);
    }
    builder.body(body).unwrap()
}

async fn send(router: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let res = router.clone().oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_download_delete_round_trip_is_super_user_only() {
    let (lake, config) = lake(temp_config(1024)).await;
    let router = setup_router(lake.lake.clone(), config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    let upload_uri = "/api/admin/datasets/upload?path=ctd/a.parquet";

    // No credentials → 401, and nothing is written.
    let (status, _) = send(
        &router,
        request("POST", upload_uri, None, Body::from("hello world")),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Admin upload → 200 with the reported size.
    let (status, body) = send(
        &router,
        request("POST", upload_uri, Some(&admin), Body::from("hello world")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(parsed["path"], "ctd/a.parquet");
    assert_eq!(parsed["size"], 11);

    // Re-upload without overwrite → 409.
    let (status, _) = send(
        &router,
        request("POST", upload_uri, Some(&admin), Body::from("again")),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Overwrite succeeds.
    let (status, _) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload?path=ctd/a.parquet&overwrite=true",
            Some(&admin),
            Body::from("replaced!"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Download returns the (replaced) bytes.
    let (status, body) = send(
        &router,
        request(
            "GET",
            "/api/admin/datasets/download?path=ctd/a.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"replaced!");

    // Delete → 204, then download → 404.
    let (status, _) = send(
        &router,
        request(
            "DELETE",
            "/api/admin/datasets?path=ctd/a.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, _) = send(
        &router,
        request(
            "GET",
            "/api/admin/datasets/download?path=ctd/a.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn upload_rejects_traversal_extension_and_oversize() {
    let (lake, config) = lake(temp_config(8)).await; // 8-byte cap
    let router = setup_router(lake.lake.clone(), config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    // Path traversal → 400.
    let (status, _) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload?path=../evil.parquet",
            Some(&admin),
            Body::from("x"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // The Beacon-internal prefix is reserved → 400.
    let (status, _) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload?path=__beacon__/x.parquet",
            Some(&admin),
            Body::from("x"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Any extension is accepted — the upload path no longer inspects it — so an
    // `.exe` within the size cap is stored like any other file.
    let (status, _) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload?path=a/b.exe",
            Some(&admin),
            Body::from("x"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Over the 8-byte cap → 413, and nothing is left behind.
    let (status, _) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload?path=big.parquet",
            Some(&admin),
            Body::from("way too many bytes"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);

    let (status, _) = send(
        &router,
        request(
            "GET",
            "/api/admin/datasets/download?path=big.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn chunked_upload_round_trip_and_download() {
    let (lake, config) = lake(temp_config(1024)).await;
    let router = setup_router(lake.lake.clone(), config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    // Initiate → 200 with an upload_id.
    let (status, body) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload/initiate?path=big/multi.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let initiated: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let upload_id = initiated["upload_id"].as_str().unwrap().to_string();
    assert!(initiated["part_size"].as_u64().unwrap() >= 5 * 1024 * 1024);

    // Two parts, in order.
    for (n, payload) in [(1, "part-one-"), (2, "part-two")] {
        let (status, _) = send(
            &router,
            request(
                "PUT",
                &format!("/api/admin/datasets/upload/part?upload_id={upload_id}&part_number={n}"),
                Some(&admin),
                Body::from(payload),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
    }

    // Complete → 200 with the assembled size.
    let (status, body) = send(
        &router,
        request(
            "POST",
            &format!("/api/admin/datasets/upload/complete?upload_id={upload_id}"),
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let completed: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(completed["path"], "big/multi.parquet");
    assert_eq!(completed["size"], "part-one-part-two".len());

    // Download returns the concatenated parts.
    let (status, body) = send(
        &router,
        request(
            "GET",
            "/api/admin/datasets/download?path=big/multi.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, b"part-one-part-two");
}

#[tokio::test(flavor = "multi_thread")]
async fn chunked_upload_rejects_unknown_session_and_out_of_order() {
    let (lake, config) = lake(temp_config(1024)).await;
    let router = setup_router(lake.lake.clone(), config.clone()).unwrap();
    let admin = basic(&config.admin.username, &config.admin.password);

    // A part for an unknown session → 404.
    let bogus = uuid::Uuid::new_v4();
    let (status, _) = send(
        &router,
        request(
            "PUT",
            &format!("/api/admin/datasets/upload/part?upload_id={bogus}&part_number=1"),
            Some(&admin),
            Body::from("x"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Initiate a real session, then skip a part → 409.
    let (_, body) = send(
        &router,
        request(
            "POST",
            "/api/admin/datasets/upload/initiate?path=gap.parquet",
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    let upload_id = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["upload_id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, _) = send(
        &router,
        request(
            "PUT",
            &format!("/api/admin/datasets/upload/part?upload_id={upload_id}&part_number=2"),
            Some(&admin),
            Body::from("x"),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Abort cleans it up → 204.
    let (status, _) = send(
        &router,
        request(
            "DELETE",
            &format!("/api/admin/datasets/upload?upload_id={upload_id}"),
            Some(&admin),
            Body::empty(),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}
