//! An external Lance table on a second S3 store, with a `PERSISTENT SECRET`
//! for that store. The secret loads before the tables at startup, so the table
//! reads its rows after a restart.
//!
//! The second store is a small S3 server in this process. It knows only the
//! requests that Lance and `object_store` send for these statements.

mod common;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use axum::body::{Body, Bytes};
use axum::extract::{Request, State};
use axum::http::{header, HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;
use common::scalar_i64;

const BUCKET: &str = "other";
const LAST_MODIFIED: &str = "Mon, 01 Jan 2024 00:00:00 GMT";
const LISTED_LAST_MODIFIED: &str = "2024-01-01T00:00:00.000Z";

/// The objects of the bucket: key to (bytes, ETag number).
#[derive(Clone, Default)]
struct Bucket {
    objects: Arc<Mutex<BTreeMap<String, (Bytes, u64)>>>,
    next_etag: Arc<Mutex<u64>>,
}

/// Start the S3 server on a free local port and return its endpoint.
async fn start_s3() -> String {
    let app = Router::new().fallback(handle).with_state(Bucket::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the S3 server");
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("S3 server");
    });
    format!("http://{address}")
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
            out.push(u8::from_str_radix(hex, 16).unwrap());
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap()
}

async fn handle(State(bucket): State<Bucket>, request: Request) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let headers = request.headers().clone();
    let body = axum::body::to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap();

    let path = percent_decode(uri.path().trim_start_matches('/'));
    let (name, key) = path.split_once('/').unwrap_or((path.as_str(), ""));
    if name != BUCKET {
        return StatusCode::NOT_FOUND.into_response();
    }
    let query: HashMap<String, String> =
        url::form_urlencoded::parse(uri.query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();

    match method {
        Method::GET if key.is_empty() && query.contains_key("list-type") => list(&bucket, &query),
        Method::GET | Method::HEAD => get(&bucket, key, &headers),
        Method::PUT => put(&bucket, key, &headers, body),
        Method::DELETE => {
            bucket.objects.lock().unwrap().remove(key);
            StatusCode::NO_CONTENT.into_response()
        }
        _ => StatusCode::NOT_IMPLEMENTED.into_response(),
    }
}

fn put(bucket: &Bucket, key: &str, headers: &HeaderMap, body: Bytes) -> Response {
    let mut objects = bucket.objects.lock().unwrap();
    // Lance commits a version with `If-None-Match: *`.
    if headers.get(header::IF_NONE_MATCH).is_some() && objects.contains_key(key) {
        return StatusCode::PRECONDITION_FAILED.into_response();
    }
    let etag = {
        let mut next = bucket.next_etag.lock().unwrap();
        *next += 1;
        *next
    };
    objects.insert(key.to_string(), (body, etag));
    (StatusCode::OK, [(header::ETAG, format!("\"{etag}\""))]).into_response()
}

fn get(bucket: &Bucket, key: &str, headers: &HeaderMap) -> Response {
    let Some((data, etag)) = bucket.objects.lock().unwrap().get(key).cloned() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let total = data.len();
    let range = headers
        .get(header::RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("bytes="))
        .map(|range| {
            let (start, end) = range.split_once('-').unwrap();
            match (start.parse::<usize>(), end.parse::<usize>()) {
                (Ok(start), Ok(end)) => (start, end.min(total - 1)),
                (Ok(start), Err(_)) => (start, total - 1),
                (Err(_), Ok(suffix)) => (total - suffix.min(total), total - 1),
                _ => panic!("bad range {range}"),
            }
        });
    let builder = Response::builder()
        .header(header::ETAG, format!("\"{etag}\""))
        .header(header::LAST_MODIFIED, LAST_MODIFIED);
    match range {
        Some((start, end)) => builder
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::CONTENT_LENGTH, end + 1 - start)
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{total}"),
            )
            .body(Body::from(data.slice(start..end + 1))),
        None => builder
            .status(StatusCode::OK)
            .header(header::CONTENT_LENGTH, total)
            .body(Body::from(data)),
    }
    .unwrap()
}

fn list(bucket: &Bucket, query: &HashMap<String, String>) -> Response {
    let prefix = query.get("prefix").cloned().unwrap_or_default();
    let delimiter = query.get("delimiter").filter(|d| !d.is_empty());
    let start_after = query.get("start-after").cloned().unwrap_or_default();

    let mut contents = String::new();
    let mut prefixes = std::collections::BTreeSet::new();
    for (key, (data, etag)) in bucket.objects.lock().unwrap().iter() {
        if !key.starts_with(&prefix) || key.as_str() <= start_after.as_str() {
            continue;
        }
        let rest = &key[prefix.len()..];
        if let Some((delimiter, at)) =
            delimiter.and_then(|d| rest.find(d.as_str()).map(|at| (d, at)))
        {
            prefixes.insert(key[..prefix.len() + at + delimiter.len()].to_string());
            continue;
        }
        contents.push_str(&format!(
            "<Contents><Key>{key}</Key><LastModified>{LISTED_LAST_MODIFIED}</LastModified>\
             <ETag>\"{etag}\"</ETag><Size>{}</Size></Contents>",
            data.len()
        ));
    }
    let common: String = prefixes
        .iter()
        .map(|p| format!("<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>"))
        .collect();
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <ListBucketResult><IsTruncated>false</IsTruncated>{contents}{common}</ListBucketResult>"
    );
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/xml")],
        xml,
    )
        .into_response()
}

#[tokio::test(flavor = "multi_thread")]
async fn persistent_secret_reaches_a_second_store_after_restart() {
    let endpoint = start_s3().await;
    let rt =
        common::restartable_runtime("lance-ext-secret", |b| b.with_secrets_encryption([7u8; 32]))
            .await;

    rt.sql(&format!(
        "CREATE PERSISTENT SECRET other (TYPE S3, KEY_ID 'test-key', SECRET 'test-secret', \
         REGION 'us-east-1', ENDPOINT '{endpoint}', 'allow_http' 'true', SCOPE 's3://{BUCKET}')"
    ))
    .await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE t (id BIGINT) STORED AS LANCE LOCATION 's3://{BUCKET}/ext.lance'"
    ))
    .await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM t").await), 2);

    let rt = rt.restart().await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM t").await), 2);
    rt.sql("INSERT INTO t VALUES (3)").await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM t").await), 3);
}
