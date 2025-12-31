//! MinIO webhook event handler.
//!
//! MinIO emits S3-compatible event notifications.
//! We translate those into our internal `ObjectEvent` representation.
//!
//! Production notes:
//! - Webhook payloads can vary slightly (single record vs `Records` array).
//! - We prefer best-effort parsing: malformed records are skipped with a warning,
//!   rather than rejecting the full payload.

use chrono::{DateTime, Utc};
use object_store::{ObjectMeta, path::Path};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::event::{EventHandler, ObjectEvent, ObjectEventResult};

pub type MinIOEvent = serde_json::Value; // Webhook payload from MinIO

#[derive(Debug, Deserialize)]
struct MinIORecord {
    #[serde(rename = "eventName")]
    event_name: String,

    #[serde(rename = "eventTime")]
    event_time: Option<String>,

    s3: MinIOS3,
}

#[derive(Debug, Deserialize)]
struct MinIOS3 {
    bucket: MinIOBucket,
    object: MinIOObject,
}

#[derive(Debug, Deserialize)]
struct MinIOBucket {
    name: String,
}

#[derive(Debug, Deserialize)]
struct MinIOObject {
    key: String,

    #[serde(default)]
    size: Option<u64>,

    #[serde(rename = "eTag")]
    #[serde(default)]
    e_tag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MinIOEventKind {
    Created,
    Deleted,
    Unsupported,
}

fn classify_event_name(event_name: &str) -> MinIOEventKind {
    // MinIO uses S3-compatible eventName values like:
    // - s3:ObjectCreated:Put
    // - s3:ObjectRemoved:Delete
    // Ignore access/replication/lifecycle/etc events.
    if event_name.contains("ObjectCreated") {
        MinIOEventKind::Created
    } else if event_name.contains("ObjectRemoved") {
        MinIOEventKind::Deleted
    } else {
        MinIOEventKind::Unsupported
    }
}

fn parse_event_time_rfc3339(event_time: Option<&str>) -> DateTime<Utc> {
    // ObjectMeta requires a timestamp; if the payload doesn't include one
    // (or it doesn't parse), fall back to now.
    event_time
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(Utc::now)
}

fn percent_decode_minio_key(input: &str) -> String {
    // S3 event notifications commonly URL-encode the key.
    // This is a small, dependency-free decoder that handles %XX and '+'.
    let bytes = input.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let h1 = bytes[i + 1];
                let h2 = bytes[i + 2];
                let from_hex = |c: u8| match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                };
                if let (Some(a), Some(b)) = (from_hex(h1), from_hex(h2)) {
                    out.push((a << 4) | b);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn extract_record_values(input: &serde_json::Value) -> Vec<serde_json::Value> {
    // MinIO commonly sends { "Records": [ ... ] }
    // Some installations might send a single record object.
    if let Some(records) = input.get("Records").and_then(|v| v.as_array()) {
        return records.to_vec();
    }

    if input.is_array() {
        return input.as_array().map(|a| a.to_vec()).unwrap_or_default();
    }

    vec![input.clone()]
}

pub struct MinIOEventHandler;

impl EventHandler<MinIOEvent> for MinIOEventHandler {
    fn handle_event(input: MinIOEvent) -> ObjectEventResult<Vec<ObjectEvent>> {
        let record_values = extract_record_values(&input);
        if record_values.is_empty() {
            debug!("minio webhook payload contained no records");
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(record_values.len());
        for record_value in record_values {
            let record: MinIORecord = match serde_json::from_value(record_value) {
                Ok(r) => r,
                Err(e) => {
                    warn!(error = %e, "failed to parse minio record; skipping");
                    continue;
                }
            };

            let bucket = record.s3.bucket.name;
            let decoded_key = percent_decode_minio_key(&record.s3.object.key);
            // object_store paths are relative; MinIO keys can sometimes be prefixed with '/'.
            let decoded_key = decoded_key.trim_start_matches('/');

            match classify_event_name(&record.event_name) {
                MinIOEventKind::Created => {
                    // MinIO can't reliably tell us whether this was a brand-new key or an overwrite.
                    // We map all ObjectCreated events to `Created`.
                    let meta = ObjectMeta {
                        location: Path::from(decoded_key),
                        size: record.s3.object.size.unwrap_or(0),
                        last_modified: parse_event_time_rfc3339(record.event_time.as_deref()),
                        e_tag: record.s3.object.e_tag,
                        version: None,
                    };
                    out.push(ObjectEvent::Created(meta));
                }
                MinIOEventKind::Deleted => {
                    out.push(ObjectEvent::Deleted(Path::from(decoded_key)));
                }
                MinIOEventKind::Unsupported => {
                    debug!(bucket = %bucket, event_name = %record.event_name, "ignoring unsupported minio event");
                }
            }
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Timelike};

    #[test]
    fn parses_object_created_record_into_object_event_created() {
        let payload = r#"{
                    "Records": [
                        {
                            "eventName": "s3:ObjectCreated:Put",
                            "eventTime": "2020-01-02T03:04:05.123Z",
                            "s3": {
                                "bucket": { "name": "my-bucket" },
                                "object": { "key": "a%2Fb%2Fc.txt", "size": 42, "eTag": "abc" }
                            }
                        }
                    ]
                }"#;

        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let events = MinIOEventHandler::handle_event(value).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            ObjectEvent::Created(meta) => {
                assert_eq!(meta.location.as_ref(), "a/b/c.txt");
                assert_eq!(meta.size, 42);
                assert_eq!(meta.e_tag.as_deref(), Some("abc"));
                let expected = Utc
                    .with_ymd_and_hms(2020, 1, 2, 3, 4, 5)
                    .unwrap()
                    .with_nanosecond(123_000_000)
                    .unwrap();
                assert_eq!(meta.last_modified.timestamp(), expected.timestamp());
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn parses_object_removed_record_into_object_event_deleted() {
        let payload = r#"{
                    "Records": [
                        {
                            "eventName": "s3:ObjectRemoved:Delete",
                            "eventTime": "2020-01-02T03:04:05Z",
                            "s3": {
                                "bucket": { "name": "my-bucket" },
                                "object": { "key": "folder%2Ffile%20name.txt" }
                            }
                        }
                    ]
                }"#;

        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let events = MinIOEventHandler::handle_event(value).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            ObjectEvent::Deleted(path) => {
                assert_eq!(path.as_ref(), "folder/file name.txt");
            }
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn skips_invalid_records_and_processes_valid_ones() {
        // First record is malformed (missing s3), second is valid.
        let payload = r#"{
                    "Records": [
                        { "eventName": "s3:ObjectCreated:Put" },
                        {
                            "eventName": "s3:ObjectRemoved:Delete",
                            "eventTime": "2020-01-02T03:04:05Z",
                            "s3": {
                                "bucket": { "name": "my-bucket" },
                                "object": { "key": "/ok%2Ffile.txt" }
                            }
                        }
                    ]
                }"#;

        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let events = MinIOEventHandler::handle_event(value).unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            ObjectEvent::Deleted(path) => assert_eq!(path.as_ref(), "ok/file.txt"),
            _ => panic!("unexpected event"),
        }
    }

    #[test]
    fn ignores_unsupported_events() {
        let payload = r#"{
                    "Records": [
                        {
                            "eventName": "s3:ObjectAccessed:Get",
                            "eventTime": "2020-01-02T03:04:05Z",
                            "s3": {
                                "bucket": { "name": "my-bucket" },
                                "object": { "key": "a%2Fb.txt" }
                            }
                        }
                    ]
                }"#;

        let value: serde_json::Value = serde_json::from_str(payload).unwrap();
        let events = MinIOEventHandler::handle_event(value).unwrap();
        assert!(events.is_empty());
    }
}
