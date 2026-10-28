//! Tests of the file format value types and their FFI forms.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use datafusion::arrow::array::{AsArray, BooleanArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, ScalarValue, Statistics};
use datafusion::error::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, stream};

use super::*;

pub(crate) fn two_columns() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]))
}

pub(crate) fn batch(values: &[i64]) -> RecordBatch {
    let names: Vec<String> = values.iter().map(|v| format!("x{v}")).collect();
    RecordBatch::try_new(
        two_columns(),
        vec![
            Arc::new(Int64Array::from(values.to_vec())),
            Arc::new(StringArray::from(names)),
        ],
    )
    .unwrap()
}

/// `a > value`, as a physical expression over [`two_columns`].
pub(crate) fn a_greater_than(value: i64) -> Arc<dyn PhysicalExpr> {
    Arc::new(BinaryExpr::new(
        Arc::new(Column::new("a", 0)),
        Operator::Gt,
        Arc::new(Literal::new(ScalarValue::Int64(Some(value)))),
    ))
}

fn eval_bool(expr: &Arc<dyn PhysicalExpr>, batch: &RecordBatch) -> BooleanArray {
    let value = expr.evaluate(batch).unwrap();
    value
        .into_array(batch.num_rows())
        .unwrap()
        .as_boolean()
        .clone()
}

#[test]
fn file_format_ffi_version_is_one() {
    assert_eq!(FILE_FORMAT_FFI_VERSION, 1);
    assert_eq!(file_format_ffi_version(), 1);
}

#[test]
fn output_kind_values() {
    assert_eq!(OutputKind::Flat.to_u32(), 0);
    assert_eq!(OutputKind::Nd.to_u32(), 1);
    assert_eq!(OutputKind::from_u32(0), Some(OutputKind::Flat));
    assert_eq!(OutputKind::from_u32(1), Some(OutputKind::Nd));
    assert_eq!(OutputKind::from_u32(2), None);
}

#[test]
fn arg_kind_values() {
    assert_eq!(ArgKind::String.to_u32(), 0);
    assert_eq!(ArgKind::Bool.to_u32(), 1);
    assert_eq!(ArgKind::StringList.to_u32(), 2);
    assert_eq!(ArgKind::from_u32(2), Some(ArgKind::StringList));
    assert_eq!(ArgKind::from_u32(3), None);
}

#[test]
fn arg_spec_round_trip() {
    let spec = ArgSpec {
        option_key: "read_dimensions".to_string(),
        kind: ArgKind::StringList,
    };
    let back = ArgSpec::try_from(&FFI_ArgSpec::from(&spec)).unwrap();
    assert_eq!(back, spec);
}

#[test]
fn arg_spec_with_unknown_kind_is_refused() {
    let mut ffi = FFI_ArgSpec::from(&ArgSpec {
        option_key: "x".to_string(),
        kind: ArgKind::Bool,
    });
    ffi.kind = 9;
    let error = ArgSpec::try_from(&ffi).unwrap_err();
    assert!(
        error.to_string().contains("unknown argument kind 9"),
        "{error}"
    );
}

#[test]
fn sort_key_round_trip() {
    let key = SortKey {
        column: "time".to_string(),
        descending: true,
        nulls_first: false,
    };
    assert_eq!(SortKey::from(&FFI_SortKey::from(&key)), key);
}

#[test]
fn scan_request_round_trip() {
    let request = ScanRequest {
        schema: two_columns(),
        filters: vec![a_greater_than(1)],
        limit: Some(10),
        batch_size: 8192,
    };
    let back = ScanRequest::from(FFI_ScanRequest::from(&request));
    assert_eq!(back.schema, request.schema);
    assert_eq!(back.limit, Some(10));
    assert_eq!(back.batch_size, 8192);
    assert_eq!(back.filters.len(), 1);
    let input = batch(&[0, 1, 2, 3]);
    assert_eq!(
        eval_bool(&back.filters[0], &input),
        eval_bool(&request.filters[0], &input)
    );
}

#[test]
fn scan_request_without_limit() {
    let request = ScanRequest {
        schema: two_columns(),
        filters: Vec::new(),
        limit: None,
        batch_size: 1,
    };
    let back = ScanRequest::from(FFI_ScanRequest::from(&request));
    assert_eq!(back.limit, None);
    assert!(back.filters.is_empty());
}

#[test]
fn write_request_round_trip() {
    for local_path in [None, Some(PathBuf::from("/tmp/out/a.exf"))] {
        let request = WriteRequest {
            path: object_store::path::Path::from("out/a.exf"),
            schema: two_columns(),
            local_path: local_path.clone(),
        };
        let back = WriteRequest::from(FFI_WriteRequest::from(&request));
        assert_eq!(back.path, request.path);
        assert_eq!(back.schema, request.schema);
        assert_eq!(back.local_path, local_path);
    }
}

#[test]
fn statistics_round_trip() {
    let statistics = Statistics {
        num_rows: Precision::Exact(3),
        total_byte_size: Precision::Absent,
        column_statistics: vec![ColumnStatistics {
            null_count: Precision::Exact(0),
            max_value: Precision::Exact(ScalarValue::Int64(Some(9))),
            min_value: Precision::Exact(ScalarValue::Int64(Some(1))),
            ..ColumnStatistics::new_unknown()
        }],
    };
    let bytes = encode_statistics(&statistics);
    assert_eq!(decode_statistics(&bytes).unwrap(), statistics);
}

#[test]
fn bad_statistics_bytes_are_an_error() {
    assert!(decode_statistics(&[0xff, 0xff, 0xff]).is_err());
}

/// Yields `first`, then panics on the next poll.
struct PanicAfterFirst {
    first: Option<RecordBatch>,
}

impl Stream for PanicAfterFirst {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.first.take() {
            Some(batch) => Poll::Ready(Some(Ok(batch))),
            None => panic!("boom in poll"),
        }
    }
}

impl RecordBatchStream for PanicAfterFirst {
    fn schema(&self) -> SchemaRef {
        two_columns()
    }
}

#[tokio::test]
async fn stream_round_trip() {
    let inner = stream::iter(vec![Ok(batch(&[1, 2])), Ok(batch(&[3]))]);
    let stream: SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(two_columns(), inner));
    let back = import_batch_stream(export_batch_stream(stream));
    assert_eq!(back.schema(), two_columns());
    let batches: Vec<RecordBatch> = back.map(|b| b.unwrap()).collect().await;
    assert_eq!(batches, vec![batch(&[1, 2]), batch(&[3])]);
}

#[tokio::test]
async fn stream_panic_becomes_error() {
    let stream: SendableRecordBatchStream = Box::pin(PanicAfterFirst {
        first: Some(batch(&[1])),
    });
    let mut back = import_batch_stream(export_batch_stream(stream));
    assert_eq!(back.next().await.unwrap().unwrap(), batch(&[1]));
    let error = back.next().await.unwrap().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("panic in plugin stream: boom in poll"),
        "{error}"
    );
    assert!(back.next().await.is_none());
}

/// Panics when it is dropped.
struct PanicOnDrop {
    dropped: Arc<AtomicBool>,
}

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
        panic!("boom in drop");
    }
}

#[tokio::test]
async fn stream_drop_panic_is_caught() {
    let dropped = Arc::new(AtomicBool::new(false));
    let guard = PanicOnDrop {
        dropped: dropped.clone(),
    };
    let inner = stream::iter(vec![Ok(batch(&[1]))]).map(move |b| {
        let _ = &guard;
        b
    });
    let stream: SendableRecordBatchStream =
        Box::pin(RecordBatchStreamAdapter::new(two_columns(), inner));
    let back = import_batch_stream(export_batch_stream(stream));
    drop(back);
    assert!(dropped.load(Ordering::SeqCst));
}
