//! Round trips of `FFI_FileFormat` through the forced foreign path.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{AsArray, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{Int64Type, SchemaRef};
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_ffi::get_library_marker_id;
use futures::{StreamExt, TryStreamExt, stream};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload};
use tokio::runtime::Handle;

use super::tests::{a_greater_than, batch, two_columns};
use super::*;

extern "C" fn foreign_marker_id() -> usize {
    get_library_marker_id() + 1
}

extern "C" fn other_version() -> u64 {
    2
}

/// A reader of `a,b` lines. It records the filters it gets and the `a` values it writes.
#[derive(Debug, Default)]
struct LineReader {
    filters_seen: Mutex<Vec<usize>>,
    written: Mutex<Vec<i64>>,
}

async fn read_lines(store: &Arc<dyn ObjectStore>, object: &ObjectMeta) -> Result<RecordBatch> {
    let bytes = store
        .get(&object.location)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let text = String::from_utf8_lossy(&bytes);
    let mut a = Vec::new();
    let mut b = Vec::new();
    for line in text.lines().filter(|l| !l.is_empty()) {
        let (x, y) = line.split_once(',').unwrap();
        a.push(x.parse::<i64>().unwrap());
        b.push(y.to_string());
    }
    Ok(RecordBatch::try_new(
        two_columns(),
        vec![
            Arc::new(Int64Array::from(a)),
            Arc::new(StringArray::from(b)),
        ],
    )?)
}

#[async_trait]
impl FormatReader for LineReader {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        Ok(two_columns())
    }

    async fn infer_stats(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        table_schema: SchemaRef,
    ) -> Result<Option<Statistics>> {
        let rows = read_lines(&store, object).await?.num_rows();
        let mut statistics = Statistics::new_unknown(&table_schema);
        statistics.num_rows = Precision::Exact(rows);
        Ok(Some(statistics))
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        self.filters_seen
            .lock()
            .unwrap()
            .push(request.filters.len());
        let batch = read_lines(&store, object).await?;
        for filter in &request.filters {
            filter.evaluate(&batch)?;
        }
        let schema = batch.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }

    fn write_ordering(&self, _: SchemaRef) -> Result<Vec<SortKey>> {
        Ok(vec![SortKey {
            column: "a".to_string(),
            descending: false,
            nulls_first: true,
        }])
    }

    async fn write(
        &self,
        store: Arc<dyn ObjectStore>,
        request: WriteRequest,
        input: SendableRecordBatchStream,
    ) -> Result<u64> {
        let batches: Vec<RecordBatch> = input.try_collect().await?;
        let mut text = String::new();
        let mut rows = 0;
        for batch in &batches {
            let a = batch.column(0).as_primitive::<Int64Type>();
            let b = batch.column(1).as_string::<i32>();
            for i in 0..batch.num_rows() {
                self.written.lock().unwrap().push(a.value(i));
                text.push_str(&format!("{},{}\n", a.value(i), b.value(i)));
                rows += 1;
            }
        }
        store
            .put(&request.path, PutPayload::from(text.into_bytes()))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(rows)
    }
}

/// A reader without a writer.
#[derive(Debug)]
struct ReadOnly;

#[async_trait]
impl FormatReader for ReadOnly {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        Ok(two_columns())
    }

    async fn open(
        &self,
        _: Arc<dyn ObjectStore>,
        _: &ObjectMeta,
        _: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            two_columns(),
            stream::empty(),
        )))
    }
}

/// A reader that panics in each call.
#[derive(Debug)]
struct Panics {
    in_future: bool,
}

#[async_trait]
impl FormatReader for Panics {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        panic!("infer_schema boom")
    }

    async fn infer_stats(
        &self,
        _: Arc<dyn ObjectStore>,
        _: &ObjectMeta,
        _: SchemaRef,
    ) -> Result<Option<Statistics>> {
        panic!("infer_stats boom")
    }

    async fn open(
        &self,
        _: Arc<dyn ObjectStore>,
        _: &ObjectMeta,
        _: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        if self.in_future {
            futures::future::ready(()).await;
        }
        panic!("open boom")
    }

    fn write_ordering(&self, _: SchemaRef) -> Result<Vec<SortKey>> {
        panic!("write_ordering boom")
    }

    async fn write(
        &self,
        _: Arc<dyn ObjectStore>,
        _: WriteRequest,
        _: SendableRecordBatchStream,
    ) -> Result<u64> {
        panic!("write boom")
    }
}

fn foreign(reader: Arc<dyn FormatReader>) -> Arc<dyn FormatReader> {
    let mut ffi = FFI_FileFormat::new(reader);
    ffi.library_marker_id = foreign_marker_id;
    ForeignFormatReader::try_new(ffi, Handle::current()).expect("import the reader")
}

async fn store_with(path: &str, text: &str) -> (Arc<dyn ObjectStore>, ObjectMeta) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from(path);
    store
        .put(&path, PutPayload::from(text.as_bytes().to_vec()))
        .await
        .unwrap();
    let meta = store.head(&path).await.unwrap();
    (store, meta)
}

fn empty_input() -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        two_columns(),
        stream::empty(),
    ))
}

fn request() -> ScanRequest {
    ScanRequest {
        schema: two_columns(),
        filters: vec![a_greater_than(1)],
        limit: None,
        batch_size: 8192,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_infer_schema_matches_local() {
    let (store, meta) = store_with("f.txt", "1,x\n").await;
    let reader = foreign(Arc::new(LineReader::default()));
    assert_eq!(
        reader.infer_schema(store, &meta).await.unwrap(),
        two_columns()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_open_returns_batches_and_sees_filters() {
    let (store, meta) = store_with("f.txt", "1,x1\n2,x2\n").await;
    let local = Arc::new(LineReader::default());
    let reader = foreign(local.clone());
    let stream = reader.open(store, &meta, request()).await.unwrap();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    assert_eq!(batches, vec![batch(&[1, 2])]);
    assert_eq!(*local.filters_seen.lock().unwrap(), vec![1]);
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_infer_stats_round_trip() {
    let (store, meta) = store_with("f.txt", "1,x\n2,y\n").await;
    let reader = foreign(Arc::new(LineReader::default()));
    let statistics = reader
        .infer_stats(store, &meta, two_columns())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(statistics.num_rows, Precision::Exact(2));
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_default_stats_are_none() {
    let (store, meta) = store_with("f.txt", "1,x\n").await;
    let reader = foreign(Arc::new(ReadOnly));
    assert!(
        reader
            .infer_stats(store, &meta, two_columns())
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_write_round_trip() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let local = Arc::new(LineReader::default());
    let reader = foreign(local.clone());
    let input: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
        two_columns(),
        stream::iter(vec![Ok(batch(&[1, 2])), Ok(batch(&[3]))]),
    ));
    let request = WriteRequest {
        path: Path::from("out/x.csv"),
        schema: two_columns(),
        local_path: None,
    };
    assert_eq!(
        reader.write(store.clone(), request, input).await.unwrap(),
        3
    );
    let text = store
        .get(&Path::from("out/x.csv"))
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    assert_eq!(text.as_ref(), b"1,x1\n2,x2\n3,x3\n");
    assert_eq!(*local.written.lock().unwrap(), vec![1, 2, 3]);
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_write_ordering_round_trip() {
    let reader = foreign(Arc::new(LineReader::default()));
    assert_eq!(
        reader.write_ordering(two_columns()).unwrap(),
        vec![SortKey {
            column: "a".to_string(),
            descending: false,
            nulls_first: true
        }]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn default_write_is_not_implemented() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let reader = foreign(Arc::new(ReadOnly));
    let request = WriteRequest {
        path: Path::from("x"),
        schema: two_columns(),
        local_path: None,
    };
    let error = reader
        .write(store, request, empty_input())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("cannot write"), "{error}");
    assert!(reader.write_ordering(two_columns()).unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn local_marker_returns_original_arc() {
    let reader: Arc<dyn FormatReader> = Arc::new(ReadOnly);
    let ffi = FFI_FileFormat::new(reader.clone());
    let back = ForeignFormatReader::try_new(ffi, Handle::current()).unwrap();
    assert!(Arc::ptr_eq(&reader, &back));
}

#[tokio::test(flavor = "multi_thread")]
async fn wrong_version_is_refused() {
    let mut ffi = FFI_FileFormat::new(Arc::new(ReadOnly));
    ffi.library_marker_id = foreign_marker_id;
    ffi.version = other_version;
    let error = ForeignFormatReader::try_new(ffi, Handle::current())
        .err()
        .expect("a refused version");
    assert!(
        error
            .to_string()
            .contains("file format FFI version 2 is not supported (want 6)"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn panics_become_errors() {
    for in_future in [false, true] {
        let (store, meta) = store_with("f.txt", "1,x\n").await;
        let reader = foreign(Arc::new(Panics { in_future }));
        let check = |error: DataFusionError, what: &str| {
            let text = error.to_string();
            assert!(text.contains("panic in file format call"), "{text}");
            assert!(text.contains(what), "{text}");
        };
        check(
            reader.infer_schema(store.clone(), &meta).await.unwrap_err(),
            "infer_schema boom",
        );
        check(
            reader
                .infer_stats(store.clone(), &meta, two_columns())
                .await
                .unwrap_err(),
            "infer_stats boom",
        );
        check(
            reader
                .open(store.clone(), &meta, request())
                .await
                .err()
                .unwrap(),
            "open boom",
        );
        check(
            reader.write_ordering(two_columns()).unwrap_err(),
            "write_ordering boom",
        );
        let write = WriteRequest {
            path: Path::from("x"),
            schema: two_columns(),
            local_path: None,
        };
        check(
            reader.write(store, write, empty_input()).await.unwrap_err(),
            "write boom",
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_stream_round_trip() {
    let (store, meta) = store_with("f.txt", "").await;
    let reader = foreign(Arc::new(ReadOnly));
    let stream = reader.open(store, &meta, request()).await.unwrap();
    assert_eq!(stream.schema(), two_columns());
    assert_eq!(stream.count().await, 0);
}
