//! Two small file formats for the loader tests.
//!
//! - `exflat` (`.exf`): one `a,b` row per line; `a` is an `Int64`, `b` a `Utf8`. It also writes.
//! - `exgrid` (`.exg`): line 1 `rows,cols`, line 2 `rows * cols` values in C order. The
//!   columns are `row` and `col` (the coordinates) and `v` over both, as nd arrays.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use beacon_plugin_sdk::datafusion::arrow::array::{
    Float64Array, Int64Array, RecordBatch, StringArray,
};
use beacon_plugin_sdk::datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_plugin_sdk::datafusion::arrow::util::display::array_value_to_string;
use beacon_plugin_sdk::datafusion::common::Statistics;
use beacon_plugin_sdk::datafusion::common::stats::Precision;
use beacon_plugin_sdk::datafusion::error::{DataFusionError, Result};
use beacon_plugin_sdk::datafusion::execution::SendableRecordBatchStream;
use beacon_plugin_sdk::datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use beacon_plugin_sdk::file_format::{
    FormatFactory, FormatReader, OutputKind, ScanRequest, WriteRequest,
};
use beacon_plugin_sdk::nd::encoding::encode_nd_record_batch;
use beacon_plugin_sdk::nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch};
use beacon_plugin_sdk::object_store::store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use futures::{StreamExt, stream};

async fn read_text(store: &Arc<dyn ObjectStore>, object: &ObjectMeta) -> Result<String> {
    let bytes = store
        .get(&object.location)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?
        .bytes()
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    Ok(String::from_utf8_lossy(&bytes).to_string())
}

fn one_batch(batch: RecordBatch) -> SendableRecordBatchStream {
    Box::pin(RecordBatchStreamAdapter::new(
        batch.schema(),
        stream::iter(vec![Ok(batch)]),
    ))
}

#[derive(Debug)]
struct ExFlat;

impl FormatFactory for ExFlat {
    fn name(&self) -> &str {
        "exflat"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["exf".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Flat
    }

    fn can_write(&self) -> bool {
        true
    }

    fn content_type(&self) -> &str {
        "text/plain"
    }

    fn create(&self, _: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        Ok(Arc::new(FlatReader))
    }
}

#[derive(Debug)]
struct FlatReader;

fn flat_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ]))
}

fn flat_batch(text: &str) -> Result<RecordBatch> {
    let mut a = Vec::new();
    let mut b = Vec::new();
    for line in text.lines().filter(|l| !l.is_empty()) {
        let (x, y) = line
            .split_once(',')
            .ok_or_else(|| DataFusionError::Execution(format!("bad exflat line: {line}")))?;
        a.push(x.trim().parse::<i64>().ok());
        b.push(y.trim().to_string());
    }
    Ok(RecordBatch::try_new(
        flat_schema(),
        vec![
            Arc::new(Int64Array::from(a)),
            Arc::new(StringArray::from(b)),
        ],
    )?)
}

#[async_trait]
impl FormatReader for FlatReader {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        Ok(flat_schema())
    }

    async fn infer_stats(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        table_schema: SchemaRef,
    ) -> Result<Option<Statistics>> {
        let rows = flat_batch(&read_text(&store, object).await?)?.num_rows();
        let mut statistics = Statistics::new_unknown(&table_schema);
        statistics.num_rows = Precision::Exact(rows);
        Ok(Some(statistics))
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        _: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        Ok(one_batch(flat_batch(&read_text(&store, object).await?)?))
    }

    /// Writes one `a,b` line per row: the first two columns, by position.
    async fn write(
        &self,
        store: Arc<dyn ObjectStore>,
        request: WriteRequest,
        mut input: SendableRecordBatchStream,
    ) -> Result<u64> {
        let mut text = String::new();
        let mut rows = 0;
        while let Some(batch) = input.next().await {
            let batch = batch?;
            for row in 0..batch.num_rows() {
                let a = array_value_to_string(batch.column(0), row)?;
                let b = array_value_to_string(batch.column(1), row)?;
                text.push_str(&format!("{a},{b}\n"));
            }
            rows += batch.num_rows() as u64;
        }
        store
            .put(&request.path, text.into_bytes().into())
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        Ok(rows)
    }
}

#[derive(Debug)]
struct ExGrid;

impl FormatFactory for ExGrid {
    fn name(&self) -> &str {
        "exgrid"
    }

    fn extensions(&self) -> Vec<String> {
        vec!["exg".to_string()]
    }

    fn output_kind(&self) -> OutputKind {
        OutputKind::Nd
    }

    fn create(&self, _: &HashMap<String, String>) -> Result<Arc<dyn FormatReader>> {
        Ok(Arc::new(GridReader))
    }
}

#[derive(Debug)]
struct GridReader;

fn grid_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("row", DataType::Int64, true),
        Field::new("col", DataType::Int64, true),
        Field::new("v", DataType::Float64, true),
    ]))
}

fn grid_batch(text: &str, wanted: &SchemaRef) -> Result<RecordBatch> {
    let bad = || DataFusionError::Execution("bad exgrid file".to_string());
    let mut lines = text.lines();
    let (rows, cols) = lines
        .next()
        .and_then(|l| l.split_once(','))
        .ok_or_else(bad)?;
    let rows: usize = rows.trim().parse().map_err(|_| bad())?;
    let cols: usize = cols.trim().parse().map_err(|_| bad())?;
    let values: Vec<f64> = lines
        .next()
        .unwrap_or_default()
        .split(',')
        .filter(|v| !v.trim().is_empty())
        .map(|v| v.trim().parse().map_err(|_| bad()))
        .collect::<Result<_>>()?;
    let dims = |spec: &[(&str, usize)]| {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
    };
    let all = [
        (
            "row",
            NdArrowArray::try_new(
                Arc::new(Int64Array::from((0..rows as i64).collect::<Vec<_>>())),
                dims(&[("row", rows)])?,
            )?,
        ),
        (
            "col",
            NdArrowArray::try_new(
                Arc::new(Int64Array::from((0..cols as i64).collect::<Vec<_>>())),
                dims(&[("col", cols)])?,
            )?,
        ),
        (
            "v",
            NdArrowArray::try_new(
                Arc::new(Float64Array::from(values)),
                dims(&[("row", rows), ("col", cols)])?,
            )?,
        ),
    ];
    // Only the columns that Beacon asks for.
    let (fields, columns): (Vec<Field>, Vec<NdArrowArray>) = all
        .into_iter()
        .filter(|(name, _)| wanted.index_of(name).is_ok())
        .map(|(name, array)| {
            let field = grid_schema().field_with_name(name).unwrap().clone();
            (field, array)
        })
        .unzip();
    let nd = NdRecordBatch::try_new(
        Arc::new(Schema::new(fields)),
        columns,
        dims(&[("row", rows), ("col", cols)])?,
    )?;
    encode_nd_record_batch(&nd)
}

#[async_trait]
impl FormatReader for GridReader {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        Ok(grid_schema())
    }

    async fn open(
        &self,
        store: Arc<dyn ObjectStore>,
        object: &ObjectMeta,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        let text = read_text(&store, object).await?;
        Ok(one_batch(grid_batch(&text, &request.schema)?))
    }
}

pub fn formats() -> Vec<Arc<dyn FormatFactory>> {
    vec![Arc::new(ExFlat), Arc::new(ExGrid)]
}
