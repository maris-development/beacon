//! The morsel planner of the ND scan.
//!
//! One planner reads one scan entry: a file, or a slice of one. Every I/O sits
//! in a planner future, and a morsel holds a chunk that is read already.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_nd_dataset::{ChunkGrid, NdDataset, NdRecordBatch, read_chunk, select_columns};
use datafusion::common::internal_err;
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::ObjectStore;

use crate::error::to_datafusion;
use crate::metrics::ScanMetrics;
use crate::opener::{NdFileOpener, NdScanOptions, open_for_read};
use crate::output::FileOutput;
use crate::prune::{chunk_masks, is_pruned};
use crate::schema::{file_columns, table_schema};
use crate::slices::NdSlice;

/// What each file of the scan of one partition shares.
#[derive(Debug)]
pub(crate) struct ScanContext {
    /// Opens each file of the format.
    pub opener: Arc<dyn NdFileOpener>,
    /// The table options of the format.
    pub options: NdScanOptions,
    /// The store that holds the files.
    pub store: Arc<dyn ObjectStore>,
    /// The encoded schema that the scan emits.
    pub projected_schema: SchemaRef,
    /// The names of the columns of the table. They set the grid of a scan with no column.
    pub table_file_columns: Vec<String>,
    /// The approximate row count of one chunk.
    pub batch_size: usize,
    /// The filter of the scan, for the chunk prune.
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The row limit of the scan and the rows that the planners of this partition planned.
    pub budget: Arc<RowBudget>,
    /// The counters of this partition.
    pub metrics: ScanMetrics,
}

/// The row limit of one partition and the rows whose read is planned.
#[derive(Debug)]
pub(crate) struct RowBudget {
    /// The row limit. `None` for no limit.
    limit: Option<usize>,
    /// The rows of the planned reads.
    planned: AtomicUsize,
}

impl RowBudget {
    /// A budget of `limit` rows with no planned read.
    pub(crate) fn new(limit: Option<usize>) -> Self {
        Self {
            limit,
            planned: AtomicUsize::new(0),
        }
    }

    /// True when the planned reads hold the rows of the limit.
    fn reached(&self) -> bool {
        self.limit
            .is_some_and(|limit| self.planned.load(Ordering::Relaxed) >= limit)
    }

    /// Add the `rows` of the next read to the planned rows. False when the limit is reached.
    fn take(&self, rows: usize) -> bool {
        let Some(limit) = self.limit else {
            return true;
        };
        // The check and the add are one step, so two planners cannot both pass the limit.
        self.planned
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < limit).then(|| count.saturating_add(rows))
            })
            .is_ok()
    }
}

/// The morselizer of one partition: one planner for each file.
#[derive(Debug)]
pub(crate) struct NdMorselizer {
    context: Arc<ScanContext>,
}

impl NdMorselizer {
    /// The morselizer of the partition that `context` describes.
    pub(crate) fn new(context: ScanContext) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

impl Morselizer for NdMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        if file.range.is_some() {
            return internal_err!("the ND scan never reads a byte range");
        }
        Ok(Box::new(Planner::Unopened {
            context: Arc::clone(&self.context),
            file,
        }))
    }
}

/// One opened entry: the file, its chunks after the slice cut and the prune.
#[derive(Debug)]
struct OpenFile {
    context: Arc<ScanContext>,
    /// The format, the path and the slice, for error text.
    label: String,
    dataset: Arc<dyn NdDataset>,
    grid: ChunkGrid,
    output: FileOutput,
    /// The numbers of the chunks to read, in the order of the grid.
    chunks: Vec<usize>,
}

impl OpenFile {
    fn describe(&self, number: usize) -> String {
        format!("{}, chunk {number} of {}", self.label, self.grid.len())
    }
}

/// The states of the planner of one entry.
#[derive(Debug)]
// Each planner lives in a `Box<dyn MorselPlanner>`, so a large variant costs no copy.
#[allow(clippy::large_enum_variant)]
enum Planner {
    Unopened {
        context: Arc<ScanContext>,
        file: PartitionedFile,
    },
    Read {
        file: Arc<OpenFile>,
        /// The next chunk to read.
        next: usize,
        /// A chunk that is read and not emitted yet, with its number in the file.
        ready: Option<(usize, NdRecordBatch)>,
    },
    Done,
}

impl MorselPlanner for Planner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        match *self {
            Planner::Unopened { context, file } => {
                // The scan does not open a file after it has the rows of its limit.
                if context.budget.reached() {
                    return Ok(None);
                }
                Ok(Some(MorselPlan::new().with_pending_planner(async move {
                    open(context, file).await
                })))
            }
            Planner::Read { file, next, ready } => {
                // The limit counts the rows of a read when the read is planned.
                let more = match file.chunks.get(next) {
                    Some(&number) => file.context.budget.take(file.grid.chunk(number).len()),
                    None => false,
                };
                if ready.is_none() && !more {
                    // Nothing is left. An empty plan would panic in FileStream.
                    return Ok(None);
                }
                let mut plan = MorselPlan::new();
                if let Some((number, batch)) = ready {
                    plan = plan.with_morsels(vec![Box::new(ChunkMorsel {
                        file: Arc::clone(&file),
                        number,
                        batch,
                    })]);
                }
                if more {
                    plan = plan.with_pending_planner(async move { read(file, next).await });
                }
                Ok(Some(plan))
            }
            Planner::Done => Ok(None),
        }
    }
}

/// Open one entry, and plan its chunks.
async fn open(context: Arc<ScanContext>, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
    let slice = NdSlice::of(&file);
    let mut label = format!(
        "{} file {}",
        context.opener.format_name(),
        file.object_meta.location
    );
    if slice.count > 1 {
        label.push_str(&format!(" slice {} of {}", slice.index, slice.count));
    }
    plan_entry(&context, &file, slice, &label)
        .await
        .map_err(|e| e.context(label.clone()))
}

async fn plan_entry(
    context: &Arc<ScanContext>,
    file: &PartitionedFile,
    slice: NdSlice,
    label: &str,
) -> Result<Box<dyn MorselPlanner>> {
    let opened = open_for_read(
        context.opener.as_ref(),
        Arc::clone(&context.store),
        &file.object_meta,
        &context.options,
    )
    .await?;
    let Some(dataset) = opened else {
        // The slices of a file all skip it. One of them counts it.
        if slice.index == 0 {
            context.metrics.files_skipped.add(1);
        }
        return Ok(Box::new(Planner::Done));
    };

    let names = file_columns(&context.projected_schema);
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    // The grid spans only the dimensions of the selected variables.
    let (dataset, grid_source) = if names.is_empty() {
        // A scan with no file column has the grid of `SELECT *` and reads no variable.
        let table: Vec<&str> = context
            .table_file_columns
            .iter()
            .map(String::as_str)
            .collect();
        let grid_source = select_columns(Arc::clone(&dataset), &table);
        (select_columns(dataset, &[]), grid_source)
    } else {
        let dataset = select_columns(dataset, &names);
        (Arc::clone(&dataset), dataset)
    };
    if grid_source.variable_names().is_empty() {
        // The file has none of the wanted columns, so it gives no row.
        return Ok(Box::new(Planner::Done));
    }
    let output = FileOutput::try_new(dataset.as_ref(), &context.projected_schema)?;
    let grid =
        ChunkGrid::try_new(grid_source.as_ref(), context.batch_size).map_err(to_datafusion)?;
    let chunks = select_chunks(context, dataset.as_ref(), &grid, slice).await;
    if chunks.is_empty() {
        return Ok(Box::new(Planner::Done));
    }

    Ok(Box::new(Planner::Read {
        file: Arc::new(OpenFile {
            context: Arc::clone(context),
            label: label.to_string(),
            dataset,
            grid,
            output,
            chunks,
        }),
        next: 0,
        ready: None,
    }))
}

/// The numbers of the chunks of `slice` that the predicate does not prune.
async fn select_chunks(
    context: &ScanContext,
    dataset: &dyn NdDataset,
    grid: &ChunkGrid,
    slice: NdSlice,
) -> Vec<usize> {
    // The slice cut comes first, on the whole grid. Every slice cuts the same grid.
    let range = slice.range(grid.len());
    // An empty slice or a count scan reads no coordinate for a mask.
    let Some(predicate) = context
        .predicate
        .as_ref()
        .filter(|_| !range.is_empty() && !dataset.variable_names().is_empty())
    else {
        return range.collect();
    };

    // The scan casts each file column to its logical table type.
    let table_schema = table_schema(&context.projected_schema);
    let masks = chunk_masks(dataset, predicate, &table_schema).await;
    let mut chunks = Vec::new();
    for number in range {
        let chunk = grid.chunk(number);
        if is_pruned(&masks, grid, &chunk) {
            context.metrics.chunks_pruned.add(1);
            context.metrics.rows_pruned.add(chunk.len());
        } else {
            chunks.push(number);
        }
    }
    chunks
}

/// Read entry `index` of the chunks of `file`, and hand it to the next planner state.
async fn read(file: Arc<OpenFile>, index: usize) -> Result<Box<dyn MorselPlanner>> {
    let number = file.chunks[index];
    let chunk = file.grid.chunk(number);
    let batch = read_chunk(file.dataset.as_ref(), &file.grid, &chunk)
        .await
        .map_err(|e| to_datafusion(e).context(file.describe(number)))?;

    file.context.metrics.chunks_read.add(1);
    file.context.metrics.rows_read.add(chunk.len());
    Ok(Box::new(Planner::Read {
        file,
        next: index + 1,
        ready: Some((number, batch)),
    }))
}

/// One chunk that is read. The stream encodes and adapts it, and does no I/O.
#[derive(Debug)]
struct ChunkMorsel {
    file: Arc<OpenFile>,
    number: usize,
    batch: NdRecordBatch,
}

impl Morsel for ChunkMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        let batch = self
            .file
            .output
            .finish(&self.batch)
            .map_err(|e| e.context(self.file.describe(self.number)));
        futures::stream::once(futures::future::ready(batch)).boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Field, Float64Type, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use beacon_nd_dataset::{GridSelection, MemoryDataset};
    use beacon_nd_encoding::encoding::{decode_nd_record_batch, encoded_schema};
    use beacon_nd_execution::NdRecordBatchExt;
    use datafusion::error::{DataFusionError, Result};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion_datasource::PartitionedFile;
    use datafusion_datasource::morsel::Morselizer;
    use futures::StreamExt;
    use object_store::memory::InMemory;

    use super::{NdMorselizer, RowBudget, ScanContext};
    use crate::metrics::ScanMetrics;
    use crate::opener::NdScanOptions;
    use crate::slices::NdSlice;
    use crate::tests::fixtures::{Entry, MemoryOpener, entry, grid_dataset, ramp};

    /// Run every planner of `files` to the end, as FileStream does, and collect the batches.
    async fn drive(
        morselizer: &NdMorselizer,
        files: Vec<PartitionedFile>,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = Vec::new();
        for file in files {
            let mut planners = vec![morselizer.plan_file(file)?];
            while let Some(planner) = planners.pop() {
                let Some(mut plan) = planner.plan()? else {
                    continue;
                };
                let morsels = plan.take_morsels();
                let ready = plan.take_ready_planners();
                let pending = plan.take_pending_planner();
                assert!(
                    !morsels.is_empty() || !ready.is_empty() || pending.is_some(),
                    "a planner returned an empty plan"
                );
                for morsel in morsels {
                    let mut stream = morsel.into_stream();
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                }
                planners.extend(ready);
                if let Some(pending) = pending {
                    planners.push(pending.await?);
                }
            }
        }
        Ok(batches)
    }

    /// The encoded schema of the Float64 columns `columns`.
    fn projected(columns: &[&str]) -> SchemaRef {
        let logical = Schema::new(
            columns
                .iter()
                .map(|name| Field::new(*name, DataType::Float64, true))
                .collect::<Vec<_>>(),
        );
        Arc::new(encoded_schema(&logical))
    }

    /// The context of a scan that reads `columns` of the test opener, with an optional `lat > threshold`.
    fn context(
        opener: Arc<MemoryOpener>,
        columns: &[&str],
        batch_size: usize,
        above: Option<f64>,
        options: NdScanOptions,
        limit: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> ScanContext {
        let logical = Schema::new(vec![Field::new("lat", DataType::Float64, true)]);
        let predicate = above.map(|threshold| {
            binary(
                col("lat", &logical).unwrap(),
                Operator::Gt,
                lit(threshold),
                &logical,
            )
            .unwrap()
        });
        ScanContext {
            opener,
            options,
            store: Arc::new(InMemory::new()),
            projected_schema: projected(columns),
            table_file_columns: ["lat", "lon", "temp"].map(String::from).to_vec(),
            batch_size,
            predicate,
            budget: Arc::new(RowBudget::new(limit)),
            metrics: ScanMetrics::new(metrics, 0),
        }
    }

    /// A morselizer that reads `columns` of the test opener, with an optional `lat > threshold`.
    fn morselizer(
        opener: Arc<MemoryOpener>,
        columns: &[&str],
        batch_size: usize,
        above: Option<f64>,
        options: NdScanOptions,
        limit: Option<usize>,
        metrics: &ExecutionPlanMetricsSet,
    ) -> NdMorselizer {
        NdMorselizer::new(context(
            opener, columns, batch_size, above, options, limit, metrics,
        ))
    }

    /// The total of decoded rows of `batches`.
    fn rows(batches: &[RecordBatch]) -> usize {
        batches
            .iter()
            .map(|batch| {
                decode_nd_record_batch(batch)
                    .unwrap()
                    .materialize()
                    .unwrap()
                    .num_rows()
            })
            .sum()
    }

    /// The decoded `temp` values of `batches`, in order.
    fn temps(batches: &[RecordBatch]) -> Vec<f64> {
        batches
            .iter()
            .flat_map(|batch| {
                let flat = decode_nd_record_batch(batch)
                    .unwrap()
                    .materialize()
                    .unwrap();
                let temp = flat.column_by_name("temp").unwrap();
                temp.as_primitive::<Float64Type>().values().to_vec()
            })
            .collect()
    }

    fn counter(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        metrics
            .clone_inner()
            .sum_by_name(name)
            .map_or(0, |value| value.as_usize())
    }

    /// The test opener with the grid fixture at `a.nd`.
    fn grid_opener() -> Arc<MemoryOpener> {
        Arc::new(MemoryOpener::default().with("a.nd", Entry::Dataset(grid_dataset("a.nd"))))
    }

    /// Slice `index` of `count` of `a.nd`.
    fn slice(index: u32, count: u32) -> PartitionedFile {
        entry("a.nd").with_extension(NdSlice { index, count })
    }

    fn skip_invalid() -> NdScanOptions {
        NdScanOptions {
            skip_invalid_files: true,
            ..NdScanOptions::default()
        }
    }

    #[tokio::test]
    async fn a_file_is_read_chunk_by_chunk() {
        let opener = grid_opener();
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            opener.clone(),
            &["temp"],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert_eq!(batches.len(), 4);
        assert!(batches.iter().all(|batch| batch.num_rows() == 1));
        assert_eq!(rows(&batches), 48);
        assert_eq!(temps(&batches).len(), 48);
        assert_eq!(counter(&metrics, "chunks_read"), 4);
        assert_eq!(counter(&metrics, "rows_read"), 48);
        assert_eq!(opener.opens(), 1);
    }

    #[tokio::test]
    async fn the_slices_of_a_file_read_each_row_once() {
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &["temp"],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let mut read = Vec::new();
        let mut chunks = Vec::new();
        for index in 0..3 {
            let batches = drive(&scan, vec![slice(index, 3)]).await.unwrap();
            chunks.push(batches.len());
            read.extend(temps(&batches));
        }

        assert_eq!(chunks, [1, 1, 2]);
        assert_eq!(read.len(), 48);
        let mut sorted = read.clone();
        sorted.sort_by(f64::total_cmp);
        let expected: Vec<f64> = (0..48).map(|value| value as f64).collect();
        assert_eq!(sorted, expected);
    }

    #[tokio::test]
    async fn a_predicate_prunes_chunks_and_counts_them() {
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &["temp", "lat"],
            12,
            Some(3.5),
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(rows(&batches), 24);
        assert_eq!(counter(&metrics, "chunks_pruned"), 2);
        assert_eq!(counter(&metrics, "rows_pruned"), 24);
    }

    #[tokio::test]
    async fn a_predicate_that_changes_between_slices_reads_no_chunk_twice() {
        let opener = grid_opener();
        let metrics = ExecutionPlanMetricsSet::new();

        // Slice 0 sees a bound. Slice 1 opens later and sees no bound.
        let first = drive(
            &morselizer(
                opener.clone(),
                &["temp", "lat"],
                12,
                Some(3.5),
                NdScanOptions::default(),
                None,
                &metrics,
            ),
            vec![slice(0, 2)],
        )
        .await
        .unwrap();
        let second = drive(
            &morselizer(
                opener,
                &["temp", "lat"],
                12,
                None,
                NdScanOptions::default(),
                None,
                &metrics,
            ),
            vec![slice(1, 2)],
        )
        .await
        .unwrap();

        // Each chunk holds its own `temp` values, so a chunk read twice repeats a value.
        let mut read = temps(&first);
        read.extend(temps(&second));
        let distinct: HashSet<u64> = read.iter().map(|value| value.to_bits()).collect();
        assert_eq!(distinct.len(), read.len(), "no chunk is read twice");
        assert_eq!(counter(&metrics, "chunks_read"), first.len() + second.len());
    }

    #[tokio::test]
    async fn a_fully_pruned_file_finishes_with_no_batch() {
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &["temp", "lat"],
            12,
            Some(100.0),
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "chunks_pruned"), 4);
    }

    #[tokio::test]
    async fn an_unreadable_file_is_skipped_or_named() {
        let opener = Arc::new(MemoryOpener::default().with("a.nd", Entry::Invalid("bad header")));

        let metrics = ExecutionPlanMetricsSet::new();
        let error = drive(
            &morselizer(
                opener.clone(),
                &["temp"],
                12,
                None,
                NdScanOptions::default(),
                None,
                &metrics,
            ),
            vec![entry("a.nd")],
        )
        .await
        .unwrap_err();
        let text = error.to_string();
        assert!(
            text.contains("memory file") && text.contains("a.nd"),
            "{text}"
        );

        let metrics = ExecutionPlanMetricsSet::new();
        let batches = drive(
            &morselizer(opener, &["temp"], 12, None, skip_invalid(), None, &metrics),
            vec![entry("a.nd")],
        )
        .await
        .unwrap();
        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "files_skipped"), 1);
    }

    #[tokio::test]
    async fn an_unsupported_file_is_refused_with_its_path() {
        let opener = Arc::new(MemoryOpener::default().with("a.nd", Entry::Unsupported("ragged")));
        let metrics = ExecutionPlanMetricsSet::new();

        let error = drive(
            &morselizer(
                opener,
                &["temp"],
                12,
                None,
                NdScanOptions::default(),
                None,
                &metrics,
            ),
            vec![entry("a.nd")],
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error.find_root(), DataFusionError::NotImplemented(_)),
            "{error}"
        );
        let text = error.to_string();
        assert!(text.contains("ragged") && text.contains("a.nd"), "{text}");
    }

    #[tokio::test]
    async fn the_prune_counts_of_the_slices_sum_to_one_scan() {
        let whole = ExecutionPlanMetricsSet::new();
        drive(
            &morselizer(
                grid_opener(),
                &["temp", "lat"],
                12,
                Some(3.5),
                NdScanOptions::default(),
                None,
                &whole,
            ),
            vec![entry("a.nd")],
        )
        .await
        .unwrap();

        let sliced = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &["temp", "lat"],
            12,
            Some(3.5),
            NdScanOptions::default(),
            None,
            &sliced,
        );
        for index in 0..4 {
            drive(&scan, vec![slice(index, 4)]).await.unwrap();
        }

        for name in ["chunks_read", "rows_read", "chunks_pruned", "rows_pruned"] {
            assert_eq!(counter(&sliced, name), counter(&whole, name), "{name}");
        }
        assert_eq!(counter(&sliced, "chunks_pruned"), 2);
        assert_eq!(counter(&sliced, "rows_pruned"), 24);
    }

    #[tokio::test]
    async fn only_the_first_slice_counts_a_skipped_file() {
        let opener = Arc::new(MemoryOpener::default().with("a.nd", Entry::Invalid("bad header")));
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(opener, &["temp"], 12, None, skip_invalid(), None, &metrics);

        for index in 0..3 {
            assert!(
                drive(&scan, vec![slice(index, 3)])
                    .await
                    .unwrap()
                    .is_empty()
            );
        }

        assert_eq!(counter(&metrics, "files_skipped"), 1);
    }

    #[tokio::test]
    async fn a_file_with_none_of_the_columns_finishes_with_no_batch() {
        let opener = grid_opener();
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            opener.clone(),
            &["salt"],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(opener.opens(), 1);
    }

    #[test]
    fn a_byte_range_is_refused() {
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &["temp"],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let error = scan.plan_file(entry("a.nd").with_range(0, 10)).unwrap_err();

        assert!(matches!(error, DataFusionError::Internal(_)), "{error}");
    }

    #[tokio::test]
    async fn a_count_scan_gives_the_rows_and_reads_nothing() {
        let dataset = grid_dataset("a.nd");
        let opener =
            Arc::new(MemoryOpener::default().with("a.nd", Entry::Dataset(dataset.clone())));
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            opener,
            &[],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert_eq!(batches.len(), 4);
        assert!(batches.iter().all(|batch| batch.num_columns() == 0));
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 48);
        assert_eq!(
            dataset.reads("temp") + dataset.reads("lat") + dataset.reads("lon"),
            0
        );
        assert_eq!(counter(&metrics, "rows_read"), 48);
    }

    #[tokio::test]
    async fn a_count_scan_on_slices_counts_each_row_once() {
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &[],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );

        let mut total = 0;
        for index in 0..3 {
            let batches = drive(&scan, vec![slice(index, 3)]).await.unwrap();
            total += batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        }

        assert_eq!(total, 48);
    }

    #[tokio::test]
    async fn a_limit_plans_no_read_after_the_rows_it_needs() {
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            grid_opener(),
            &["temp"],
            12,
            None,
            NdScanOptions::default(),
            Some(13),
            &metrics,
        );

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert_eq!(batches.len(), 2);
        assert_eq!(rows(&batches), 24);
        assert_eq!(counter(&metrics, "chunks_read"), 2);
    }

    /// temp(time=1, lat=8, lon=6), lat, lon and flag(time=1, lat=8, lon=6, band=3).
    fn banded_dataset() -> MemoryDataset {
        MemoryDataset::try_new(
            "a.nd",
            vec![
                ramp("temp", &[("time", 1), ("lat", 8), ("lon", 6)]),
                ramp("lat", &[("lat", 8)]),
                ramp("lon", &[("lon", 6)]),
                ramp("flag", &[("time", 1), ("lat", 8), ("lon", 6), ("band", 3)]),
            ],
        )
        .unwrap()
    }

    /// The batches of a count scan of `dataset` on a table with the file columns `table`.
    async fn count_scan(dataset: MemoryDataset, table: &[&str]) -> Vec<RecordBatch> {
        let opener = Arc::new(MemoryOpener::default().with("a.nd", Entry::Dataset(dataset)));
        let metrics = ExecutionPlanMetricsSet::new();
        let mut context = context(
            opener,
            &[],
            12,
            None,
            NdScanOptions::default(),
            None,
            &metrics,
        );
        context.table_file_columns = table.iter().map(|name| name.to_string()).collect();
        drive(&NdMorselizer::new(context), vec![entry("a.nd")])
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_count_scan_uses_the_grid_of_the_table_columns() {
        let dataset = banded_dataset();

        let batches = count_scan(dataset.clone(), &["temp", "lat", "lon"]).await;

        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 48);
        let reads: usize = ["temp", "lat", "lon", "flag"]
            .iter()
            .map(|name| dataset.reads(name))
            .sum();
        assert_eq!(reads, 0);
    }

    #[tokio::test]
    async fn a_count_scan_of_a_file_with_no_table_column_gives_no_row() {
        let batches = count_scan(banded_dataset(), &["salt"]).await;

        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn a_limit_of_zero_reads_nothing() {
        let opener = grid_opener();
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            opener.clone(),
            &["temp"],
            12,
            None,
            NdScanOptions::default(),
            Some(0),
            &metrics,
        );

        // `drive` asserts that no planner returns an empty plan.
        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "chunks_read"), 0);
        assert_eq!(opener.opens(), 0);
    }

    #[tokio::test]
    async fn a_limit_counts_over_the_files_of_one_partition() {
        let opener = Arc::new(
            MemoryOpener::default()
                .with("a.nd", Entry::Dataset(grid_dataset("a.nd")))
                .with("b.nd", Entry::Dataset(grid_dataset("b.nd"))),
        );
        let metrics = ExecutionPlanMetricsSet::new();
        let scan = morselizer(
            opener.clone(),
            &["temp"],
            12,
            None,
            NdScanOptions::default(),
            Some(13),
            &metrics,
        );

        let first = drive(&scan, vec![entry("a.nd")]).await.unwrap();
        let second = drive(&scan, vec![entry("b.nd")]).await.unwrap();

        assert_eq!(first.len(), 2);
        assert!(second.is_empty());
        assert_eq!(counter(&metrics, "chunks_read"), 2);
        assert_eq!(opener.opens(), 1);
    }

    #[tokio::test]
    async fn a_file_off_the_grid_is_skipped_and_counted() {
        let metrics = ExecutionPlanMetricsSet::new();
        let options = NdScanOptions {
            grid: GridSelection::Given(vec!["depth".to_string()]),
            skip_unbroadcastable: true,
            ..NdScanOptions::default()
        };
        let scan = morselizer(grid_opener(), &["temp"], 12, None, options, None, &metrics);

        let batches = drive(&scan, vec![entry("a.nd")]).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "files_skipped"), 1);
    }
}
