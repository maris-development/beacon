//! The morsel planner of the NetCDF scan.
//!
//! One planner reads one scan entry: a file, or a slice of one. Every I/O sits
//! in a planner future, and a morsel holds a chunk that is read already.

use std::sync::Arc;

use arrow::datatypes::{FieldRef, SchemaRef};
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::nd::NdRecordBatch;
use beacon_nd_array::array::subset::ArraySubset;
use beacon_nd_array::arrow::metrics::ReadMetrics;
use beacon_nd_array::arrow::partition::FilePartitions;
use beacon_nd_array::arrow::pushdown_filter::PushdownFilter;
use beacon_nd_array::dataset::project_read_dimensions_or_skip;
use datafusion::common::internal_err;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner, Morselizer};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::ObjectStore;

use super::chunks::FileScan;
use super::output::{refuse_ragged, FileOutput};
use super::prune::{coordinate_masks, is_pruned};
use super::slices::NetcdfSlice;
use crate::datafusion::reader::NetcdfInput;

/// Which files a scan reads past in place of an error.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct SkipFiles {
    /// Skip a file that does not fit `read_dimensions`.
    pub unbroadcastable: bool,
    /// Skip a file the reader cannot open.
    pub invalid: bool,
}

/// What every file of one partition's scan shares.
#[derive(Debug)]
pub(super) struct ScanContext {
    pub store: Arc<dyn ObjectStore>,
    /// The encoded schema the scan emits.
    pub projected_schema: SchemaRef,
    /// The encoded `PARTITIONED BY` columns of the table.
    pub partition_fields: Vec<FieldRef>,
    pub read_dimensions: Option<Vec<String>>,
    pub skip: SkipFiles,
    pub batch_size: usize,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub metrics: ReadMetrics,
}

#[derive(Debug)]
pub(super) struct NetcdfMorselizer {
    context: Arc<ScanContext>,
}

impl NetcdfMorselizer {
    pub(super) fn new(context: ScanContext) -> Self {
        Self {
            context: Arc::new(context),
        }
    }
}

impl Morselizer for NetcdfMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        if file.range.is_some() {
            return internal_err!("NetCDF scans never read byte ranges");
        }
        Ok(Box::new(Planner::Unopened {
            context: self.context.clone(),
            file,
        }))
    }
}

/// One opened entry: the file, its chunks after the slice cut and the prune.
#[derive(Debug)]
struct OpenFile {
    context: Arc<ScanContext>,
    /// The path and the slice, for error text.
    label: String,
    scan: FileScan,
    output: FileOutput,
    /// The chunks to read, each with its number in the file.
    chunks: Vec<(usize, ArraySubset)>,
    /// How many chunks the file has.
    total: usize,
}

impl OpenFile {
    fn describe(&self, number: usize) -> String {
        format!("{}, chunk {number} of {}", self.label, self.total)
    }
}

#[derive(Debug)]
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
                Ok(Some(MorselPlan::new().with_pending_planner(async move {
                    open(context, file).await
                })))
            }
            Planner::Read { file, next, ready } => {
                let mut plan = MorselPlan::new();
                let more = next < file.chunks.len();
                if ready.is_none() && !more {
                    // Nothing is left. An empty plan would panic in FileStream.
                    return Ok(None);
                }
                if let Some((number, batch)) = ready {
                    plan = plan.with_morsels(vec![Box::new(ChunkMorsel {
                        file: file.clone(),
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
    let slice = NetcdfSlice::of(&file);
    let mut label = format!("NetCDF file {}", file.object_meta.location);
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
    slice: NetcdfSlice,
    label: &str,
) -> Result<Box<dyn MorselPlanner>> {
    let skipped = |context: &ScanContext| -> Box<dyn MorselPlanner> {
        // The slices of a file all skip it. One of them counts it.
        if slice.index == 0 {
            context.metrics.files_skipped.add(1);
        }
        Box::new(Planner::Done)
    };

    let input = NetcdfInput::new(&context.store, &file.object_meta);
    let dataset = match input.open().await {
        Ok(dataset) => dataset,
        Err(e) if context.skip.invalid => {
            tracing::warn!(file = %file.object_meta.location, error = %e, "skipping an unreadable NetCDF file");
            return Ok(skipped(context));
        }
        Err(e) => return Err(DataFusionError::Execution(format!("failed to open: {e}"))),
    };

    let Some(dataset) = project_read_dimensions_or_skip(
        dataset,
        context.read_dimensions.clone(),
        context.skip.unbroadcastable,
        None,
    )
    .map_err(|e| DataFusionError::Execution(e.to_string()))?
    else {
        return Ok(skipped(context));
    };
    refuse_ragged(&dataset)?;

    let partitions = FilePartitions::new(
        context.partition_fields.clone(),
        file.partition_values.clone(),
    );
    let Some((dataset, output)) =
        FileOutput::resolve(dataset, &context.projected_schema, &partitions)?
    else {
        // The file has none of the wanted columns, so it gives no row.
        return Ok(Box::new(Planner::Done));
    };
    let scan = FileScan::try_new(dataset, context.batch_size)?;

    // The slice cut comes first, on the full list. Every slice builds the same list.
    let total = scan.chunks().len();
    let range = slice.range(total);
    let mut chunks: Vec<(usize, ArraySubset)> = range
        .clone()
        .zip(scan.chunks()[range].iter().cloned())
        .collect();
    if chunks.is_empty() {
        // An empty slice reads no coordinate for a mask.
        return Ok(Box::new(Planner::Done));
    }
    if let Some(predicate) = &context.predicate {
        let masks = coordinate_masks(&scan, &PushdownFilter::new(predicate.clone())).await;
        let before = chunks.len();
        let rows_before: usize = chunks.iter().map(|(_, chunk)| chunk.rows()).sum();
        chunks.retain(|(_, chunk)| !is_pruned(&masks, scan.dims(), chunk));
        let rows_after: usize = chunks.iter().map(|(_, chunk)| chunk.rows()).sum();
        context.metrics.chunks_pruned.add(before - chunks.len());
        context.metrics.rows_pruned.add(rows_before - rows_after);
    }
    if chunks.is_empty() {
        return Ok(Box::new(Planner::Done));
    }

    Ok(Box::new(Planner::Read {
        file: Arc::new(OpenFile {
            context: context.clone(),
            label: label.to_string(),
            scan,
            output,
            chunks,
            total,
        }),
        next: 0,
        ready: None,
    }))
}

/// Read entry `index` of the chunks of `file`, and hand it to the next planner state.
async fn read(file: Arc<OpenFile>, index: usize) -> Result<Box<dyn MorselPlanner>> {
    let (number, chunk) = &file.chunks[index];
    let batch = file
        .scan
        .read(chunk)
        .await
        .map_err(|e| e.context(file.describe(*number)))?;

    file.context.metrics.chunks_read.add(1);
    file.context.metrics.rows_read.add(chunk.rows());
    Ok(Box::new(Planner::Read {
        file: file.clone(),
        next: index + 1,
        ready: Some((*number, batch)),
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
pub(in crate::datafusion) mod tests {
    use std::path::{Path as FsPath, PathBuf};

    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Field, Float64Type, Schema};
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, nd_encoded_field_of};
    use beacon_nd_execution::NdRecordBatchExt;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{binary, col, lit};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::local::LocalFileSystem;

    use super::*;
    use crate::datafusion::format_tests::write_regular_netcdf;

    /// Run every planner of `files` to the end, as FileStream does, and collect the batches.
    pub(in crate::datafusion) async fn drive(
        morselizer: &NetcdfMorselizer,
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

    fn entry(path: &FsPath) -> PartitionedFile {
        let location = object_store::path::Path::from_absolute_path(path).unwrap();
        let size = std::fs::metadata(path).unwrap().len();
        PartitionedFile::new(location.to_string(), size)
    }

    /// A morselizer that reads `V0`, with an optional `V0 > threshold` predicate.
    fn morselizer(
        batch_size: usize,
        above: Option<f64>,
        skip: SkipFiles,
        metrics: &ExecutionPlanMetricsSet,
    ) -> NetcdfMorselizer {
        let logical = Schema::new(vec![Field::new("V0", DataType::Float64, true)]);
        let predicate = above.map(|threshold| {
            binary(
                col("V0", &logical).unwrap(),
                Operator::Gt,
                lit(threshold),
                &logical,
            )
            .unwrap()
        });
        NetcdfMorselizer::new(ScanContext {
            store: Arc::new(LocalFileSystem::new()),
            projected_schema: Arc::new(Schema::new(vec![nd_encoded_field_of(
                &logical.field(0).clone(),
            )])),
            partition_fields: vec![],
            read_dimensions: None,
            skip,
            batch_size,
            predicate,
            metrics: ReadMetrics::new(metrics, 0),
        })
    }

    /// The `V0` values of the encoded batches, in order.
    fn values(batches: &[RecordBatch]) -> Vec<f64> {
        batches
            .iter()
            .flat_map(|batch| {
                let flat = decode_nd_record_batch(batch)
                    .unwrap()
                    .materialize()
                    .unwrap();
                flat.column(0)
                    .as_primitive::<Float64Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn counter(metrics: &ExecutionPlanMetricsSet, name: &str) -> usize {
        metrics
            .clone_inner()
            .sum_by_name(name)
            .map_or(0, |value| value.as_usize())
    }

    fn file_of(rows: usize) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = write_regular_netcdf(dir.path(), "data.nc", rows);
        (dir, path)
    }

    #[tokio::test]
    async fn a_file_is_read_chunk_by_chunk() {
        let (_dir, path) = file_of(4000);
        let metrics = ExecutionPlanMetricsSet::new();
        let morselizer = morselizer(1000, None, SkipFiles::default(), &metrics);

        let batches = drive(&morselizer, vec![entry(&path)]).await.unwrap();

        let expected: Vec<f64> = (0..4000).map(|row| row as f64).collect();
        assert_eq!(values(&batches), expected);
        assert!(batches.len() > 1, "one batch per chunk");
        assert_eq!(counter(&metrics, "chunks_read"), batches.len());
        assert_eq!(counter(&metrics, "rows_read"), 4000);
    }

    #[tokio::test]
    async fn the_slices_of_a_file_read_each_row_once() {
        let (_dir, path) = file_of(4000);
        let metrics = ExecutionPlanMetricsSet::new();
        let morselizer = morselizer(500, None, SkipFiles::default(), &metrics);

        let mut read = Vec::new();
        for index in 0..3 {
            let slice = entry(&path).with_extension(NetcdfSlice { index, count: 3 });
            read.extend(values(&drive(&morselizer, vec![slice]).await.unwrap()));
        }

        let expected: Vec<f64> = (0..4000).map(|row| row as f64).collect();
        assert_eq!(read, expected);
    }

    #[tokio::test]
    async fn a_predicate_prunes_chunks_and_counts_them() {
        let (_dir, path) = file_of(4000);
        let whole = ExecutionPlanMetricsSet::new();
        let all = drive(
            &morselizer(500, None, SkipFiles::default(), &whole),
            vec![entry(&path)],
        )
        .await
        .unwrap();

        let metrics = ExecutionPlanMetricsSet::new();
        let pruned = drive(
            &morselizer(500, Some(1999.5), SkipFiles::default(), &metrics),
            vec![entry(&path)],
        )
        .await
        .unwrap();

        assert!(pruned.len() < all.len(), "some chunks are pruned");
        // A kept chunk is read whole, so every row above the bound is there.
        let read = values(&pruned);
        assert!((2000..4000).all(|row| read.contains(&(row as f64))));
        assert_eq!(
            counter(&metrics, "chunks_read") + counter(&metrics, "chunks_pruned"),
            all.len()
        );
        assert_eq!(
            counter(&metrics, "rows_read") + counter(&metrics, "rows_pruned"),
            4000
        );
    }

    #[tokio::test]
    async fn a_predicate_that_changes_between_slices_reads_no_chunk_twice() {
        let (_dir, path) = file_of(4000);
        let metrics = ExecutionPlanMetricsSet::new();
        let slice = |index| entry(&path).with_extension(NetcdfSlice { index, count: 2 });

        // Slice 0 sees no bound. Slice 1 opens later and sees a tighter one.
        let first = drive(
            &morselizer(500, None, SkipFiles::default(), &metrics),
            vec![slice(0)],
        )
        .await
        .unwrap();
        let second = drive(
            &morselizer(500, Some(2999.5), SkipFiles::default(), &metrics),
            vec![slice(1)],
        )
        .await
        .unwrap();

        let mut read = values(&first);
        read.extend(values(&second));
        let distinct: std::collections::HashSet<u64> = read.iter().map(|v| v.to_bits()).collect();
        assert_eq!(distinct.len(), read.len(), "no row is read twice");
        assert!((3000..4000).all(|row| read.contains(&(row as f64))));
    }

    #[tokio::test]
    async fn a_fully_pruned_file_finishes_with_no_batch() {
        let (_dir, path) = file_of(4000);
        let metrics = ExecutionPlanMetricsSet::new();
        let morselizer = morselizer(500, Some(1.0e9), SkipFiles::default(), &metrics);

        let batches = drive(&morselizer, vec![entry(&path)]).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "rows_pruned"), 4000);
    }

    #[tokio::test]
    async fn an_unreadable_file_is_skipped_or_named() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.nc");
        std::fs::write(&path, b"this is not a netcdf file").unwrap();

        let metrics = ExecutionPlanMetricsSet::new();
        let skip = SkipFiles {
            invalid: true,
            ..SkipFiles::default()
        };
        let batches = drive(&morselizer(500, None, skip, &metrics), vec![entry(&path)])
            .await
            .unwrap();
        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "files_skipped"), 1);

        let error = drive(
            &morselizer(500, None, SkipFiles::default(), &metrics),
            vec![entry(&path)],
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("broken.nc"), "{error}");
    }

    #[tokio::test]
    async fn a_ragged_file_is_refused_with_its_path() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("test_files/wod_ctd_1964.nc");
        let metrics = ExecutionPlanMetricsSet::new();

        let error = drive(
            &morselizer(500, None, SkipFiles::default(), &metrics),
            vec![entry(&path)],
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error.find_root(), DataFusionError::NotImplemented(_)),
            "{error}"
        );
        let text = error.to_string();
        assert!(
            text.contains("ragged") && text.contains("wod_ctd_1964.nc"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn the_prune_counts_of_the_slices_sum_to_one_scan() {
        let (_dir, path) = file_of(4000);
        let whole = ExecutionPlanMetricsSet::new();
        drive(
            &morselizer(500, Some(1999.5), SkipFiles::default(), &whole),
            vec![entry(&path)],
        )
        .await
        .unwrap();

        let sliced = ExecutionPlanMetricsSet::new();
        let morselizer = morselizer(500, Some(1999.5), SkipFiles::default(), &sliced);
        for index in 0..3 {
            let slice = entry(&path).with_extension(NetcdfSlice { index, count: 3 });
            drive(&morselizer, vec![slice]).await.unwrap();
        }

        for name in ["chunks_read", "rows_read", "chunks_pruned", "rows_pruned"] {
            assert_eq!(counter(&sliced, name), counter(&whole, name), "{name}");
        }
    }

    #[tokio::test]
    async fn only_the_first_slice_counts_a_skipped_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("broken.nc");
        std::fs::write(&path, b"this is not a netcdf file").unwrap();
        let metrics = ExecutionPlanMetricsSet::new();
        let skip = SkipFiles {
            invalid: true,
            ..SkipFiles::default()
        };
        let morselizer = morselizer(500, None, skip, &metrics);

        for index in 0..4 {
            let slice = entry(&path).with_extension(NetcdfSlice { index, count: 4 });
            assert!(drive(&morselizer, vec![slice]).await.unwrap().is_empty());
        }

        assert_eq!(counter(&metrics, "files_skipped"), 1);
    }

    #[tokio::test]
    async fn a_file_with_none_of_the_columns_finishes_with_no_batch() {
        let (_dir, path) = file_of(4000);
        let metrics = ExecutionPlanMetricsSet::new();
        let wanted = Field::new("no_such_column", DataType::Float64, true);
        let morselizer = NetcdfMorselizer::new(ScanContext {
            store: Arc::new(LocalFileSystem::new()),
            projected_schema: Arc::new(Schema::new(vec![nd_encoded_field_of(&wanted)])),
            partition_fields: vec![],
            read_dimensions: None,
            skip: SkipFiles::default(),
            batch_size: 500,
            predicate: None,
            metrics: ReadMetrics::new(&metrics, 0),
        });

        let batches = drive(&morselizer, vec![entry(&path)]).await.unwrap();

        assert!(batches.is_empty());
        assert_eq!(counter(&metrics, "chunks_read"), 0);
    }

    #[test]
    fn a_byte_range_is_refused() {
        let metrics = ExecutionPlanMetricsSet::new();
        let morselizer = morselizer(500, None, SkipFiles::default(), &metrics);
        let ranged = PartitionedFile::new("a.nc", 100).with_range(0, 50);

        let error = morselizer.plan_file(ranged).unwrap_err();

        assert!(
            error.to_string().contains("never read byte ranges"),
            "{error}"
        );
    }
}
