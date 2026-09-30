use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatchOptions};
use arrow::datatypes::{FieldRef, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use beacon_datafusion_ext::scan_adapt::batch_adapter_factory;
use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
use datafusion::physical_expr_adapter::BatchAdapter;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use indexmap::IndexMap;

use crate::NdArrayD;
use crate::projection::DatasetProjection;
use std::ops::Range;

use crate::array::subset::ArraySubset;
use crate::arrow::batch::{
    ChunkGrid, RaggedPlan, build_dataset_schema, chunk_grid, chunk_is_pruned,
    compute_predicate_masks, plan_ragged_read, read_chunk, read_ragged_range,
};
use crate::arrow::metrics::ReadMetrics;
use crate::arrow::nd_provider::read_nd_chunk;
use crate::arrow::partition::FilePartitions;
use crate::arrow::pushdown_filter::PushdownFilter;
use crate::dataset::AnyDataset;

/// What the scan wants out of a file, and so what a read unit becomes.
///
/// The decision reaches down to the read itself, because the two want
/// different batches: a column read wants `beacon.nd`-encoded chunks for the
/// `NdSourceExec` above it to decode, and `COUNT(*)` wants flat ones it can
/// take a row count off.
#[derive(Debug)]
enum Output {
    /// Columns: nd-encoded batches, reordered and null-filled onto the
    /// projected schema.
    Columns {
        adapter: Arc<BatchAdapter>,
        /// What the file's `PARTITIONED BY` columns are called, when the scan
        /// projects any.
        partition_fields: Vec<FieldRef>,
        /// Those columns, nd-encoded as rank-0 arrays. One value each, constant
        /// for the whole file, so they are built once here and appended to
        /// every batch of it. See [`FilePartitions`].
        partition_columns: Vec<ArrayRef>,
    },
    /// `COUNT(*)`: flat batches, of which only the row count leaves, under the
    /// (empty) projected schema. See [`count_projection`].
    ///
    /// A scan of nothing but partition columns comes here too. It wants no
    /// column of the file either, and the row count is what says how many times
    /// each partition value repeats.
    Rows {
        schema: SchemaRef,
        partitions: FilePartitions,
    },
    /// The file holds none of the columns the query projects, so it has nothing
    /// to contribute and is not read at all.
    Nothing,
}

impl Output {
    /// Whether the read encodes its chunks, rather than broadcasting them flat.
    fn encoded(&self) -> bool {
        matches!(self, Output::Columns { .. })
    }

    /// `batches` turned into what the scan wants.
    fn finish(
        &self,
        batches: BoxStream<'static, Result<RecordBatch>>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        match self {
            Output::Columns {
                adapter,
                partition_fields,
                partition_columns,
            } => {
                let adapter = adapter.clone();
                let fields = partition_fields.clone();
                let columns = partition_columns.clone();
                batches
                    .and_then(move |batch| {
                        let adapted = with_partitions(&batch, &fields, &columns)
                            .and_then(|batch| adapter.adapt_batch(&batch))
                            .map_err(|e| {
                                DataFusionError::Execution(format!(
                                    "Failed to adapt the batch onto the scan's schema: {e}"
                                ))
                            });
                        futures::future::ready(adapted)
                    })
                    .boxed()
            }
            Output::Rows { schema, partitions } => {
                let schema = schema.clone();
                let partitions = partitions.clone();
                batches
                    .and_then(move |batch| {
                        futures::future::ready(count_batch(&schema, &partitions, batch.num_rows()))
                    })
                    .boxed()
            }
            // `FileRead::plan` pairs `Nothing` with no units, so nothing
            // reaches here.
            Output::Nothing => futures::stream::empty().boxed(),
        }
    }
}

/// One unit of work: what one read of a file produces.
///
/// The two dataset shapes divide differently, so a plan holds whichever unit
/// its file is made of.
#[derive(Debug, Clone)]
enum Work {
    /// One hyperslab of a regular dataset's chunk grid.
    Grid(ArraySubset),
    /// One batch of a ragged dataset's plan, as a range of passing casts.
    ///
    /// A ragged dataset has no chunk grid. Its batches are cut where the cast
    /// boundaries fall, which takes the cumulative offsets and the predicate
    /// masks to work out. After that a range reads on its own, as a chunk does.
    Ragged(Range<usize>),
}

/// The units of a file worth reading, and how to read one.
///
/// [`ChunkPlan::build`] applies the predicate as it fills the list, so every
/// unit in it is a read that can produce rows.
#[derive(Debug)]
pub(crate) struct ChunkPlan {
    units: Vec<Work>,
    read: Arc<ReadKind>,
    /// Whether a chunk leaves nd-encoded. See [`Output`].
    encoded: bool,
    /// What the predicate excluded before the list existed. A slice holds
    /// none, so that a file's counts are reported once.
    pruned: Pruned,
}

/// The chunks (regular) or casts (ragged) the predicate excluded from a file,
/// and the rows they held.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pruned {
    pub chunks: usize,
    pub rows: usize,
}

#[derive(Debug)]
enum ReadKind {
    Grid {
        arrays: Arc<IndexMap<String, Arc<dyn NdArrayD>>>,
        dims: Arc<Vec<String>>,
        schema: Arc<Schema>,
    },
    Ragged {
        plan: Arc<RaggedPlan>,
    },
}

impl ChunkPlan {
    /// Open `dataset` and list the units worth reading.
    ///
    /// A regular dataset is cut on its chunk grid; a ragged one on its batch
    /// plan. The list depends only on the dataset, `batch_size` and the
    /// predicate, so two plans of one file agree unit by unit. The parts of a
    /// split file rely on that.
    ///
    /// # The predicate is applied here, not later
    ///
    /// The coordinate arrays are read once, before the list is filled, and a
    /// chunk no row of which can meet the predicate never enters it. So the
    /// list holds the work the query actually needs, and the parts of a file
    /// divide *that*.
    ///
    /// A ragged dataset already works this way: `plan_ragged_read` applies the
    /// predicate when it chooses which casts survive, so its plan holds only the
    /// batches that have something in them.
    pub(crate) async fn build(
        dataset: AnyDataset,
        batch_size: usize,
        predicate: Option<PushdownFilter>,
        encoded: bool,
    ) -> Result<Self> {
        let regular = match dataset {
            AnyDataset::Regular(regular) => regular,
            AnyDataset::Ragged { ragged, .. } => {
                let plan = plan_ragged_read(ragged, batch_size, predicate)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                // Casts the predicate excluded before the plan existed. No
                // unit can account for them, so the plan keeps their count.
                let pruned = plan
                    .pruned
                    .map_or_else(Pruned::default, |(chunks, rows)| Pruned { chunks, rows });
                let units = plan.ranges.iter().cloned().map(Work::Ragged).collect();
                return Ok(Self {
                    units,
                    read: Arc::new(ReadKind::Ragged {
                        plan: Arc::new(plan),
                    }),
                    encoded,
                    pruned,
                });
            }
        };

        let ChunkGrid { dims, chunks } = chunk_grid(&regular, batch_size)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        let arrays = Arc::new(regular.arrays);
        let schema = build_dataset_schema(&arrays);

        // Read once for the file, before anything is listed. Computing them per
        // unit instead would read the coordinate arrays once per chunk.
        let dim_masks = compute_predicate_masks(&arrays, predicate)
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // Only the chunks that can hold a row the query wants. The predicate is
        // applied again above the scan, so dropping one here only drops rows
        // that would have been dropped there.
        let (wanted, pruned): (Vec<ArraySubset>, Vec<ArraySubset>) = chunks
            .into_iter()
            .partition(|subset| !chunk_is_pruned(&dim_masks, &dims, subset));

        let pruned = Pruned {
            chunks: pruned.len(),
            rows: pruned.iter().map(|subset| subset.rows()).sum(),
        };

        Ok(Self {
            units: wanted.into_iter().map(Work::Grid).collect(),
            read: Arc::new(ReadKind::Grid {
                arrays,
                dims: Arc::new(dims),
                schema,
            }),
            encoded,
            pruned,
        })
    }

    /// How many units are left to read. For tests and diagnostics.
    pub(crate) fn len(&self) -> usize {
        self.units.len()
    }

    /// A copy of part `index` of `count`. See [`part_range`].
    fn slice(&self, index: usize, count: usize) -> Self {
        let range = part_range(self.units.len(), index, count);
        Self {
            units: self.units[range].to_vec(),
            read: Arc::clone(&self.read),
            encoded: self.encoded,
            pruned: Pruned::default(),
        }
    }

    /// One lazy stream per unit, in list order. Nothing is read until a
    /// stream is polled.
    fn into_streams(
        self,
        metrics: Option<ReadMetrics>,
    ) -> Vec<BoxStream<'static, Result<RecordBatch>>> {
        let Self {
            units,
            read,
            encoded,
            ..
        } = self;
        units
            .into_iter()
            .map(|work| read_unit(&read, encoded, work, metrics.clone()))
            .collect()
    }

    /// Every unit, one after the other.
    pub(crate) fn stream(
        self,
        metrics: Option<ReadMetrics>,
    ) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::iter(self.into_streams(metrics))
            .flatten()
            .boxed()
    }
}

/// The units that part `index` of `count` reads, out of `len`.
///
/// The parts are contiguous and differ in size by one unit at most. Between
/// them they cover `0..len` exactly once. A part past the end of a short list
/// is empty.
pub(crate) fn part_range(len: usize, index: usize, count: usize) -> Range<usize> {
    let count = count.max(1);
    let index = index.min(count);
    (index * len / count)..((index + 1).min(count) * len / count)
}

/// The batches one unit of work produces.
fn read_unit(
    read: &ReadKind,
    encoded: bool,
    work: Work,
    metrics: Option<ReadMetrics>,
) -> BoxStream<'static, Result<RecordBatch>> {
    match (work, read) {
        (
            Work::Grid(subset),
            ReadKind::Grid {
                arrays,
                dims,
                schema,
            },
        ) => {
            let arrays = arrays.clone();
            let dims = dims.clone();
            let schema = schema.clone();
            let flat = !encoded;
            futures::stream::once(async move {
                // The rows this chunk holds, as the scan will broadcast them.
                // An encoded batch carries the lot in one row, so counting the
                // batch would say nothing.
                if let Some(metrics) = &metrics {
                    metrics.chunks_read.add(1);
                    metrics.rows_read.add(subset.rows());
                }
                // No masks here: `build` applied them when it listed the unit.
                if flat {
                    return read_chunk(&arrays, subset, schema, &dims, &[])
                        .await
                        .map_err(|e| DataFusionError::Execution(e.to_string()));
                }
                let nd = read_nd_chunk(&arrays, &dims, schema, subset).await?;
                beacon_datafusion_ext::nd::encode_nd_record_batch(&nd).map(Some)
            })
            .filter_map(|batch| futures::future::ready(batch.transpose()))
            .boxed()
        }
        (Work::Ragged(range), ReadKind::Ragged { plan }) => {
            let plan = plan.clone();
            // A ragged read is flat already, and its plan applied the
            // predicate when it chose which casts survive.
            futures::stream::once(async move {
                let flat = read_ragged_range(&plan, range)
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;
                if let Some(metrics) = &metrics {
                    metrics.chunks_read.add(1);
                    metrics.rows_read.add(flat.num_rows());
                }
                if encoded {
                    beacon_datafusion_ext::nd::encode_flat_batch_as_nd(&flat)
                } else {
                    Ok(flat)
                }
            })
            .boxed()
        }
        // `build` pairs the unit with the kind, so a mismatch is a bug in this
        // file rather than bad input.
        _ => futures::stream::once(async {
            Err(DataFusionError::Internal(
                "nd read: work unit does not match the dataset it came from".to_string(),
            ))
        })
        .boxed(),
    }
}

/// Read a whole dataset as flat, broadcast batches, in file order.
///
/// This is the [`ChunkPlan`] a `COUNT(*)` builds, minus the counting: the whole
/// list, and the predicate pruning chunks it cannot use. It exists for the
/// readers' own tests. A scan goes through [`FileRead::plan`], which resolves
/// a projection and encodes.
pub async fn flat_stream(
    dataset: AnyDataset,
    batch_size: usize,
    predicate: Option<PushdownFilter>,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    Ok(ChunkPlan::build(dataset, batch_size, predicate, false)
        .await?
        .stream(None))
}

/// One file, opened and planned: the units to read from it, and what a unit's
/// batches become.
///
/// This is what one part of a scan reads. A split file has several parts, and
/// each builds its own `FileRead` and keeps its own slice. See
/// [`FileRead::part`].
///
/// Every format that reads through the nd pipeline plans a file the same way, so
/// the planning lives here rather than four times over. What differs between
/// them (how a file is opened, which dimensions it reads on) happens before
/// this and is handed in as an [`AnyDataset`].
#[derive(Debug)]
pub struct FileRead {
    /// `None` when the file holds none of the projected columns. There is
    /// nothing to list, so nothing is opened for reading either.
    plan: Option<ChunkPlan>,
    output: Arc<Output>,
}

impl FileRead {
    /// A file the scan decided not to read at all.
    ///
    /// Nothing is listed and nothing is streamed, so the file costs no I/O.
    /// This is what a format returns for a file it ruled out *before* reading
    /// it. That decision belongs to the format, because only the format knows
    /// what it can prove from its own metadata.
    ///
    /// This is not the same as a file that holds none of the projected columns.
    /// [`plan`](Self::plan) reaches that state on its own, having opened the
    /// file to find out.
    pub fn skipped() -> Self {
        Self {
            plan: None,
            output: Arc::new(Output::Nothing),
        }
    }

    /// Plan `dataset` for a scan that wants `projected_schema`.
    ///
    /// Resolves the projection, lists the units, and decides what a batch of
    /// them becomes. `predicate` is a hint: it prunes chunks that cannot hold a
    /// row the query wants, and the scan is expected to apply it again above.
    ///
    /// A chunk the predicate excludes is dropped before the list exists, so no
    /// reader can count it. The plan keeps the count instead. See
    /// [`pruned`](Self::pruned).
    ///
    /// `partitions` are the table's `PARTITIONED BY` columns and this file's
    /// values for them. They are in the file's path rather than in the file, so
    /// they are appended to its batches here. See [`FilePartitions`]. Pass
    /// [`FilePartitions::none`] for an unpartitioned table.
    pub async fn plan(
        dataset: AnyDataset,
        projected_schema: SchemaRef,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        partitions: FilePartitions,
    ) -> Result<Self> {
        let dataset_schema: SchemaRef = Arc::new(
            crate::arrow::schema::any_dataset_to_arrow_schema(&dataset).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Failed to derive an Arrow schema from the dataset: {e}"
                ))
            })?,
        );

        // The `PARTITIONED BY` columns this scan projects, in the order it wants
        // them. They are added to every batch below rather than read: the value
        // is in the file's path, not in the file.
        let partition_fields = partitions.projected_fields(&projected_schema);

        // The columns of this file the query needs, in file order. A partition
        // column shadows a variable of the same name (the path wins, as it does
        // for every other format), so it never counts as one of these.
        let projection: Vec<usize> = dataset_schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, field)| {
                !partitions.holds(field.name()) && projected_schema.index_of(field.name()).is_ok()
            })
            .map(|(index, _)| index)
            .collect();

        let pushdown = predicate.clone().map(PushdownFilter::new);

        // How much of the file itself the query wants, partition columns aside.
        let wanted_file_columns = projected_schema.fields().len() - partition_fields.len();

        // Nothing of this file was projected. That is two different situations,
        // and they must not be confused: the query wanted no column at all, or
        // it wanted columns this file does not have.
        let (output, projection) = if projection.is_empty() {
            if wanted_file_columns > 0 {
                // The query named columns and this file has none of them. A
                // collection is not obliged to be uniform (of one CORA year, 2%
                // of the files carry no `TEMP` and 10% no `DEPH`), so this is an
                // ordinary file, not a broken one.
                //
                // It contributes no rows. Its row count is a property of the
                // arrays being read, and there are none; inventing one would
                // mean picking a grid from variables the query never asked for
                // and returning that many nulls.
                return Ok(Self::skipped());
            }

            // `COUNT(*)`, or a scan of nothing but partition columns: no column
            // of the file is wanted, so the read is driven by columns of its own
            // and only the row counts leave.
            let counted = count_projection(&dataset, &dataset_schema, &predicate);
            (
                Output::Rows {
                    schema: projected_schema,
                    partitions,
                },
                counted,
            )
        } else {
            // The scan carries nd columns, so adaptation happens in the encoded
            // (struct) domain: reorder and null-fill onto the projected schema.
            // The partition columns join the source there, so the adapter puts
            // them wherever the projection asked for them.
            let mut source_fields: Vec<FieldRef> =
                beacon_datafusion_ext::nd::encoded_schema(&dataset_schema.project(&projection)?)
                    .fields()
                    .to_vec();
            source_fields.extend(partition_fields.iter().cloned());
            let source_schema: SchemaRef = Arc::new(Schema::new(source_fields));

            let partition_columns = partitions.scalar_columns(&projected_schema)?;
            // Every column here is nd-encoded, and an nd column casts leniently
            // under every merge rule. The strict rule stands in for the one the
            // session holds, because the adapter never asks it.
            let adapter =
                batch_adapter_factory(projected_schema, Arc::new(DefaultArrowTypeWidening::new()))
                    .make_adapter(&source_schema)?;
            (
                Output::Columns {
                    adapter: Arc::new(adapter),
                    partition_fields,
                    partition_columns,
                },
                projection,
            )
        };

        let dataset = project(dataset, &dataset_schema, projection)?;
        let plan = ChunkPlan::build(dataset, batch_size, pushdown, output.encoded()).await?;

        Ok(Self {
            plan: Some(plan),
            output: Arc::new(output),
        })
    }

    /// How many units this read holds. For tests and diagnostics.
    pub fn units(&self) -> usize {
        self.plan.as_ref().map_or(0, ChunkPlan::len)
    }

    /// A read of part `index` of `count` of the units.
    ///
    /// The parts are contiguous slices of the pruned unit list, balanced to
    /// one unit, and between them they cover every unit exactly once. The
    /// parts of a file agree on the slices because they slice one list: a
    /// shared plan, or plans built from the same inputs.
    ///
    /// The plan itself does not change, so several parts can slice one
    /// shared plan. Each slice owns its units.
    pub fn slice(&self, index: usize, count: usize) -> Self {
        Self {
            plan: self.plan.as_ref().map(|plan| plan.slice(index, count)),
            output: Arc::clone(&self.output),
        }
    }

    /// What the predicate excluded from the file when it was planned. A slice
    /// reports nothing, so that the file's counts are reported once.
    pub fn pruned(&self) -> Pruned {
        self.plan.as_ref().map_or_else(Pruned::default, |plan| plan.pruned)
    }

    /// One lazy stream per unit, in list order.
    ///
    /// Each becomes one DataFusion morsel. `metrics` are the reading
    /// partition's, so what each partition read is what it reports.
    pub fn into_streams(
        self,
        metrics: Option<ReadMetrics>,
    ) -> Vec<BoxStream<'static, Result<RecordBatch>>> {
        let Some(plan) = self.plan else {
            // The file holds none of the projected columns. See
            // [`FileRead::plan`].
            return Vec::new();
        };
        let output = self.output;
        plan.into_streams(metrics)
            .into_iter()
            .map(|batches| output.finish(batches))
            .collect()
    }

    /// Every unit of this read, one after the other.
    pub fn stream(self, metrics: Option<ReadMetrics>) -> BoxStream<'static, Result<RecordBatch>> {
        futures::stream::iter(self.into_streams(metrics))
            .flatten()
            .boxed()
    }
}

/// `batch` with the file's `PARTITIONED BY` columns appended.
///
/// Each is one value on no axis, so it broadcasts over whatever grid the file's
/// own columns define and reaches every row the file contributes. Nothing is
/// built per row, and nothing is built per batch: the columns are the file's,
/// and [`FileRead::plan`] made them once.
fn with_partitions(
    batch: &RecordBatch,
    fields: &[FieldRef],
    columns: &[ArrayRef],
) -> Result<RecordBatch> {
    if fields.is_empty() {
        return Ok(batch.clone());
    }

    let schema: Vec<FieldRef> = batch
        .schema()
        .fields()
        .iter()
        .cloned()
        .chain(fields.iter().cloned())
        .collect();
    let values: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .cloned()
        .chain(columns.iter().cloned())
        .collect();

    RecordBatch::try_new_with_options(
        Arc::new(Schema::new(schema)),
        values,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

/// The batch a read that wants no column of the file emits for `rows` rows.
///
/// A plain `COUNT(*)` emits the row count and nothing else. A scan of nothing
/// but partition columns emits those columns instead: rank-0 columns alone
/// would define a rank-0 grid, which holds one row, so the rows of the read are
/// stated as an axis of their own.
fn count_batch(
    schema: &SchemaRef,
    partitions: &FilePartitions,
    rows: usize,
) -> Result<RecordBatch> {
    let (columns, encoded_rows) = if schema.fields().is_empty() {
        (Vec::new(), rows)
    } else {
        (partitions.row_columns(schema, rows)?, 1)
    };

    RecordBatch::try_new_with_options(
        schema.clone(),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(encoded_rows)),
    )
    .map_err(|e| DataFusionError::Execution(format!("Failed to build a count batch: {e}")))
}

/// The columns a `COUNT(*)` reads, out of a file the query wants no column of.
///
/// Reading no column at all would give an empty stream and a count of zero. The
/// read is driven by the widest variable instead, so the row count is the full
/// broadcast row count (a scalar attribute like `.Conventions` would give one
/// row), plus any column the predicate names, so a pushed-down filter still
/// applies ([`PushdownFilter`] matches by name).
fn count_projection(
    dataset: &AnyDataset,
    dataset_schema: &SchemaRef,
    predicate: &Option<Arc<dyn PhysicalExpr>>,
) -> Vec<usize> {
    let driver = dataset
        .fields()
        .keys()
        .max_by_key(|name| {
            dataset
                .get_array(name)
                .map(|array| array.shape().iter().product::<usize>())
                .unwrap_or(0)
        })
        .and_then(|name| dataset_schema.index_of(name).ok())
        .unwrap_or(0);

    let mut projection = vec![driver];
    if let Some(predicate) = predicate {
        for column in datafusion::physical_expr::utils::collect_columns(predicate) {
            if let Ok(index) = dataset_schema.index_of(column.name()) {
                projection.push(index);
            }
        }
    }
    projection.sort_unstable();
    projection.dedup();
    projection
}

/// Keep only `projection` of `dataset`, or all of it when that is everything.
fn project(
    dataset: AnyDataset,
    dataset_schema: &SchemaRef,
    projection: Vec<usize>,
) -> Result<AnyDataset> {
    if projection.len() == dataset_schema.fields().len() {
        return Ok(dataset);
    }
    dataset
        .project(&DatasetProjection {
            dimension_projection: None,
            index_projection: Some(projection),
        })
        .map_err(|e| DataFusionError::Execution(format!("Failed to project the dataset: {e}")))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;

    use super::*;
    use crate::NdArray;
    use crate::dataset::Dataset;

    /// A projection that wants no column, so a plan takes the `COUNT(*)` path.
    fn no_columns() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    /// A dataset of `rows` values on one dimension.
    async fn dataset(rows: usize) -> AnyDataset {
        let values = NdArray::<i64>::try_new_from_vec_in_mem(
            (0..rows as i64).collect(),
            vec![rows],
            vec!["row".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("value".to_string(), Arc::new(values));
        AnyDataset::Regular(Dataset::new("shared".to_string(), arrays).await)
    }

    /// A CF contiguous ragged-array dataset of `casts` casts, each holding one
    /// more observation than the last.
    ///
    /// Uneven on purpose: a plan over equal casts would divide the same way
    /// whatever the batch size, and hide a mistake in the mapping back to the
    /// dataset's own indices.
    async fn ragged_dataset(casts: usize) -> (AnyDataset, usize) {
        let sizes: Vec<i32> = (1..=casts as i32).collect();
        let observations: usize = sizes.iter().map(|size| *size as usize).sum();

        let row_size = NdArray::<i32>::try_new_from_vec_in_mem(
            sizes,
            vec![casts],
            vec!["casts".to_string()],
            None,
        )
        .unwrap();
        // The attribute that marks the dataset as ragged and names the
        // observation dimension `row_size` counts into.
        let sample_dimension = NdArray::<String>::try_new_from_vec_in_mem(
            vec!["obs".to_string()],
            vec![],
            vec![] as Vec<String>,
            None,
        )
        .unwrap();
        let station = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..casts).map(|cast| cast as f64).collect(),
            vec![casts],
            vec!["casts".to_string()],
            None,
        )
        .unwrap();
        let temperature = NdArray::<f64>::try_new_from_vec_in_mem(
            (0..observations).map(|obs| obs as f64 * 0.5).collect(),
            vec![observations],
            vec!["obs".to_string()],
            None,
        )
        .unwrap();

        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("row_size".to_string(), Arc::new(row_size));
        arrays.insert(
            "row_size.sample_dimension".to_string(),
            Arc::new(sample_dimension),
        );
        arrays.insert("station".to_string(), Arc::new(station));
        arrays.insert("temperature".to_string(), Arc::new(temperature));

        let dataset = Dataset::new("ragged".to_string(), arrays).await;
        let any = AnyDataset::try_from_dataset(dataset).await.unwrap();
        assert!(
            matches!(any, AnyDataset::Ragged { .. }),
            "fixture is ragged"
        );
        (any, observations)
    }

    /// Drain an encoded stream into the rows it read.
    async fn drain(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .map(|batch| {
                beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                    .unwrap()
                    .num_rows()
            })
            .sum()
    }

    /// Drain a flat stream into the rows it read.
    ///
    /// A flat batch is already broadcast, so it needs no decoding. This is what
    /// the `COUNT(*)` path counts.
    async fn drain_flat(stream: BoxStream<'static, Result<RecordBatch>>) -> usize {
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        batches.iter().map(|batch| batch.num_rows()).sum()
    }

    // ── pruning on the predicate ───────────────────────────────────────

    /// `value > threshold`, as the scan would push it down.
    fn greater_than(column: &str, threshold: i64) -> PushdownFilter {
        PushdownFilter::new(greater_than_expr(column, threshold))
    }

    fn greater_than_expr(column: &str, threshold: i64) -> Arc<dyn PhysicalExpr> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};
        use datafusion::scalar::ScalarValue;

        Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column, 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(threshold)))),
        ))
    }

    /// Every value an encoded read returned, decoded and broadcast.
    async fn values_read(stream: BoxStream<'static, Result<RecordBatch>>) -> Vec<i64> {
        use arrow::array::Int64Array;

        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut values = Vec::new();
        for batch in &batches {
            let flat = beacon_datafusion_ext::nd::decode_nd_record_batch(batch)
                .unwrap()
                .materialize()
                .unwrap();
            let column = flat
                .column_by_name("value")
                .expect("the fixture has one column")
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("an i64 column")
                .clone();
            values.extend(column.iter().flatten());
        }
        values
    }

    /// The plan holds only the chunks the predicate keeps, and keeps every row
    /// the query wants.
    ///
    /// Both halves matter and neither implies the other. Reading everything is
    /// correct but pointless; reading less is pointless if it drops a row the
    /// query asked for, and nothing about that raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_plan_holds_only_the_chunks_the_predicate_keeps() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let whole = ChunkPlan::build(dataset(ROWS).await, BATCH, None, true)
            .await
            .expect("the read builds");
        let chunks = whole.len();
        let all = values_read(whole.stream(None)).await;
        assert_eq!(all.len(), ROWS, "the unfiltered read returns the file");

        let pruned = ChunkPlan::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
        )
        .await
        .expect("the read builds");

        // The fixture counts up, so a chunk below the threshold holds nothing
        // the query wants, and it is left out of the list.
        let listed = pruned.len();
        assert!(
            listed > 0 && listed < chunks,
            "the plan should hold some of the {chunks} chunks, it holds {listed}"
        );

        // The rows read come from the listed chunks and no others. The last
        // chunk of the file is short, so this is a bound rather than a product.
        let kept = values_read(pruned.stream(None)).await;
        assert!(
            kept.len() <= listed * BATCH && kept.len() > (listed - 1) * BATCH,
            "{} rows off {listed} chunks of at most {BATCH}",
            kept.len()
        );

        // Nothing the predicate keeps may go missing.
        let wanted: Vec<i64> = all.iter().copied().filter(|v| *v > THRESHOLD).collect();
        let returned: std::collections::HashSet<i64> = kept.iter().copied().collect();
        assert!(
            wanted.iter().all(|value| returned.contains(value)),
            "the read dropped a row the predicate keeps"
        );
    }

    /// A predicate no row can meet leaves an empty plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_predicate_nothing_meets_lists_no_work() {
        const ROWS: usize = 10_000;

        let plan = ChunkPlan::build(
            dataset(ROWS).await,
            512,
            Some(greater_than("value", ROWS as i64 * 10)),
            true,
        )
        .await
        .expect("the read builds");

        assert_eq!(plan.len(), 0, "no chunk can hold a matching row");
        assert_eq!(drain(plan.stream(None)).await, 0);
    }

    /// The flat read and the nd read skip the same chunks.
    ///
    /// `COUNT(*)` goes one way and a column read the other. They must agree
    /// about which chunks hold nothing, or a count stops matching its own rows.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn both_modes_skip_the_same_chunks() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 512;
        const THRESHOLD: i64 = 8_000;

        let nd = ChunkPlan::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
        )
        .await
        .expect("the read builds");
        let nd_rows = drain(nd.stream(None)).await;

        let flat = ChunkPlan::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            false,
        )
        .await
        .expect("the read builds");
        let flat_rows = drain_flat(flat.stream(None)).await;

        assert_eq!(nd_rows, flat_rows, "the two modes read the same chunks");
        assert!(
            nd_rows > 0 && nd_rows < ROWS,
            "and they pruned some of them"
        );
    }

    /// A predicate on a column the file does not bound prunes nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_unrelated_predicate_reads_the_whole_file() {
        const ROWS: usize = 4_000;

        let plan = ChunkPlan::build(
            dataset(ROWS).await,
            512,
            Some(greater_than("no_such_column", 10)),
            true,
        )
        .await
        .expect("the read builds");

        assert_eq!(drain(plan.stream(None)).await, ROWS);
    }

    // ── parts of a split file ──────────────────────────────────────────

    /// The parts of a list are contiguous, balanced, and cover it once.
    #[test]
    fn parts_are_contiguous_balanced_and_cover_the_list_once() {
        for len in [0_usize, 1, 5, 7, 64, 1_000] {
            for count in [1_usize, 2, 3, 8, 24] {
                let ranges: Vec<Range<usize>> =
                    (0..count).map(|index| part_range(len, index, count)).collect();

                assert_eq!(ranges[0].start, 0, "len={len} count={count}: starts at 0");
                assert_eq!(ranges[count - 1].end, len, "len={len} count={count}: ends at len");
                for pair in ranges.windows(2) {
                    assert_eq!(pair[0].end, pair[1].start, "len={len} count={count}: contiguous");
                }
                let sizes: Vec<usize> = ranges.iter().map(|range| range.len()).collect();
                let (min, max) = (sizes.iter().min().unwrap(), sizes.iter().max().unwrap());
                assert!(max - min <= 1, "len={len} count={count}: balanced, got {sizes:?}");
            }
        }
    }

    /// The parts of a file read every row once between them.
    ///
    /// A unit in two parts is a row returned twice, and one in no part is a row
    /// lost, and neither raises an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_parts_of_a_file_read_every_row_once() {
        const ROWS: usize = 10_000;

        for count in [1_usize, 2, 3, 8, 40] {
            let mut columns = 0;
            let mut counted = 0;
            for index in 0..count {
                let wanted: SchemaRef = Arc::new(Schema::new(vec![
                    beacon_datafusion_ext::nd::nd_encoded_field(
                        "value",
                        &arrow::datatypes::DataType::Int64,
                    ),
                ]));
                let read = FileRead::plan(
                    dataset(ROWS).await,
                    wanted,
                    512,
                    None,
                    FilePartitions::none(),
                )
                .await
                .expect("a column read plans");
                columns += drain(read.slice(index, count).stream(None)).await;

                let read = FileRead::plan(
                    dataset(ROWS).await,
                    no_columns(),
                    512,
                    None,
                    FilePartitions::none(),
                )
                .await
                .expect("a count plans");
                counted += drain_flat(read.slice(index, count).stream(None)).await;
            }
            assert_eq!(columns, ROWS, "count={count}: every row read once");
            assert_eq!(counted, ROWS, "count={count}: every row counted once");
        }
    }

    /// The parts of a ragged file read every observation once between them.
    ///
    /// A ragged dataset has no chunk grid, so its list holds ranges of its
    /// batch plan instead. The property is the same.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_parts_of_a_ragged_file_read_every_row_once() {
        const CASTS: usize = 60;

        // A batch size well under the file, so the plan holds many ranges to
        // divide, and one over it, so it holds one.
        for batch_size in [8_usize, 64, usize::MAX] {
            for count in [1_usize, 2, 3, 8] {
                let mut rows = 0;
                let mut observations = 0;
                for index in 0..count {
                    let (source, total) = ragged_dataset(CASTS).await;
                    observations = total;
                    let plan = ChunkPlan::build(source, batch_size, None, true)
                        .await
                        .expect("the plan builds");
                    rows += drain(plan.slice(index, count).stream(None)).await;
                }
                assert_eq!(
                    rows, observations,
                    "batch_size={batch_size} count={count}: every row once"
                );
            }
        }
    }

    /// A file is pruned before it is sliced, so the parts divide only the
    /// chunks the predicate keeps.
    ///
    /// A predicate on time prunes a prefix of the chunk list. A slice of the
    /// whole list would leave the first parts with nothing to read.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_parts_divide_only_the_chunks_the_predicate_keeps() {
        const ROWS: usize = 10_000;
        const BATCH: usize = 100;
        const THRESHOLD: i64 = 5_000;
        const PARTS: usize = 4;

        let kept = ChunkPlan::build(
            dataset(ROWS).await,
            BATCH,
            Some(greater_than("value", THRESHOLD)),
            true,
        )
        .await
        .unwrap()
        .len();

        let mut sizes = Vec::new();
        for index in 0..PARTS {
            let wanted: SchemaRef = Arc::new(Schema::new(vec![
                beacon_datafusion_ext::nd::nd_encoded_field(
                    "value",
                    &arrow::datatypes::DataType::Int64,
                ),
            ]));
            let read = FileRead::plan(
                dataset(ROWS).await,
                wanted,
                BATCH,
                Some(greater_than_expr("value", THRESHOLD)),
                FilePartitions::none(),
            )
            .await
            .unwrap();
            // The plan keeps the pruned count, and a slice reports none of it.
            assert_eq!(read.pruned().chunks + kept, ROWS / BATCH, "every chunk is kept or pruned");
            assert_eq!(read.slice(index, PARTS).pruned(), Pruned::default());
            sizes.push(read.slice(index, PARTS).units());
        }

        assert_eq!(sizes.iter().sum::<usize>(), kept, "the parts cover the kept chunks");
        assert!(
            sizes.iter().all(|size| *size >= kept / PARTS),
            "every part has its share of the kept chunks: {sizes:?}"
        );
    }

    /// A file holding none of the projected columns contributes nothing.
    ///
    /// A collection is not obliged to be uniform. Of one CORA year, 2% of the
    /// files carry no `TEMP` and 10% no `DEPH`, so `SELECT TEMP` meets files
    /// that have none of what it asked for. Those are ordinary files, and the
    /// scan must not fail on them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_file_without_any_projected_column_is_read_as_nothing() {
        let wanted: SchemaRef = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            // The dataset holds "value" and nothing else.
            "absent",
            arrow::datatypes::DataType::Float64,
            true,
        )]));

        let planned = FileRead::plan(
            dataset(64).await,
            wanted,
            16,
            None,
            FilePartitions::none(),
        )
        .await
        .expect("a file without the column is planned, not rejected");

        assert_eq!(planned.units(), 0, "nothing is listed to read");
        let batches: Vec<RecordBatch> = planned
            .stream(None)
            .try_collect()
            .await
            .expect("and the stream is clean, not an error");
        assert!(batches.is_empty(), "it contributes no rows");
    }

    /// A file the scan ruled out before reading it reads as nothing.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_skipped_file_lists_nothing_and_emits_nothing() {
        let skipped = FileRead::skipped();

        assert_eq!(skipped.units(), 0, "nothing is listed to read");
        let batches: Vec<RecordBatch> = skipped
            .stream(None)
            .try_collect()
            .await
            .expect("the stream is clean, not an error");
        assert!(batches.is_empty(), "it contributes no rows");
    }

    /// A `COUNT(*)` still counts. It projects no column *because it wants none*,
    /// which is the case the check above has to keep telling apart.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_count_over_the_same_file_still_counts_it() {
        const ROWS: usize = 64;

        let planned = FileRead::plan(
            dataset(ROWS).await,
            no_columns(),
            16,
            None,
            FilePartitions::none(),
        )
        .await
        .expect("a count is planned");

        assert!(planned.units() > 0, "a count has work to do");
        assert_eq!(drain_flat(planned.stream(None)).await, ROWS);
    }
}
