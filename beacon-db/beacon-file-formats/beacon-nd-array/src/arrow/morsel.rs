//! nd scans on DataFusion's morsel API.
//!
//! DataFusion does the parallel work. Its shared queue hands the entries of a
//! scan to whichever partition is free, and each entry becomes morsels. This
//! module only says what an entry of an nd scan is, and how it becomes morsels.
//!
//! ```text
//! plan time   split_files     a file becomes n entries, "part i of n", when
//!                             the scan has fewer files than partitions
//! run time    NdMorselizer    a partition takes an entry, opens the file,
//!                             prunes its chunk list, keeps slice i of n,
//!                             and makes one morsel per unit of that slice
//! ```
//!
//! The parts of a split file share one pruned chunk list. It lives in
//! [`ScanPlans`] on the format's source, so only the parts of one query use
//! it. They also share one open through DataFusion's file metadata cache. The
//! first part that needs either one computes it, and the other parts wait for
//! it. Each part then takes its own slice of the list. See [`NdScan`].

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::{FieldRef, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileGroup, FileOpenFuture, FileOpener, FileScanConfig};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadata, FileMetadataCache,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::morsel::{Morsel, MorselPlan, MorselPlanner};
use futures::FutureExt;
use futures::stream::BoxStream;
use parking_lot::Mutex;

pub use datafusion_datasource::morsel::Morselizer;

use crate::arrow::file_read::FileRead;
use crate::arrow::metrics::ReadMetrics;
use crate::arrow::partition::FilePartitions;
use crate::dataset::AnyDataset;

/// How a format opens one of its files.
///
/// The only thing a format supplies. Everything else about a scan is the same
/// for every nd format, because all of them read through [`FileRead`].
#[async_trait::async_trait]
pub trait OpenFile: Send + Sync + 'static {
    /// Open `file` and read its metadata.
    ///
    /// The result depends on the file and on the reader's options only, never
    /// on the query, because the parts of a file share it through the cache.
    async fn open(&self, file: &PartitionedFile) -> Result<AnyDataset>;

    /// Narrow an opened dataset to what this scan reads on. `None` skips the
    /// file.
    fn narrow(&self, dataset: AnyDataset) -> Result<Option<AnyDataset>> {
        Ok(Some(dataset))
    }

    /// Names every option that changes what [`narrow`] returns.
    ///
    /// It is part of the fingerprint of a cached plan, so two tables that
    /// narrow one file differently never share a plan.
    ///
    /// [`narrow`]: Self::narrow
    fn narrow_tag(&self) -> String {
        String::new()
    }

    /// Names the reader and every option that changes what [`open`] returns.
    ///
    /// A cached open is used only when its tag matches, so two tables that
    /// open one path with different readers never share a dataset.
    ///
    /// [`open`]: Self::open
    fn cache_tag(&self) -> String;
}

/// Which part of a split file a queue entry reads.
///
/// [`split_files`] sets it at plan time. An entry without it reads its whole
/// file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FilePart {
    pub index: usize,
    pub count: usize,
}

impl FilePart {
    /// The part of `file`: the whole file when it is not split.
    pub fn of(file: &PartitionedFile) -> Self {
        file.extension::<FilePart>()
            .copied()
            .unwrap_or(Self { index: 0, count: 1 })
    }
}

/// Split and deal the files of a scan over `target_partitions` file groups.
///
/// With fewer files than partitions, each file becomes
/// `ceil(target_partitions / files)` entries, and entry `i` reads part `i` of
/// the file. With at least as many files as partitions, no file is split. The
/// entries go round robin into `target_partitions` groups, so the parts of one
/// file land in different groups, and the first groups hold different files.
///
/// No entry is a byte range. An nd file divides on its chunk list, and only an
/// open shows that list, so a part is a fraction of it. See [`FileRead::slice`].
///
/// Returns `None` for one partition or for no files. The scan then keeps the
/// groups it has.
pub fn split_files(file_groups: &[FileGroup], target_partitions: usize) -> Option<Vec<FileGroup>> {
    if target_partitions <= 1 {
        return None;
    }
    let files: Vec<PartitionedFile> = file_groups
        .iter()
        .flat_map(FileGroup::iter)
        .cloned()
        .collect();
    if files.is_empty() {
        return None;
    }

    let count = if files.len() >= target_partitions {
        1
    } else {
        target_partitions.div_ceil(files.len())
    };
    // Part 0 of every file first, then part 1, and so on, so that partitions
    // that start together open different files.
    let entries = (0..count).flat_map(|index| {
        files.iter().map(move |file| {
            if count == 1 {
                file.clone()
            } else {
                file.clone().with_extension(FilePart { index, count })
            }
        })
    });

    let mut groups: Vec<Vec<PartitionedFile>> = vec![Vec::new(); target_partitions];
    for (index, entry) in entries.enumerate() {
        groups[index % target_partitions].push(entry);
    }
    Some(groups.into_iter().map(FileGroup::new).collect())
}

/// Refuse a scan whose table declares partition columns.
///
/// A `PARTITIONED BY` column lives in the path of a file. A reader that cannot
/// map its rows to such a path refuses the table. A Zarr table is made of
/// groups inside a store, not of files, and Atlas and BBF read a collection as
/// one unit. A query that silently drops a column it asked for is worse than
/// an error.
///
/// `format` names the reader in the error, since a user reaches this through
/// `CREATE EXTERNAL TABLE ... PARTITIONED BY` and needs to know which format
/// refused.
pub fn reject_partition_columns(format: &str, config: &FileScanConfig) -> Result<()> {
    let columns = config.table_partition_cols();
    if columns.is_empty() {
        return Ok(());
    }

    let names: Vec<&str> = columns.iter().map(|field| field.name().as_str()).collect();
    Err(DataFusionError::NotImplemented(format!(
        "{format} does not support partitioned tables (PARTITIONED BY {}). \
         A partition column is part of a file's path, and this reader scans a \
         collection as one unit, so it cannot tell which file a row came from.",
        names.join(", ")
    )))
}

/// The memory a cached open claims in DataFusion's file metadata cache.
///
/// An opened dataset holds lazy arrays and parsed headers, and no reader
/// reports their size. This fixed claim keeps the entries countable against
/// the cache limit. An entry leaves the cache when the last part of its file
/// opens, so it holds only the files of scans that run now. No plan goes in
/// this cache. See [`ScanPlans`].
const CACHED_OPEN_SIZE: usize = 256 * 1024;

/// Everything a plan of a file depends on, apart from the file itself.
///
/// [`FileRead::plan`] reads the narrowed dataset, the projected schema,
/// `batch_size`, the predicate and the partition columns with their values.
/// The file is the entry key (path, size and modification time), and the
/// reader is the source that owns the [`ScanPlans`]. The narrowing is
/// [`OpenFile::narrow_tag`]. A part uses a plan only when the fingerprints are
/// equal, so a source clone with another configuration never uses it.
struct ScanFingerprint {
    narrow_tag: String,
    projected_schema: SchemaRef,
    batch_size: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    partition_fields: Vec<FieldRef>,
    partition_values: Vec<ScalarValue>,
}

impl PartialEq for ScanFingerprint {
    fn eq(&self, other: &Self) -> bool {
        // The predicate compares as an expression tree, not as its display text.
        let predicates_equal = match (&self.predicate, &other.predicate) {
            (None, None) => true,
            (Some(ours), Some(theirs)) => ours.as_ref() == theirs.as_ref(),
            _ => false,
        };
        predicates_equal
            && self.batch_size == other.batch_size
            && self.narrow_tag == other.narrow_tag
            && self.projected_schema == other.projected_schema
            && self.partition_fields == other.partition_fields
            && self.partition_values == other.partition_values
    }
}

/// A planned file: the pruned unit list, and whether narrow skipped the file.
struct PlannedFile {
    read: FileRead,
    skipped: bool,
}

/// One file's plan, set by the first part that needs it.
type PlanCell = tokio::sync::OnceCell<Arc<PlannedFile>>;

/// A plan of one file for one configuration, and the parts that took it.
struct PlanEntry {
    /// The file's size and modification time in nanoseconds, when planned.
    version: (u64, Option<i64>),
    fingerprint: ScanFingerprint,
    plan: Arc<PlanCell>,
    taken: usize,
}

/// The plans of the split files of one scan.
///
/// A format's source owns one, and the source is part of one query's plan, so
/// no other query sees these plans. The source's clones share it: DataFusion
/// clones the source for each partition of a run. The [`ScanFingerprint`] of
/// an entry keeps a clone with another configuration off it.
///
/// An entry leaves when the last part of its file takes it. What a stopped run
/// leaves is dropped with the source.
#[derive(Default)]
pub struct ScanPlans {
    files: Mutex<HashMap<String, Vec<PlanEntry>>>,
}

impl std::fmt::Debug for ScanPlans {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanPlans")
            .field("files", &self.files.lock().len())
            .finish()
    }
}

impl ScanPlans {
    /// Whether no plan is held. For tests and diagnostics.
    pub fn is_empty(&self) -> bool {
        self.files.lock().is_empty()
    }

    /// The plan cell of `file` for `fingerprint`, added when it is missing.
    ///
    /// Each call takes one part of the file. The call that takes the last
    /// part removes the entry, so a later run of the same plan builds anew.
    fn take(
        &self,
        file: &PartitionedFile,
        fingerprint: ScanFingerprint,
        count: usize,
    ) -> Arc<PlanCell> {
        let meta = &file.object_meta;
        let key = meta.location.to_string();
        let version = (meta.size, meta.last_modified.timestamp_nanos_opt());
        let mut files = self.files.lock();
        let entries = files.entry(key.clone()).or_default();
        let found = entries
            .iter()
            .position(|entry| entry.version == version && entry.fingerprint == fingerprint);
        let plan = match found {
            Some(index) => {
                let entry = &mut entries[index];
                entry.taken += 1;
                let plan = Arc::clone(&entry.plan);
                if entry.taken >= count {
                    entries.remove(index);
                }
                plan
            }
            None => {
                let plan = Arc::new(PlanCell::new());
                if count > 1 {
                    entries.push(PlanEntry {
                        version,
                        fingerprint,
                        plan: Arc::clone(&plan),
                        taken: 1,
                    });
                }
                plan
            }
        };
        if entries.is_empty() {
            files.remove(&key);
        }
        plan
    }
}

/// One file's open, shared by the parts of that file.
#[derive(Default)]
struct OpenCell {
    /// Set by the first part that opens the file. The other parts wait on it.
    dataset: tokio::sync::OnceCell<AnyDataset>,
    /// Parts of the file that took the open.
    taken: AtomicUsize,
}

/// The value this module puts in DataFusion's file metadata cache.
struct CachedOpen {
    tag: String,
    cell: Arc<OpenCell>,
}

impl FileMetadata for CachedOpen {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn memory_size(&self) -> usize {
        CACHED_OPEN_SIZE
    }

    fn extra_info(&self) -> HashMap<String, String> {
        HashMap::from([("nd_reader".to_string(), self.tag.clone())])
    }
}

/// What a scan reads from each file, for one partition.
///
/// A format builds one in `create_morselizer` and hands it to
/// [`NdMorselizer`], or to [`NdFileOpener`] for a direct caller.
#[derive(Clone)]
pub struct NdScan {
    pub files: Arc<dyn OpenFile>,
    pub projected_schema: SchemaRef,
    pub batch_size: usize,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    /// The table's `PARTITIONED BY` columns, nd-encoded as the scan carries
    /// them. Each file brings its own values on its `PartitionedFile`.
    pub partition_fields: Vec<FieldRef>,
    /// This partition's counters, registered once. See [`ReadMetrics::new`].
    pub metrics: ReadMetrics,
    /// DataFusion's file metadata cache, from the session's runtime. Scans
    /// share an open through it. With `None`, the part that builds a plan
    /// opens the file for it.
    pub metadata_cache: Option<Arc<dyn FileMetadataCache>>,
    /// The plans of this scan's split files. The source owns them, so they
    /// belong to one query.
    pub plans: Arc<ScanPlans>,
}

impl NdScan {
    /// The scan `config` describes, for `partition`.
    pub fn new(
        files: Arc<dyn OpenFile>,
        config: &FileScanConfig,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        metrics: ReadMetrics,
        metadata_cache: Option<Arc<dyn FileMetadataCache>>,
        plans: Arc<ScanPlans>,
    ) -> Result<Self> {
        Ok(Self {
            files,
            projected_schema: config.projected_schema()?,
            batch_size,
            predicate,
            partition_fields: config.table_partition_cols().clone(),
            metrics,
            metadata_cache,
            plans,
        })
    }

    /// Open the part of a file that `file` names, and plan its slice.
    ///
    /// The parts of a split file share one plan in [`ScanPlans`]. The first
    /// part that needs it builds it, and the others wait. Part 0 of each run
    /// reports the pruned chunks and a skipped file, so that each run counts
    /// them once, whichever part built the plan.
    pub async fn read(&self, file: &PartitionedFile) -> Result<FileRead> {
        let part = FilePart::of(file);
        self.plan(file, part).await.map_err(|error| {
            // DataFusion's file stream does not add the path to an error.
            DataFusionError::Execution(format!(
                "Failed to open {}: {error}",
                file.object_meta.location
            ))
        })
    }

    async fn plan(&self, file: &PartitionedFile, part: FilePart) -> Result<FileRead> {
        if part.count <= 1 {
            let planned = self.plan_dataset(self.files.open(file).await?, file).await?;
            self.report(&planned);
            return Ok(planned.read);
        }

        let cell = self.plans.take(file, self.fingerprint(file), part.count);
        let cache = self.metadata_cache.as_deref();
        let planned = match cell.get() {
            // The plan is there, so this part needs no open. It still counts
            // as a part of the cached open, so that the last part takes the
            // entry out of the cache.
            Some(planned) => {
                if let Some(cache) = cache {
                    count_open(cache, file, &self.files.cache_tag(), part, None);
                }
                Arc::clone(planned)
            }
            None => {
                let opened = match cache {
                    Some(cache) => Some(self.open_cached(cache, file, part).await?),
                    None => None,
                };
                let planned = cell
                    .get_or_try_init(|| async {
                        let dataset = match opened {
                            Some(dataset) => dataset,
                            None => self.files.open(file).await?,
                        };
                        Ok::<_, DataFusionError>(Arc::new(self.plan_dataset(dataset, file).await?))
                    })
                    .await?;
                Arc::clone(planned)
            }
        };
        if part.index == 0 {
            self.report(&planned);
        }
        Ok(planned.read.slice(part.index, part.count))
    }

    /// Report a file's pruned chunks, or its skip, into this partition's
    /// counters.
    fn report(&self, planned: &PlannedFile) {
        if planned.skipped {
            self.metrics.files_skipped.add(1);
            return;
        }
        let pruned = planned.read.pruned();
        self.metrics.chunks_pruned.add(pruned.chunks);
        self.metrics.rows_pruned.add(pruned.rows);
    }

    /// Narrow `dataset` and plan it.
    async fn plan_dataset(&self, dataset: AnyDataset, file: &PartitionedFile) -> Result<PlannedFile> {
        let Some(dataset) = self.files.narrow(dataset)? else {
            return Ok(PlannedFile {
                read: FileRead::skipped(),
                skipped: true,
            });
        };
        let read = FileRead::plan(
            dataset,
            self.projected_schema.clone(),
            self.batch_size,
            self.predicate.clone(),
            FilePartitions::new(self.partition_fields.clone(), file.partition_values.clone()),
        )
        .await?;
        Ok(PlannedFile {
            read,
            skipped: false,
        })
    }

    /// Everything this scan's plan of `file` depends on.
    fn fingerprint(&self, file: &PartitionedFile) -> ScanFingerprint {
        ScanFingerprint {
            narrow_tag: self.files.narrow_tag(),
            projected_schema: self.projected_schema.clone(),
            batch_size: self.batch_size,
            predicate: self.predicate.clone(),
            partition_fields: self.partition_fields.clone(),
            partition_values: file.partition_values.clone(),
        }
    }

    /// Open the file through DataFusion's metadata cache.
    ///
    /// The parts of one file share one open. The last part of the file takes
    /// the entry out, so the cache does not keep the file open after the scan.
    async fn open_cached(
        &self,
        cache: &dyn FileMetadataCache,
        file: &PartitionedFile,
        part: FilePart,
    ) -> Result<AnyDataset> {
        let tag = self.files.cache_tag();
        let cell = match cached_cell(cache, file, &tag) {
            Some(cell) => cell,
            None => {
                let cell = Arc::new(OpenCell::default());
                let value = CachedOpen {
                    tag: tag.clone(),
                    cell: Arc::clone(&cell),
                };
                cache.put(
                    &file.object_meta.location,
                    CachedFileMetadataEntry::new(file.object_meta.clone(), Arc::new(value)),
                );
                cell
            }
        };

        let opened = cell
            .dataset
            .get_or_try_init(|| self.files.open(file))
            .await
            .cloned();
        count_open(cache, file, &tag, part, Some(&cell));
        opened
    }
}

/// Count one part of `file` against its cached open, the one in `cell` or the
/// one the cache holds. The last part takes the entry out of the cache.
fn count_open(
    cache: &dyn FileMetadataCache,
    file: &PartitionedFile,
    tag: &str,
    part: FilePart,
    cell: Option<&Arc<OpenCell>>,
) {
    let Some(cell) = cell.cloned().or_else(|| cached_cell(cache, file, tag)) else {
        return;
    };
    let taken = cell.taken.fetch_add(1, Ordering::AcqRel) + 1;
    if taken >= part.count
        && cached_cell(cache, file, tag).is_some_and(|cached| Arc::ptr_eq(&cached, &cell))
    {
        cache.remove(&file.object_meta.location);
    }
}

/// The open cell the cache holds for `file`, when it is valid for this reader.
fn cached_cell(
    cache: &dyn FileMetadataCache,
    file: &PartitionedFile,
    tag: &str,
) -> Option<Arc<OpenCell>> {
    let entry = cache.get(&file.object_meta.location)?;
    if !entry.is_valid_for(&file.object_meta) {
        return None;
    }
    let cached = entry.file_metadata.as_any().downcast_ref::<CachedOpen>()?;
    (cached.tag == tag).then(|| Arc::clone(&cached.cell))
}

/// One partition's [`Morselizer`] for an nd format.
///
/// A format returns this from `FileSource::create_morselizer`.
pub struct NdMorselizer {
    scan: NdScan,
}

impl NdMorselizer {
    pub fn new(scan: NdScan) -> Self {
        Self { scan }
    }
}

impl std::fmt::Debug for NdMorselizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NdMorselizer").finish_non_exhaustive()
    }
}

impl Morselizer for NdMorselizer {
    fn plan_file(&self, file: PartitionedFile) -> Result<Box<dyn MorselPlanner>> {
        Ok(Box::new(PartPlanner {
            file,
            scan: self.scan.clone(),
        }))
    }
}

/// Opens one queue entry: a whole file, or one part of it.
struct PartPlanner {
    file: PartitionedFile,
    scan: NdScan,
}

impl std::fmt::Debug for PartPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartPlanner")
            .field("file", &self.file.object_meta.location)
            .field("part", &FilePart::of(&self.file))
            .finish_non_exhaustive()
    }
}

impl MorselPlanner for PartPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let Self { file, scan } = *self;
        let open = async move {
            let read = scan.read(&file).await?;
            Ok(Box::new(OpenedPlanner {
                read,
                metrics: scan.metrics,
            }) as Box<dyn MorselPlanner>)
        };
        Ok(Some(MorselPlan::new().with_pending_planner(open)))
    }
}

/// An opened part: one morsel per unit of its slice.
struct OpenedPlanner {
    read: FileRead,
    metrics: ReadMetrics,
}

impl std::fmt::Debug for OpenedPlanner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenedPlanner")
            .field("units", &self.read.units())
            .finish_non_exhaustive()
    }
}

impl MorselPlanner for OpenedPlanner {
    fn plan(self: Box<Self>) -> Result<Option<MorselPlan>> {
        let morsels: Vec<Box<dyn Morsel>> = self
            .read
            .into_streams(Some(self.metrics))
            .into_iter()
            .map(|stream| Box::new(UnitMorsel { stream }) as Box<dyn Morsel>)
            .collect();
        if morsels.is_empty() {
            return Ok(None);
        }
        Ok(Some(MorselPlan::new().with_morsels(morsels)))
    }
}

/// One unit of a file: one chunk of a regular dataset, or one batch range of
/// a ragged one.
///
/// The stream is lazy. The unit is read when DataFusion makes it the active
/// reader.
struct UnitMorsel {
    stream: BoxStream<'static, Result<RecordBatch>>,
}

impl std::fmt::Debug for UnitMorsel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnitMorsel").finish_non_exhaustive()
    }
}

impl Morsel for UnitMorsel {
    fn into_stream(self: Box<Self>) -> BoxStream<'static, Result<RecordBatch>> {
        self.stream
    }
}

/// A plain [`FileOpener`] for an nd format: open one entry and read it.
///
/// For callers that ask a source for its opener directly. A scan goes through
/// [`NdMorselizer`].
pub struct NdFileOpener {
    scan: NdScan,
}

impl NdFileOpener {
    pub fn new(scan: NdScan) -> Self {
        Self { scan }
    }
}

impl FileOpener for NdFileOpener {
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let scan = self.scan.clone();
        Ok(async move {
            let read = scan.read(&file).await?;
            Ok(read.stream(Some(scan.metrics)))
        }
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::datatypes::{Schema, SchemaRef};
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, FileSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::TaskContext;
    use datafusion::execution::cache::DefaultFilesMetadataCache;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::limit::GlobalLimitExec;
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
    use datafusion::prelude::SessionConfig;
    use futures::{StreamExt, TryStreamExt};
    use indexmap::IndexMap;
    use parking_lot::Mutex;

    use super::*;
    use crate::NdArray;
    use crate::NdArrayD;
    use crate::dataset::Dataset;

    /// A projection that wants no column, so a plan takes the `COUNT(*)` path
    /// and a batch carries only its row count. These tests are about who reads
    /// what, not about column values.
    fn no_columns() -> SchemaRef {
        Arc::new(Schema::empty())
    }

    /// A dataset of `rows` values on one dimension, counting up from 0.
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
        AnyDataset::Regular(Dataset::new("morsel".to_string(), arrays).await)
    }

    /// A file of `rows` rows, named `f-{index}`.
    fn file(index: usize, rows: usize) -> PartitionedFile {
        PartitionedFile::new(format!("f-{index:04}.nc"), rows as u64)
    }

    /// An opener over in-memory datasets, recording what it was asked to open.
    ///
    /// A file's row count is its `object_meta.size`, so a test says how much
    /// work each file holds by how big it claims to be.
    #[derive(Default)]
    struct Fake {
        opened: Mutex<Vec<String>>,
        /// Paths that fail to open.
        broken: HashSet<String>,
        /// Narrow calls. A plan narrows once, before its predicate masks, so
        /// this counts the plans that read the coordinate arrays.
        narrowed: std::sync::atomic::AtomicUsize,
        /// Narrow every file to nothing, as `skip_unbroadcastable` does.
        skips: bool,
    }

    impl Fake {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn breaking(path: &str) -> Arc<Self> {
            Arc::new(Self {
                broken: HashSet::from([path.to_string()]),
                ..Self::default()
            })
        }

        fn skipping() -> Arc<Self> {
            Arc::new(Self {
                skips: true,
                ..Self::default()
            })
        }

        fn opened(&self) -> Vec<String> {
            self.opened.lock().clone()
        }

        fn plans(&self) -> usize {
            self.narrowed.load(Ordering::Acquire)
        }
    }

    #[async_trait::async_trait]
    impl OpenFile for Fake {
        async fn open(&self, file: &PartitionedFile) -> Result<AnyDataset> {
            let path = file.object_meta.location.to_string();
            if self.broken.contains(&path) {
                return Err(DataFusionError::Execution("the disk said no".to_string()));
            }
            self.opened.lock().push(path);
            Ok(dataset(file.object_meta.size as usize).await)
        }

        fn narrow(&self, dataset: AnyDataset) -> Result<Option<AnyDataset>> {
            self.narrowed.fetch_add(1, Ordering::AcqRel);
            Ok((!self.skips).then_some(dataset))
        }

        fn cache_tag(&self) -> String {
            "fake".to_string()
        }
    }

    /// A [`FileSource`] over [`Fake`], wired the way the nd formats wire theirs.
    ///
    /// A clone shares the plans, as a format's clones do. [`Self::new_query`]
    /// stands for the source another query builds.
    #[derive(Clone)]
    struct FakeSource {
        opener: Arc<Fake>,
        batch_size: usize,
        predicate: Option<Arc<dyn PhysicalExpr>>,
        cache: Option<Arc<dyn FileMetadataCache>>,
        plans: Arc<ScanPlans>,
        table_schema: TableSchema,
        metrics: ExecutionPlanMetricsSet,
    }

    impl FakeSource {
        fn new(opener: Arc<Fake>, batch_size: usize) -> Self {
            Self {
                opener,
                batch_size,
                predicate: None,
                cache: None,
                plans: Arc::default(),
                table_schema: TableSchema::from_file_schema(no_columns()),
                metrics: ExecutionPlanMetricsSet::new(),
            }
        }

        /// The source of another query over the same files, opener and cache.
        fn new_query(&self) -> Self {
            Self {
                plans: Arc::default(),
                metrics: ExecutionPlanMetricsSet::new(),
                ..self.clone()
            }
        }

        fn with_cache(mut self) -> Self {
            self.cache = Some(Arc::new(DefaultFilesMetadataCache::new(64 * 1024 * 1024)));
            self
        }

        fn with_predicate(mut self, predicate: Arc<dyn PhysicalExpr>) -> Self {
            self.predicate = Some(predicate);
            self
        }

        fn scan(&self, config: &FileScanConfig, partition: usize) -> Result<NdScan> {
            NdScan::new(
                self.opener.clone(),
                config,
                self.batch_size,
                self.predicate.clone(),
                ReadMetrics::new(&self.metrics, partition),
                self.cache.clone(),
                Arc::clone(&self.plans),
            )
        }

        fn metric(&self, name: &str) -> usize {
            self.metrics
                .clone_inner()
                .sum_by_name(name)
                .map_or(0, |value| value.as_usize())
        }
    }

    impl FileSource for FakeSource {
        fn create_file_opener(
            &self,
            _object_store: Arc<dyn object_store::ObjectStore>,
            config: &FileScanConfig,
            partition: usize,
        ) -> Result<Arc<dyn FileOpener>> {
            Ok(Arc::new(NdFileOpener::new(self.scan(config, partition)?)))
        }

        fn create_morselizer(
            &self,
            _object_store: Arc<dyn object_store::ObjectStore>,
            config: &FileScanConfig,
            partition: usize,
        ) -> Result<Box<dyn Morselizer>> {
            Ok(Box::new(NdMorselizer::new(self.scan(config, partition)?)))
        }

        fn table_schema(&self) -> &TableSchema {
            &self.table_schema
        }

        fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(self.clone())
        }

        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }

        fn file_type(&self) -> &str {
            "fake"
        }
    }

    /// The groups a format plans for `files` over `partitions`.
    fn groups(files: Vec<PartitionedFile>, partitions: usize) -> Vec<FileGroup> {
        split_files(&[FileGroup::new(files.clone())], partitions)
            .unwrap_or_else(|| vec![FileGroup::new(files)])
    }

    fn config(source: &FakeSource, groups: Vec<FileGroup>) -> FileScanConfig {
        FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(source.clone()) as Arc<dyn FileSource>,
        )
        .with_file_groups(groups)
        .build()
    }

    /// A scan of `files` split over `partitions`, as a format plans it.
    fn scan(source: &FakeSource, files: Vec<PartitionedFile>, partitions: usize) -> Arc<dyn ExecutionPlan> {
        DataSourceExec::from_data_source(config(source, groups(files, partitions)))
    }

    fn context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    /// The longest any scan here may take before it is treated as stuck.
    const BEFORE_STUCK: std::time::Duration = std::time::Duration::from_secs(20);

    /// Run every partition of `plan` at the same time. Return the rows each
    /// partition read.
    async fn run(plan: &Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Result<Vec<usize>> {
        let partitions = plan.output_partitioning().partition_count();
        let mut tasks = Vec::new();
        for partition in 0..partitions {
            let stream = plan.execute(partition, Arc::clone(&context))?;
            tasks.push(tokio::spawn(async move {
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                Ok::<usize, DataFusionError>(batches.iter().map(|b| b.num_rows()).sum())
            }));
        }

        let mut rows = Vec::new();
        for (partition, task) in tasks.into_iter().enumerate() {
            let read = tokio::time::timeout(BEFORE_STUCK, task)
                .await
                .unwrap_or_else(|_| panic!("partition {partition} never finished"))
                .expect("the partition finishes")?;
            rows.push(read);
        }
        Ok(rows)
    }

    /// Start every partition one after the other, then drain them all.
    ///
    /// Each partition takes its first batch before the next partition starts.
    /// A partition holds its entry until it has read every morsel of it, so
    /// partition `k` takes the `k`-th entry of the queue. This makes the spread
    /// over the partitions deterministic.
    async fn run_in_turn(plan: &Arc<dyn ExecutionPlan>, context: Arc<TaskContext>) -> Vec<usize> {
        let partitions = plan.output_partitioning().partition_count();
        let mut streams = Vec::new();
        let mut rows = Vec::new();
        for partition in 0..partitions {
            let mut stream = plan.execute(partition, Arc::clone(&context)).unwrap();
            let first = stream.next().await.transpose().unwrap();
            rows.push(first.map_or(0, |batch| batch.num_rows()));
            streams.push(stream);
        }
        for (partition, stream) in streams.into_iter().enumerate() {
            let rest: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            rows[partition] += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        rows
    }

    /// The split: few files become parts, many files stay whole.
    #[test]
    fn a_scan_is_split_to_fill_its_partitions() {
        const PARTITIONS: usize = 24;

        for (files, parts) in [(1, 24), (3, 8), (5, 5), (23, 2), (24, 1), (3_584, 1)] {
            let planned = split_files(
                &[FileGroup::new((0..files).map(|i| file(i, 32)).collect())],
                PARTITIONS,
            )
            .unwrap_or_else(|| panic!("{files} files are split"));

            assert_eq!(planned.len(), PARTITIONS, "{files} files: one group per partition");
            let entries: Vec<&PartitionedFile> = planned.iter().flat_map(FileGroup::iter).collect();
            assert_eq!(entries.len(), files * parts, "{files} files: {parts} parts each");
            assert!(
                entries.iter().all(|entry| entry.range.is_none()),
                "no entry is a byte range"
            );

            // The first groups start on different files.
            let starts: HashSet<String> = planned
                .iter()
                .take(files.min(PARTITIONS))
                .map(|group| group.iter().next().unwrap().object_meta.location.to_string())
                .collect();
            assert_eq!(starts.len(), files.min(PARTITIONS), "{files} files: distinct starts");

            // Each file appears with every part index once.
            let mut seen: HashSet<(String, usize)> = HashSet::new();
            for entry in &entries {
                let part = FilePart::of(entry);
                assert_eq!(part.count, parts);
                assert!(seen.insert((entry.object_meta.location.to_string(), part.index)));
            }
        }
    }

    /// One partition, or no files, is left alone.
    #[test]
    fn a_scan_with_nothing_to_divide_is_left_alone() {
        let group = FileGroup::new((0..10).map(|i| file(i, 32)).collect());
        assert!(split_files(&[group], 1).is_none(), "one partition divides nothing");
        assert!(split_files(&[], 8).is_none(), "no files to divide");
        assert!(
            split_files(&[FileGroup::new(vec![])], 8).is_none(),
            "an empty group is no files either"
        );
    }

    /// A partitioned table splits like any other, values and all.
    #[test]
    fn a_split_keeps_the_values_of_each_path() {
        use datafusion::scalar::ScalarValue;

        let year = |value: &str| vec![ScalarValue::Utf8(Some(value.to_string()))];
        let mut first = file(0, 32);
        first.partition_values = year("2023");
        let mut second = file(1, 32);
        second.partition_values = year("2024");
        let planned = split_files(&[FileGroup::new(vec![first, second])], 8).unwrap();

        for entry in planned.iter().flat_map(FileGroup::iter) {
            let expected = if entry.object_meta.location.as_ref() == "f-0000.nc" {
                year("2023")
            } else {
                year("2024")
            };
            assert_eq!(entry.partition_values, expected, "each part keeps its file's values");
        }
    }

    /// One big file over many partitions: every chunk once, on several
    /// partitions.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn one_big_file_is_read_once_by_several_partitions() {
        const ROWS: usize = 1_024;
        const BATCH: usize = 16;
        const PARTITIONS: usize = 8;

        let source = FakeSource::new(Fake::new(), BATCH);
        let plan = scan(&source, vec![file(0, ROWS)], PARTITIONS);

        let rows = run_in_turn(&plan, context()).await;

        assert_eq!(rows.iter().sum::<usize>(), ROWS, "every row once");
        assert_eq!(source.metric("chunks_read"), ROWS / BATCH, "every chunk once");
        assert!(
            rows.iter().filter(|read| **read > 0).count() > 1,
            "more than one partition read the file: {rows:?}"
        );
        assert_eq!(rows, vec![ROWS / PARTITIONS; PARTITIONS], "the parts are balanced");
    }

    /// The same scan with all partitions at once still reads every row once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn one_big_file_read_by_racing_partitions_returns_every_row_once() {
        const ROWS: usize = 1_024;
        const PARTITIONS: usize = 8;

        let source = FakeSource::new(Fake::new(), 16).with_cache();
        let plan = scan(&source, vec![file(0, ROWS)], PARTITIONS);

        let rows = run(&plan, context()).await.expect("it reads");
        assert_eq!(rows.iter().sum::<usize>(), ROWS);
    }

    /// Many small files: no split, and every row once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn many_small_files_are_not_split_and_read_once() {
        const FILES: usize = 200;
        const ROWS: usize = 64;
        const PARTITIONS: usize = 8;

        let opener = Fake::new();
        let source = FakeSource::new(opener.clone(), 16);
        let plan = scan(&source, (0..FILES).map(|i| file(i, ROWS)).collect(), PARTITIONS);

        let rows = run(&plan, context()).await.expect("it reads");

        assert_eq!(rows.iter().sum::<usize>(), FILES * ROWS, "every row once");
        let opened = opener.opened();
        assert_eq!(opened.len(), FILES, "every file was opened once");
        assert_eq!(opened.iter().collect::<HashSet<_>>().len(), FILES, "no file twice");
    }

    /// Without the cache, the part that builds the plan opens the file, and
    /// the other parts need no open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn without_the_cache_the_part_that_plans_opens_the_file() {
        const PARTITIONS: usize = 4;

        let opener = Fake::new();
        let source = FakeSource::new(opener.clone(), 16);
        let plan = scan(&source, vec![file(0, 256)], PARTITIONS);

        let rows = run_in_turn(&plan, context()).await;
        assert_eq!(rows.iter().sum::<usize>(), 256);
        assert_eq!(opener.opened().len(), 1, "one open for the plan");
        assert!(source.plans.is_empty(), "the last part took the plan");
    }

    /// With the cache, the parts of a file share one open.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn with_the_cache_the_parts_of_a_file_share_one_open() {
        const PARTITIONS: usize = 8;

        let opener = Fake::new();
        let source = FakeSource::new(opener.clone(), 16).with_cache();
        let plan = scan(&source, vec![file(0, 512), file(1, 512)], PARTITIONS);

        let rows = run_in_turn(&plan, context()).await;

        assert_eq!(rows.iter().sum::<usize>(), 1_024, "every row once");
        let mut opened = opener.opened();
        opened.sort();
        assert_eq!(opened, ["f-0000.nc", "f-0001.nc"], "one open per file, not per part");
        let cache = source.cache.as_ref().unwrap();
        assert!(
            cache.list_entries().is_empty(),
            "the last part of each file took its entry out of the cache"
        );
    }

    /// A cached open of another reader is not used.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cached_open_of_another_reader_is_not_used() {
        let opener = Fake::new();
        let source = FakeSource::new(opener.clone(), 16).with_cache();
        let cache = source.cache.clone().unwrap();
        let entry = file(0, 256);

        struct Other;
        impl FileMetadata for Other {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
            fn memory_size(&self) -> usize {
                1
            }
            fn extra_info(&self) -> HashMap<String, String> {
                HashMap::new()
            }
        }
        cache.put(
            &entry.object_meta.location,
            CachedFileMetadataEntry::new(entry.object_meta.clone(), Arc::new(Other)),
        );

        let plan = scan(&source, vec![entry], 2);
        let rows = run_in_turn(&plan, context()).await;
        assert_eq!(rows.iter().sum::<usize>(), 256);
        assert_eq!(opener.opened().len(), 1, "the parts opened the file themselves, once");
    }

    /// A scan runs again after `reset_state`, returns the same rows, and
    /// counts its pruned chunks once per run.
    ///
    /// A finished run took every entry out of the plans, so the second run
    /// builds its plans again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_scan_runs_again_after_reset_state() {
        const ROWS: usize = 512;
        const BATCH: usize = 16;
        const PARTITIONS: usize = 4;
        // The rows at or below 255 hold nothing the predicate keeps.
        const PRUNED_PER_FILE: usize = 256 / BATCH;

        for files in [1_usize, 30] {
            let opener = Fake::new();
            let source = FakeSource::new(opener.clone(), BATCH)
                .with_cache()
                .with_predicate(greater_than(255));
            let plan = scan(&source, (0..files).map(|i| file(i, ROWS)).collect(), PARTITIONS);

            let first = run(&plan, context()).await.expect("the first run reads");
            assert_eq!(first.iter().sum::<usize>(), files * 256, "{files} files");
            assert_eq!(source.metric("chunks_pruned"), files * PRUNED_PER_FILE, "{files} files");

            let again = Arc::clone(&plan).reset_state().expect("the plan resets");
            let second = run(&again, context()).await.expect("the second run reads");
            assert_eq!(second.iter().sum::<usize>(), files * 256, "{files} files, rerun");
            assert_eq!(
                source.metric("chunks_pruned"),
                2 * files * PRUNED_PER_FILE,
                "{files} files: each run counts once"
            );
            assert!(source.plans.is_empty(), "{files} files: no plan is left");
        }
    }

    /// A run that stops early does not change the rows of the next run, and
    /// each run counts its pruned chunks once.
    ///
    /// The first run takes one batch of part 0 and stops. Its plan stays with
    /// three parts not taken. The next run takes that plan for its first three
    /// parts, and builds a new one for the last. Either plan comes from the
    /// same, unchanged file, so every row comes back once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_cancelled_run_is_followed_by_a_correct_run() {
        const ROWS: usize = 1_024;
        const BATCH: usize = 16;
        const PARTITIONS: usize = 4;

        let opener = Fake::new();
        let source = FakeSource::new(opener.clone(), BATCH)
            .with_cache()
            .with_predicate(greater_than(767));
        let plan = scan(&source, vec![file(0, ROWS)], PARTITIONS);

        let context = context();
        let mut stream = plan.execute(0, Arc::clone(&context)).unwrap();
        stream.next().await.expect("one batch").expect("it reads");
        drop(stream);
        assert_eq!(source.metric("chunks_pruned"), 48, "the stopped run counted once");

        let again = Arc::clone(&plan).reset_state().expect("the plan resets");
        let rows = run(&again, Arc::clone(&context)).await.expect("the next run reads");
        assert_eq!(rows.iter().sum::<usize>(), ROWS / 4, "every kept row once");
        assert_eq!(source.metric("chunks_pruned"), 96, "and the next run counted once");
        assert_eq!(opener.plans(), 2, "the next run built a plan for its last part");
    }

    /// With work stealing off, each partition reads its own entries, and the
    /// scan still returns every row once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_scan_without_work_stealing_reads_every_row_once() {
        const PARTITIONS: usize = 4;

        for files in [1_usize, 10] {
            let source = FakeSource::new(Fake::new(), 16).with_cache();
            let plan = scan(&source, (0..files).map(|i| file(i, 256)).collect(), PARTITIONS);

            let mut config = SessionConfig::new();
            config.options_mut().execution.enable_file_stream_work_stealing = false;
            let context = Arc::new(TaskContext::default().with_session_config(config));

            let rows = run(&plan, context).await.expect("it reads");
            assert_eq!(rows.iter().sum::<usize>(), files * 256, "{files} files");
        }
    }

    /// An ordered scan, and one partitioned by file group, keep each partition
    /// on its own group, and still return every row once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_scan_that_keeps_its_groups_reads_every_row_once() {
        const PARTITIONS: usize = 4;

        for files in [1_usize, 10] {
            for ordered in [true, false] {
                let source = FakeSource::new(Fake::new(), 16);
                let files_list: Vec<_> = (0..files).map(|i| file(i, 256)).collect();
                let mut config = config(&source, groups(files_list, PARTITIONS));
                if ordered {
                    config.preserve_order = true;
                } else {
                    config.partitioned_by_file_group = true;
                }
                let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);

                let rows = run(&plan, context()).await.expect("it reads");
                assert_eq!(
                    rows.iter().sum::<usize>(),
                    files * 256,
                    "{files} files, ordered={ordered}"
                );
            }
        }
    }

    /// A `LIMIT` scan finishes and returns the limit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn a_limit_scan_finishes_and_returns_the_limit() {
        const LIMIT: usize = 100;

        let source = FakeSource::new(Fake::new(), 16).with_cache();
        let config = FileScanConfigBuilder::from(config(&source, groups(vec![file(0, 4_096)], 8)))
            .with_limit(Some(LIMIT))
            .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
        let plan: Arc<dyn ExecutionPlan> = Arc::new(GlobalLimitExec::new(
            Arc::new(CoalescePartitionsExec::new(scan)),
            0,
            Some(LIMIT),
        ));

        let batches = tokio::time::timeout(
            BEFORE_STUCK,
            datafusion::physical_plan::collect(plan, context()),
        )
        .await
        .expect("the limit scan finishes")
        .expect("it reads");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), LIMIT);
    }

    /// `value > threshold`, built anew on each call.
    fn greater_than(threshold: i64) -> Arc<dyn PhysicalExpr> {
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::expressions::{BinaryExpr, Column, Literal};

        Arc::new(BinaryExpr::new(
            Arc::new(Column::new("value", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int64(Some(threshold)))),
        ))
    }

    /// A split file with a predicate: the predicate still skips chunks, the
    /// skip counts once, and the parts divide the kept chunks.
    ///
    /// One query builds the chunk list once, with or without the metadata
    /// cache, so its coordinate arrays are read once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_split_file_with_a_predicate_is_planned_once_per_query() {
        const ROWS: usize = 1_024;
        const BATCH: usize = 16;
        const PARTITIONS: usize = 4;

        for cached in [true, false] {
            let opener = Fake::new();
            let mut source = FakeSource::new(opener.clone(), BATCH).with_predicate(greater_than(767));
            if cached {
                source = source.with_cache();
            }
            let plan = scan(&source, vec![file(0, ROWS)], PARTITIONS);

            let rows = run_in_turn(&plan, context()).await;

            // The fixture counts up, so the first three quarters hold nothing
            // above 767.
            assert_eq!(rows.iter().sum::<usize>(), ROWS / 4, "cached={cached}: kept chunks only");
            assert!(
                rows.iter().all(|read| *read == ROWS / 4 / PARTITIONS),
                "cached={cached}: the parts divide the kept chunks: {rows:?}"
            );
            assert_eq!(
                source.metric("chunks_pruned"),
                3 * ROWS / 4 / BATCH,
                "cached={cached}: the skip counts once per file"
            );
            assert_eq!(
                source.metric("rows_pruned"),
                3 * ROWS / 4,
                "cached={cached}: the pruned rows count once per file"
            );
            assert_eq!(opener.plans(), 1, "cached={cached}: one plan");
            assert_eq!(opener.opened().len(), 1, "cached={cached}: one open");
            assert!(source.plans.is_empty(), "cached={cached}: the plan left with the query");
        }
    }

    /// Racing partitions over a cached split file still read every kept row
    /// once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn racing_parts_of_a_cached_plan_read_every_kept_row_once() {
        const ROWS: usize = 4_096;
        const PARTITIONS: usize = 8;

        for _ in 0..20 {
            let source = FakeSource::new(Fake::new(), 16)
                .with_cache()
                .with_predicate(greater_than(1_023));
            let plan = scan(&source, vec![file(0, ROWS)], PARTITIONS);
            let rows = run(&plan, context()).await.expect("it reads");
            assert_eq!(rows.iter().sum::<usize>(), ROWS - 1_024);
        }
    }

    /// A source clone with another predicate shares the plans of the source,
    /// and still never uses a plan built for the first predicate.
    ///
    /// Filter pushdown makes such a clone. Scan A plans and holds its first two
    /// parts. Scan B then runs in full on the same plans and the same cached
    /// open. Scan A then finishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_clone_with_another_predicate_does_not_use_the_plan() {
        const ROWS: usize = 1_024;
        const PARTITIONS: usize = 4;

        let opener = Fake::new();
        let first = FakeSource::new(opener.clone(), 16)
            .with_cache()
            .with_predicate(greater_than(767));
        let second = first.clone().with_predicate(greater_than(511));
        let first_plan = scan(&first, vec![file(0, ROWS)], PARTITIONS);
        let second_plan = scan(&second, vec![file(0, ROWS)], PARTITIONS);

        let context = context();
        let mut held = Vec::new();
        let mut first_rows = 0;
        for partition in 0..2 {
            let mut stream = first_plan.execute(partition, Arc::clone(&context)).unwrap();
            first_rows += stream.next().await.unwrap().unwrap().num_rows();
            held.push(stream);
        }

        let second_rows: usize = run_in_turn(&second_plan, Arc::clone(&context)).await.iter().sum();
        assert_eq!(second_rows, ROWS / 2, "the second scan keeps the rows above 511");

        for stream in held {
            let rest: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            first_rows += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        for partition in 2..PARTITIONS {
            let stream = first_plan.execute(partition, Arc::clone(&context)).unwrap();
            let rest: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            first_rows += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        assert_eq!(first_rows, ROWS / 4, "the first scan keeps the rows above 767");

        assert_eq!(opener.plans(), 2, "one plan per predicate");
        assert!(first.plans.is_empty(), "no plan is left");
    }

    /// Two sources of one table in one run, as a self-join plans them, with
    /// different predicates: each reads its own rows from its own plan.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn two_sources_of_one_table_in_one_run_keep_their_own_plans() {
        const ROWS: usize = 1_024;
        const PARTITIONS: usize = 4;

        let opener = Fake::new();
        let left = FakeSource::new(opener.clone(), 16)
            .with_cache()
            .with_predicate(greater_than(767));
        let right = left.new_query().with_predicate(greater_than(511));
        let left_plan = scan(&left, vec![file(0, ROWS)], PARTITIONS);
        let right_plan = scan(&right, vec![file(0, ROWS)], PARTITIONS);

        let context = context();
        let (left_rows, right_rows) =
            futures::join!(run(&left_plan, Arc::clone(&context)), run(&right_plan, Arc::clone(&context)));
        assert_eq!(left_rows.unwrap().iter().sum::<usize>(), ROWS / 4, "rows above 767");
        assert_eq!(right_rows.unwrap().iter().sum::<usize>(), ROWS / 2, "rows above 511");
        assert!(opener.plans() >= 2, "each source built its own plan");
        assert!(left.plans.is_empty() && right.plans.is_empty(), "no plan is left");
    }

    /// Two queries of one file with the same predicate each build their own
    /// chunk list, even while both run.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_queries_with_the_same_predicate_each_build_their_own_list() {
        const ROWS: usize = 1_024;
        const PARTITIONS: usize = 4;

        let opener = Fake::new();
        let first = FakeSource::new(opener.clone(), 16)
            .with_cache()
            .with_predicate(greater_than(767));
        let second = first.new_query().with_predicate(greater_than(767));
        let first_plan = scan(&first, vec![file(0, ROWS)], PARTITIONS);
        let second_plan = scan(&second, vec![file(0, ROWS)], PARTITIONS);

        let context = context();
        let mut held = first_plan.execute(0, Arc::clone(&context)).unwrap();
        let mut rows = held.next().await.unwrap().unwrap().num_rows();

        let mut second_stream = second_plan.execute(0, Arc::clone(&context)).unwrap();
        let mut second_rows = second_stream.next().await.unwrap().unwrap().num_rows();
        assert_eq!(opener.plans(), 2, "the second query built its own list");

        let rest: Vec<RecordBatch> = held.try_collect().await.unwrap();
        rows += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
        let rest: Vec<RecordBatch> = second_stream.try_collect().await.unwrap();
        second_rows += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
        for partition in 1..PARTITIONS {
            for (plan, total) in [(&first_plan, &mut rows), (&second_plan, &mut second_rows)] {
                let stream = plan.execute(partition, Arc::clone(&context)).unwrap();
                let rest: Vec<RecordBatch> = stream.try_collect().await.unwrap();
                *total += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
            }
        }
        assert_eq!(rows, ROWS / 4, "the first query reads every kept row once");
        assert_eq!(second_rows, ROWS / 4, "and so does the second");
        assert_eq!(opener.plans(), 2, "neither query used the other's list");
    }

    /// DataFusion's metadata cache holds only opens, and nothing after the
    /// query.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_metadata_cache_holds_no_plan_data() {
        const PARTITIONS: usize = 4;

        let source = FakeSource::new(Fake::new(), 16)
            .with_cache()
            .with_predicate(greater_than(767));
        let cache = source.cache.clone().unwrap();
        let plan = scan(&source, vec![file(0, 1_024)], PARTITIONS);

        let context = context();
        let mut held = plan.execute(0, Arc::clone(&context)).unwrap();
        let first = held.next().await.unwrap().unwrap().num_rows();
        let entries = cache.list_entries();
        assert_eq!(entries.len(), 1, "one open while the query runs");
        assert!(
            entries.values().all(|entry| entry.size_bytes == CACHED_OPEN_SIZE),
            "the entry claims an open and nothing more: {entries:?}"
        );
        assert!(!source.plans.is_empty(), "the plan is with the query");

        let rest: Vec<RecordBatch> = held.try_collect().await.unwrap();
        let mut rows: usize = rest.iter().map(|batch| batch.num_rows()).sum::<usize>() + first;
        for partition in 1..PARTITIONS {
            let stream = plan.execute(partition, Arc::clone(&context)).unwrap();
            let rest: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            rows += rest.iter().map(|batch| batch.num_rows()).sum::<usize>();
        }
        assert_eq!(rows, 256, "every kept row once");
        assert!(cache.list_entries().is_empty(), "nothing is left in the cache");
        assert!(source.plans.is_empty(), "and no plan is left with the query");
    }

    /// A second run on the same source, after the first one finished, plans
    /// again: the first run took every entry out of the plans.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_scan_after_a_finished_scan_plans_again() {
        const ROWS: usize = 1_024;
        const PARTITIONS: usize = 4;

        let opener = Fake::new();
        let source = FakeSource::new(opener.clone(), 16)
            .with_cache()
            .with_predicate(greater_than(767));

        for run_number in 1..=2 {
            let plan = scan(&source, vec![file(0, ROWS)], PARTITIONS);
            let rows = run_in_turn(&plan, context()).await;
            assert_eq!(rows.iter().sum::<usize>(), ROWS / 4, "run {run_number}");
            assert_eq!(opener.plans(), run_number, "run {run_number}: one plan per run");
            assert_eq!(opener.opened().len(), run_number, "run {run_number}: one open per run");
        }
    }

    /// A skipped file with parts: no rows, and the skip counts once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_skipped_split_file_returns_no_rows_and_counts_once() {
        const PARTITIONS: usize = 4;

        for cached in [true, false] {
            let opener = Fake::skipping();
            let mut source = FakeSource::new(opener.clone(), 16);
            if cached {
                source = source.with_cache();
            }
            let plan = scan(&source, vec![file(0, 512)], PARTITIONS);

            let rows = run_in_turn(&plan, context()).await;
            assert_eq!(rows, vec![0; PARTITIONS], "cached={cached}: no rows");
            assert_eq!(source.metric("files_skipped"), 1, "cached={cached}: one skip");
            assert_eq!(opener.plans(), 1, "cached={cached}: one plan");
        }
    }

    /// Two scans that narrow one file differently never share a plan.
    #[test]
    fn the_fingerprint_tells_scans_apart() {
        let scan = |batch_size: usize, predicate: Option<Arc<dyn PhysicalExpr>>| NdScan {
            files: Fake::new(),
            projected_schema: no_columns(),
            batch_size,
            predicate,
            partition_fields: Vec::new(),
            metrics: ReadMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            metadata_cache: None,
            plans: Arc::default(),
        };
        let entry = file(0, 32);
        let base = scan(16, Some(greater_than(5))).fingerprint(&entry);

        assert!(
            scan(16, Some(greater_than(5))).fingerprint(&entry) == base,
            "an equal predicate built anew is the same scan"
        );
        assert!(scan(16, Some(greater_than(6))).fingerprint(&entry) != base, "another predicate");
        assert!(scan(16, None).fingerprint(&entry) != base, "no predicate");
        assert!(scan(32, Some(greater_than(5))).fingerprint(&entry) != base, "another batch size");

        let mut other_schema = scan(16, Some(greater_than(5)));
        other_schema.projected_schema = Arc::new(Schema::new(vec![
            arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Int64, true),
        ]));
        assert!(other_schema.fingerprint(&entry) != base, "another projection");

        let mut valued = entry.clone();
        valued.partition_values = vec![ScalarValue::Utf8(Some("2024".to_string()))];
        assert!(
            scan(16, Some(greater_than(5))).fingerprint(&valued) != base,
            "other partition values"
        );
    }

    /// A file that will not open fails the scan, and says which file it was.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_file_that_will_not_open_names_itself() {
        let source = FakeSource::new(Fake::breaking("f-0003.nc"), 16);
        let plan = scan(&source, (0..8).map(|i| file(i, 32)).collect(), 1);

        let error = run(&plan, context()).await.expect_err("the scan fails");
        let message = error.to_string();
        assert!(message.contains("f-0003.nc"), "the error names the file: {message}");
        assert!(
            message.contains("the disk said no"),
            "and keeps what the reader said: {message}"
        );
    }
}
