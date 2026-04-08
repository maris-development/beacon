//! DataFusion integration for Atlas collections.
//!
//! Atlas stores data as collections containing partition directories, where each
//! partition holds column chunks and metadata. DataFusion discovers top-level
//! `atlas.json` files through listing, and this module translates that metadata
//! layout into executable scan tasks.
//!
//! Execution flow:
//! 1. Listing finds one or more collection metadata files (`atlas.json`).
//! 2. `create_physical_plan` expands those collection files into partition
//!    metadata scan tasks (`atlas_partition.json`).
//! 3. Tasks are grouped to match DataFusion target partitions, including
//!    round-robin duplication when there are fewer partitions than workers.
//! 4. `AtlasSource` creates openers that share per-partition dataset queues,
//!    allowing concurrent workers to cooperatively drain remaining work.
//!
//! This design keeps the physical plan simple while enabling practical
//! work-stealing behavior for skewed collections.
//!
//! ASCII flow graph:
//! ```text
//! ListingTable scan input
//!          |
//!          v
//!   atlas.json files
//!          |
//!          v
//! +---------------------------+
//! | create_physical_plan      |
//! | - find collections        |
//! | - expand partitions       |
//! +---------------------------+
//!          |
//!          v
//! atlas_partition.json tasks
//!          |
//!          v
//! +---------------------------+
//! | build_target_partition_   |
//! | file_groups               |
//! | - balance N over T        |
//! | - round-robin duplicate   |
//! +---------------------------+
//!          |
//!          v
//! DataSourceExec (FileGroups)
//!          |
//!          v
//! AtlasSource -> AtlasOpener
//!          |
//!          v
//! Shared per-partition queues
//!          |
//!          v
//! RecordBatch stream to DataFusion
//! ```
use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        listing::PartitionedFile,
        physical_plan::{FileGroup, FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    error::DataFusionError,
    physical_plan::ExecutionPlan,
};
use object_store::{ObjectMeta, ObjectStore};

use crate::{
    array::io_cache::IoCache,
    collection::load_collection_state,
    consts::{COLLECTION_METADATA_FILE, DEFAULT_IO_CACHE_BYTES},
    datafusion::source::AtlasSource,
    schema::AtlasSchema,
};

pub mod opener;
pub mod pruning;
pub mod source;
pub mod table;

const ATLAS_FILE_EXTENSION: &str = "atlas";

fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn collect_atlas_metadata_files(conf: &FileScanConfig) -> Vec<ObjectMeta> {
    // DataFusion passes listing results as file groups; flatten to raw metadata
    // so Atlas-specific planning can re-group by partition work.
    conf.file_groups
        .iter()
        .flat_map(|group| group.files().iter())
        .map(|file| file.object_meta.clone())
        .collect()
}

/// Expand discovered Atlas collections into partition metadata scan groups.
///
/// The returned groups are aligned with DataFusion worker parallelism so that
/// all target partitions can remain busy when possible.
async fn build_partition_file_groups(
    object_store: Arc<dyn ObjectStore>,
    atlas_metas: &[ObjectMeta],
    io_cache: Arc<IoCache>,
    target_partitions: usize,
) -> datafusion::error::Result<Vec<FileGroup>> {
    let mut partition_files: Vec<PartitionedFile> = Vec::new();

    for meta in find_atlas_collections(atlas_metas) {
        let collection_dir = collection_directory_from_meta(&meta);
        let collection_state = load_collection_state(
            object_store.clone(),
            collection_dir.clone(),
            io_cache.clone(),
        )
        .await
        .map_err(|e| {
            execution_error(format!(
                "Failed to load Atlas collection at '{}': {}",
                meta.location, e
            ))
        })?;

        for partition_name in &collection_state.metadata().partitions {
            // Each partition is represented by its atlas_partition.json metadata file.
            let partition_meta_path = collection_dir
                .child("partitions")
                .child(partition_name.as_str())
                .child(crate::consts::PARTITION_METADATA_FILE);
            partition_files.push(PartitionedFile::new(partition_meta_path.to_string(), 0));
        }
    }

    // Convert the discovered partition files into worker-sized file groups.
    Ok(build_target_partition_file_groups(
        partition_files,
        target_partitions,
    ))
}

/// Build worker-oriented file groups for Atlas partition metadata scans.
///
/// When there are fewer unique partitions than workers, file paths are
/// duplicated in round-robin order. Openers then coordinate through shared
/// per-partition queues to avoid duplicate dataset processing.
fn build_target_partition_file_groups(
    partition_files: Vec<PartitionedFile>,
    target_partitions: usize,
) -> Vec<FileGroup> {
    if partition_files.is_empty() {
        return Vec::new();
    }

    let effective_target_partitions = target_partitions.max(1);
    let partition_count = partition_files.len();

    if partition_count < effective_target_partitions {
        // Duplicate partition metadata tasks in round-robin so all workers can
        // cooperate on the shared per-partition dataset queues.
        return (0..effective_target_partitions)
            .map(|i| FileGroup::new(vec![partition_files[i % partition_count].clone()]))
            .collect();
    }

    // Evenly spread unique partition metadata files across target workers.
    let groups = effective_target_partitions;
    let base_group_size = partition_count / groups;
    let remainder = partition_count % groups;
    let mut offset = 0;
    let mut file_groups = Vec::with_capacity(groups);

    for group_idx in 0..groups {
        // Spread the remainder across the earliest groups for stable balancing.
        let group_size = base_group_size + usize::from(group_idx < remainder);
        let end = offset + group_size;
        file_groups.push(FileGroup::new(partition_files[offset..end].to_vec()));
        offset = end;
    }

    file_groups
}

/// Check if this ObjectMeta represents an Atlas metadata file ("atlas.json").
pub fn is_atlas_metadata(meta: &ObjectMeta) -> bool {
    let loc = meta.location.as_ref();
    loc.ends_with(COLLECTION_METADATA_FILE)
}

/// Given a list of ObjectMeta, find all top-level atlas.json files.
/// A top-level atlas.json is one that is not nested inside a partition directory.
pub fn find_atlas_collections(objects: &[ObjectMeta]) -> Vec<ObjectMeta> {
    objects
        .iter()
        .filter(|meta| is_atlas_metadata(meta))
        .cloned()
        .collect()
}

/// Extract the collection directory path from an atlas.json ObjectMeta.
/// e.g. "collections/my-collection/atlas.json" -> "collections/my-collection"
pub fn collection_directory_from_meta(meta: &ObjectMeta) -> object_store::path::Path {
    let loc = meta.location.as_ref();
    let dir = loc
        .strip_suffix(COLLECTION_METADATA_FILE)
        .unwrap_or(loc)
        .trim_end_matches('/');
    object_store::path::Path::from(dir)
}

#[derive(Debug)]
pub struct AtlasFormatFactory;

impl AtlasFormatFactory {
    /// Create a new Atlas file-format factory.
    pub fn new() -> Self {
        Self
    }
}

impl Default for AtlasFormatFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl GetExt for AtlasFormatFactory {
    fn get_ext(&self) -> String {
        ATLAS_FILE_EXTENSION.to_string()
    }
}

impl FileFormatFactory for AtlasFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(self.default())
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(AtlasFormat)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for AtlasFormatFactory {
    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<beacon_datafusion_ext::format_ext::DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|meta| is_atlas_metadata(meta))
            .map(|meta| {
                beacon_datafusion_ext::format_ext::DatasetMetadata::new(
                    meta.location.to_string(),
                    self.file_format_name(),
                )
            })
            .collect();

        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        "atlas".to_string()
    }
}

#[derive(Clone, Debug, Default)]
/// DataFusion file format implementation for Atlas collection metadata files.
pub struct AtlasFormat;

#[async_trait::async_trait]
impl FileFormat for AtlasFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        ATLAS_FILE_EXTENSION.to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(ATLAS_FILE_EXTENSION.to_string())
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let atlas_objects = find_atlas_collections(objects);
        if atlas_objects.is_empty() {
            return Err(execution_error("No atlas.json files found to infer schema"));
        }

        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
        let mut schemas: Vec<AtlasSchema> = Vec::new();

        for meta in &atlas_objects {
            let collection_dir = collection_directory_from_meta(meta);
            let state = load_collection_state(store.clone(), collection_dir, io_cache.clone())
                .await
                .map_err(|e| {
                    execution_error(format!(
                        "Failed to load Atlas collection at '{}': {}",
                        meta.location, e
                    ))
                })?;

            for partition_schema in state.partition_schemas() {
                schemas.push(partition_schema.clone());
            }
        }

        if schemas.is_empty() {
            // No partitions found in any discovered collections; return an empty schema with no tables.
            return Ok(Arc::new(arrow::datatypes::Schema::empty()));
        }

        let merged = AtlasSchema::merge_all_with_mode(
            &schemas,
            crate::schema::AtlasSuperTypingMode::General,
        )
        .map_err(|e| execution_error(format!("Failed to merge Atlas schemas: {}", e)))?;

        Ok(Arc::new(merged.to_arrow_schema()))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // DataFusion lists collection metadata files; Atlas executes at the
        // partition level, so we first convert listing results into partition
        // metadata tasks and then rebalance them by target worker count.
        let atlas_metas = collect_atlas_metadata_files(&conf);

        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;
        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
        let target_partitions = state.config_options().execution.target_partitions;
        let file_groups =
            build_partition_file_groups(object_store, &atlas_metas, io_cache, target_partitions)
                .await?;

        let source = AtlasSource::new();
        let conf = FileScanConfigBuilder::from(conf)
            .with_file_groups(file_groups)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(&self) -> Arc<dyn FileSource> {
        Arc::new(AtlasSource::new())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use beacon_datafusion_ext::{
        format_ext::FileFormatFactoryExt, listing_table_factory_ext::ListingTableFactoryExt,
    };
    use datafusion::{
        datasource::{
            file_format::FileFormat,
            listing::{
                ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
            },
            physical_plan::FileGroup,
        },
        execution::object_store::ObjectStoreUrl,
        prelude::{SessionConfig, SessionContext},
    };
    use futures::{TryStreamExt, stream};
    use object_store::{ObjectMeta, ObjectStore, local::LocalFileSystem, path::Path};

    use crate::{collection::AtlasCollection, column::Column, schema::AtlasSuperTypingMode};

    use super::{
        ATLAS_FILE_EXTENSION, AtlasFormat, AtlasFormatFactory, build_target_partition_file_groups,
    };

    fn meta(path: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    fn group_paths(file_groups: &[FileGroup]) -> Vec<Vec<String>> {
        file_groups
            .iter()
            .map(|group| {
                group
                    .files()
                    .iter()
                    .map(|file| file.object_meta.location.to_string())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn planner_returns_empty_groups_for_empty_input() {
        let file_groups = build_target_partition_file_groups(Vec::new(), 8);
        assert!(file_groups.is_empty());
    }

    #[test]
    fn planner_balances_when_partitions_exceed_target() {
        let files = (0..10)
            .map(|idx| {
                PartitionedFile::new(
                    format!("collections/c/partitions/p-{idx}/atlas_partition.json"),
                    0,
                )
            })
            .collect();

        let file_groups = build_target_partition_file_groups(files, 4);
        assert_eq!(file_groups.len(), 4);

        let mut sizes: Vec<usize> = file_groups
            .iter()
            .map(|group| group.files().len())
            .collect();
        sizes.sort_unstable();
        assert_eq!(sizes, vec![2, 2, 3, 3]);

        let all_paths: Vec<String> = file_groups
            .iter()
            .flat_map(|group| group.files().iter())
            .map(|file| file.object_meta.location.to_string())
            .collect();
        assert_eq!(all_paths.len(), 10);
    }

    #[test]
    fn planner_creates_one_group_per_partition_when_counts_match() {
        let files = (0..3)
            .map(|idx| {
                PartitionedFile::new(
                    format!("collections/c/partitions/p-{idx}/atlas_partition.json"),
                    0,
                )
            })
            .collect();

        let file_groups = build_target_partition_file_groups(files, 3);
        assert_eq!(file_groups.len(), 3);
        assert!(file_groups.iter().all(|group| group.files().len() == 1));
    }

    #[test]
    fn planner_duplicates_round_robin_when_target_exceeds_partitions() {
        let files = vec![
            PartitionedFile::new("collections/c/partitions/p-0/atlas_partition.json", 0),
            PartitionedFile::new("collections/c/partitions/p-1/atlas_partition.json", 0),
        ];

        let file_groups = build_target_partition_file_groups(files, 5);
        assert_eq!(file_groups.len(), 5);
        assert!(file_groups.iter().all(|group| group.files().len() == 1));

        let paths = group_paths(&file_groups)
            .into_iter()
            .map(|group| group[0].clone())
            .collect::<Vec<_>>();
        assert_eq!(
            paths,
            vec![
                "collections/c/partitions/p-0/atlas_partition.json".to_string(),
                "collections/c/partitions/p-1/atlas_partition.json".to_string(),
                "collections/c/partitions/p-0/atlas_partition.json".to_string(),
                "collections/c/partitions/p-1/atlas_partition.json".to_string(),
                "collections/c/partitions/p-0/atlas_partition.json".to_string(),
            ]
        );
    }

    #[test]
    fn discover_datasets_returns_only_atlas_metadata_files() {
        let factory = AtlasFormatFactory::new();
        let objects = vec![
            meta("collections/a/atlas.json"),
            meta("collections/a/partitions/p0/atlas_partition.json"),
            meta("collections/b/atlas.json"),
            meta("collections/b/data.bin"),
        ];

        let discovered = factory
            .discover_datasets(&objects)
            .expect("discover datasets");

        let mut paths = discovered
            .iter()
            .map(|dataset| dataset.file_path.clone())
            .collect::<Vec<_>>();
        paths.sort();

        assert_eq!(
            paths,
            vec![
                "collections/a/atlas.json".to_string(),
                "collections/b/atlas.json".to_string(),
            ]
        );
        assert!(discovered.iter().all(|dataset| dataset.format == "atlas"));
    }

    #[tokio::test]
    async fn datafusion_reads_atlas_collection_with_multiple_partitions() -> anyhow::Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);
        let collection_path = Path::from("collections/pipeline");

        let mut collection = AtlasCollection::create(
            store,
            collection_path.clone(),
            "pipeline",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let mut writer = collection.create_partition("part-00000", None).await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-a",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![10_i32, 20_i32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let mut writer = collection.create_partition("part-00001", None).await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "dataset-b",
                stream::iter(vec![Column::new_from_vec(
                    "temperature".to_string(),
                    vec![30_i32, 40_i32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(8));
        let state = ctx.state();

        let file_format: Arc<dyn FileFormat> = Arc::new(AtlasFormat);
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(ATLAS_FILE_EXTENSION)
            .with_target_partitions(state.config_options().execution.target_partitions)
            .with_collect_stat(true);

        let absolute_collection_path = temp_dir.path().join("collections/pipeline");
        let table_url = ListingTableUrl::parse(format!(
            "file://{}",
            absolute_collection_path.to_string_lossy()
        ))?;
        let schema = listing_options.infer_schema(&state, &table_url).await?;

        let config = ListingTableConfig::new(table_url)
            .with_listing_options(listing_options)
            .with_schema(schema);
        let table = ListingTable::try_new(config)?;
        ctx.register_table("atlas_pipeline", Arc::new(table))?;

        let batches = ctx
            .sql("SELECT __entry_key, temperature FROM atlas_pipeline")
            .await?
            .collect()
            .await?;

        let total_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>();
        assert_eq!(total_rows, 4);

        let mut entry_keys = Vec::new();
        let mut temperatures = Vec::new();
        for batch in &batches {
            let entry = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("entry key should be Utf8");
            let temp = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("temperature should be Int32");

            for row in 0..batch.num_rows() {
                entry_keys.push(entry.value(row).to_string());
                temperatures.push(temp.value(row));
            }
        }

        entry_keys.sort();
        temperatures.sort();

        assert_eq!(
            entry_keys,
            vec!["dataset-a", "dataset-a", "dataset-b", "dataset-b"]
        );
        assert_eq!(temperatures, vec![10, 20, 30, 40]);

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_sql_registers_and_queries_atlas_collection() -> anyhow::Result<()>
    {
        let temp_dir = tempfile::tempdir()?;
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(temp_dir.path())?);
        let collection_path = Path::from("collections/sensor");

        let mut collection = AtlasCollection::create(
            store.clone(),
            collection_path.clone(),
            "sensor",
            None,
            AtlasSuperTypingMode::General,
        )
        .await?;

        let mut writer = collection.create_partition("part-00000", None).await?;
        writer
            .writer_mut()?
            .write_dataset_columns(
                "reading-1",
                futures::stream::iter(vec![Column::new_from_vec(
                    "value".to_string(),
                    vec![1_i32, 2_i32],
                    vec![2],
                    vec!["x".to_string()],
                    None,
                )?]),
            )
            .await?;
        writer.finish().await?;

        let mut files = vec![];
        let mut stream = store.list(None);
        while let Some(result) = stream.try_next().await? {
            files.push(result);
        }
        // println!("Files: {:#?}", files);
        let store_url = ObjectStoreUrl::parse("file://")?;

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        ctx.register_object_store(store_url.as_ref(), store.clone());
        {
            let state_ref = ctx.state_ref();
            let mut state = state_ref.write();
            state.register_file_format(Arc::new(AtlasFormatFactory::new()), true)?;

            state.table_factories_mut().insert(
                "ATLAS".to_owned(),
                Arc::new(ListingTableFactoryExt::new(store_url)),
            );
        }

        let sql = format!(
            "CREATE EXTERNAL TABLE sensor_data STORED AS ATLAS LOCATION '/collections/sensor/atlas.json'",
        );
        println!("Executing SQL:\n{}", sql);
        ctx.sql(&sql).await?.collect().await?;

        let batches = ctx
            .sql("SELECT __entry_key, value FROM sensor_data")
            .await?
            .collect()
            .await?;

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        let mut entry_keys = Vec::new();
        let mut values = Vec::new();
        for batch in &batches {
            let entry = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("entry key should be Utf8");
            let value = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("value should be Int32");

            for row in 0..batch.num_rows() {
                entry_keys.push(entry.value(row).to_string());
                values.push(value.value(row));
            }
        }

        entry_keys.sort();
        values.sort();
        assert_eq!(entry_keys, vec!["reading-1", "reading-1"]);
        assert_eq!(values, vec![1, 2]);

        Ok(())
    }
}
