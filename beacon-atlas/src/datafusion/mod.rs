use std::{any::Any, collections::HashMap, fmt::Debug, sync::Arc};

use arrow::datatypes::SchemaRef;
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
pub mod source;

const ATLAS_FILE_EXTENSION: &str = "atlas.json";

fn execution_error(message: impl Into<String>) -> DataFusionError {
    DataFusionError::Execution(message.into())
}

fn collect_atlas_metadata_files(conf: &FileScanConfig) -> Vec<ObjectMeta> {
    conf.file_groups
        .iter()
        .flat_map(|group| group.files().iter())
        .map(|file| file.object_meta.clone())
        .collect()
}

async fn build_partition_file_groups(
    object_store: Arc<dyn ObjectStore>,
    atlas_metas: &[ObjectMeta],
    io_cache: Arc<IoCache>,
) -> datafusion::error::Result<Vec<FileGroup>> {
    let mut file_groups: Vec<FileGroup> = Vec::new();

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

        let mut files: Vec<PartitionedFile> = Vec::new();
        for partition_name in &collection_state.metadata().partitions {
            // Each partition is represented by its atlas_partition.json metadata file.
            let partition_meta_path = collection_dir
                .child("partitions")
                .child(partition_name.as_str())
                .child(crate::consts::PARTITION_METADATA_FILE);
            files.push(PartitionedFile::new(partition_meta_path.to_string(), 0));
        }

        if !files.is_empty() {
            file_groups.push(FileGroup::new(files));
        }
    }

    Ok(file_groups)
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
            return Err(execution_error(
                "No partitions found in Atlas collection(s) to infer schema",
            ));
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
        // Expand collection metadata files into per-partition metadata files.
        let atlas_metas = collect_atlas_metadata_files(&conf);

        let object_store = state
            .runtime_env()
            .object_store(conf.object_store_url.clone())?;
        let io_cache = Arc::new(IoCache::new(DEFAULT_IO_CACHE_BYTES));
        let file_groups = build_partition_file_groups(object_store, &atlas_metas, io_cache).await?;

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
    use datafusion::{
        datasource::{
            file_format::FileFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        prelude::SessionContext,
    };
    use futures::stream;
    use object_store::{ObjectStore, local::LocalFileSystem, path::Path};

    use crate::{collection::AtlasCollection, column::Column, schema::AtlasSuperTypingMode};

    use super::{ATLAS_FILE_EXTENSION, AtlasFormat};

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

        let ctx = SessionContext::new();
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
}
