pub mod arrow;
pub mod csv;
pub mod netcdf;
pub mod parquet;
pub mod zarr;

use beacon_atlas::{partition::ops::read::Dataset, prelude::*};
use futures::{Stream, StreamExt};
use object_store::ObjectStore;

/// Create an Atlas collection rooted at a local filesystem directory.
///
/// The provided directory is canonicalized first so object-store paths are
/// deterministic and independent from the current working directory.
pub(crate) async fn create_collection_from_directory<
    S: object_store::ObjectStore + Clone,
    P: AsRef<std::path::Path>,
>(
    store: S,
    path: P,
    name: String,
    description: Option<String>,
    super_typing_mode: AtlasSuperTypingMode,
) -> anyhow::Result<AtlasCollection<S>> {
    let canonicalized_path = std::fs::canonicalize(path)?;
    let collection_directory = object_store::path::Path::from_filesystem_path(canonicalized_path)?;
    let collection = AtlasCollection::create(
        store,
        collection_directory,
        name,
        description,
        super_typing_mode,
    )
    .await?;
    Ok(collection)
}

/// Open an existing Atlas collection from a local filesystem directory.
pub(crate) async fn open_collection_from_directory<
    S: object_store::ObjectStore + Clone,
    P: AsRef<std::path::Path>,
>(
    store: S,
    path: P,
) -> anyhow::Result<AtlasCollection<S>> {
    let canonicalized_path = std::fs::canonicalize(path)?;
    let collection_directory = object_store::path::Path::from_filesystem_path(canonicalized_path)?;
    AtlasCollection::open(store, collection_directory).await
}

/// Write a stream of datasets into a new partition in an existing collection.
pub(crate) async fn create_partition<
    S: ObjectStore + Clone,
    DS: Stream<Item = anyhow::Result<Dataset>> + Send + 'static,
>(
    mut atlas: AtlasCollection<S>,
    partition_name: String,
    partition_description: Option<String>,
    stream: DS,
) -> anyhow::Result<AtlasCollection<S>> {
    let mut writer = atlas
        .create_partition(partition_name, partition_description.as_deref())
        .await?;

    let partition_writer = writer.writer_mut()?;

    let mut pinned_stream = Box::pin(stream);
    while let Some(dataset) = pinned_stream.next().await {
        let dataset = dataset?;
        partition_writer.write_dataset(dataset).await?;
    }

    // Finalize the partition after writing all datasets
    let _ = writer.finish().await?;

    Ok(atlas)
}
