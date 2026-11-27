//! Helpers that open the fixtures of `test_files/` through the ND contract.
#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::ArrowPrimitiveType;
use beacon_arrow_hdf5::{Hdf5Opener, ReadOptions};
use beacon_nd_dataset::{NdDataset, Region, Result};
use beacon_nd_scan::NdFileOpener;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

/// A store rooted at `test_files/`.
pub fn store() -> Arc<dyn ObjectStore> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
    Arc::new(LocalFileSystem::new_with_prefix(root).expect("test_files store"))
}

/// Open the fixture `test_files/<name>` with `options`.
pub async fn try_open_with(name: &str, options: ReadOptions) -> Result<Arc<dyn NdDataset>> {
    let store = store();
    let meta = store.head(&Path::from(name)).await.unwrap();
    Hdf5Opener::new(options).open(store, &meta).await
}

/// Open the fixture `test_files/<name>` with `options`.
pub async fn open_with(name: &str, options: ReadOptions) -> Arc<dyn NdDataset> {
    try_open_with(name, options)
        .await
        .unwrap_or_else(|e| panic!("cannot open {name}: {e}"))
}

/// Open the fixture `test_files/<name>` with the default options.
pub async fn open(name: &str) -> Arc<dyn NdDataset> {
    open_with(name, ReadOptions::default()).await
}

/// All values of the variable `name`, with `None` for null.
pub async fn values<T: ArrowPrimitiveType>(
    dataset: &dyn NdDataset,
    name: &str,
) -> Vec<Option<T::Native>> {
    let variable = dataset
        .variable(name)
        .unwrap_or_else(|| panic!("no variable '{name}'"));
    let read = variable
        .read(&Region::full(variable.dimensions()))
        .await
        .unwrap();
    read.values().as_primitive::<T>().iter().collect()
}

/// All strings of the variable `name`.
pub async fn strings(dataset: &dyn NdDataset, name: &str) -> Vec<Option<String>> {
    let variable = dataset
        .variable(name)
        .unwrap_or_else(|| panic!("no variable '{name}'"));
    let read = variable
        .read(&Region::full(variable.dimensions()))
        .await
        .unwrap();
    let values = read.values();
    (0..values.len())
        .map(|i| (!values.is_null(i)).then(|| values.as_string::<i32>().value(i).to_string()))
        .collect()
}

/// The dimension names of the variable `name`.
pub fn dimension_names(dataset: &dyn NdDataset, name: &str) -> Vec<String> {
    dataset
        .variable(name)
        .unwrap_or_else(|| panic!("no variable '{name}'"))
        .dimensions()
        .names()
        .into_iter()
        .map(str::to_string)
        .collect()
}
