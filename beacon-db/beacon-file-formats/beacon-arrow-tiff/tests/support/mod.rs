//! Helpers that open the fixtures of `test-files/` through the ND contract.
#![allow(dead_code)]

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::datatypes::ArrowPrimitiveType;
use beacon_arrow_tiff::TiffOpener;
use beacon_nd_dataset::{NdDataset, Region, Result};
use beacon_nd_scan::NdFileOpener;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};

/// The bytes of the fixture `test-files/<name>`.
pub fn fixture_bytes(name: &str) -> Vec<u8> {
    let path = format!("{}/test-files/{name}", env!("CARGO_MANIFEST_DIR"));
    std::fs::read(&path).unwrap_or_else(|e| panic!("cannot read {path}: {e}"))
}

/// Open `bytes` as the file `tests/fixture.tif` of a memory store.
pub async fn open_bytes(bytes: Vec<u8>) -> Result<Arc<dyn NdDataset>> {
    let store = Arc::new(InMemory::new());
    let path = Path::from("tests/fixture.tif");
    store.put(&path, bytes.into()).await.unwrap();
    let meta = store.head(&path).await.unwrap();
    TiffOpener.open(store as Arc<dyn ObjectStore>, &meta).await
}

/// Open the fixture `test-files/<name>`.
pub async fn open(name: &str) -> Arc<dyn NdDataset> {
    open_bytes(fixture_bytes(name))
        .await
        .unwrap_or_else(|e| panic!("cannot open {name}: {e}"))
}

/// The values of `region` of the variable `name`, with `None` for null.
pub async fn read<T: ArrowPrimitiveType>(
    dataset: &dyn NdDataset,
    name: &str,
    region: &Region,
) -> Result<Vec<Option<T::Native>>> {
    let variable = dataset
        .variable(name)
        .unwrap_or_else(|| panic!("missing variable {name}"));
    let array = variable.read(region).await?;
    let values = array.values().as_primitive::<T>();
    Ok((0..values.len())
        .map(|i| values.is_valid(i).then(|| values.value(i)))
        .collect())
}

/// The values of the whole variable `name`.
pub async fn read_all<T: ArrowPrimitiveType>(
    dataset: &dyn NdDataset,
    name: &str,
) -> Vec<Option<T::Native>> {
    let variable = dataset.variable(name).expect("variable");
    read::<T>(dataset, name, &Region::full(variable.dimensions()))
        .await
        .expect("read")
}

/// Compare every value of a `width` x `height` grid with `expected(row, col)`.
pub fn assert_grid<T: PartialEq + std::fmt::Debug>(
    values: &[T],
    width: usize,
    height: usize,
    expected: impl Fn(usize, usize) -> T,
) {
    assert_eq!(values.len(), width * height);
    for row in 0..height {
        for col in 0..width {
            assert_eq!(
                values[row * width + col],
                expected(row, col),
                "pixel (row {row}, col {col})"
            );
        }
    }
}
