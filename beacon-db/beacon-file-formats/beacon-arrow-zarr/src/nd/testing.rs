//! In-memory Zarr stores for the tests of the ND framework module.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use serde_json::{Map, Value};
use zarrs::array::builder::ArrayBuilderFillValue;
use zarrs::array::{ArrayBuilder, ArraySubset, DataType, Element, FillValue, data_type};
use zarrs::group::GroupBuilder;
use zarrs_object_store::AsyncObjectStore;

use super::ZarrArray;
use crate::util::ZarrStorage;

pub(crate) use super::attributes::attributes_of;

/// A writable in-memory Zarr store for tests.
pub(crate) struct TestStore {
    zarr: Arc<AsyncObjectStore<Arc<InMemory>>>,
    memory: Arc<InMemory>,
}

impl TestStore {
    /// An empty store.
    pub(crate) fn new() -> Self {
        let memory = Arc::new(InMemory::new());
        let zarr = Arc::new(AsyncObjectStore::new(memory.clone()));
        Self { zarr, memory }
    }

    /// The read storage that the reader takes.
    pub(crate) fn storage(&self) -> ZarrStorage {
        ZarrStorage::new(self.zarr.clone())
    }

    /// The object store under the Zarr store, for a DataFusion session or an opener.
    pub(crate) fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.memory.clone()
    }
}

/// A Rust type that the helpers write as a Zarr data type.
pub(crate) trait TestElement: Element + Send + Sync + 'static {
    /// The Zarr data type of the type.
    fn data_type() -> DataType;
    /// The Zarr fill value of the array metadata.
    fn fill_value() -> ArrayBuilderFillValue;
}

macro_rules! test_element {
    ($type:ty, $data_type:ident, $fill:expr) => {
        impl TestElement for $type {
            fn data_type() -> DataType {
                data_type::$data_type()
            }
            fn fill_value() -> ArrayBuilderFillValue {
                $fill.into()
            }
        }
    };
}

test_element!(bool, bool, FillValue::from(false));
test_element!(i8, int8, 0i8);
test_element!(i16, int16, 0i16);
test_element!(i32, int32, 0i32);
test_element!(i64, int64, 0i64);
test_element!(u8, uint8, 0u8);
test_element!(u16, uint16, 0u16);
test_element!(u32, uint32, 0u32);
test_element!(u64, uint64, 0u64);
test_element!(f32, float32, 0.0f32);
test_element!(f64, float64, 0.0f64);
test_element!(String, string, "");
test_element!(Vec<u8>, bytes, Vec::<u8>::new());

/// Write one group node with `attributes` at `path`.
pub(crate) async fn write_group(store: &TestStore, path: &str, attributes: Value) {
    GroupBuilder::new()
        .attributes(object(attributes))
        .build(store.zarr.clone(), path)
        .unwrap()
        .async_store_metadata()
        .await
        .unwrap();
}

/// Write one array with its values. `dims` gives a name or `None` for each axis.
pub(crate) async fn write_array<T: TestElement>(
    store: &TestStore,
    path: &str,
    shape: &[u64],
    chunks: &[u64],
    dims: &[Option<&str>],
    attributes: Value,
    values: Vec<T>,
) -> Arc<ZarrArray> {
    let array = ArrayBuilder::new(
        shape.to_vec(),
        chunks.to_vec(),
        T::data_type(),
        T::fill_value(),
    )
    .dimension_names(Some(dims.iter().copied()))
    .attributes(object(attributes))
    .build(store.zarr.clone(), path)
    .unwrap();
    array.async_store_metadata().await.unwrap();
    array
        .async_store_array_subset(&ArraySubset::new_with_shape(shape.to_vec()), values)
        .await
        .unwrap();
    Arc::new(
        ZarrArray::async_open(store.storage().inner(), path)
            .await
            .unwrap(),
    )
}

/// Write the metadata of one `complex64` array, a type that Beacon does not read.
pub(crate) async fn write_unsupported_array(store: &TestStore, path: &str, shape: &[u64]) {
    ArrayBuilder::new(
        shape.to_vec(),
        shape.to_vec(),
        data_type::complex64(),
        FillValue::from([0u8; 8]),
    )
    .build(store.zarr.clone(), path)
    .unwrap()
    .async_store_metadata()
    .await
    .unwrap();
}

/// The JSON object of `value`.
fn object(value: Value) -> Map<String, Value> {
    match value {
        Value::Object(map) => map,
        other => panic!("the attributes are a JSON object, not {other}"),
    }
}
