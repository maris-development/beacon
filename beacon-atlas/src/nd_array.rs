use beacon_nd_arrow::array::{backend::ArrayBackend, subset::ArraySubset};
use object_store::ObjectStore;

use crate::schema::_type::AtlasType;

#[derive(Debug)]
pub struct AtlasArrayFileBackend<S: ObjectStore, T: AtlasType> {
    _marker: std::marker::PhantomData<T>,
    store: S,
    shape: Vec<usize>,
    fill_value: Option<T>,
    dimensions: Vec<String>,
}

#[async_trait::async_trait]
impl<S: ObjectStore, T: AtlasType> ArrayBackend<T> for AtlasArrayFileBackend<S, T> {
    fn len(&self) -> usize {
        self.shape.iter().product()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        todo!()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }
}
