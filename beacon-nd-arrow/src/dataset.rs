use crate::{NdArrayD, datatypes::NdArrayDataType};
use indexmap::IndexMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Dataset {
    pub name: String,
    pub dimensions: IndexMap<String, usize>,
    pub arrays: IndexMap<String, Arc<dyn NdArrayD>>,
}

impl Dataset {
    pub async fn new(name: String, arrays: IndexMap<String, Arc<dyn NdArrayD>>) -> Self {
        let mut dimensions = indexmap::IndexMap::new();
        for array in arrays.values() {
            for (dim_name, dim_size) in array.dimensions().iter().zip(array.shape().iter()) {
                dimensions.insert(dim_name.clone(), *dim_size);
            }
        }
        Self {
            name,
            dimensions,
            arrays,
        }
    }

    pub fn get_array_names(&self) -> Vec<String> {
        self.arrays.keys().cloned().collect()
    }

    pub fn get_array(&self, name: &str) -> Option<&Arc<dyn NdArrayD>> {
        self.arrays.get(name)
    }

    pub fn get_array_datatype(&self, name: &str) -> Option<NdArrayDataType> {
        self.get_array(name).map(|array| array.datatype())
    }
}
