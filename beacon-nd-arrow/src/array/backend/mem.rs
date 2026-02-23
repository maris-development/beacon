use arrow::array::ArrayRef;

use crate::array::backend::ArrayBackend;

#[derive(Debug, Clone)]
pub struct InMemoryArrayBackend {
    array: ArrayRef,
    shape: Vec<usize>,
    dimensions: Vec<String>,
}

impl InMemoryArrayBackend {
    pub fn new(array: ArrayRef, shape: Vec<usize>, dimensions: Vec<String>) -> Self {
        Self {
            array,
            shape,
            dimensions,
        }
    }
}

#[async_trait::async_trait]
impl ArrayBackend for InMemoryArrayBackend {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
        Ok(self.array.slice(start, length))
    }
}
