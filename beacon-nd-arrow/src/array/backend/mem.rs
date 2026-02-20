use arrow::array::ArrayRef;

use crate::array::backend::ArrayBackend;

pub struct InMemoryArrayBackend {
    array: ArrayRef,
}

impl InMemoryArrayBackend {
    pub fn new(array: ArrayRef) -> Self {
        Self { array }
    }
}

#[async_trait::async_trait]
impl ArrayBackend for InMemoryArrayBackend {
    fn len(&self) -> usize {
        self.array.len()
    }

    async fn slice(&self, start: usize, length: usize) -> anyhow::Result<ArrayRef> {
        Ok(self.array.slice(start, length))
    }
}
