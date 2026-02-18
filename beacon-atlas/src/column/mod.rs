pub mod reader;
pub mod writer;

use std::sync::Arc;

pub use reader::ColumnReader;
pub use writer::ColumnWriter;

use crate::array::{self, Array, ArrayValues, store::ChunkStore};

pub struct Column {
    pub array: Array<Arc<dyn ChunkStore>>,
    pub name: String,
}

impl Column {
    pub fn new(array: Array<Arc<dyn ChunkStore>>, name: String) -> Self {
        Self { array, name }
    }

    /// Creates a single-chunk column from owned values with an inferred Arrow data type.
    /// The shape and dimensions are used to determine the structure of the array, but the actual data is stored in a single chunk.
    /// The shape is used for validation to ensure that the number of values matches the expected structure, but it does not affect how the data is stored in the column.
    ///
    /// Scalar values (shape is empty) are treated as single-element arrays, allowing for consistent handling of both scalar and array data.
    pub fn new_from_vec<T: ArrayValues>(
        values: T,
        dimensions: Vec<String>,
        shape: Vec<usize>,
        name: String,
    ) -> anyhow::Result<Self> {
        if values.len() == 0 {
            anyhow::bail!("Values cannot be empty");
        }

        // Check if it is a scalar value (shape is empty) and if so, treat it as a single-element array.
        if values.len() == 1 {
            // Shape is allowed to be empty for scalar values
            return Self::new_from_vec(values, dimensions, shape, name);
        }

        if shape.len() != dimensions.len() {
            anyhow::bail!(
                "Shape length ({}) does not match dimensions length ({})",
                shape.len(),
                dimensions.len()
            );
        }

        if shape.iter().product::<usize>() != values.len() {
            anyhow::bail!(
                "Product of shape dimensions ({}) does not match number of values ({})",
                shape.iter().product::<usize>(),
                values.len()
            );
        }

        let array = array::from_vec_auto(values, dimensions.clone(), shape.clone());

        Ok(Self { array, name })
    }

    pub fn from_ndarray<T, S, D>(
        array: ndarray::ArrayBase<S, D>,
        dimensions: Vec<String>,
        name: String,
    ) -> anyhow::Result<Self>
    where
        S: ndarray::Data<Elem = T>,
        D: ndarray::Dimension,
        Vec<T>: ArrayValues,
        T: Clone,
    {
        let array = array::from_ndarray(array, dimensions)?;
        Ok(Self { array, name })
    }
}
