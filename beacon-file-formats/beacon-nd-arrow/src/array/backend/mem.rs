use crate::array::backend::{ArrayBackend, BackendSubsetResult};
use crate::array::compat_typings::ArrowTypeConversion;
use crate::array::subset::ArraySubset;
use crate::error::{NdArrowError, Result};

#[derive(Debug, Clone)]
pub struct InMemoryArrayBackend<T: ArrowTypeConversion> {
    array: ndarray::ArrayD<T>,
    validity: Option<ndarray::ArrayD<bool>>,
    shape: Vec<usize>,
    dimensions: Vec<String>,
    fill_value: Option<T>,
}

impl<T: ArrowTypeConversion> InMemoryArrayBackend<T> {
    /// Construct a backend without a validity mask. Infallible — there is no
    /// validity mask whose shape could disagree with the array.
    pub fn new(
        array: ndarray::ArrayD<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T>,
    ) -> Self {
        Self {
            array,
            validity: None,
            shape,
            dimensions,
            fill_value,
        }
    }

    /// Construct a backend with an optional validity mask.
    ///
    /// Returns [`NdArrowError::ValidityShapeMismatch`] if the mask's shape does
    /// not match the array shape (previously this panicked via `assert_eq!`).
    pub fn new_with_validity(
        array: ndarray::ArrayD<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T>,
        validity: Option<ndarray::ArrayD<bool>>,
    ) -> Result<Self> {
        if let Some(validity_array) = &validity
            && validity_array.shape() != shape.as_slice()
        {
            let validity_shape = validity_array.shape().to_vec();
            tracing::error!(
                ?validity_shape,
                array_shape = ?shape,
                "validity shape does not match array shape"
            );
            return Err(NdArrowError::ValidityShapeMismatch {
                validity_shape,
                array_shape: shape,
            });
        }

        Ok(Self {
            array,
            validity,
            shape,
            dimensions,
            fill_value,
        })
    }
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion + Clone> ArrayBackend<T> for InMemoryArrayBackend<T> {
    fn len(&self) -> usize {
        self.array.len()
    }

    fn shape(&self) -> Vec<usize> {
        self.shape.clone()
    }

    fn dimensions(&self) -> Vec<String> {
        self.dimensions.clone()
    }

    fn fill_value(&self) -> Option<T> {
        self.fill_value.clone()
    }

    async fn read_subset_with_validity(
        &self,
        subset: ArraySubset,
    ) -> anyhow::Result<BackendSubsetResult<T>> {
        self.validate_subset(&subset)?;

        let data_view = self.array.view();
        let sliced_values = data_view.slice_each_axis(|axis| {
            let axis_index = axis.axis.index();
            let start = subset.start[axis_index] as isize;
            let end = (subset.start[axis_index] + subset.shape[axis_index]) as isize;
            ndarray::Slice::new(start, Some(end), 1)
        });

        let sliced_validity = self.validity.as_ref().map(|validity| {
            let validity_view = validity.view();
            let sliced = validity_view.slice_each_axis(|axis| {
                let axis_index = axis.axis.index();
                let start = subset.start[axis_index] as isize;
                let end = (subset.start[axis_index] + subset.shape[axis_index]) as isize;
                ndarray::Slice::new(start, Some(end), 1)
            });
            sliced.to_owned()
        });

        Ok(BackendSubsetResult::new(
            sliced_values.to_owned(),
            sliced_validity,
        ))
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        Ok(self.read_subset_with_validity(subset).await?.values)
    }
}
