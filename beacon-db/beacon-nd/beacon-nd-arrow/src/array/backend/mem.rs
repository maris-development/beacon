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

#[cfg(test)]
mod tests {
    use super::*;

    fn dims(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    fn grid() -> ndarray::ArrayD<i32> {
        ndarray::ArrayD::from_shape_vec(vec![3, 4], (0..12).collect()).unwrap()
    }

    #[test]
    fn new_with_validity_rejects_a_mask_shaped_differently_from_the_array() {
        let validity = ndarray::ArrayD::from_elem(vec![4, 3], true);
        let err = InMemoryArrayBackend::new_with_validity(
            grid(),
            vec![3, 4],
            dims(&["y", "x"]),
            None,
            Some(validity),
        )
        .unwrap_err();

        assert!(
            matches!(err, NdArrowError::ValidityShapeMismatch { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn new_without_validity_reports_no_fill_value_by_default() {
        let backend = InMemoryArrayBackend::new(grid(), vec![3, 4], dims(&["y", "x"]), None);
        assert_eq!(backend.fill_value(), None);
        assert_eq!(backend.shape(), vec![3, 4]);
        assert_eq!(backend.len(), 12);
    }

    #[tokio::test]
    async fn read_subset_slices_the_requested_window_in_row_major_order() {
        let backend = InMemoryArrayBackend::new(grid(), vec![3, 4], dims(&["y", "x"]), Some(-1));
        let values = backend
            .read_subset(ArraySubset::new(vec![1, 1], vec![2, 2]))
            .await
            .unwrap();

        assert_eq!(values.shape(), &[2, 2]);
        assert_eq!(values.into_raw_vec_and_offset().0, vec![5, 6, 9, 10]);
    }

    #[tokio::test]
    async fn read_subset_slices_the_validity_mask_in_lockstep_with_the_values() {
        // Mark exactly the element at (1, 1) — value 5 — as null.
        let mut validity = ndarray::ArrayD::from_elem(vec![3, 4], true);
        validity[[1, 1]] = false;

        let backend = InMemoryArrayBackend::new_with_validity(
            grid(),
            vec![3, 4],
            dims(&["y", "x"]),
            None,
            Some(validity),
        )
        .unwrap();

        let result = backend
            .read_subset_with_validity(ArraySubset::new(vec![1, 1], vec![2, 2]))
            .await
            .unwrap();

        let mask = result.validity.expect("validity should survive slicing");
        assert_eq!(mask.shape(), &[2, 2]);
        assert_eq!(
            mask.into_raw_vec_and_offset().0,
            vec![false, true, true, true]
        );
    }

    #[tokio::test]
    async fn read_subset_of_zero_length_returns_an_empty_array() {
        let backend = InMemoryArrayBackend::new(grid(), vec![3, 4], dims(&["y", "x"]), None);
        let values = backend
            .read_subset(ArraySubset::new(vec![3, 0], vec![0, 4]))
            .await
            .unwrap();
        assert_eq!(values.shape(), &[0, 4]);
        assert_eq!(values.len(), 0);
    }

    #[tokio::test]
    async fn read_subset_validates_bounds_before_slicing() {
        let backend = InMemoryArrayBackend::new(grid(), vec![3, 4], dims(&["y", "x"]), None);
        let err = backend
            .read_subset(ArraySubset::new(vec![0, 0], vec![3, 5]))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("exceeds axis size"),
            "unexpected error: {err}"
        );
    }
}
