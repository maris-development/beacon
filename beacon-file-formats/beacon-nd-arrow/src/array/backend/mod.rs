pub mod mem;

use std::{fmt::Debug, sync::Arc};

use crate::{
    NdArrowArrayDispatch,
    array::{NdArrowArray, compat_typings::ArrowTypeConversion, subset::ArraySubset},
};

#[derive(Debug, Clone)]
pub struct BackendSubsetResult<T: ArrowTypeConversion> {
    pub values: ndarray::ArrayD<T>,
    pub validity: Option<ndarray::ArrayD<bool>>,
}

impl<T: ArrowTypeConversion> BackendSubsetResult<T> {
    pub fn new(values: ndarray::ArrayD<T>, validity: Option<ndarray::ArrayD<bool>>) -> Self {
        Self { values, validity }
    }
}

#[async_trait::async_trait]
pub trait ArrayBackend<T: ArrowTypeConversion>: Send + Sync + 'static + Debug {
    fn into_dyn_array(self) -> anyhow::Result<Arc<dyn NdArrowArray>>
    where
        Self: Sized + 'static,
    {
        Ok(Arc::new(NdArrowArrayDispatch::new(self)?))
    }

    fn len(&self) -> usize;

    /// Returns `true` when the array contains no elements.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn shape(&self) -> Vec<usize>;
    /// Returns the shape of the array for chunking purposes. By default, this is the same as `shape()`, but it can be overridden to provide a different shape for chunking.
    fn chunk_shape(&self) -> Vec<usize> {
        self.shape()
    }
    fn dimensions(&self) -> Vec<String>;

    fn validate_subset(&self, subset: &ArraySubset) -> anyhow::Result<()> {
        let shape = self.shape();
        if subset.start.len() != shape.len() {
            return Err(anyhow::anyhow!(
                "Subset start rank {} does not match array rank {}",
                subset.start.len(),
                shape.len()
            ));
        }

        if subset.shape.len() != shape.len() {
            return Err(anyhow::anyhow!(
                "Subset shape rank {} does not match array rank {}",
                subset.shape.len(),
                shape.len()
            ));
        }

        for axis in 0..shape.len() {
            let start = subset.start[axis];
            let len = subset.shape[axis];
            let size = shape[axis];

            if start > size {
                return Err(anyhow::anyhow!(
                    "Subset start index {} exceeds axis size {} for axis {}",
                    start,
                    size,
                    axis
                ));
            }

            if start + len > size {
                return Err(anyhow::anyhow!(
                    "Subset end index {} exceeds axis size {} for axis {}",
                    start + len,
                    size,
                    axis
                ));
            }
        }

        Ok(())
    }

    fn fill_value(&self) -> Option<T> {
        None
    }

    async fn read_subset_with_validity(
        &self,
        subset: ArraySubset,
    ) -> anyhow::Result<BackendSubsetResult<T>> {
        let values = self.read_subset(subset).await?;
        Ok(BackendSubsetResult::new(values, None))
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>>;
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion, A: ArrayBackend<T> + ?Sized> ArrayBackend<T> for Arc<A> {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn shape(&self) -> Vec<usize> {
        (**self).shape()
    }

    fn dimensions(&self) -> Vec<String> {
        (**self).dimensions()
    }

    fn fill_value(&self) -> Option<T> {
        (**self).fill_value()
    }

    async fn read_subset_with_validity(
        &self,
        subset: ArraySubset,
    ) -> anyhow::Result<BackendSubsetResult<T>> {
        (**self).read_subset_with_validity(subset).await
    }

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>> {
        (**self).read_subset(subset).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::backend::mem::InMemoryArrayBackend;

    fn backend(shape: Vec<usize>) -> InMemoryArrayBackend<i32> {
        let len: usize = shape.iter().product();
        let dimensions = (0..shape.len()).map(|i| format!("d{i}")).collect();
        InMemoryArrayBackend::new(
            ndarray::ArrayD::from_shape_vec(shape.clone(), vec![0; len]).unwrap(),
            shape,
            dimensions,
            None,
        )
    }

    #[test]
    fn validate_subset_accepts_the_full_extent() {
        let b = backend(vec![3, 4]);
        assert!(
            b.validate_subset(&ArraySubset::new(vec![0, 0], vec![3, 4]))
                .is_ok()
        );
    }

    #[test]
    fn validate_subset_accepts_a_zero_length_window_at_the_upper_edge() {
        // `start == size` with `len == 0` is the empty tail window; it must not
        // be treated as out of bounds (chunk generators can produce it).
        let b = backend(vec![3, 4]);
        assert!(
            b.validate_subset(&ArraySubset::new(vec![3, 0], vec![0, 4]))
                .is_ok()
        );
    }

    #[test]
    fn validate_subset_rejects_a_start_rank_mismatch() {
        let b = backend(vec![3, 4]);
        let err = b
            .validate_subset(&ArraySubset::new(vec![0], vec![3, 4]))
            .unwrap_err();
        assert!(
            err.to_string().contains("start rank"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_subset_rejects_a_shape_rank_mismatch() {
        let b = backend(vec![3, 4]);
        let err = b
            .validate_subset(&ArraySubset::new(vec![0, 0], vec![3]))
            .unwrap_err();
        assert!(
            err.to_string().contains("shape rank"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_subset_rejects_a_start_past_the_axis_end() {
        let b = backend(vec![3, 4]);
        let err = b
            .validate_subset(&ArraySubset::new(vec![4, 0], vec![0, 4]))
            .unwrap_err();
        assert!(
            err.to_string().contains("start index 4 exceeds axis size 3"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn validate_subset_rejects_a_window_running_past_the_axis_end() {
        let b = backend(vec![3, 4]);
        let err = b
            .validate_subset(&ArraySubset::new(vec![2, 0], vec![2, 4]))
            .unwrap_err();
        assert!(
            err.to_string().contains("end index 4 exceeds axis size 3"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn zero_length_axis_makes_the_backend_empty() {
        let b = backend(vec![0, 4]);
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
        assert!(
            b.validate_subset(&ArraySubset::new(vec![0, 0], vec![0, 4]))
                .is_ok()
        );
    }

    #[test]
    fn chunk_shape_defaults_to_the_full_shape() {
        let b = backend(vec![2, 5]);
        assert_eq!(b.chunk_shape(), vec![2, 5]);
    }

    #[tokio::test]
    async fn read_subset_with_validity_defaults_to_no_mask() {
        // The blanket default delegates to `read_subset` and reports no validity.
        let b = backend(vec![2, 2]);
        let result = b
            .read_subset_with_validity(ArraySubset::new(vec![0, 0], vec![2, 2]))
            .await
            .unwrap();
        assert!(result.validity.is_none());
        assert_eq!(result.values.shape(), &[2, 2]);
    }

    #[tokio::test]
    async fn arc_delegates_every_backend_method() {
        let b = Arc::new(backend(vec![2, 3]));
        assert_eq!(ArrayBackend::<i32>::shape(&b), vec![2, 3]);
        assert_eq!(ArrayBackend::<i32>::len(&b), 6);
        assert_eq!(
            ArrayBackend::<i32>::dimensions(&b),
            vec!["d0".to_string(), "d1".to_string()]
        );
        assert_eq!(ArrayBackend::<i32>::fill_value(&b), None);
        let values = b
            .read_subset(ArraySubset::new(vec![0, 0], vec![1, 3]))
            .await
            .unwrap();
        assert_eq!(values.shape(), &[1, 3]);
    }
}
