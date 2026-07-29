pub mod mem;

use std::{fmt::Debug, sync::Arc};

use crate::{array::subset::ArraySubset, datatypes::NdArrayType};

#[async_trait::async_trait]
pub trait ArrayBackend<T: NdArrayType>: Send + Sync + 'static + Debug {
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

    async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ndarray::ArrayD<T>>;
}

#[async_trait::async_trait]
impl<T: NdArrayType, A: ArrayBackend<T> + ?Sized> ArrayBackend<T> for Arc<A> {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn shape(&self) -> Vec<usize> {
        (**self).shape()
    }

    fn chunk_shape(&self) -> Vec<usize> {
        (**self).chunk_shape()
    }

    fn dimensions(&self) -> Vec<String> {
        (**self).dimensions()
    }

    fn fill_value(&self) -> Option<T> {
        (**self).fill_value()
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
    fn test_validate_subset_accepts_the_full_extent() {
        let b = backend(vec![3, 4]);
        assert!(
            b.validate_subset(&ArraySubset {
                start: vec![0, 0],
                shape: vec![3, 4]
            })
            .is_ok()
        );
    }

    #[test]
    fn test_validate_subset_accepts_a_zero_length_window_at_the_upper_edge() {
        // `start == size` with `len == 0` is the empty tail window a chunk
        // generator can legitimately produce; it must not be rejected.
        let b = backend(vec![3, 4]);
        assert!(
            b.validate_subset(&ArraySubset {
                start: vec![3, 0],
                shape: vec![0, 4]
            })
            .is_ok()
        );
    }

    #[test]
    fn test_validate_subset_rejects_rank_mismatches() {
        let b = backend(vec![3, 4]);
        let err = b
            .validate_subset(&ArraySubset {
                start: vec![0],
                shape: vec![3, 4],
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("start rank"),
            "unexpected error: {err}"
        );

        let err = b
            .validate_subset(&ArraySubset {
                start: vec![0, 0],
                shape: vec![3],
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("shape rank"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_validate_subset_rejects_out_of_bounds_windows() {
        let b = backend(vec![3, 4]);
        let err = b
            .validate_subset(&ArraySubset {
                start: vec![4, 0],
                shape: vec![0, 4],
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("start index 4 exceeds axis size 3"),
            "unexpected error: {err}"
        );

        let err = b
            .validate_subset(&ArraySubset {
                start: vec![2, 0],
                shape: vec![2, 4],
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("end index 4 exceeds axis size 3"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_zero_length_axis_makes_the_backend_empty() {
        let b = backend(vec![0, 4]);
        assert_eq!(b.len(), 0);
        assert!(b.is_empty());
        assert_eq!(b.chunk_shape(), vec![0, 4]);
    }

    #[tokio::test]
    async fn test_arc_delegates_every_backend_method() {
        let b = Arc::new(backend(vec![2, 3]));
        assert_eq!(ArrayBackend::<i32>::shape(&b), vec![2, 3]);
        assert_eq!(ArrayBackend::<i32>::len(&b), 6);
        assert_eq!(ArrayBackend::<i32>::chunk_shape(&b), vec![2, 3]);
        assert_eq!(ArrayBackend::<i32>::fill_value(&b), None);
        let values = b
            .read_subset(ArraySubset {
                start: vec![0, 0],
                shape: vec![1, 3],
            })
            .await
            .unwrap();
        assert_eq!(values.shape(), &[1, 3]);
    }
}
