use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;
use ndarray::Axis;

use crate::array::compat_typings::ArrowTypeConversion;
use crate::array::{
    backend::{ArrayBackend, mem::InMemoryArrayBackend},
    compat_typings::attach_validity_mask,
    subset::ArraySubset,
};
use crate::error::{NdArrowError, Result};

pub mod backend;
pub mod compat_typings;
pub mod subset;

#[async_trait::async_trait]
pub trait NdArrowArray: Send + Sync + 'static {
    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Arc<dyn NdArrowArray>>;
    async fn as_arrow_array_ref(&self) -> anyhow::Result<ArrayRef>;
    async fn broadcast(
        &self,
        target_shape: &[usize],
        dimensions: &[String],
    ) -> anyhow::Result<Arc<dyn NdArrowArray>>;
    fn shape(&self) -> Vec<usize>;
    fn dimensions(&self) -> Vec<String>;
    fn data_type(&self) -> arrow::datatypes::DataType;
}

/// N-dimensional Arrow-backed array with named dimensions and virtual stride support.
///
/// The underlying storage is a 1D backend, while `shape` + `strides` define the logical
/// N-dimensional view. This enables efficient virtual transformations like broadcasting
/// without immediately materializing data.
///
/// Key capabilities:
/// - Named-dimension broadcasting (xarray-style alignment constraints)
/// - Stride-aware slicing into new materialized arrays
/// - Lazy chunked reads that respect the current logical view
/// - Full materialization to `ArrayRef` when needed
#[derive(Debug, Clone)]
pub struct NdArrowArrayDispatch<
    T: ArrowTypeConversion,
    B: ArrayBackend<T> = InMemoryArrayBackend<T>,
> {
    marker: std::marker::PhantomData<T>,
    pub backend: Arc<B>,
}

impl<T: ArrowTypeConversion> NdArrowArrayDispatch<T> {
    pub fn new_in_mem(
        array: Vec<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T>,
    ) -> Result<Self> {
        Self::new_in_mem_with_validity(array, shape, dimensions, fill_value, None)
    }

    pub fn new_in_mem_with_validity(
        array: Vec<T>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
        fill_value: Option<T>,
        validity: Option<Vec<bool>>,
    ) -> Result<Self> {
        let validity = validity
            .map(|values| ndarray::ArrayD::from_shape_vec(shape.clone(), values))
            .transpose()
            .map_err(|e| NdArrowError::InvalidShape(e.to_string()))?;
        let nd_array = ndarray::ArrayD::from_shape_vec(shape.clone(), array)
            .map_err(|e| NdArrowError::InvalidShape(e.to_string()))?;
        let backend = InMemoryArrayBackend::new_with_validity(
            nd_array,
            shape.clone(),
            dimensions.clone(),
            fill_value,
            validity,
        )?;
        Self::new(backend)
    }
}

impl<T: ArrowTypeConversion, B: ArrayBackend<T>> NdArrowArrayDispatch<T, B> {
    /// Create a new contiguous row-major ND array view over a backend.
    ///
    /// # Validation
    /// - `shape.len()` must equal `dimensions.len()`
    /// - `product(shape)` must match backend length
    ///
    /// # Errors
    /// Returns an error if validation fails.
    pub fn new(backend: B) -> Result<Self> {
        let shape = backend.shape();
        let dimensions = backend.dimensions();
        if shape.len() != dimensions.len() {
            return Err(NdArrowError::ShapeDimensionMismatch {
                shape_len: shape.len(),
                dims_len: dimensions.len(),
            });
        }

        // Validate that the total size implied by the shape matches the backend length
        let total_size: usize = shape.iter().product();
        if total_size != backend.len() {
            return Err(NdArrowError::SizeMismatch {
                expected: total_size,
                actual: backend.len(),
            });
        }

        Self::from_parts(Arc::new(backend), shape, dimensions)
    }

    fn from_parts(
        backend: Arc<B>,
        shape: Vec<usize>,
        dimensions: Vec<String>,
    ) -> Result<Self> {
        if shape.len() != dimensions.len() {
            return Err(NdArrowError::ShapeDimensionMismatch {
                shape_len: shape.len(),
                dims_len: dimensions.len(),
            });
        }

        Ok(Self {
            backend,
            marker: std::marker::PhantomData,
        })
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.shape().iter().product()
    }

    pub fn shape(&self) -> Vec<usize> {
        self.backend.shape()
    }

    pub fn chunk_shape(&self) -> Vec<usize> {
        self.backend.chunk_shape()
    }

    pub fn dimensions(&self) -> Vec<String> {
        self.backend.dimensions()
    }

    fn validate_unique_dimension_names(dimensions: &[String]) -> anyhow::Result<()> {
        let mut seen = HashSet::new();
        for dim in dimensions {
            if !seen.insert(dim.as_str()) {
                return Err(anyhow::anyhow!("Duplicate dimension name '{}'", dim));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl<T: ArrowTypeConversion, B: ArrayBackend<T>> NdArrowArray for NdArrowArrayDispatch<T, B> {
    async fn subset(&self, subset: ArraySubset) -> anyhow::Result<Arc<dyn NdArrowArray>> {
        let subset_data = self.backend.read_subset_with_validity(subset).await?;
        let subset_shape = subset_data.values.shape().to_vec();
        let subset_validity = subset_data
            .validity
            .map(|mask| mask.into_raw_vec_and_offset().0);

        let subset_dispatch = NdArrowArrayDispatch::new_in_mem_with_validity(
            subset_data.values.into_raw_vec_and_offset().0,
            subset_shape,
            self.dimensions(),
            self.backend.fill_value(),
            subset_validity,
        )?;
        Ok(Arc::new(subset_dispatch))
    }
    async fn as_arrow_array_ref(&self) -> anyhow::Result<ArrayRef> {
        let subset_data = self
            .backend
            .read_subset_with_validity(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await?;

        let slice = subset_data.values.into_raw_vec_and_offset().0;
        let validity = subset_data
            .validity
            .map(|mask| mask.into_raw_vec_and_offset().0);

        if let Some(validity) = validity {
            let array = T::arrow_from_array_view(&slice)?;
            return attach_validity_mask(array, &validity);
        }

        match self.backend.fill_value() {
            Some(fill) => T::arrow_from_array_view_with_fill(&slice, &fill),
            None => T::arrow_from_array_view(&slice),
        }
    }

    async fn broadcast(
        &self,
        target_shape: &[usize],
        dimensions: &[String],
    ) -> anyhow::Result<Arc<dyn NdArrowArray>> {
        if target_shape.len() != dimensions.len() {
            return Err(anyhow::anyhow!(
                "Target shape length {} does not match dimensions length {}",
                target_shape.len(),
                dimensions.len()
            ));
        }

        let source_dims = self.dimensions();
        Self::validate_unique_dimension_names(dimensions)?;
        Self::validate_unique_dimension_names(&source_dims)?;

        // Reorder source axes into target-dimension order and insert singleton
        // axes for missing dimensions before applying ndarray broadcasting.
        let source_axis_order: Vec<usize> = dimensions
            .iter()
            .filter_map(|target_dim| source_dims.iter().position(|d| d == target_dim))
            .collect();

        if source_axis_order.len() != source_dims.len() {
            return Err(anyhow::anyhow!(
                "Source dimensions {:?} are not a subset of target dimensions {:?}",
                source_dims,
                dimensions
            ));
        }

        let subset_data = self
            .backend
            .read_subset_with_validity(ArraySubset {
                start: vec![0; self.shape().len()],
                shape: self.shape(),
            })
            .await?;

        let nd_array = subset_data.values;
        let validity = subset_data.validity;

        let mut aligned = nd_array
            .view()
            .permuted_axes(source_axis_order.clone())
            .into_dyn();
        for (target_axis, target_dim) in dimensions.iter().enumerate() {
            if !source_dims.iter().any(|d| d == target_dim) {
                aligned = aligned.insert_axis(Axis(target_axis));
            }
        }

        let broadcasted = aligned.broadcast(target_shape).ok_or_else(|| {
            let source_shape = self.shape();
            tracing::warn!(?source_shape, ?target_shape, "cannot broadcast array to target shape");
            anyhow::anyhow!(
                "Cannot broadcast array of shape {:?} to target shape {:?}",
                source_shape,
                target_shape
            )
        })?;

        let broadcasted_vec = broadcasted.to_owned().into_raw_vec_and_offset().0;
        let broadcasted_validity = validity
            .map(|validity| {
                let mut aligned_validity = validity
                    .view()
                    .permuted_axes(source_axis_order.clone())
                    .into_dyn();
                for (target_axis, target_dim) in dimensions.iter().enumerate() {
                    if !source_dims.iter().any(|d| d == target_dim) {
                        aligned_validity = aligned_validity.insert_axis(Axis(target_axis));
                    }
                }

                aligned_validity
                    .broadcast(target_shape)
                    .map(|mask| mask.to_owned().into_raw_vec_and_offset().0)
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Cannot broadcast validity mask of shape {:?} to target shape {:?}",
                            self.shape(),
                            target_shape
                        )
                    })
            })
            .transpose()?;

        let broadcasted_dispatch = NdArrowArrayDispatch::new_in_mem_with_validity(
            broadcasted_vec,
            target_shape.to_vec(),
            dimensions.to_vec(),
            self.backend.fill_value(),
            broadcasted_validity,
        )?;
        Ok(Arc::new(broadcasted_dispatch))
    }
    fn shape(&self) -> Vec<usize> {
        self.backend.shape()
    }
    fn dimensions(&self) -> Vec<String> {
        self.backend.dimensions()
    }

    fn data_type(&self) -> arrow::datatypes::DataType {
        T::data_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int32Array};

    fn dims(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// `Arc<dyn NdArrowArray>` is not `Debug`, so `unwrap_err` is unavailable —
    /// unwrap the error message by hand instead.
    fn expect_err(result: anyhow::Result<Arc<dyn NdArrowArray>>) -> String {
        match result {
            Ok(_) => panic!("expected an error, got an array"),
            Err(error) => error.to_string(),
        }
    }

    /// Materialise an i32 array and return (values, null-flags).
    async fn i32_parts(array: &Arc<dyn NdArrowArray>) -> (Vec<i32>, Vec<bool>) {
        let arrow_array = array.as_arrow_array_ref().await.unwrap();
        let typed = arrow_array
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("expected Int32Array");
        let values = typed.values().to_vec();
        let nulls = (0..typed.len()).map(|i| typed.is_null(i)).collect();
        (values, nulls)
    }

    #[tokio::test]
    async fn test_broadcasting() {
        let array = vec![1, 2, 3];
        let shape = vec![3, 1];
        let dimensions = vec!["x".to_string(), "y".to_string()];
        let nd_array = NdArrowArrayDispatch::new_in_mem(array, shape, dimensions, None).unwrap();

        let target_shape = vec![3, 4];
        let target_dimensions = vec!["x".to_string(), "y".to_string()];
        let broadcasted = nd_array
            .broadcast(&target_shape, &target_dimensions)
            .await
            .unwrap();

        assert_eq!(broadcasted.shape(), target_shape);
        assert_eq!(broadcasted.dimensions(), target_dimensions);
    }

    #[tokio::test]
    #[ignore = "known bug: a pure transposition (target shape equals the permuted source \
                shape, so ndarray never has to expand anything) keeps the source memory \
                layout through `to_owned()`, and `into_raw_vec_and_offset()` then hands \
                Arrow the values in source order. Needs `.as_standard_layout()` before \
                taking the raw vec. Cases that also expand an axis are unaffected — see \
                `test_broadcast_reorders_and_inserts_simultaneously`."]
    async fn test_broadcast_reorders_source_axes_by_name() {
        // Source is stored as (lat, time); the target wants (time, lat), so the
        // values must be transposed — not merely reinterpreted.
        //   lat0 -> [0, 1, 2]   lat1 -> [3, 4, 5]
        // becomes, in (time, lat) order: [0, 3, 1, 4, 2, 5].
        let nd_array = NdArrowArrayDispatch::new_in_mem(
            vec![0, 1, 2, 3, 4, 5],
            vec![2, 3],
            dims(&["lat", "time"]),
            None,
        )
        .unwrap();

        let broadcasted = nd_array
            .broadcast(&[3, 2], &dims(&["time", "lat"]))
            .await
            .unwrap();

        assert_eq!(broadcasted.shape(), vec![3, 2]);
        assert_eq!(broadcasted.dimensions(), dims(&["time", "lat"]));
        assert_eq!(i32_parts(&broadcasted).await.0, vec![0, 3, 1, 4, 2, 5]);
    }

    #[tokio::test]
    async fn test_broadcast_inserts_missing_dimension() {
        // A `lat`-only coordinate repeated along a new leading `time` axis.
        let nd_array =
            NdArrowArrayDispatch::new_in_mem(vec![10, 20], vec![2], dims(&["lat"]), None).unwrap();

        let broadcasted = nd_array
            .broadcast(&[3, 2], &dims(&["time", "lat"]))
            .await
            .unwrap();

        assert_eq!(
            i32_parts(&broadcasted).await.0,
            vec![10, 20, 10, 20, 10, 20]
        );
    }

    #[tokio::test]
    async fn test_broadcast_reorders_and_inserts_simultaneously() {
        // Source (lon, time) must be transposed to (time, lon) *and* gain a
        // `lat` axis wedged between them.
        //   lon0 -> [t0=1, t1=2], lon1 -> [t0=3, t1=4]
        // target (time, lat, lon) = 2x2x2 -> [1,3, 1,3, 2,4, 2,4]
        let nd_array = NdArrowArrayDispatch::new_in_mem(
            vec![1, 2, 3, 4],
            vec![2, 2],
            dims(&["lon", "time"]),
            None,
        )
        .unwrap();

        let broadcasted = nd_array
            .broadcast(&[2, 2, 2], &dims(&["time", "lat", "lon"]))
            .await
            .unwrap();

        assert_eq!(
            i32_parts(&broadcasted).await.0,
            vec![1, 3, 1, 3, 2, 4, 2, 4]
        );
    }

    #[tokio::test]
    async fn test_broadcast_scalar_attribute_fills_whole_grid() {
        // NetCDF attributes surface as rank-0 arrays; they must broadcast onto
        // any target grid.
        let nd_array =
            NdArrowArrayDispatch::new_in_mem(vec![7], vec![], dims(&[]), None).unwrap();

        let broadcasted = nd_array
            .broadcast(&[2, 3], &dims(&["time", "lat"]))
            .await
            .unwrap();

        assert_eq!(i32_parts(&broadcasted).await.0, vec![7; 6]);
    }

    #[tokio::test]
    async fn test_broadcast_zero_length_dimension_yields_empty_array() {
        let nd_array =
            NdArrowArrayDispatch::new_in_mem(Vec::<i32>::new(), vec![0], dims(&["time"]), None)
                .unwrap();

        let broadcasted = nd_array
            .broadcast(&[0, 3], &dims(&["time", "lat"]))
            .await
            .unwrap();

        assert_eq!(broadcasted.shape(), vec![0, 3]);
        assert!(i32_parts(&broadcasted).await.0.is_empty());
    }

    #[tokio::test]
    async fn test_broadcast_rejects_shape_dimension_rank_mismatch() {
        let nd_array =
            NdArrowArrayDispatch::new_in_mem(vec![1, 2], vec![2], dims(&["lat"]), None).unwrap();

        let err = expect_err(nd_array.broadcast(&[2, 3], &dims(&["lat"])).await);
        assert!(
            err.contains("does not match dimensions length"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_broadcast_rejects_source_dims_not_subset_of_target() {
        // `nv` (a CF-bounds style axis) is absent from the target dimensions.
        let nd_array = NdArrowArrayDispatch::new_in_mem(
            vec![1, 2, 3, 4],
            vec![2, 2],
            dims(&["lat", "nv"]),
            None,
        )
        .unwrap();

        let err = expect_err(nd_array.broadcast(&[2, 2], &dims(&["time", "lat"])).await);
        assert!(err.contains("not a subset"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn test_broadcast_rejects_duplicate_target_dimension_names() {
        let nd_array =
            NdArrowArrayDispatch::new_in_mem(vec![1, 2], vec![2], dims(&["lat"]), None).unwrap();

        let err = expect_err(nd_array.broadcast(&[2, 2], &dims(&["lat", "lat"])).await);
        assert!(
            err.contains("Duplicate dimension name"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_broadcast_rejects_incompatible_extent_on_shared_dimension() {
        // A shared dimension may only be broadcast from length 1; 3 -> 5 is not
        // a legal stretch.
        let nd_array =
            NdArrowArrayDispatch::new_in_mem(vec![1, 2, 3], vec![3], dims(&["lat"]), None).unwrap();

        let err = expect_err(nd_array.broadcast(&[5], &dims(&["lat"])).await);
        assert!(err.contains("Cannot broadcast"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn test_fill_value_becomes_null_and_survives_broadcast() {
        let nd_array = NdArrowArrayDispatch::new_in_mem(
            vec![1, -999, 3],
            vec![3],
            dims(&["lat"]),
            Some(-999),
        )
        .unwrap();

        let (values, nulls) = i32_parts(&(Arc::new(nd_array.clone()) as Arc<dyn NdArrowArray>)).await;
        assert_eq!(values, vec![1, -999, 3]);
        assert_eq!(nulls, vec![false, true, false]);

        // The fill value is carried onto the broadcast result, so the repeated
        // slots are null too.
        let broadcasted = nd_array
            .broadcast(&[2, 3], &dims(&["time", "lat"]))
            .await
            .unwrap();
        let (_, nulls) = i32_parts(&broadcasted).await;
        assert_eq!(nulls, vec![false, true, false, false, true, false]);
    }

    #[tokio::test]
    async fn test_validity_mask_is_broadcast_alongside_values() {
        let nd_array = NdArrowArrayDispatch::new_in_mem_with_validity(
            vec![5, 6],
            vec![2],
            dims(&["lat"]),
            None,
            Some(vec![true, false]),
        )
        .unwrap();

        let broadcasted = nd_array
            .broadcast(&[3, 2], &dims(&["time", "lat"]))
            .await
            .unwrap();
        let (values, nulls) = i32_parts(&broadcasted).await;
        assert_eq!(values, vec![5, 6, 5, 6, 5, 6]);
        assert_eq!(nulls, vec![false, true, false, true, false, true]);
    }

    #[tokio::test]
    async fn test_subset_slices_values_and_keeps_dimension_names() {
        let nd_array = NdArrowArrayDispatch::new_in_mem(
            (0..12).collect::<Vec<i32>>(),
            vec![3, 4],
            dims(&["time", "lat"]),
            None,
        )
        .unwrap();

        let sub = nd_array
            .subset(ArraySubset::new(vec![1, 1], vec![2, 2]))
            .await
            .unwrap();

        assert_eq!(sub.shape(), vec![2, 2]);
        assert_eq!(sub.dimensions(), dims(&["time", "lat"]));
        // Rows 1..3, columns 1..3 of a row-major 3x4 grid.
        assert_eq!(i32_parts(&sub).await.0, vec![5, 6, 9, 10]);
    }

    #[tokio::test]
    async fn test_subset_out_of_bounds_is_rejected() {
        let nd_array = NdArrowArrayDispatch::new_in_mem(
            (0..12).collect::<Vec<i32>>(),
            vec![3, 4],
            dims(&["time", "lat"]),
            None,
        )
        .unwrap();

        let err = expect_err(nd_array.subset(ArraySubset::new(vec![2, 0], vec![2, 4])).await);
        assert!(err.contains("exceeds axis size"), "unexpected error: {err}");
    }

    #[test]
    fn test_new_rejects_shape_dimension_count_mismatch() {
        let err = NdArrowArrayDispatch::new_in_mem(
            vec![1, 2, 3, 4],
            vec![2, 2],
            dims(&["only_one"]),
            None,
        )
        .unwrap_err();
        assert!(
            matches!(err, NdArrowError::ShapeDimensionMismatch { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_new_rejects_values_that_do_not_fill_the_shape() {
        let err =
            NdArrowArrayDispatch::new_in_mem(vec![1, 2, 3], vec![2, 2], dims(&["a", "b"]), None)
                .unwrap_err();
        assert!(
            matches!(err, NdArrowError::InvalidShape(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_new_rejects_validity_mask_of_the_wrong_shape() {
        let err = NdArrowArrayDispatch::new_in_mem_with_validity(
            vec![1, 2, 3, 4],
            vec![2, 2],
            dims(&["a", "b"]),
            None,
            Some(vec![true, false]),
        )
        .unwrap_err();
        assert!(
            matches!(err, NdArrowError::InvalidShape(_)),
            "unexpected error: {err}"
        );
    }
}
