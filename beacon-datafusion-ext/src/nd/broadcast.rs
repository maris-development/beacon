//! Virtual broadcast views over flat C-order arrays.
//!
//! A [`BroadcastMap`] captures a name-aligned (xarray-style) broadcast of a
//! source array onto a target dimension grid as pure index arithmetic: per
//! target axis, a source stride. A stride of `0` means the source does not
//! advance along that axis (the axis is missing from the source, or has size
//! 1 in it). No data moves until [`BroadcastMap::gather_indices`] feeds an
//! Arrow `take`.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, RunArray, UInt64Array};
use arrow::datatypes::Int64Type;
use datafusion::common::plan_err;
use datafusion::error::{DataFusionError, Result};

use super::dimensions::Dimensions;

/// Broadcast of a source dimension set onto a target dimension set,
/// represented as one source stride per target axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BroadcastMap {
    target_shape: Vec<usize>,
    /// Source stride per target axis; `0` = source does not advance.
    strides: Vec<usize>,
    source_len: usize,
}

/// Run-length structure of a broadcast view: the flattened output consists of
/// `num_runs` runs of `run_len` consecutive equal source elements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunStructure {
    pub run_len: usize,
    pub num_runs: usize,
}

impl BroadcastMap {
    /// Build the broadcast of `source` onto `target`.
    ///
    /// Rules (matching `beacon-nd-array`'s `permuted_axes`-based broadcast):
    /// - every source dimension must appear in `target` (matched by name;
    ///   reordering is allowed and expressed through the strides);
    /// - a source axis must have the same size as the target axis, or size 1
    ///   (which expands).
    pub fn try_new(source: &Dimensions, target: &Dimensions) -> Result<Self> {
        let source_strides = source.c_strides();
        let mut strides = vec![0usize; target.rank()];

        for (source_axis, dim) in source.iter().enumerate() {
            let Some(target_axis) = target.position(dim.name()) else {
                return plan_err!(
                    "cannot broadcast {source} to {target}: dimension '{}' is missing from the target",
                    dim.name()
                );
            };

            let target_size = target.get(target_axis).size();
            if dim.size() == target_size {
                strides[target_axis] = source_strides[source_axis];
            } else if dim.size() == 1 {
                strides[target_axis] = 0;
            } else {
                return plan_err!(
                    "cannot broadcast {source} to {target}: dimension '{}' has size {} but the target expects {}",
                    dim.name(),
                    dim.size(),
                    target_size
                );
            }
        }

        Ok(Self {
            target_shape: target.shape(),
            strides,
            source_len: source.num_elements(),
        })
    }

    pub fn target_shape(&self) -> &[usize] {
        &self.target_shape
    }

    pub fn num_target_elements(&self) -> usize {
        self.target_shape.iter().product()
    }

    /// True when the broadcast is a no-op: every target row maps to the source
    /// row of the same index.
    pub fn is_identity(&self) -> bool {
        if self.source_len != self.num_target_elements() {
            return false;
        }
        let mut acc = 1usize;
        for axis in (0..self.target_shape.len()).rev() {
            let size = self.target_shape[axis];
            // A stride mismatch on a size-1 axis is irrelevant: its coordinate
            // is always 0.
            if size > 1 && self.strides[axis] != acc {
                return false;
            }
            acc = acc.saturating_mul(size);
        }
        true
    }

    /// Source index for one flattened target row. Prefer [`Self::gather_indices`]
    /// for bulk access.
    pub fn source_index_of(&self, target_row: usize) -> usize {
        let mut rem = target_row;
        let mut idx = 0usize;
        for axis in (0..self.target_shape.len()).rev() {
            let size = self.target_shape[axis].max(1);
            let coord = rem % size;
            rem /= size;
            idx += coord * self.strides[axis];
        }
        idx
    }

    /// Take indices for the (optionally selected) broadcast view, in row-major
    /// target order.
    ///
    /// `keep` gives, per target axis, an optional sorted list of kept
    /// coordinates; `None` keeps the whole axis. Passing `None` for the slice
    /// itself keeps everything.
    pub fn gather_indices(&self, keep: Option<&[Option<&[u32]>]>) -> UInt64Array {
        let rank = self.target_shape.len();
        // Per-axis list of precomputed source offsets (coordinate * stride).
        let mut axis_offsets: Vec<Vec<u64>> = Vec::with_capacity(rank);
        for axis in 0..rank {
            let stride = self.strides[axis] as u64;
            let offsets = match keep.and_then(|k| k[axis]) {
                Some(coords) => coords.iter().map(|&c| c as u64 * stride).collect(),
                None => (0..self.target_shape[axis] as u64)
                    .map(|c| c * stride)
                    .collect(),
            };
            axis_offsets.push(offsets);
        }

        let total: usize = axis_offsets.iter().map(|offsets| offsets.len()).product();
        let mut out = Vec::with_capacity(total);
        if total > 0 {
            fill_indices(&axis_offsets, 0, 0, &mut out);
        }
        UInt64Array::from(out)
    }

    /// Run-length structure of the flattened broadcast: maximal run of
    /// trailing axes along which the source does not advance.
    pub fn run_structure(&self) -> RunStructure {
        let mut run_len = 1usize;
        for axis in (0..self.target_shape.len()).rev() {
            if self.strides[axis] == 0 {
                run_len = run_len.saturating_mul(self.target_shape[axis]);
            } else {
                break;
            }
        }
        let total = self.num_target_elements();
        RunStructure {
            run_len,
            num_runs: if run_len == 0 { 0 } else { total / run_len.max(1) },
        }
    }

    /// Encode the full (unselected) broadcast view as a run-end encoded array.
    ///
    /// Returns `None` when the view has no run structure (`run_len <= 1`), in
    /// which case a plain gather is the better representation.
    pub fn to_run_array(&self, values: &ArrayRef) -> Result<Option<ArrayRef>> {
        let RunStructure { run_len, num_runs } = self.run_structure();
        if run_len <= 1 || num_runs == 0 {
            return Ok(None);
        }

        // The run values are the broadcast view over the leading axes only
        // (everything before the trailing zero-stride run).
        let mut trailing = 1usize;
        let mut split = self.target_shape.len();
        while split > 0 && trailing < run_len {
            split -= 1;
            trailing *= self.target_shape[split];
        }
        let prefix = BroadcastMap {
            target_shape: self.target_shape[..split].to_vec(),
            strides: self.strides[..split].to_vec(),
            source_len: self.source_len,
        };
        let run_values = if prefix.is_identity() {
            values.clone()
        } else {
            arrow::compute::take(values.as_ref(), &prefix.gather_indices(None), None)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        };

        let run_ends: Int64Array = (1..=num_runs).map(|i| Some((i * run_len) as i64)).collect();
        let run_array = RunArray::<Int64Type>::try_new(&run_ends, run_values.as_ref())
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;
        Ok(Some(Arc::new(run_array)))
    }
}

/// Row-major cartesian sum of per-axis offsets. The innermost axis is a tight
/// loop; outer axes recurse (rank is small in practice).
fn fill_indices(axis_offsets: &[Vec<u64>], axis: usize, base: u64, out: &mut Vec<u64>) {
    if axis_offsets.is_empty() {
        out.push(base);
        return;
    }
    if axis == axis_offsets.len() - 1 {
        out.extend(axis_offsets[axis].iter().map(|&off| base + off));
        return;
    }
    for &off in &axis_offsets[axis] {
        fill_indices(axis_offsets, axis + 1, base + off, out);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nd::dimensions::Dimension;
    use arrow::array::{Array, AsArray, Int32Array};

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    fn indices(map: &BroadcastMap, keep: Option<&[Option<&[u32]>]>) -> Vec<u64> {
        map.gather_indices(keep).values().to_vec()
    }

    #[test]
    fn outer_dim_broadcast_is_runs() {
        // time[2] onto (time, lon=3): each time value repeats 3x contiguously.
        let map = BroadcastMap::try_new(&dims(&[("time", 2)]), &dims(&[("time", 2), ("lon", 3)]))
            .unwrap();
        assert_eq!(indices(&map, None), vec![0, 0, 0, 1, 1, 1]);
        assert_eq!(
            map.run_structure(),
            RunStructure {
                run_len: 3,
                num_runs: 2
            }
        );
    }

    #[test]
    fn inner_dim_broadcast_is_tiled() {
        // lon[3] onto (time=2, lon): the lon pattern tiles, no runs.
        let map = BroadcastMap::try_new(&dims(&[("lon", 3)]), &dims(&[("time", 2), ("lon", 3)]))
            .unwrap();
        assert_eq!(indices(&map, None), vec![0, 1, 2, 0, 1, 2]);
        assert_eq!(map.run_structure().run_len, 1);
    }

    #[test]
    fn middle_dim_broadcast() {
        // lat[2] onto (time=2, lat=2, lon=2): runs of 2, tiled twice.
        let map = BroadcastMap::try_new(
            &dims(&[("lat", 2)]),
            &dims(&[("time", 2), ("lat", 2), ("lon", 2)]),
        )
        .unwrap();
        assert_eq!(indices(&map, None), vec![0, 0, 1, 1, 0, 0, 1, 1]);
        assert_eq!(
            map.run_structure(),
            RunStructure {
                run_len: 2,
                num_runs: 4
            }
        );
    }

    #[test]
    fn scalar_broadcast() {
        let map =
            BroadcastMap::try_new(&Dimensions::scalar(), &dims(&[("time", 2), ("lon", 2)]))
                .unwrap();
        assert_eq!(indices(&map, None), vec![0, 0, 0, 0]);
        assert_eq!(
            map.run_structure(),
            RunStructure {
                run_len: 4,
                num_runs: 1
            }
        );
    }

    #[test]
    fn identity_detected() {
        let target = dims(&[("time", 2), ("lon", 3)]);
        let map = BroadcastMap::try_new(&target, &target).unwrap();
        assert!(map.is_identity());

        let map =
            BroadcastMap::try_new(&dims(&[("lon", 3)]), &dims(&[("time", 2), ("lon", 3)]))
                .unwrap();
        assert!(!map.is_identity());
    }

    #[test]
    fn size_one_expansion() {
        // time[1] onto time[4]: stride 0.
        let map =
            BroadcastMap::try_new(&dims(&[("time", 1)]), &dims(&[("time", 4)])).unwrap();
        assert_eq!(indices(&map, None), vec![0, 0, 0, 0]);
    }

    #[test]
    fn transposed_dims_gather_correctly() {
        // Source stored (lon, time); target order (time, lon). Matching
        // beacon-nd-array's permuted_axes broadcast, the view transposes.
        let map = BroadcastMap::try_new(
            &dims(&[("lon", 3), ("time", 2)]),
            &dims(&[("time", 2), ("lon", 3)]),
        )
        .unwrap();
        // source flat layout: (l0,t0),(l0,t1),(l1,t0),(l1,t1),(l2,t0),(l2,t1)
        assert_eq!(indices(&map, None), vec![0, 2, 4, 1, 3, 5]);
        assert!(!map.is_identity());
    }

    #[test]
    fn size_mismatch_rejected() {
        let result =
            BroadcastMap::try_new(&dims(&[("time", 2)]), &dims(&[("time", 4)]));
        assert!(result.is_err());
    }

    #[test]
    fn gather_with_axis_selection() {
        // sst(time=2, lon=3), keep time=[1], lon=[0, 2].
        let source = dims(&[("time", 2), ("lon", 3)]);
        let map = BroadcastMap::try_new(&source, &source).unwrap();
        let keep: Vec<Option<&[u32]>> = vec![Some(&[1]), Some(&[0, 2])];
        assert_eq!(indices(&map, Some(&keep)), vec![3, 5]);
    }

    #[test]
    fn gather_selection_composes_with_broadcast() {
        // lat[3] onto (time=2, lat=3); keep time=[0], lat=[2] → single row
        // reading source element 2. The unfiltered broadcast never exists.
        let map = BroadcastMap::try_new(&dims(&[("lat", 3)]), &dims(&[("time", 2), ("lat", 3)]))
            .unwrap();
        let keep: Vec<Option<&[u32]>> = vec![Some(&[0]), Some(&[2])];
        assert_eq!(indices(&map, Some(&keep)), vec![2]);
    }

    #[test]
    fn run_array_round_trip() {
        // time values [10, 20] broadcast onto (time=2, lon=3) → REE with 2 runs.
        let values: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let map = BroadcastMap::try_new(&dims(&[("time", 2)]), &dims(&[("time", 2), ("lon", 3)]))
            .unwrap();
        let ree = map.to_run_array(&values).unwrap().unwrap();
        let run = ree.as_any().downcast_ref::<RunArray<Int64Type>>().unwrap();
        assert_eq!(run.len(), 6);
        assert_eq!(
            run.run_ends().values(),
            &[3i64, 6]
        );
        let inner = run.values().as_primitive::<arrow::datatypes::Int32Type>();
        assert_eq!(inner.values(), &[10, 20]);
    }

    #[test]
    fn run_array_none_when_no_runs() {
        let values: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
        let map = BroadcastMap::try_new(&dims(&[("lon", 3)]), &dims(&[("time", 2), ("lon", 3)]))
            .unwrap();
        assert!(map.to_run_array(&values).unwrap().is_none());
    }

    #[test]
    fn empty_axis_yields_no_indices() {
        let map =
            BroadcastMap::try_new(&dims(&[("time", 0)]), &dims(&[("time", 0), ("lon", 3)]))
                .unwrap();
        assert_eq!(indices(&map, None), Vec::<u64>::new());
    }
}
