//! Virtual broadcast views over flat C-order arrays.
//!
//! A [`BroadcastMap`] captures a name-aligned (xarray-style) broadcast of a
//! source array onto a target dimension grid as pure index arithmetic: per
//! target axis, a source stride. A stride of `0` means the source does not
//! advance along that axis (the axis is missing from the source, or has size
//! 1 in it). No data moves until [`BroadcastMap::gather_indices`] feeds an
//! Arrow `take`.

use arrow::array::UInt64Array;
use datafusion::common::plan_err;
use datafusion::error::Result;

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

    fn num_target_elements(&self) -> usize {
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

    /// Take indices for the broadcast view, in row-major target order.
    pub fn gather_indices(&self) -> UInt64Array {
        let rank = self.target_shape.len();
        // Per-axis list of precomputed source offsets (coordinate * stride).
        let axis_offsets: Vec<Vec<u64>> = (0..rank)
            .map(|axis| {
                let stride = self.strides[axis] as u64;
                (0..self.target_shape[axis] as u64)
                    .map(|c| c * stride)
                    .collect()
            })
            .collect();

        let total: usize = axis_offsets.iter().map(|offsets| offsets.len()).product();
        let mut out = Vec::with_capacity(total);
        if total > 0 {
            fill_indices(&axis_offsets, 0, 0, &mut out);
        }
        UInt64Array::from(out)
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

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    fn indices(map: &BroadcastMap) -> Vec<u64> {
        map.gather_indices().values().to_vec()
    }

    #[test]
    fn outer_dim_broadcast_repeats() {
        // time[2] onto (time, lon=3): each time value repeats 3x contiguously.
        let map = BroadcastMap::try_new(&dims(&[("time", 2)]), &dims(&[("time", 2), ("lon", 3)]))
            .unwrap();
        assert_eq!(indices(&map), vec![0, 0, 0, 1, 1, 1]);
    }

    #[test]
    fn inner_dim_broadcast_tiles() {
        // lon[3] onto (time=2, lon): the lon pattern tiles.
        let map = BroadcastMap::try_new(&dims(&[("lon", 3)]), &dims(&[("time", 2), ("lon", 3)]))
            .unwrap();
        assert_eq!(indices(&map), vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn middle_dim_broadcast() {
        // lat[2] onto (time=2, lat=2, lon=2): runs of 2, tiled twice.
        let map = BroadcastMap::try_new(
            &dims(&[("lat", 2)]),
            &dims(&[("time", 2), ("lat", 2), ("lon", 2)]),
        )
        .unwrap();
        assert_eq!(indices(&map), vec![0, 0, 1, 1, 0, 0, 1, 1]);
    }

    #[test]
    fn scalar_broadcast() {
        let map =
            BroadcastMap::try_new(&Dimensions::scalar(), &dims(&[("time", 2), ("lon", 2)])).unwrap();
        assert_eq!(indices(&map), vec![0, 0, 0, 0]);
    }

    #[test]
    fn identity_detected() {
        let target = dims(&[("time", 2), ("lon", 3)]);
        let map = BroadcastMap::try_new(&target, &target).unwrap();
        assert!(map.is_identity());

        let map =
            BroadcastMap::try_new(&dims(&[("lon", 3)]), &dims(&[("time", 2), ("lon", 3)])).unwrap();
        assert!(!map.is_identity());
    }

    #[test]
    fn size_one_expansion() {
        // time[1] onto time[4]: stride 0.
        let map = BroadcastMap::try_new(&dims(&[("time", 1)]), &dims(&[("time", 4)])).unwrap();
        assert_eq!(indices(&map), vec![0, 0, 0, 0]);
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
        assert_eq!(indices(&map), vec![0, 2, 4, 1, 3, 5]);
        assert!(!map.is_identity());
    }

    #[test]
    fn size_mismatch_rejected() {
        let result = BroadcastMap::try_new(&dims(&[("time", 2)]), &dims(&[("time", 4)]));
        assert!(result.is_err());
    }

    #[test]
    fn missing_target_dimension_rejected() {
        // A source axis the target does not carry cannot be broadcast away —
        // dropping it would silently collapse values.
        let result =
            BroadcastMap::try_new(&dims(&[("depth", 2)]), &dims(&[("time", 2), ("lon", 3)]));
        assert!(result.is_err());
    }

    #[test]
    fn size_one_axis_does_not_break_identity() {
        // A degenerate axis carries no information, so a stride mismatch on it
        // must not disqualify the zero-copy identity path.
        let source = dims(&[("time", 1), ("lon", 3)]);
        let map = BroadcastMap::try_new(&source, &source).unwrap();
        assert!(map.is_identity());
        assert_eq!(indices(&map), vec![0, 1, 2]);
    }

    #[test]
    fn empty_axis_yields_no_indices() {
        let map =
            BroadcastMap::try_new(&dims(&[("time", 0)]), &dims(&[("time", 0), ("lon", 3)])).unwrap();
        assert_eq!(indices(&map), Vec::<u64>::new());
    }
}
