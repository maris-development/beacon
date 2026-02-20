use arrow::array::{ArrayRef, UInt64Array, new_empty_array};

use crate::NdArrowArray;

#[derive(Debug)]
pub struct ArrayBroadcastView {
    array: NdArrowArray,
    out_shape: Vec<usize>,
    strides: Vec<isize>,
}

impl ArrayBroadcastView {
    pub fn new(array: NdArrowArray, out_shape: Vec<usize>, strides: Vec<isize>) -> Self {
        Self {
            array,
            out_shape,
            strides,
        }
    }

    pub fn len(&self) -> usize {
        if self.out_shape.is_empty() {
            1
        } else {
            self.out_shape.iter().product()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn take(&self, offset: usize, len: usize) -> ArrayRef {
        // Slices the NdArrowArray after broadcasting. The broadcasting strides and outshape are calculated before.
        // When slicing it should slice along the broadcasted shape.
        if len == 0 {
            return new_empty_array(self.array.values().data_type());
        }

        let total_len = self.len();
        if offset >= total_len {
            return new_empty_array(self.array.values().data_type());
        }

        let take_len = len.min(total_len - offset);
        let indices =
            UInt64Array::from_iter_values((offset..(offset + take_len)).map(|out_flat| {
                map_out_flat_to_in_flat(out_flat, &self.out_shape, &self.strides) as u64
            }));

        arrow::compute::take(self.array.values().as_ref(), &indices, None)
            .expect("broadcast take failed: invalid virtual broadcast mapping")
    }

    pub fn take_all(&self) -> ArrayRef {
        self.take(0, self.len())
    }
}

fn map_out_flat_to_in_flat(mut out_flat: usize, shape: &[usize], strides: &[isize]) -> usize {
    if shape.is_empty() {
        return 0;
    }

    let mut in_flat: isize = 0;
    for (dim_size, stride) in shape.iter().rev().zip(strides.iter().rev()) {
        let coord = out_flat % *dim_size;
        out_flat /= *dim_size;
        in_flat += (*stride) * coord as isize;
    }

    debug_assert!(in_flat >= 0);
    in_flat as usize
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;

    use crate::{
        NdArrowArray,
        dimensions::{Dimension, Dimensions},
    };

    use super::ArrayBroadcastView;

    #[test]
    fn take_from_virtual_broadcasted_window() {
        let array = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![10, 20, 30])),
            Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]),
        )
        .unwrap();

        let view = ArrayBroadcastView::new(array, vec![2, 3], vec![0, 1]);
        let taken = view.take(1, 3);
        let values = taken.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[20, 30, 10]);
    }

    #[test]
    fn take_truncates_to_available_broadcast_length() {
        let array = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![7])),
            Dimensions::new(vec![Dimension::try_new("x", 1).unwrap()]),
        )
        .unwrap();

        let view = ArrayBroadcastView::new(array, vec![2, 2], vec![0, 0]);
        let taken = view.take(3, 10);
        let values = taken.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[7]);
    }

    #[test]
    fn take_out_of_bounds_returns_empty() {
        let array = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2])),
            Dimensions::new(vec![Dimension::try_new("x", 2).unwrap()]),
        )
        .unwrap();

        let view = ArrayBroadcastView::new(array, vec![2], vec![1]);
        let taken = view.take(2, 1);
        assert_eq!(taken.len(), 0);
    }

    #[test]
    fn take_from_1d_broadcasted_to_3d_array() {
        let array = NdArrowArray::new(
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Dimensions::new(vec![Dimension::try_new("x", 3).unwrap()]),
        )
        .unwrap();

        // Virtual broadcast from [3] to [2, 2, 3].
        // The first two axes are broadcasted (stride 0), the last axis maps to input x.
        let view = ArrayBroadcastView::new(array, vec![2, 2, 3], vec![0, 0, 1]);

        let taken = view.take(4, 5);
        let values = taken.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[2, 3, 1, 2, 3]);
    }
}
