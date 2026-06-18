//! Direct construction of broadcast columns as encoded Arrow arrays.
//!
//! Instead of expanding a broadcast coordinate to `O(rows)` flat values and then
//! re-compressing, this module builds the encoded array straight from the *unbroadcast*
//! source values plus the broadcast geometry:
//!
//! - [`ColumnEncoding::RunEndEncoded`] → a [`RunArray`] with arithmetic run-ends; only
//!   one value is gathered per run, so construction is `O(num_runs)`.
//! - [`ColumnEncoding::Dictionary`] → a [`DictionaryArray`] whose values are the source
//!   array and whose keys are the per-row source indices.
//! - [`ColumnEncoding::Flat`] → the plain broadcast result (a gather over the source).
//!
//! The decoded result of every encoding is identical to the old "broadcast then
//! flatten" output; see the tests. Choosing *which* encoding to build is the job of
//! [`crate::encoding::classify_column_encoding`]; wiring this into the flatten pipeline
//! is #279.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, RunArray, UInt16Array, UInt32Array,
    make_array,
};
use arrow::datatypes::{DataType, Field, Int32Type, Int64Type, UInt16Type, UInt32Type};

use crate::encoding::{BroadcastGeometry, ColumnEncoding, broadcast_geometry};

/// Maps each row of the flattened (broadcast) output back to the index of its value in
/// the row-major source array. Precomputed strides keep `source_index` cheap.
struct GatherPlan {
    /// One `(target_stride, target_dim_size, source_stride)` triple per source dim.
    terms: Vec<(usize, usize, usize)>,
}

impl GatherPlan {
    fn build(source_dims: &[String], target_dims: &[String], target_shape: &[usize]) -> Self {
        let nt = target_dims.len();

        // Row-major strides over the target shape.
        let mut tstride = vec![1usize; nt];
        for t in (0..nt.saturating_sub(1)).rev() {
            tstride[t] = tstride[t + 1] * target_shape[t + 1];
        }

        // Source shape/strides follow the source dim order; each source dim's size is
        // taken from the matching target dim.
        let source_shape: Vec<usize> = source_dims
            .iter()
            .map(|d| target_shape[target_dims.iter().position(|x| x == d).unwrap()])
            .collect();
        let ns = source_shape.len();
        let mut sstride = vec![1usize; ns];
        for p in (0..ns.saturating_sub(1)).rev() {
            sstride[p] = sstride[p + 1] * source_shape[p + 1];
        }

        let terms = source_dims
            .iter()
            .enumerate()
            .map(|(p, d)| {
                let ti = target_dims.iter().position(|x| x == d).unwrap();
                (tstride[ti], target_shape[ti], sstride[p])
            })
            .collect();

        Self { terms }
    }

    /// Index into the source array for the given flattened output row.
    #[inline]
    fn source_index(&self, target_flat: usize) -> usize {
        self.terms
            .iter()
            .map(|&(tstride, dim, sstride)| ((target_flat / tstride) % dim) * sstride)
            .sum()
    }
}

/// Build the broadcast of `source_values` as an array with the requested `encoding`.
///
/// `source_values` must be the unbroadcast source column in row-major source-dim order,
/// with length equal to the product of the source dim sizes. See
/// [`crate::encoding::broadcast_geometry`] for the dimension/shape contract.
pub fn build_broadcast_array(
    source_values: &dyn Array,
    source_dims: &[String],
    target_dims: &[String],
    target_shape: &[usize],
    encoding: ColumnEncoding,
) -> anyhow::Result<ArrayRef> {
    let geo = broadcast_geometry(source_dims, target_dims, target_shape).ok_or_else(|| {
        anyhow::anyhow!("invalid broadcast geometry: target dims/shape mismatch or unknown source dim")
    })?;

    if source_values.len() != geo.cardinality {
        anyhow::bail!(
            "source_values length {} does not match source cardinality {}",
            source_values.len(),
            geo.cardinality
        );
    }

    let plan = GatherPlan::build(source_dims, target_dims, target_shape);

    match encoding {
        ColumnEncoding::Flat => {
            let idx = UInt32Array::from_iter_values(
                (0..geo.total_rows).map(|i| plan.source_index(i) as u32),
            );
            Ok(arrow::compute::take(source_values, &idx, None)?)
        }
        ColumnEncoding::Dictionary => build_dictionary(source_values, &plan, &geo),
        ColumnEncoding::RunEndEncoded => build_run_end(source_values, &plan, &geo),
    }
}

fn build_dictionary(
    source_values: &dyn Array,
    plan: &GatherPlan,
    geo: &BroadcastGeometry,
) -> anyhow::Result<ArrayRef> {
    let values: ArrayRef = make_array(source_values.to_data());
    if geo.cardinality <= u16::MAX as usize {
        let keys =
            UInt16Array::from_iter_values((0..geo.total_rows).map(|i| plan.source_index(i) as u16));
        Ok(Arc::new(DictionaryArray::<UInt16Type>::try_new(keys, values)?))
    } else {
        let keys =
            UInt32Array::from_iter_values((0..geo.total_rows).map(|i| plan.source_index(i) as u32));
        Ok(Arc::new(DictionaryArray::<UInt32Type>::try_new(keys, values)?))
    }
}

fn build_run_end(
    source_values: &dyn Array,
    plan: &GatherPlan,
    geo: &BroadcastGeometry,
) -> anyhow::Result<ArrayRef> {
    // One value per run, gathered at the run's first row.
    let run_value_idx = UInt32Array::from_iter_values(
        (0..geo.num_runs).map(|r| plan.source_index(r * geo.run_length) as u32),
    );
    let run_values = arrow::compute::take(source_values, &run_value_idx, None)?;

    if geo.total_rows <= i32::MAX as usize {
        let run_ends = Int32Array::from_iter_values(
            (0..geo.num_runs).map(|r| ((r + 1) * geo.run_length) as i32),
        );
        Ok(Arc::new(RunArray::<Int32Type>::try_new(
            &run_ends,
            run_values.as_ref(),
        )?))
    } else {
        let run_ends = Int64Array::from_iter_values(
            (0..geo.num_runs).map(|r| ((r + 1) * geo.run_length) as i64),
        );
        Ok(Arc::new(RunArray::<Int64Type>::try_new(
            &run_ends,
            run_values.as_ref(),
        )?))
    }
}

/// The Arrow [`DataType`] that [`build_broadcast_array`] will produce for a column whose
/// values have type `value_type`. Use this to declare a scan's output schema so it
/// matches the constructed arrays exactly (needed by #279).
pub fn broadcast_encoded_data_type(
    value_type: &DataType,
    source_dims: &[String],
    target_dims: &[String],
    target_shape: &[usize],
    encoding: ColumnEncoding,
) -> anyhow::Result<DataType> {
    let geo = broadcast_geometry(source_dims, target_dims, target_shape).ok_or_else(|| {
        anyhow::anyhow!("invalid broadcast geometry: target dims/shape mismatch or unknown source dim")
    })?;

    Ok(match encoding {
        ColumnEncoding::Flat => value_type.clone(),
        ColumnEncoding::Dictionary => {
            let key = if geo.cardinality <= u16::MAX as usize {
                DataType::UInt16
            } else {
                DataType::UInt32
            };
            DataType::Dictionary(Box::new(key), Box::new(value_type.clone()))
        }
        ColumnEncoding::RunEndEncoded => {
            let run_end = if geo.total_rows <= i32::MAX as usize {
                DataType::Int32
            } else {
                DataType::Int64
            };
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", run_end, false)),
                Arc::new(Field::new("values", value_type.clone(), true)),
            )
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::array::{NdArrowArray, NdArrowArrayDispatch};
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use arrow::datatypes::RunEndIndexType;

    fn dims(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    const T: [&str; 3] = ["time", "lat", "lon"];
    const SHAPE: [usize; 3] = [4, 3, 5]; // total 60

    // --- decode helpers (mirror the proven flatten path) ---------------------

    fn flatten_run<R: RunEndIndexType>(run: &RunArray<R>) -> ArrayRef {
        let logical: Vec<u32> = (0..run.len() as u32).collect();
        let phys = run.get_physical_indices(&logical).unwrap();
        let idx = UInt32Array::from_iter_values(phys.into_iter().map(|i| i as u32));
        arrow::compute::take(run.values().as_ref(), &idx, None).unwrap()
    }

    fn decode(array: &dyn Array) -> ArrayRef {
        match array.data_type() {
            DataType::RunEndEncoded(_, _) => {
                if let Some(run) = array.as_any().downcast_ref::<RunArray<Int32Type>>() {
                    flatten_run(run)
                } else if let Some(run) = array.as_any().downcast_ref::<RunArray<Int64Type>>() {
                    flatten_run(run)
                } else {
                    panic!("unexpected run-end index type");
                }
            }
            DataType::Dictionary(_, value_type) => {
                arrow::compute::cast(array, value_type.as_ref()).unwrap()
            }
            _ => make_array(array.to_data()),
        }
    }

    fn assert_same(a: &dyn Array, b: &dyn Array) {
        assert_eq!(a.to_data(), b.to_data());
    }

    // --- tests ---------------------------------------------------------------

    #[test]
    fn ree_decodes_to_flat() {
        let values = Int32Array::from((0..4).collect::<Vec<i32>>()); // one per time
        let flat =
            build_broadcast_array(&values, &dims(&["time"]), &dims(&T), &SHAPE, ColumnEncoding::Flat)
                .unwrap();
        let ree = build_broadcast_array(
            &values,
            &dims(&["time"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::RunEndEncoded,
        )
        .unwrap();
        assert!(matches!(ree.data_type(), DataType::RunEndEncoded(_, _)));
        assert_same(decode(ree.as_ref()).as_ref(), flat.as_ref());
    }

    #[test]
    fn dict_decodes_to_flat() {
        let values = Float64Array::from((0..5).map(|i| i as f64).collect::<Vec<f64>>()); // per lon
        let flat =
            build_broadcast_array(&values, &dims(&["lon"]), &dims(&T), &SHAPE, ColumnEncoding::Flat)
                .unwrap();
        let dict = build_broadcast_array(
            &values,
            &dims(&["lon"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Dictionary,
        )
        .unwrap();
        assert!(matches!(dict.data_type(), DataType::Dictionary(_, _)));
        assert_same(decode(dict.as_ref()).as_ref(), flat.as_ref());
    }

    #[test]
    fn string_dictionary_decodes_to_flat() {
        let values = StringArray::from(vec!["a", "b", "c", "d", "e"]); // per lon
        let flat =
            build_broadcast_array(&values, &dims(&["lon"]), &dims(&T), &SHAPE, ColumnEncoding::Flat)
                .unwrap();
        let dict = build_broadcast_array(
            &values,
            &dims(&["lon"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Dictionary,
        )
        .unwrap();
        assert_same(decode(dict.as_ref()).as_ref(), flat.as_ref());
    }

    #[test]
    fn scalar_attribute_is_one_run() {
        let values = Int32Array::from(vec![42]);
        let ree = build_broadcast_array(
            &values,
            &[],
            &dims(&T),
            &SHAPE,
            ColumnEncoding::RunEndEncoded,
        )
        .unwrap();
        let run = ree.as_any().downcast_ref::<RunArray<Int32Type>>().unwrap();
        assert_eq!(run.values().len(), 1); // a single physical run covers all rows
        assert_eq!(ree.len(), 60); // logical length

        let expected = Int32Array::from(vec![42; 60]);
        assert_same(decode(ree.as_ref()).as_ref(), &expected);
    }

    #[test]
    fn non_contiguous_source_decodes_to_flat() {
        // source spans time (outer) and lon (inner), broadcast across lat.
        let values = Int32Array::from((0..20).collect::<Vec<i32>>()); // time*lon = 4*5
        let flat = build_broadcast_array(
            &values,
            &dims(&["time", "lon"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Flat,
        )
        .unwrap();
        let dict = build_broadcast_array(
            &values,
            &dims(&["time", "lon"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Dictionary,
        )
        .unwrap();
        assert_same(decode(dict.as_ref()).as_ref(), flat.as_ref());
    }

    #[tokio::test]
    async fn flat_matches_real_broadcast_path() {
        // Anchor the gather logic to the existing NdArrowArray broadcast implementation.
        let data: Vec<i32> = (0..4).collect();
        let source = NdArrowArrayDispatch::<i32>::new_in_mem(
            data.clone(),
            vec![4],
            dims(&["time"]),
            None,
        )
        .unwrap();

        let reference = source
            .broadcast(&SHAPE, &dims(&T))
            .await
            .unwrap()
            .as_arrow_array_ref()
            .await
            .unwrap();

        let source_arrow = source.as_arrow_array_ref().await.unwrap();
        let flat = build_broadcast_array(
            source_arrow.as_ref(),
            &dims(&["time"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Flat,
        )
        .unwrap();

        assert_same(flat.as_ref(), reference.as_ref());
    }

    #[test]
    fn data_type_helper_matches_constructed_array() {
        let cases = [
            (dims(&["time"]), ColumnEncoding::RunEndEncoded, 4usize),
            (dims(&["lon"]), ColumnEncoding::Dictionary, 5usize),
            (dims(&["time", "lat", "lon"]), ColumnEncoding::Flat, 60usize),
        ];
        for (source_dims, encoding, card) in cases {
            let values = Int32Array::from((0..card as i32).collect::<Vec<i32>>());
            let built =
                build_broadcast_array(&values, &source_dims, &dims(&T), &SHAPE, encoding).unwrap();
            let declared = broadcast_encoded_data_type(
                &DataType::Int32,
                &source_dims,
                &dims(&T),
                &SHAPE,
                encoding,
            )
            .unwrap();
            assert_eq!(built.data_type(), &declared, "mismatch for {encoding:?}");
        }
    }

    #[test]
    fn wrong_source_length_errors() {
        let values = Int32Array::from(vec![1, 2, 3]); // should be 4 for "time"
        let err = build_broadcast_array(
            &values,
            &dims(&["time"]),
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Flat,
        );
        assert!(err.is_err());
    }

    #[test]
    fn invalid_geometry_errors() {
        let values = Int32Array::from(vec![1, 2, 3, 4]);
        let err = build_broadcast_array(
            &values,
            &dims(&["depth"]), // not in target
            &dims(&T),
            &SHAPE,
            ColumnEncoding::Flat,
        );
        assert!(err.is_err());
    }
}
