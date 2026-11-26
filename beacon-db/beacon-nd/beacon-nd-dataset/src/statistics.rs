//! Statistics of one variable.

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, PrimitiveArray, downcast_primitive_array,
};
use arrow::compute::{max, max_boolean, min, min_boolean};
use arrow::datatypes::{ArrowPrimitiveType, DataType};

use crate::{NdVariable, Region, Result};

/// The minimum and maximum of a rank-0 or rank-1 variable.
///
/// `None` for a variable of rank 2 or more, a variable with no element, or a
/// type without a range: these cases do not read. Nulls are not in the range;
/// with no value left, also `None`. Each result array has length 1 and the
/// type of the variable. A float NaN is the largest value.
pub async fn value_range(variable: &dyn NdVariable) -> Result<Option<(ArrayRef, ArrayRef)>> {
    let dimensions = variable.dimensions();
    if dimensions.rank() > 1 || dimensions.num_elements() == 0 || !has_range(variable.data_type()) {
        return Ok(None);
    }
    let array = variable.read(&Region::full(dimensions)).await?;
    Ok(range_of(array.values().as_ref()))
}

/// True for Boolean, the integer types, Float32, Float64 and each Timestamp unit.
fn has_range(data_type: &DataType) -> bool {
    data_type.is_integer()
        || matches!(
            data_type,
            DataType::Boolean | DataType::Float32 | DataType::Float64 | DataType::Timestamp(_, _)
        )
}

/// The minimum and maximum of the values that are not null.
fn range_of(values: &dyn Array) -> Option<(ArrayRef, ArrayRef)> {
    if let Some(values) = values.as_boolean_opt() {
        let min: ArrayRef = Arc::new(BooleanArray::from(vec![min_boolean(values)?]));
        let max: ArrayRef = Arc::new(BooleanArray::from(vec![max_boolean(values)?]));
        return Some((min, max));
    }
    downcast_primitive_array!(
        values => Some((single(values, min(values)?), single(values, max(values)?))),
        _ => None,
    )
}

/// One value in an array of the type of `values`.
fn single<T: ArrowPrimitiveType>(values: &PrimitiveArray<T>, value: T::Native) -> ArrayRef {
    // The source type keeps the unit and the time zone of a timestamp.
    let array =
        PrimitiveArray::<T>::from_value(value, 1).with_data_type(values.data_type().clone());
    Arc::new(array)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, StringArray,
        TimestampNanosecondArray,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use nd_arrow_array::{Dimensions, NdArrowArray};

    use super::*;
    use crate::MemoryVariable;
    use crate::memory::testing::{dims, ramp};

    /// A variable with these values on the dimension `n`.
    fn variable(values: ArrayRef) -> MemoryVariable {
        let dimensions = dims(&[("n", values.len())]);
        MemoryVariable::new("v", NdArrowArray::try_new(values, dimensions).unwrap())
    }

    #[tokio::test]
    async fn a_rank_1_variable_gives_its_range() {
        let v = variable(Arc::new(Float64Array::from(vec![3.0, 1.0, 2.0])));
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        assert_eq!(min.data_type(), &DataType::Float64);
        assert_eq!(max.data_type(), &DataType::Float64);
        assert_eq!(min.as_ref(), &Float64Array::from(vec![1.0]));
        assert_eq!(max.as_ref(), &Float64Array::from(vec![3.0]));
        assert_eq!(v.reads(), 1);
    }

    #[tokio::test]
    async fn nulls_are_not_in_the_range() {
        let v = variable(Arc::new(Int32Array::from(vec![Some(5), None, Some(-2)])));
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        assert_eq!(min.as_ref(), &Int32Array::from(vec![-2]));
        assert_eq!(max.as_ref(), &Int32Array::from(vec![5]));
    }

    #[tokio::test]
    async fn a_rank_0_variable_gives_its_value_two_times() {
        let values: ArrayRef = Arc::new(Float64Array::from(vec![7.0]));
        let v = MemoryVariable::new(
            "v",
            NdArrowArray::try_new(values, Dimensions::scalar()).unwrap(),
        );
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        assert_eq!(min.as_ref(), &Float64Array::from(vec![7.0]));
        assert_eq!(max.as_ref(), &Float64Array::from(vec![7.0]));
    }

    #[tokio::test]
    async fn a_timestamp_keeps_its_type() {
        let v = variable(Arc::new(TimestampNanosecondArray::from(vec![20, 10])));
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        assert_eq!(min.data_type(), &data_type);
        assert_eq!(max.data_type(), &data_type);
        assert_eq!(min.as_ref(), &TimestampNanosecondArray::from(vec![10]));
        assert_eq!(max.as_ref(), &TimestampNanosecondArray::from(vec![20]));
    }

    #[tokio::test]
    async fn a_timestamp_keeps_its_time_zone() {
        let values = TimestampNanosecondArray::from(vec![20, 10]).with_timezone("+01:00");
        let v = variable(Arc::new(values));
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, Some("+01:00".into()));
        assert_eq!(min.data_type(), &data_type);
        assert_eq!(max.data_type(), &data_type);
    }

    #[tokio::test]
    async fn a_boolean_has_a_range() {
        let v = variable(Arc::new(BooleanArray::from(vec![true, false])));
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        assert_eq!(min.as_ref(), &BooleanArray::from(vec![false]));
        assert_eq!(max.as_ref(), &BooleanArray::from(vec![true]));
    }

    #[tokio::test]
    async fn a_nan_is_the_maximum_in_the_total_order() {
        let v = variable(Arc::new(Float64Array::from(vec![1.0, f64::NAN])));
        let (min, max) = value_range(&v).await.unwrap().unwrap();
        assert_eq!(min.as_ref(), &Float64Array::from(vec![1.0]));
        assert!(
            max.as_primitive::<arrow::datatypes::Float64Type>()
                .value(0)
                .is_nan()
        );
    }

    #[tokio::test]
    async fn a_rank_2_variable_has_no_range_and_no_read() {
        let v = ramp("v", &[("a", 2), ("b", 2)]);
        assert!(value_range(&v).await.unwrap().is_none());
        assert_eq!(v.reads(), 0);
    }

    #[tokio::test]
    async fn all_nulls_have_no_range() {
        let v = variable(Arc::new(Float64Array::from(vec![None, None])));
        assert!(value_range(&v).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn an_empty_variable_has_no_range() {
        let v = ramp("v", &[("n", 0)]);
        assert!(value_range(&v).await.unwrap().is_none());
        assert_eq!(v.reads(), 0);
    }

    #[tokio::test]
    async fn a_string_has_no_range_and_no_read() {
        let v = variable(Arc::new(StringArray::from(vec!["b", "a"])));
        assert!(value_range(&v).await.unwrap().is_none());
        assert_eq!(v.reads(), 0);
    }
}
