//! The DataFusion statistics of one file.

use arrow::datatypes::{DataType, Schema};
use beacon_nd_dataset::{NdDataset, NdVariable, value_range};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

use crate::cast_rule::keeps_values;
use crate::error::to_datafusion;

/// The statistics of one file for the columns of `schema`.
///
/// A variable of rank 0 or 1 gets its exact minimum and maximum in the type of its field.
/// Each other column is unknown, also a column that the file does not have.
/// A column is also unknown when the cast to the field type can change a value.
/// The row count is not set. A larger variable is not read, because a full read costs too much.
pub async fn file_statistics(dataset: &dyn NdDataset, schema: &Schema) -> Result<Statistics> {
    let mut statistics = Statistics::default();
    for field in schema.fields() {
        let column = match dataset.variable(field.name()) {
            Some(variable)
                if variable.dimensions().rank() <= 1
                    && keeps_values(variable.data_type(), field.data_type()) =>
            {
                column_statistics(variable.as_ref(), field.data_type()).await?
            }
            _ => ColumnStatistics::new_unknown(),
        };
        statistics = statistics.add_column_statistics(column);
    }
    Ok(statistics)
}

/// The exact minimum and maximum of `variable` in `field_type`, or unknown when it has no range.
async fn column_statistics(
    variable: &dyn NdVariable,
    field_type: &DataType,
) -> Result<ColumnStatistics> {
    let Some((min, max)) = value_range(variable).await.map_err(to_datafusion)? else {
        return Ok(ColumnStatistics::new_unknown());
    };
    // DataFusion merges the statistics of the files in the type of the table.
    let min = ScalarValue::try_from_array(&min, 0)?.cast_to(field_type)?;
    let max = ScalarValue::try_from_array(&max, 0)?.cast_to(field_type)?;
    Ok(ColumnStatistics::new_unknown()
        .with_min_value(Precision::Exact(min))
        .with_max_value(Precision::Exact(max)))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use beacon_nd_dataset::{Dimension, Dimensions, MemoryDataset, MemoryVariable, NdArrowArray};
    use datafusion::common::ColumnStatistics;
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;

    use super::file_statistics;
    use crate::testing::grid_dataset;

    #[tokio::test]
    async fn statistics_cover_the_rank_0_and_rank_1_variables() {
        let dataset = grid_dataset("a.nd");
        let schema = Schema::new(
            ["lat", "temp", "salt"]
                .map(|name| Field::new(name, DataType::Float64, true))
                .to_vec(),
        );

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        assert_eq!(statistics.num_rows, Precision::Absent);
        assert_eq!(statistics.column_statistics.len(), 3);
        let lat = &statistics.column_statistics[0];
        assert_eq!(
            lat.min_value,
            Precision::Exact(ScalarValue::Float64(Some(0.0)))
        );
        assert_eq!(
            lat.max_value,
            Precision::Exact(ScalarValue::Float64(Some(7.0)))
        );
        assert_eq!(
            statistics.column_statistics[1],
            ColumnStatistics::new_unknown()
        );
        assert_eq!(
            statistics.column_statistics[2],
            ColumnStatistics::new_unknown()
        );
        assert_eq!(dataset.reads("temp"), 0);
    }

    /// A dataset with the 1-D variable `n` of `values`, and a table schema with `n` in `field_type`.
    fn one_variable(values: ArrayRef, field_type: DataType) -> (MemoryDataset, Schema) {
        let x = Dimensions::try_new([Dimension::new("x", values.len())]).unwrap();
        let n = MemoryVariable::new("n", NdArrowArray::try_new(values, x).unwrap());
        let dataset = MemoryDataset::try_new("a.nd", vec![n]).unwrap();
        (
            dataset,
            Schema::new(vec![Field::new("n", field_type, true)]),
        )
    }

    #[tokio::test]
    async fn an_int32_variable_under_a_float64_field_gives_float64_statistics() {
        let values = Arc::new(Int32Array::from(vec![1, 5, 3]));
        let (dataset, schema) = one_variable(values, DataType::Float64);

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        let n = &statistics.column_statistics[0];
        assert_eq!(
            n.min_value,
            Precision::Exact(ScalarValue::Float64(Some(1.0)))
        );
        assert_eq!(
            n.max_value,
            Precision::Exact(ScalarValue::Float64(Some(5.0)))
        );
    }

    #[tokio::test]
    async fn a_float64_variable_under_a_float32_field_has_no_statistics() {
        let values = Arc::new(Float64Array::from(vec![1.0, 5.0, 3.0]));
        let (dataset, schema) = one_variable(values, DataType::Float32);

        let statistics = file_statistics(&dataset, &schema).await.unwrap();

        assert_eq!(
            statistics.column_statistics[0],
            ColumnStatistics::new_unknown()
        );
    }
}
