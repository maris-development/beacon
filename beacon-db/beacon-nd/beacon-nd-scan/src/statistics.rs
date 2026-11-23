//! The DataFusion statistics of one file.

use arrow::datatypes::Schema;
use beacon_nd_dataset::{NdDataset, NdVariable, value_range};
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::error::Result;
use datafusion::scalar::ScalarValue;

use crate::error::to_datafusion;

/// The statistics of one file for the columns of `schema`.
///
/// A variable of rank 0 or 1 gets its exact minimum and maximum. Each other
/// column is unknown, also a column that the file does not have. The row
/// count is not set. A larger variable is not read, because a full read costs too much.
pub async fn file_statistics(dataset: &dyn NdDataset, schema: &Schema) -> Result<Statistics> {
    let mut statistics = Statistics::default();
    for field in schema.fields() {
        let column = match dataset.variable(field.name()) {
            Some(variable) if variable.dimensions().rank() <= 1 => {
                column_statistics(variable.as_ref()).await?
            }
            _ => ColumnStatistics::new_unknown(),
        };
        statistics = statistics.add_column_statistics(column);
    }
    Ok(statistics)
}

/// The exact minimum and maximum of `variable`, or unknown when it has no range.
async fn column_statistics(variable: &dyn NdVariable) -> Result<ColumnStatistics> {
    let Some((min, max)) = value_range(variable).await.map_err(to_datafusion)? else {
        return Ok(ColumnStatistics::new_unknown());
    };
    let min = ScalarValue::try_from_array(&min, 0)?;
    let max = ScalarValue::try_from_array(&max, 0)?;
    Ok(ColumnStatistics::new_unknown()
        .with_min_value(Precision::Exact(min))
        .with_max_value(Precision::Exact(max)))
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
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
}
