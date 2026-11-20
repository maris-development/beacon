//! The analysis of one file: its statistics, cast to the column types of a layout.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Context;
use arrow::datatypes::DataType;
use beacon_datafusion_ext::format_ext::try_file_format_factory_ext;
use beacon_datafusion_ext::listing_factory::try_listing_factory_from_session;
use beacon_datafusion_ext::type_widening::ArrowTypeWideningStrategy;
use beacon_external_table::statistics_cache::StatisticsRegistration;
use datafusion::common::ColumnStatistics;
use datafusion::datasource::file_format::FileFormat;
use datafusion::execution::SessionState;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use object_store::{ObjectMeta, ObjectStore};

use crate::format::{FileFormatStatistics, FileStatisticsOutput};
use crate::layout::Layout;

/// The range of one statistics column in one file.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ColumnValues {
    /// The minimum, in the statistics type of the column.
    pub min: Option<ScalarValue>,
    /// The maximum, in the statistics type of the column.
    pub max: Option<ScalarValue>,
    /// The number of nulls, if the format reports it.
    pub null_count: Option<u64>,
}

/// The analysis of one file version.
#[derive(Debug, Clone)]
pub(crate) struct AnalyzedFile {
    pub object: ObjectMeta,
    pub stamp: u64,
    /// Whether the format could not read the statistics of the file.
    pub failed: bool,
    pub row_count: Option<u64>,
    /// One entry for each column of the layout, in layout order.
    pub columns: Vec<ColumnValues>,
}

/// The format a refresh analyzes the files of a table with.
///
/// Only `create_for_analysis` gives real statistics. Some formats built in
/// another way give unknown statistics, so that a query never pays for them.
///
/// # Errors
///
/// Returns an error when the table has no location or the format is unknown.
pub(crate) fn analysis_format(
    state: &SessionState,
    registration: &StatisticsRegistration,
) -> anyhow::Result<Arc<dyn FileFormat>> {
    let url = registration
        .urls
        .first()
        .context("the table has no location")?;
    let file_type = registration.file_type.as_str();
    if let Some(factory) = try_file_format_factory_ext(state, file_type) {
        let listing = try_listing_factory_from_session(state)
            .context("the session has no listing factory")?;
        return Ok(factory.create_for_analysis(
            state,
            &registration.format_options,
            url,
            &listing,
        )?);
    }
    let factory = state
        .get_file_format_factory(file_type)
        .with_context(|| format!("no format is registered for '{file_type}'"))?;
    Ok(factory.create(state, &registration.format_options)?)
}

/// Reads the statistics of `object` and casts them to the types of `layout`.
///
/// A file the format cannot read gives a failed analysis with no ranges, so it
/// is not analyzed again until it changes.
pub(crate) async fn analyze(
    format: &Arc<dyn FileFormat>,
    session: Arc<SessionContext>,
    store: Arc<dyn ObjectStore>,
    object: ObjectMeta,
    stamp: u64,
    layout: &Layout,
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> AnalyzedFile {
    let started = Instant::now();
    let output = format
        .fetch_statistics(session, store, object.clone())
        .await;
    tracing::debug!(
        path = %object.location,
        ok = output.is_ok(),
        elapsed_ms = started.elapsed().as_millis() as u64,
        "analyzed a file"
    );
    match output {
        Ok(output) => {
            let (row_count, columns) = column_values(&output, layout, type_widening);
            AnalyzedFile {
                object,
                stamp,
                failed: false,
                row_count,
                columns,
            }
        }
        Err(error) => {
            tracing::debug!(
                path = %object.location,
                error = format!("{error:#}"),
                "the analysis of a file failed; it is stored with no statistics"
            );
            AnalyzedFile {
                object,
                stamp,
                failed: true,
                row_count: None,
                columns: vec![ColumnValues::default(); layout.columns.len()],
            }
        }
    }
}

/// The row count and the range of each layout column in `output`.
///
/// The statistics match the fields of the file schema by position, and the
/// layout columns by name. A column the file does not have gives no range.
pub(crate) fn column_values(
    output: &FileStatisticsOutput,
    layout: &Layout,
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> (Option<u64>, Vec<ColumnValues>) {
    let statistics = &output.statistics;
    let row_count = statistics.num_rows.get_value().map(|rows| *rows as u64);
    let by_name: HashMap<&str, usize> = output
        .schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| (field.name().as_str(), index))
        .collect();
    let columns = layout
        .columns
        .iter()
        .map(|column| {
            let Some(index) = by_name.get(column.name.as_str()) else {
                return ColumnValues::default();
            };
            let (Some(field), Some(file_column)) = (
                output.schema.fields().get(*index),
                statistics.column_statistics.get(*index),
            ) else {
                return ColumnValues::default();
            };
            let cast = |value: Option<&ScalarValue>| {
                cast_value(
                    value?,
                    field.data_type(),
                    &column.table_type,
                    &column.data_type,
                    type_widening,
                )
            };
            ColumnValues {
                min: cast(file_column.min_value.get_value()),
                max: cast(file_column.max_value.get_value()),
                null_count: null_count(file_column),
            }
        })
        .collect();
    (row_count, columns)
}

/// `value` of a file column of type `source`, in the statistics type `stored`.
///
/// `None` when the scan reads the values of this file as null: a lenient cast
/// of the table strategy, or a failed cast. A null value is no range either.
///
/// `get_value` gives `Exact` and `Inexact` bounds, as DataFusion's own pruning
/// takes them. That is correct while an inexact bound is wider than the true bound.
fn cast_value(
    value: &ScalarValue,
    source: &DataType,
    table_type: &DataType,
    stored: &DataType,
    type_widening: &dyn ArrowTypeWideningStrategy,
) -> Option<ScalarValue> {
    if type_widening.casts_leniently(source, table_type) {
        return None;
    }
    value.cast_to(stored).ok().filter(|value| !value.is_null())
}

/// The null count of a column. An absent count stays absent.
///
/// DataFusion prunes `IS NOT NULL` on `null_count != row_count`, so two unknown
/// counts stored as zero read as "each value is null" and drop a file full of values.
fn null_count(column: &ColumnStatistics) -> Option<u64> {
    column.null_count.get_value().map(|count| *count as u64)
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema};
    use beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening;
    use datafusion::common::Statistics;
    use datafusion::common::stats::Precision;

    use super::*;

    fn layout() -> Layout {
        let table = Schema::new(vec![
            Field::new("TEMP", DataType::Float64, true),
            Field::new("DEPTH", DataType::Int64, true),
        ]);
        Layout::new(&table, None).expect("a layout")
    }

    fn known(min: ScalarValue, max: ScalarValue, nulls: usize) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(nulls),
            min_value: Precision::Exact(min),
            max_value: Precision::Exact(max),
            ..Default::default()
        }
    }

    fn output(
        fields: Vec<Field>,
        columns: Vec<ColumnStatistics>,
        rows: Precision<usize>,
    ) -> FileStatisticsOutput {
        FileStatisticsOutput {
            schema: Arc::new(Schema::new(fields)),
            statistics: Statistics {
                num_rows: rows,
                total_byte_size: Precision::Absent,
                column_statistics: columns,
            },
        }
    }

    #[test]
    fn columns_match_by_name_and_cast_to_the_table_type() {
        // The file has DEPTH first, as Int32, and no TEMP.
        let file = output(
            vec![Field::new("DEPTH", DataType::Int32, true)],
            vec![known(
                ScalarValue::Int32(Some(3)),
                ScalarValue::Int32(Some(9)),
                1,
            )],
            Precision::Exact(10),
        );

        let (rows, columns) = column_values(&file, &layout(), &DefaultArrowTypeWidening::new());

        assert_eq!(rows, Some(10));
        assert_eq!(columns[0], ColumnValues::default(), "the file has no TEMP");
        assert_eq!(columns[1].min, Some(ScalarValue::Int64(Some(3))));
        assert_eq!(columns[1].max, Some(ScalarValue::Int64(Some(9))));
        assert_eq!(columns[1].null_count, Some(1));
    }

    #[test]
    fn a_lenient_cast_or_a_failed_cast_gives_no_range() {
        let file = output(
            vec![
                Field::new("TEMP", DataType::Utf8, true),
                Field::new("DEPTH", DataType::Utf8, true),
            ],
            vec![
                known(
                    ScalarValue::Utf8(Some("a".into())),
                    ScalarValue::Utf8(Some("b".into())),
                    0,
                ),
                known(
                    ScalarValue::Utf8(Some("7".into())),
                    ScalarValue::Utf8(Some("8".into())),
                    0,
                ),
            ],
            Precision::Absent,
        );

        let strict = column_values(&file, &layout(), &DefaultArrowTypeWidening::new()).1;
        let lenient = column_values(
            &file,
            &layout(),
            &DefaultArrowTypeWidening::keeping_first_type(),
        )
        .1;

        assert_eq!(strict[0].min, None, "'a' does not cast to a float");
        assert_eq!(strict[1].min, Some(ScalarValue::Int64(Some(7))));
        assert_eq!(
            lenient[1].min, None,
            "the scan reads a lenient cast as null"
        );
        assert_eq!(lenient[1].null_count, Some(0), "the null count stays");
    }

    #[test]
    fn absent_values_and_counts_stay_absent() {
        let file = output(
            vec![Field::new("TEMP", DataType::Float64, true)],
            vec![ColumnStatistics {
                max_value: Precision::Inexact(ScalarValue::Float64(Some(9.0))),
                ..Default::default()
            }],
            Precision::Absent,
        );

        let (rows, columns) = column_values(&file, &layout(), &DefaultArrowTypeWidening::new());

        assert_eq!(rows, None);
        assert_eq!(columns[0].min, None);
        assert_eq!(columns[0].max, Some(ScalarValue::Float64(Some(9.0))));
        assert_eq!(columns[0].null_count, None);
    }
}
