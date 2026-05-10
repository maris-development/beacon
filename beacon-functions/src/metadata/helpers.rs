use arrow::datatypes::FieldRef;
use datafusion::common::{ColumnStatistics, stats::Precision};
use datafusion::scalar::ScalarValue;

/// One column's worth of statistics as plain strings, ready to push into Arrow arrays.
pub struct ColumnStatRow {
    pub column_name: Option<String>,
    pub data_type: Option<String>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub is_exact: Option<bool>,
}

/// Build one [`ColumnStatRow`] per column statistic.
///
/// When `fields` is non-empty it is zipped with `col_stats` to supply field
/// names and data-type strings; pass `&[]` when the schema is unavailable
/// (e.g. the global statistics cache view, which is schema-agnostic).
pub fn column_stat_rows(fields: &[FieldRef], col_stats: &[ColumnStatistics]) -> Vec<ColumnStatRow> {
    col_stats
        .iter()
        .enumerate()
        .map(|(i, col_stat)| {
            let field = fields.get(i);
            let (min_value, is_exact) = precision_to_str_and_exact(&col_stat.min_value);
            let (max_value, _) = precision_to_str_and_exact(&col_stat.max_value);
            ColumnStatRow {
                column_name: field.map(|f| f.name().clone()),
                data_type: field.map(|f| f.data_type().to_string()),
                min_value,
                max_value,
                is_exact,
            }
        })
        .collect()
}

pub fn precision_to_str_and_exact(p: &Precision<ScalarValue>) -> (Option<String>, Option<bool>) {
    match p {
        Precision::Exact(v) => (Some(v.to_string()), Some(true)),
        Precision::Inexact(v) => (Some(v.to_string()), Some(false)),
        Precision::Absent => (None, None),
    }
}
