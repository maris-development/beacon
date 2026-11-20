//! The schema of a statistics table, from the schema of its external table.
//!
//! A statistics table has one row for each file. Six base columns describe the
//! file version. Then each statistics column `X` of the table has one struct
//! column `X: Struct<min, max, null_count>`, with `min` and `max` in the type
//! of `X`.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit};

/// The most statistics columns a table can have without the option `statistics_columns`.
pub const MAX_STATISTICS_COLUMNS: usize = 1000;

/// The hash of the file version (see `stamp`).
pub(crate) const STAMP: &str = "_stamp";
/// The object path of the file.
pub(crate) const PATH: &str = "_path";
/// The size of the file in bytes.
pub(crate) const SIZE: &str = "_size";
/// The modification time of the file.
pub(crate) const LAST_MODIFIED: &str = "_last_modified";
/// Whether the analysis of the file failed.
pub(crate) const FAILED: &str = "_failed";
/// The number of rows of the file, if the format reports it.
pub(crate) const ROW_COUNT: &str = "_row_count";

/// The field metadata key on a statistics column: the name of its table column.
const COLUMN_KEY: &str = "beacon.statistics.column";
/// The field metadata key on [`STAMP`]: the fingerprint of the layout.
///
/// It is field metadata, because a Lance table keeps field metadata but not schema metadata.
const LAYOUT_KEY: &str = "beacon.statistics.layout";

/// Why a table schema gives no statistics layout.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum LayoutError {
    /// The table has more range columns than [`MAX_STATISTICS_COLUMNS`], and names none.
    #[error(
        "the table has {count} columns that can hold a range, more than the {max} that \
         statistics cover; name the statistics columns with the option 'statistics_columns'"
    )]
    TooManyColumns { count: usize, max: usize },
    /// The option names a column that the table does not have.
    #[error("the statistics column '{0}' is not a column of the table")]
    UnknownColumn(String),
    /// The option names a column whose type has no range, such as a list.
    #[error("the statistics column '{name}' has type {data_type}, which holds no range")]
    NoRange { name: String, data_type: DataType },
}

/// One statistics column: a table column and its struct column in the statistics table.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct StatisticsColumn {
    /// The name of the column in the external table.
    pub name: String,
    /// The name of the struct column in the statistics table.
    pub stored: String,
    /// The type of the column in the external table.
    pub table_type: DataType,
    /// The type of `min` and `max` in the statistics table.
    pub data_type: DataType,
}

/// The statistics columns of a table, and the schema of its statistics table.
#[derive(Debug, Clone)]
pub(crate) struct Layout {
    pub columns: Vec<StatisticsColumn>,
    pub schema: SchemaRef,
    by_name: HashMap<String, usize>,
}

impl Layout {
    /// The layout for `table_schema`.
    ///
    /// # Arguments
    ///
    /// * `table_schema` - The columns of the files of the table.
    /// * `columns` - The statistics columns the table names, or `None` for each range column.
    ///
    /// # Errors
    ///
    /// Returns an error when a named column is unknown or holds no range, or when
    /// the table names no columns and has more than [`MAX_STATISTICS_COLUMNS`] range columns.
    pub(crate) fn new(
        table_schema: &Schema,
        columns: Option<&[String]>,
    ) -> Result<Self, LayoutError> {
        let chosen: Vec<(&Field, DataType)> = match columns {
            Some(names) => {
                let mut seen = HashSet::new();
                let mut chosen = Vec::with_capacity(names.len());
                for name in names.iter().filter(|name| seen.insert(name.as_str())) {
                    let field = table_schema
                        .field_with_name(name)
                        .map_err(|_| LayoutError::UnknownColumn(name.clone()))?;
                    let data_type =
                        range_type(field.data_type()).ok_or_else(|| LayoutError::NoRange {
                            name: name.clone(),
                            data_type: field.data_type().clone(),
                        })?;
                    chosen.push((field, data_type));
                }
                chosen
            }
            None => {
                let chosen: Vec<(&Field, DataType)> = table_schema
                    .fields()
                    .iter()
                    .filter_map(|field| Some((field.as_ref(), range_type(field.data_type())?)))
                    .collect();
                if chosen.len() > MAX_STATISTICS_COLUMNS {
                    return Err(LayoutError::TooManyColumns {
                        count: chosen.len(),
                        max: MAX_STATISTICS_COLUMNS,
                    });
                }
                chosen
            }
        };

        let columns: Vec<StatisticsColumn> = chosen
            .into_iter()
            .enumerate()
            .map(|(index, (field, data_type))| StatisticsColumn {
                name: field.name().clone(),
                stored: stored_name(index, field.name()),
                table_type: field.data_type().clone(),
                data_type,
            })
            .collect();
        let schema = statistics_schema(&columns);
        let by_name = columns
            .iter()
            .enumerate()
            .map(|(index, column)| (column.name.clone(), index))
            .collect();
        Ok(Self {
            columns,
            schema,
            by_name,
        })
    }

    /// The statistics column of the table column `name`.
    pub(crate) fn column(&self, name: &str) -> Option<&StatisticsColumn> {
        self.by_name.get(name).map(|index| &self.columns[*index])
    }

    /// Whether a stored statistics table has this layout.
    pub(crate) fn matches(&self, stored: &Schema) -> bool {
        let fingerprint = stored
            .field_with_name(STAMP)
            .ok()
            .and_then(|field| field.metadata().get(LAYOUT_KEY));
        fingerprint == Some(&fingerprint_of(&self.columns))
    }
}

/// The type a statistics table keeps the range of `data_type` in, or `None` when it has no range.
///
/// The view types keep their plain form; nested types, intervals and null hold no range.
fn range_type(data_type: &DataType) -> Option<DataType> {
    use DataType::*;
    match data_type {
        Boolean
        | Int8
        | Int16
        | Int32
        | Int64
        | UInt8
        | UInt16
        | UInt32
        | UInt64
        | Float16
        | Float32
        | Float64
        | Date32
        | Date64
        | Time32(_)
        | Time64(_)
        | Timestamp(_, _)
        | Duration(_)
        | Decimal128(_, _)
        | Utf8
        | LargeUtf8
        | Binary
        | LargeBinary => Some(data_type.clone()),
        Utf8View => Some(Utf8),
        BinaryView => Some(Binary),
        _ => None,
    }
}

/// The struct column name of table column `name` at position `index`.
///
/// A name that is empty, starts with `_` or holds a `.` gets `_c<index>`: the
/// base columns start with `_`, and Lance reads a `.` as a nested field path.
/// Each other name starts with another character, so no two names collide.
fn stored_name(index: usize, name: &str) -> String {
    if name.is_empty() || name.starts_with('_') || name.contains('.') {
        format!("_c{index}")
    } else {
        name.to_string()
    }
}

/// The schema of a statistics table with `columns`.
fn statistics_schema(columns: &[StatisticsColumn]) -> SchemaRef {
    let layout = HashMap::from([(LAYOUT_KEY.to_string(), fingerprint_of(columns))]);
    let mut fields = vec![
        Field::new(STAMP, DataType::UInt64, false).with_metadata(layout),
        Field::new(PATH, DataType::Utf8, false),
        Field::new(SIZE, DataType::UInt64, false),
        Field::new(
            LAST_MODIFIED,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(FAILED, DataType::Boolean, false),
        Field::new(ROW_COUNT, DataType::UInt64, true),
    ];
    fields.extend(columns.iter().map(|column| {
        Field::new(&column.stored, range_struct(&column.data_type), true).with_metadata(
            HashMap::from([(COLUMN_KEY.to_string(), column.name.clone())]),
        )
    }));
    Arc::new(Schema::new(fields))
}

/// The struct type of one statistics column: `min` and `max` of `data_type`, and the null count.
pub(crate) fn range_struct(data_type: &DataType) -> DataType {
    DataType::Struct(range_fields(data_type))
}

/// The fields of [`range_struct`].
pub(crate) fn range_fields(data_type: &DataType) -> Fields {
    Fields::from(vec![
        Field::new("min", data_type.clone(), true),
        Field::new("max", data_type.clone(), true),
        Field::new("null_count", DataType::UInt64, true),
    ])
}

/// A text that changes when a column, its name or its type changes.
fn fingerprint_of(columns: &[StatisticsColumn]) -> String {
    let columns: Vec<String> = columns
        .iter()
        .map(|column| format!("{}\u{1f}{}", column.name, column.data_type))
        .collect();
    format!("v1\u{1d}{}", columns.join("\u{1e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn schema(fields: Vec<Field>) -> Schema {
        Schema::new(fields)
    }

    fn float(name: &str) -> Field {
        Field::new(name, DataType::Float64, true)
    }

    fn names(layout: &Layout) -> Vec<(&str, &str)> {
        layout
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.stored.as_str()))
            .collect()
    }

    #[test]
    fn each_range_column_gets_a_struct_column() {
        let table = schema(vec![
            float("TEMP"),
            Field::new("TIME", DataType::Utf8View, true),
            Field::new(
                "TAGS",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);

        let layout = Layout::new(&table, None).expect("a layout");

        assert_eq!(names(&layout), [("TEMP", "TEMP"), ("TIME", "TIME")]);
        assert_eq!(
            layout.columns[1].data_type,
            DataType::Utf8,
            "a view keeps its plain form"
        );
        let time = layout
            .schema
            .field_with_name("TIME")
            .expect("a struct column");
        assert_eq!(time.data_type(), &range_struct(&DataType::Utf8));
        assert_eq!(layout.schema.fields().len(), 6 + 2);
    }

    #[test]
    fn more_than_the_limit_needs_the_option() {
        let fields: Vec<Field> = (0..=MAX_STATISTICS_COLUMNS)
            .map(|index| float(&format!("c{index}")))
            .collect();
        let table = schema(fields);

        let error = Layout::new(&table, None).expect_err("too many columns");
        let named = Layout::new(&table, Some(&["c7".to_string(), "c3".to_string()]));

        assert_eq!(
            error,
            LayoutError::TooManyColumns {
                count: MAX_STATISTICS_COLUMNS + 1,
                max: MAX_STATISTICS_COLUMNS
            }
        );
        assert!(error.to_string().contains("statistics_columns"));
        assert_eq!(
            names(&named.expect("a layout")),
            [("c7", "c7"), ("c3", "c3")]
        );
    }

    #[test]
    fn the_option_names_known_range_columns_only() {
        let table = schema(vec![
            float("TEMP"),
            Field::new("RAW", DataType::Struct(Fields::empty()), true),
        ]);

        assert_eq!(
            Layout::new(&table, Some(&["PSAL".to_string()])).expect_err("unknown"),
            LayoutError::UnknownColumn("PSAL".to_string())
        );
        assert!(matches!(
            Layout::new(&table, Some(&["RAW".to_string()])),
            Err(LayoutError::NoRange { .. })
        ));
        let repeated = Layout::new(&table, Some(&["TEMP".to_string(), "TEMP".to_string()]));
        assert_eq!(repeated.expect("a layout").columns.len(), 1);
    }

    #[test]
    fn unsafe_names_get_a_position_name() {
        let table = schema(vec![
            float("sea water.temp"),
            float("_path"),
            float("depth below"),
            float("_c0"),
        ]);

        let layout = Layout::new(&table, None).expect("a layout");

        assert_eq!(
            names(&layout),
            [
                ("sea water.temp", "_c0"),
                ("_path", "_c1"),
                ("depth below", "depth below"),
                ("_c0", "_c3"),
            ]
        );
        assert_eq!(layout.column("_path").expect("a column").stored, "_c1");
    }

    #[test]
    fn a_changed_column_type_changes_the_fingerprint() {
        let before = Layout::new(&schema(vec![float("TEMP")]), None).expect("a layout");
        let after = Layout::new(
            &schema(vec![Field::new("TEMP", DataType::Float32, true)]),
            None,
        )
        .expect("a layout");

        assert!(before.matches(&before.schema));
        assert!(!after.matches(&before.schema));
        assert!(!before.matches(&Schema::empty()));
    }
}
