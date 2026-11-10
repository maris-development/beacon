//! The stored form of an external table.

use std::{collections::HashMap, sync::Arc, time::Duration};

use beacon_datafusion_ext::{
    format_options::{format_option, parse_bool_option},
    table_ext::TableDefinition,
    type_widening::{
        ArrowTypeWideningStrategy, DefaultArrowTypeWidening, TypeConflict, strategy_by_name,
    },
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    catalog::TableProvider,
    common::{Result, plan_err},
    prelude::SessionContext,
};

use crate::ExternalTableBuilder;

/// The stored configuration of an external table.
///
/// The field names and the typetag name `"listing_table"` are a stored format.
/// Older Beacon versions wrote the same form, so do not rename them.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ExternalTableDefinition {
    /// The table name.
    pub name: String,
    /// The data location, relative to the object store base URL. It can hold a glob.
    pub location: String,
    /// The file format identifier, for example `parquet`, `csv` or `netcdf`.
    pub file_type: String,
    /// The table file schema: declared, or the last inference. *An empty schema indicates no inference yet (backwards compat 2.0.x).*
    pub schema: SchemaRef,
    /// The type widening strategy used for this table.
    /// Definitions from older Beacon versions use the default strategy.
    #[serde(default = "default_type_widening")]
    pub type_widening: Arc<dyn ArrowTypeWideningStrategy>,
    /// Whether schema inference runs the files on parallel tasks. Without it, one task infers them concurrently.
    #[serde(default)]
    pub parallel_schema_inference: bool,
    /// Whether schema inference skips a file that it cannot read, and merges the other files.
    #[serde(default)]
    pub skip_invalid_files_on_inference: bool,
    /// Whether a scan skips a file that it cannot read, and continues with the next file.
    #[serde(default)]
    pub skip_invalid_files_on_scan: bool,
    /// Whether `schema` was declared or inferred. A refresh infers an inferred schema again.
    #[serde(default)]
    pub schema_source: SchemaSource,
    /// The SQL text that created the table, if known.
    pub definition: Option<String>,
    /// The partition column names in the folder paths.
    pub partition_cols: Vec<String>,
    /// The format options. A key that starts with `__` is a Beacon marker, not a format option.
    pub options: HashMap<String, String>,
    /// If true, a create does nothing when the table already exists.
    pub if_not_exists: bool,
    /// When Beacon lists the files again. Definitions from older Beacon versions use the default.
    #[serde(default)]
    pub refresh_policy: RefreshPolicy,
    /// Whether to use the listing cache when scanning the table (file listing cache).
    #[serde(default)]
    pub use_listing_cache: bool,
    /// Whether to use statistics pruning when scanning the table (pruning predicate pushdown).
    #[serde(default)]
    pub use_statistics_pruning: bool,
    /// Whether to use the statistics cache when scanning the table (pruning predicate pushdown).
    #[serde(default)]
    pub use_statistics_cache: bool,
}

/// The `OPTIONS` key that names the type widening strategy of the table: `default` or `numpy`.
pub const TYPE_WIDENING_OPTION: &str = "type_widening";
/// The `OPTIONS` key that names the type conflict rule of the table: `fail` or `keep_first`.
pub const TYPE_WIDENING_ON_CONFLICT_OPTION: &str = "type_widening_on_conflict";

/// The `OPTIONS` key that runs schema inference on parallel tasks: `true` or `false` (default).
pub const PARALLEL_SCHEMA_INFERENCE_OPTION: &str = "parallel_schema_inference";

/// The `OPTIONS` key that skips a file that schema inference cannot read: `true` or `false` (default).
pub const SKIP_INVALID_FILES_ON_INFERENCE_OPTION: &str = "skip_invalid_files_on_inference";
/// The `OPTIONS` key that skips a file that a scan cannot read: `true` or `false` (default).
pub const SKIP_INVALID_FILES_ON_SCAN_OPTION: &str = "skip_invalid_files_on_scan";

/// The `OPTIONS` key that prunes the files of a scan with the file statistics: `true` or `false` (default).
pub const USE_STATISTICS_PRUNING_OPTION: &str = "use_statistics_pruning";
/// The `OPTIONS` key that names the statistics columns, separated by commas.
/// Without it, the statistics cover each column that can hold a range.
pub const STATISTICS_COLUMNS_OPTION: &str = "statistics_columns";
/// The `OPTIONS` key that sets the time in seconds between two statistics refreshes.
pub const STATISTICS_REFRESH_INTERVAL_OPTION: &str = "statistics_refresh_interval";

/// The time between two statistics refreshes without the refresh option.
pub const DEFAULT_STATISTICS_REFRESH_INTERVAL: Duration = Duration::from_secs(900);

/// The table options that Beacon reads. They are not format options.
const TABLE_OPTIONS: [&str; 8] = [
    TYPE_WIDENING_OPTION,
    TYPE_WIDENING_ON_CONFLICT_OPTION,
    PARALLEL_SCHEMA_INFERENCE_OPTION,
    SKIP_INVALID_FILES_ON_INFERENCE_OPTION,
    SKIP_INVALID_FILES_ON_SCAN_OPTION,
    USE_STATISTICS_PRUNING_OPTION,
    STATISTICS_COLUMNS_OPTION,
    STATISTICS_REFRESH_INTERVAL_OPTION,
];

/// Where the schema of an external table comes from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaSource {
    /// The `CREATE EXTERNAL TABLE` statement declared the columns.
    #[default]
    Declared,
    /// Beacon inferred the columns from the files.
    Inferred,
}

/// When Beacon lists the files of an external table again and updates its schema.
///
/// Beacon stores the policy but does not apply it yet. Only `REFRESH` lists the files again.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum RefreshPolicy {
    /// Only on `REFRESH`.
    #[default]
    Manual,
    /// At a fixed interval.
    Interval { seconds: u64 },
    /// When the object store reports a change below the table location.
    Events,
}

impl ExternalTableDefinition {
    /// The format options without the Beacon markers.
    ///
    /// A marker such as the crawler owner tag (`__crawler__`) is not a DataFusion
    /// config key. The format factory fails on a key that it does not know.
    pub fn format_options(&self) -> HashMap<String, String> {
        self.options
            .iter()
            .filter(|(key, _)| !key.starts_with("__") && !is_table_option(key))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect()
    }

    /// Whether the schema comes from the files and not from the statement.
    ///
    /// An empty schema is always inferred: a statement cannot declare zero
    /// columns. Older Beacon versions store an inferred schema that way.
    pub fn schema_inferred(&self) -> bool {
        self.schema_source == SchemaSource::Inferred || self.schema.fields().is_empty()
    }
}

/// The type widening strategy that `options` name.
///
/// Without the strategy option, the strategy is `default`. Without the conflict option, the rule is `fail`.
///
/// # Errors
///
/// Returns an error when an option names no strategy or no conflict rule.
pub fn type_widening_from_options(
    options: &HashMap<String, String>,
) -> Result<Arc<dyn ArrowTypeWideningStrategy>> {
    let on_conflict = match format_option(options, TYPE_WIDENING_ON_CONFLICT_OPTION) {
        None => TypeConflict::default(),
        Some(value) => match TypeConflict::parse(value) {
            Ok(on_conflict) => on_conflict,
            Err(value) => {
                return plan_err!(
                    "the option '{TYPE_WIDENING_ON_CONFLICT_OPTION}' names no conflict rule: '{value}'. The rules are 'fail' and 'keep_first'"
                );
            }
        },
    };
    let name = format_option(options, TYPE_WIDENING_OPTION).map_or("default", String::as_str);
    match strategy_by_name(name, on_conflict) {
        Ok(strategy) => Ok(strategy),
        Err(value) => plan_err!(
            "the option '{TYPE_WIDENING_OPTION}' names no strategy: '{value}'. The strategies are 'default' and 'numpy'"
        ),
    }
}

/// Whether `options` turn on parallel schema inference. The default is `false`.
///
/// # Errors
///
/// Returns an error when the value is not a boolean.
pub fn parallel_schema_inference_from_options(options: &HashMap<String, String>) -> Result<bool> {
    bool_table_option(options, PARALLEL_SCHEMA_INFERENCE_OPTION)
}

/// The boolean table option `key` in `options`. The default is `false`.
///
/// # Errors
///
/// Returns an error when the value is not a boolean.
pub fn bool_table_option(options: &HashMap<String, String>, key: &str) -> Result<bool> {
    format_option(options, key).map_or(Ok(false), |value| parse_bool_option(key, value))
}

/// The statistics columns that `options` name, or `None` without the option.
///
/// # Errors
///
/// Returns an error when the option names no column.
pub fn statistics_columns_from_options(
    options: &HashMap<String, String>,
) -> Result<Option<Vec<String>>> {
    let Some(value) = format_option(options, STATISTICS_COLUMNS_OPTION) else {
        return Ok(None);
    };
    let columns: Vec<String> = value
        .split(',')
        .map(str::trim)
        .filter(|column| !column.is_empty())
        .map(str::to_string)
        .collect();
    if columns.is_empty() {
        return plan_err!("the option '{STATISTICS_COLUMNS_OPTION}' names no column");
    }
    Ok(Some(columns))
}

/// The time between two statistics refreshes that `options` set. The default is 900 seconds.
///
/// # Errors
///
/// Returns an error when the value is not a positive number of seconds.
pub fn statistics_refresh_interval_from_options(
    options: &HashMap<String, String>,
) -> Result<Duration> {
    let Some(value) = format_option(options, STATISTICS_REFRESH_INTERVAL_OPTION) else {
        return Ok(DEFAULT_STATISTICS_REFRESH_INTERVAL);
    };
    match value.trim().parse::<u64>() {
        Ok(seconds) if seconds > 0 => Ok(Duration::from_secs(seconds)),
        _ => plan_err!(
            "the option '{STATISTICS_REFRESH_INTERVAL_OPTION}' needs a positive number of seconds, not '{value}'"
        ),
    }
}

fn default_type_widening() -> Arc<dyn ArrowTypeWideningStrategy> {
    Arc::new(DefaultArrowTypeWidening::new())
}

/// Whether `key` is a Beacon table option, with or without the `format.` prefix.
fn is_table_option(key: &str) -> bool {
    let key = key.strip_prefix("format.").unwrap_or(key);
    TABLE_OPTIONS.contains(&key)
}

#[async_trait::async_trait]
#[typetag::serde(name = "listing_table")]
impl TableDefinition for ExternalTableDefinition {
    async fn build_provider(
        &self,
        context: Arc<SessionContext>,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let table = ExternalTableBuilder::new(self.clone())
            .build(&context.state())
            .await?;
        Ok(Arc::new(table))
    }

    fn table_name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        datasource::TableType,
    };

    use super::*;

    fn definition() -> ExternalTableDefinition {
        ExternalTableDefinition {
            name: "obs".to_string(),
            location: "obs/**/*.parquet".to_string(),
            file_type: "parquet".to_string(),
            schema: Arc::new(Schema::empty()),
            definition: None,
            partition_cols: vec!["year".to_string()],
            options: HashMap::from([
                ("__crawler__".to_string(), "c1".to_string()),
                ("format.has_header".to_string(), "true".to_string()),
            ]),
            if_not_exists: false,
            schema_source: SchemaSource::Inferred,
            refresh_policy: RefreshPolicy::default(),
            type_widening: default_type_widening(),
            parallel_schema_inference: false,
            skip_invalid_files_on_inference: false,
            skip_invalid_files_on_scan: false,
            use_listing_cache: false,
            use_statistics_pruning: false,
            use_statistics_cache: false,
        }
    }

    #[test]
    fn round_trips_through_typetag() {
        let definition: Arc<dyn TableDefinition> = Arc::new(definition());

        let json = serde_json::to_value(&definition).unwrap();
        assert_eq!(json["definition_type"], "listing_table");

        let restored: Arc<dyn TableDefinition> = serde_json::from_value(json).unwrap();
        assert_eq!(restored.table_name(), "obs");
        assert_eq!(restored.table_type(), TableType::Base);
    }

    #[test]
    fn loads_the_form_an_older_beacon_stored() {
        // The JSON shape that Beacon 2.1 and older write for an external table.
        let stored = serde_json::json!({
            "definition_type": "listing_table",
            "name": "argo",
            "location": "argo/*.nc",
            "file_type": "netcdf",
            "schema": serde_json::to_value(Schema::new(vec![
                Field::new("time", DataType::Float64, true),
            ])).unwrap(),
            "definition": "CREATE EXTERNAL TABLE argo STORED AS NETCDF LOCATION 'argo/*.nc'",
            "partition_cols": [],
            "options": { "__crawler__": "c1" },
            "if_not_exists": true
        });

        let restored: Box<dyn TableDefinition> = serde_json::from_value(stored.clone()).unwrap();
        assert_eq!(restored.table_name(), "argo");

        // The fields that older versions do not write get their defaults.
        let mut stored = stored;
        stored.as_object_mut().unwrap().remove("definition_type");
        let definition: ExternalTableDefinition = serde_json::from_value(stored).unwrap();
        assert_eq!(definition.schema_source, SchemaSource::Declared);
        assert_eq!(definition.refresh_policy, RefreshPolicy::Manual);
        assert_eq!(
            format!("{:?}", definition.type_widening),
            "DefaultArrowTypeWidening { on_conflict: Fail }"
        );
    }

    #[test]
    fn stores_an_inferred_schema_and_the_refresh_policy() {
        let mut original = definition();
        original.schema = Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Float32,
            true,
        )]));
        original.refresh_policy = RefreshPolicy::Interval { seconds: 900 };

        let json = serde_json::to_value(&original).unwrap();
        assert_eq!(json["schema_source"], "inferred");
        assert_eq!(
            json["refresh_policy"],
            serde_json::json!({ "mode": "interval", "seconds": 900 })
        );

        let restored: ExternalTableDefinition = serde_json::from_value(json).unwrap();
        assert_eq!(restored.schema, original.schema);
        assert!(restored.schema_inferred());
        assert_eq!(restored.refresh_policy, original.refresh_policy);
    }

    #[test]
    fn an_older_empty_schema_is_inferred() {
        let mut older = definition();
        older.schema_source = SchemaSource::Declared;
        assert!(older.schema_inferred());
    }

    #[test]
    fn a_declared_schema_is_not_inferred() {
        let mut declared = definition();
        declared.schema = Arc::new(Schema::new(vec![Field::new(
            "depth",
            DataType::Float32,
            true,
        )]));
        declared.schema_source = SchemaSource::Declared;
        assert!(!declared.schema_inferred());
    }

    #[test]
    fn format_options_drop_the_beacon_markers() {
        let options = definition().format_options();
        assert_eq!(options.len(), 1);
        assert_eq!(options["format.has_header"], "true");
    }

    fn options(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn strategy(pairs: &[(&str, &str)]) -> Result<String> {
        type_widening_from_options(&options(pairs)).map(|strategy| format!("{strategy:?}"))
    }

    #[test]
    fn no_type_widening_option_gives_the_default_strategy() {
        assert_eq!(
            strategy(&[]).unwrap(),
            "DefaultArrowTypeWidening { on_conflict: Fail }"
        );
    }

    #[test]
    fn the_sql_spelling_names_the_strategy_and_the_conflict_rule() {
        assert_eq!(
            strategy(&[
                ("format.type_widening", "numpy"),
                ("format.type_widening_on_conflict", "keep_first"),
            ])
            .unwrap(),
            "NumpyArrowTypeWidening { on_conflict: KeepFirst }"
        );
    }

    #[test]
    fn the_bare_spelling_names_the_conflict_rule() {
        assert_eq!(
            strategy(&[("type_widening_on_conflict", "keep_first")]).unwrap(),
            "DefaultArrowTypeWidening { on_conflict: KeepFirst }"
        );
    }

    #[test]
    fn parallel_schema_inference_reads_both_spellings_and_defaults_to_off() {
        assert!(!parallel_schema_inference_from_options(&options(&[])).unwrap());
        assert!(
            parallel_schema_inference_from_options(&options(&[(
                "format.parallel_schema_inference",
                "true"
            )]))
            .unwrap()
        );
        assert!(
            parallel_schema_inference_from_options(&options(&[(
                "parallel_schema_inference",
                "on"
            )]))
            .unwrap()
        );
        assert!(
            parallel_schema_inference_from_options(&options(&[(
                "parallel_schema_inference",
                "maybe"
            )]))
            .is_err()
        );
    }

    #[test]
    fn an_unknown_type_widening_value_errors() {
        assert!(strategy(&[("format.type_widening", "polars")]).is_err());
        assert!(strategy(&[("format.type_widening_on_conflict", "maybe")]).is_err());
    }

    #[test]
    fn format_options_drop_the_type_widening_options() {
        let definition = ExternalTableDefinition {
            options: options(&[
                ("format.type_widening", "numpy"),
                ("type_widening_on_conflict", "fail"),
                ("format.parallel_schema_inference", "true"),
                ("format.skip_invalid_files_on_inference", "true"),
                ("skip_invalid_files_on_scan", "true"),
                ("format.use_statistics_pruning", "true"),
                ("statistics_columns", "temp"),
                ("format.statistics_refresh_interval", "60"),
                ("format.has_header", "true"),
            ]),
            ..definition()
        };
        let options = definition.format_options();
        assert_eq!(options.len(), 1);
        assert_eq!(options["format.has_header"], "true");
    }

    #[test]
    fn statistics_pruning_is_off_without_the_option() {
        assert!(!bool_table_option(&options(&[]), USE_STATISTICS_PRUNING_OPTION).unwrap());
        let on = options(&[("format.use_statistics_pruning", "true")]);
        assert!(bool_table_option(&on, USE_STATISTICS_PRUNING_OPTION).unwrap());
    }

    #[test]
    fn statistics_columns_split_on_commas() {
        assert_eq!(
            statistics_columns_from_options(&options(&[])).unwrap(),
            None
        );
        let named = options(&[("format.statistics_columns", " TIME, DEPTH ,")]);
        assert_eq!(
            statistics_columns_from_options(&named).unwrap(),
            Some(vec!["TIME".to_string(), "DEPTH".to_string()])
        );
        assert!(
            statistics_columns_from_options(&options(&[("statistics_columns", " , ")])).is_err()
        );
    }

    #[test]
    fn the_refresh_interval_is_positive_seconds() {
        assert_eq!(
            statistics_refresh_interval_from_options(&options(&[])).unwrap(),
            DEFAULT_STATISTICS_REFRESH_INTERVAL
        );
        let minute = options(&[("statistics_refresh_interval", "60")]);
        assert_eq!(
            statistics_refresh_interval_from_options(&minute).unwrap(),
            Duration::from_secs(60)
        );
        for bad in ["0", "-5", "soon"] {
            let bad = options(&[("statistics_refresh_interval", bad)]);
            assert!(statistics_refresh_interval_from_options(&bad).is_err());
        }
    }

    #[test]
    fn an_empty_schema_means_inferred() {
        assert!(definition().schema_inferred());
    }
}
