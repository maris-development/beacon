//! Typed, consumer-facing table extensions (MCP descriptor, query presets).
//!
//! Extensions are metadata *about how to use* a table — distinct from its storage
//! definition and from format `options`. They are stored decoupled from the table
//! definition in a `tables://<name>/extensions.json` sidecar, so they:
//!
//! - apply uniformly to every table type (listing/Iceberg/Delta/SQL/remote/view),
//! - can be edited without rebuilding the provider,
//! - survive provider re-registration (materialized-view refresh, Iceberg alter),
//! - are removed automatically on `DROP TABLE` (the table directory is deleted).
//!
//! This module owns the typed contract, schema validation, and the
//! read/modify/write logic shared by the SQL DDL path (`SET/DROP EXTENSION`,
//! `SHOW EXTENSIONS`) and the REST path (`Runtime` methods).

use std::sync::{Arc, OnceLock};

use anyhow::Context;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use beacon_data_lake::{SchemaPersistenceService, TABLES_OBJECT_STORE_URL};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The comparison operators a [`PresetFilter`] may use.
/// The comparison operators a [`PresetFilter`] may use. Serialized with the
/// symbolic/SQL spelling shown, so stored preset JSON stays human-readable, and
/// any other value is rejected at parse time with a clear error.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub enum PresetOp {
    #[serde(rename = "=")]
    Eq,
    #[serde(rename = "!=")]
    Ne,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "between")]
    Between,
    #[serde(rename = "in")]
    In,
}

impl PresetOp {
    /// The SQL spelling of this operator.
    pub fn as_sql(self) -> &'static str {
        match self {
            PresetOp::Eq => "=",
            PresetOp::Ne => "!=",
            PresetOp::Lt => "<",
            PresetOp::Lte => "<=",
            PresetOp::Gt => ">",
            PresetOp::Gte => ">=",
            PresetOp::Between => "BETWEEN",
            PresetOp::In => "IN",
        }
    }
}

/// The full set of extensions attached to a table — the `extensions.json`
/// document. Missing kinds are omitted from the serialized form.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TableExtensions {
    /// MCP descriptor: how downstream MCP servers should surface this table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp: Option<McpExtension>,
    /// Named, predefined filter sets consumers can apply downstream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset: Option<PresetExtension>,
}

/// MCP descriptor: how a downstream MCP server should expose this table as a
/// tool/resource.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "enabled": true,
    "tool_name": "query_ocean_observations",
    "description": "Argo float observations by location, depth, and time.",
    "exposed_columns": ["lat", "lon", {"name": "depth", "description": "measurement depth in meters"}]
}))]
#[serde(deny_unknown_fields)]
pub struct McpExtension {
    /// Whether downstream MCP servers should expose this table at all.
    #[serde(default)]
    pub enabled: bool,
    /// Tool name to expose. Downstream may default to the table name if unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_name: Option<String>,
    /// Human-readable description of what the table contains / means (maps to the
    /// MCP `Tool.description`, so the model knows what the table is).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Human-readable title for the generated tool (MCP `Tool.title`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Columns to expose, optionally documented. `None` (omitted) exposes all
    /// columns. Each entry is either a bare column name (`"lat"`) or an object
    /// `{ "name": "depth", "description": "measurement depth in meters" }`
    /// describing what the column means.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exposed_columns: Option<Vec<ExposedColumn>>,
    /// Free-form advisory guard rails surfaced to the agent (in the generated
    /// tool's description and via `describe_table`). Any key/value pairs are
    /// allowed and beacon does **not** enforce them — they are hints for the model
    /// (e.g. `{"recommended_row_limit": 10000, "note": "filter by time first"}`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub guardrails: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

/// A column surfaced through the MCP tool — a bare name, or a name plus a
/// human-readable description of what it represents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum ExposedColumn {
    /// Just the column name.
    Name(String),
    /// A column name together with a description of its meaning.
    Documented(ColumnDoc),
}

/// A documented column: its name and what it represents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct ColumnDoc {
    /// Column name (must exist in the table schema).
    pub name: String,
    /// What the column means / represents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ExposedColumn {
    /// The column name.
    pub fn name(&self) -> &str {
        match self {
            ExposedColumn::Name(name) => name,
            ExposedColumn::Documented(doc) => &doc.name,
        }
    }

    /// The column's description, if documented.
    pub fn description(&self) -> Option<&str> {
        match self {
            ExposedColumn::Name(_) => None,
            ExposedColumn::Documented(doc) => doc.description.as_deref(),
        }
    }
}

/// A set of named, predefined filters consumers can apply.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "presets": [{
        "name": "north_atlantic_surface",
        "description": "Surface measurements in the North Atlantic",
        "filters": [
            { "column": "lat", "op": "between", "value": [0, 60] },
            { "column": "depth", "op": "<=", "value": 10 }
        ]
    }]
}))]
#[serde(deny_unknown_fields)]
pub struct PresetExtension {
    /// The named presets.
    pub presets: Vec<Preset>,
}

/// A single named preset: a bundle of filters applied together.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct Preset {
    /// Unique (within the table) preset name.
    pub name: String,
    /// Optional human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// The filters that make up the preset.
    pub filters: Vec<PresetFilter>,
}

/// A single predefined filter within a [`Preset`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct PresetFilter {
    /// Column the filter applies to (must exist in the table schema).
    pub column: String,
    /// Comparison operator.
    pub op: PresetOp,
    /// Filter value: a scalar, `[lo, hi]` for `between`, or `[..]` for `in`.
    #[schema(value_type = Object)]
    pub value: serde_json::Value,
}

impl TableExtensions {
    /// Whether no extensions are set.
    pub fn is_empty(&self) -> bool {
        self.mcp.is_none() && self.preset.is_none()
    }

    /// Parse the JSON payload for one extension `kind` and splice it into the
    /// document, leaving the other kinds untouched.
    pub fn set_kind(&mut self, kind: &str, json: &str) -> anyhow::Result<()> {
        match kind.to_ascii_lowercase().as_str() {
            "mcp" => {
                self.mcp = Some(
                    serde_json::from_str(json).context("invalid 'mcp' extension payload")?,
                );
            }
            "preset" => {
                self.preset = Some(
                    serde_json::from_str(json).context("invalid 'preset' extension payload")?,
                );
            }
            other => anyhow::bail!(
                "unknown extension kind '{other}'; expected one of: mcp, preset"
            ),
        }
        Ok(())
    }

    /// Remove one extension `kind` from the document.
    pub fn drop_kind(&mut self, kind: &str) -> anyhow::Result<()> {
        match kind.to_ascii_lowercase().as_str() {
            "mcp" => self.mcp = None,
            "preset" => self.preset = None,
            other => anyhow::bail!(
                "unknown extension kind '{other}'; expected one of: mcp, preset"
            ),
        }
        Ok(())
    }

    /// Validate every set extension against the table's Arrow schema.
    pub fn validate(&self, schema: &Schema) -> anyhow::Result<()> {
        if let Some(mcp) = &self.mcp {
            mcp.validate(schema)?;
        }
        if let Some(preset) = &self.preset {
            preset.validate(schema)?;
        }
        Ok(())
    }
}

impl McpExtension {
    fn validate(&self, schema: &Schema) -> anyhow::Result<()> {
        if let Some(name) = &self.tool_name {
            anyhow::ensure!(
                is_valid_tool_name(name),
                "mcp tool_name '{name}' must be 1-64 characters of letters, digits, '_' or '-' \
                 (MCP/Anthropic tool-name rules)"
            );
        }
        if let Some(columns) = &self.exposed_columns {
            for column in columns {
                ensure_column(schema, column.name())?;
            }
        }
        Ok(())
    }

    /// The names of the curated exposed columns, if any are set.
    pub fn exposed_column_names(&self) -> Option<Vec<&str>> {
        self.exposed_columns
            .as_ref()
            .map(|cols| cols.iter().map(ExposedColumn::name).collect())
    }
}

/// Whether `name` satisfies MCP/Anthropic tool-name rules: 1-64 characters of
/// `[A-Za-z0-9_-]`. A non-conforming name can make a client reject the entire
/// tool list, so it is rejected when the extension is set.
pub fn is_valid_tool_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

impl PresetExtension {
    fn validate(&self, schema: &Schema) -> anyhow::Result<()> {
        let mut seen = std::collections::HashSet::new();
        for preset in &self.presets {
            if !seen.insert(preset.name.as_str()) {
                anyhow::bail!("duplicate preset name '{}'", preset.name);
            }
            for filter in &preset.filters {
                ensure_column(schema, &filter.column)?;
                validate_filter_value_shape(&preset.name, filter)?;
            }
        }
        Ok(())
    }
}

/// `between` requires a two-element array; `in` requires a non-empty array.
fn validate_filter_value_shape(preset: &str, filter: &PresetFilter) -> anyhow::Result<()> {
    match filter.op {
        PresetOp::Between => {
            let ok = filter.value.as_array().is_some_and(|a| a.len() == 2);
            anyhow::ensure!(
                ok,
                "preset '{preset}' filter on '{}' uses 'between' but value is not a two-element array",
                filter.column
            );
        }
        PresetOp::In => {
            let ok = filter.value.as_array().is_some_and(|a| !a.is_empty());
            anyhow::ensure!(
                ok,
                "preset '{preset}' filter on '{}' uses 'in' but value is not a non-empty array",
                filter.column
            );
        }
        _ => {}
    }
    Ok(())
}

fn ensure_column(schema: &Schema, column: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        schema.column_with_name(column).is_some(),
        "column '{column}' does not exist in the table schema"
    );
    Ok(())
}

/// Arrow schema produced by `SHOW EXTENSIONS FOR <table>`: a single JSON column.
pub fn show_extensions_arrow_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| Arc::new(Schema::new(vec![Field::new("extensions", DataType::Utf8, false)])))
        .clone()
}

/// The table's live Arrow schema, erroring if the table is not registered.
async fn table_schema(ctx: &Arc<SessionContext>, name: &str) -> anyhow::Result<SchemaRef> {
    let provider = ctx
        .table_provider(name)
        .await
        .map_err(|_| anyhow::anyhow!("table '{name}' not found"))?;
    Ok(provider.schema())
}

fn persistence(ctx: &Arc<SessionContext>) -> SchemaPersistenceService {
    SchemaPersistenceService::new(ctx.clone(), TABLES_OBJECT_STORE_URL.clone())
}

/// Load a table's extensions, returning an empty set if none are stored.
pub async fn get_table_extensions(
    ctx: &Arc<SessionContext>,
    name: &str,
) -> anyhow::Result<TableExtensions> {
    anyhow::ensure!(ctx.table_exist(name)?, "table '{name}' not found");
    match persistence(ctx).load_table_extensions_json(name).await? {
        Some(json) => Ok(serde_json::from_str(&json)
            .context("stored table extensions are not valid")?),
        None => Ok(TableExtensions::default()),
    }
}

/// Set (or replace) a single extension kind from its JSON payload, validating it
/// against the table schema before persisting.
pub async fn set_table_extension(
    ctx: &Arc<SessionContext>,
    name: &str,
    kind: &str,
    json: &str,
) -> anyhow::Result<()> {
    let schema = table_schema(ctx, name).await?;
    let mut extensions = get_table_extensions(ctx, name).await?;
    extensions.set_kind(kind, json)?;
    extensions.validate(&schema)?;
    persistence(ctx)
        .persist_table_extensions_json(name, serde_json::to_string_pretty(&extensions)?)
        .await?;
    Ok(())
}

/// Remove a single extension kind. The sidecar is deleted if nothing remains.
pub async fn drop_table_extension(
    ctx: &Arc<SessionContext>,
    name: &str,
    kind: &str,
) -> anyhow::Result<()> {
    let mut extensions = get_table_extensions(ctx, name).await?;
    extensions.drop_kind(kind)?;
    write_or_remove(ctx, name, &extensions).await
}

/// Replace the entire extensions document (the REST `PUT` surface), validating it
/// against the table schema. An empty document removes the sidecar.
pub async fn set_table_extensions(
    ctx: &Arc<SessionContext>,
    name: &str,
    extensions: TableExtensions,
) -> anyhow::Result<()> {
    let schema = table_schema(ctx, name).await?;
    extensions.validate(&schema)?;
    write_or_remove(ctx, name, &extensions).await
}

/// Remove all extensions for a table.
pub async fn delete_table_extensions(
    ctx: &Arc<SessionContext>,
    name: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(ctx.table_exist(name)?, "table '{name}' not found");
    persistence(ctx).remove_table_extensions_json(name).await?;
    Ok(())
}

async fn write_or_remove(
    ctx: &Arc<SessionContext>,
    name: &str,
    extensions: &TableExtensions,
) -> anyhow::Result<()> {
    let service = persistence(ctx);
    if extensions.is_empty() {
        service.remove_table_extensions_json(name).await?;
    } else {
        service
            .persist_table_extensions_json(name, serde_json::to_string_pretty(extensions)?)
            .await?;
    }
    Ok(())
}

/// Build the single-row `SHOW EXTENSIONS FOR <table>` result.
pub async fn show_table_extensions_batch(
    ctx: &Arc<SessionContext>,
    name: &str,
) -> anyhow::Result<RecordBatch> {
    let extensions = get_table_extensions(ctx, name).await?;
    let json = serde_json::to_string_pretty(&extensions)?;
    let column = StringArray::from(vec![Some(json)]);
    RecordBatch::try_new(show_extensions_arrow_schema(), vec![Arc::new(column)])
        .context("failed to build SHOW EXTENSIONS batch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("lat", DataType::Float64, true),
            Field::new("lon", DataType::Float64, true),
            Field::new("depth", DataType::Float64, true),
        ])
    }

    #[test]
    fn validates_preset_against_schema() {
        let mut ext = TableExtensions::default();
        ext.set_kind(
            "preset",
            r#"{"presets":[{"name":"p","filters":[{"column":"lat","op":"between","value":[0,60]}]}]}"#,
        )
        .unwrap();
        assert!(ext.validate(&schema()).is_ok());
    }

    #[test]
    fn rejects_unknown_column() {
        let mut ext = TableExtensions::default();
        ext.set_kind(
            "preset",
            r#"{"presets":[{"name":"p","filters":[{"column":"nope","op":"=","value":1}]}]}"#,
        )
        .unwrap();
        let err = ext.validate(&schema()).unwrap_err().to_string();
        assert!(err.contains("does not exist"), "unexpected: {err}");
    }

    #[test]
    fn rejects_bad_operator_at_parse() {
        // An unknown operator no longer parses into the typed `op` enum at all.
        let mut ext = TableExtensions::default();
        let err = ext
            .set_kind(
                "preset",
                r#"{"presets":[{"name":"p","filters":[{"column":"lat","op":"~~","value":1}]}]}"#,
            )
            .unwrap_err();
        assert!(err.to_string().contains("preset"), "unexpected: {err}");
    }

    #[test]
    fn rejects_unknown_field_at_parse() {
        // `deny_unknown_fields` rejects typos/extra keys instead of dropping them.
        let mut ext = TableExtensions::default();
        assert!(ext
            .set_kind(
                "preset",
                r#"{"presets":[{"name":"p","filters":[],"bogus":1}]}"#,
            )
            .is_err());
        assert!(ext.set_kind("mcp", r#"{"enabled":true,"nope":1}"#).is_err());
    }

    #[test]
    fn rejects_between_without_pair() {
        let mut ext = TableExtensions::default();
        ext.set_kind(
            "preset",
            r#"{"presets":[{"name":"p","filters":[{"column":"lat","op":"between","value":5}]}]}"#,
        )
        .unwrap();
        assert!(ext.validate(&schema()).unwrap_err().to_string().contains("between"));
    }

    #[test]
    fn rejects_duplicate_preset_names() {
        let mut ext = TableExtensions::default();
        ext.set_kind(
            "preset",
            r#"{"presets":[{"name":"p","filters":[]},{"name":"p","filters":[]}]}"#,
        )
        .unwrap();
        assert!(ext.validate(&schema()).unwrap_err().to_string().contains("duplicate preset"));
    }

    #[test]
    fn mcp_rejects_invalid_tool_name() {
        let mut ext = TableExtensions::default();
        ext.set_kind("mcp", r#"{"enabled":true,"tool_name":"bad name!"}"#)
            .unwrap();
        let err = ext.validate(&schema()).unwrap_err().to_string();
        assert!(err.contains("tool_name"), "unexpected: {err}");
        // A clean name passes.
        let mut ok = TableExtensions::default();
        ok.set_kind("mcp", r#"{"enabled":true,"tool_name":"query_obs"}"#)
            .unwrap();
        assert!(ok.validate(&schema()).is_ok());
    }

    #[test]
    fn mcp_exposed_columns_must_exist() {
        let mut ext = TableExtensions::default();
        ext.set_kind("mcp", r#"{"enabled":true,"exposed_columns":["lat","ghost"]}"#)
            .unwrap();
        assert!(ext.validate(&schema()).unwrap_err().to_string().contains("does not exist"));
    }

    #[test]
    fn mcp_accepts_documented_columns() {
        // Columns may be bare names or {name, description} objects, mixed freely.
        let mut ext = TableExtensions::default();
        ext.set_kind(
            "mcp",
            r#"{"enabled":true,"exposed_columns":["lat",{"name":"depth","description":"meters"}]}"#,
        )
        .unwrap();
        assert!(ext.validate(&schema()).is_ok());
        let cols = ext.mcp.as_ref().unwrap().exposed_columns.as_ref().unwrap();
        assert_eq!((cols[0].name(), cols[0].description()), ("lat", None));
        assert_eq!((cols[1].name(), cols[1].description()), ("depth", Some("meters")));
        // A documented column with an unknown key is rejected (deny_unknown_fields).
        let mut bad = TableExtensions::default();
        assert!(bad
            .set_kind(
                "mcp",
                r#"{"enabled":true,"exposed_columns":[{"name":"lat","unit":"deg"}]}"#,
            )
            .is_err());
    }

    #[test]
    fn unknown_kind_is_rejected() {
        let mut ext = TableExtensions::default();
        assert!(ext.set_kind("bogus", "{}").is_err());
        assert!(ext.drop_kind("bogus").is_err());
    }

    #[test]
    fn set_and_drop_kinds_are_independent() {
        let mut ext = TableExtensions::default();
        ext.set_kind("mcp", r#"{"enabled":true}"#).unwrap();
        ext.set_kind("preset", r#"{"presets":[]}"#).unwrap();
        ext.drop_kind("mcp").unwrap();
        assert!(ext.mcp.is_none());
        assert!(ext.preset.is_some());
        assert!(!ext.is_empty());
    }
}
