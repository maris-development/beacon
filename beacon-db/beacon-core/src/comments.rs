//! Free-text comments on tables and columns (`COMMENT ON TABLE | COLUMN`).
//!
//! Comments are stored in a `db://<table>/comments.json` sidecar, next to the
//! table's `table.json`, so they:
//!
//! - apply to every table type (listing/Lance/Iceberg/Delta/SQL/remote/view),
//! - survive provider re-registration (materialized-view refresh, Iceberg alter),
//! - are removed automatically on `DROP TABLE` (the table directory is deleted).
//!
//! Readers do not open the sidecar themselves. [`TableComments::apply`] merges
//! the comments into the Arrow schema under the [`COMMENT_METADATA_KEY`]: the
//! table comment goes into the schema metadata, each column comment into the
//! metadata of its field.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::{Field, Fields, Schema, SchemaRef};
use beacon_datafusion_ext::consts::DEFAULT_DB_STORE_URL_OBJECT_URL;
use datafusion::prelude::SessionContext;
use datafusion::sql::TableReference;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::schema_persistence::SchemaPersistenceService;

/// The schema and field metadata key that carries a comment.
pub const COMMENT_METADATA_KEY: &str = "comment";

/// The format version of the stored `comments.json` document.
const COMMENTS_VERSION: u32 = 1;

/// The comments of one table: an optional table comment and one comment per
/// column, keyed by the exact column name.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[schema(example = json!({
    "table": "Argo float profiles by location, depth and time.",
    "columns": { "depth": "Measurement depth in meters" }
}))]
#[serde(deny_unknown_fields)]
pub struct TableComments {
    /// The comment on the table itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// The comment on each column, keyed by column name.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, String>,
}

/// The stored form of [`TableComments`], with its format version.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredComments {
    version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    columns: BTreeMap<String, String>,
}

impl TableComments {
    /// Whether the table has no comments at all.
    pub fn is_empty(&self) -> bool {
        self.table.is_none() && self.columns.is_empty()
    }

    /// Set (`Some`) or remove (`None`) the table comment.
    pub fn set_table(&mut self, comment: Option<String>) {
        self.table = comment.filter(|text| !text.is_empty());
    }

    /// Set (`Some`) or remove (`None`) the comment on `column`.
    pub fn set_column(&mut self, column: &str, comment: Option<String>) {
        match comment.filter(|text| !text.is_empty()) {
            Some(text) => {
                self.columns.insert(column.to_string(), text);
            }
            None => {
                self.columns.remove(column);
            }
        }
    }

    /// Move the comment of column `from` to column `to`.
    pub fn rename_column(&mut self, from: &str, to: &str) {
        if let Some(text) = self.columns.remove(from) {
            self.columns.insert(to.to_string(), text);
        }
    }

    /// Check that every commented column exists in `schema`.
    pub fn validate(&self, schema: &Schema) -> anyhow::Result<()> {
        for column in self.columns.keys() {
            ensure_column(schema, column)?;
        }
        Ok(())
    }

    /// A copy of `schema` with the comments in its metadata.
    ///
    /// A comment replaces any `comment` metadata the field already has. A
    /// comment on a column that is not in `schema` is ignored.
    pub fn apply(&self, schema: &Schema) -> Schema {
        if self.is_empty() {
            return schema.clone();
        }
        let fields: Fields = schema
            .fields()
            .iter()
            .map(|field| match self.columns.get(field.name()) {
                Some(text) => {
                    let mut metadata = field.metadata().clone();
                    metadata.insert(COMMENT_METADATA_KEY.to_string(), text.clone());
                    Arc::new(Field::clone(field).with_metadata(metadata))
                }
                None => field.clone(),
            })
            .collect();
        let mut metadata = schema.metadata().clone();
        if let Some(text) = &self.table {
            metadata.insert(COMMENT_METADATA_KEY.to_string(), text.clone());
        }
        Schema::new_with_metadata(fields, metadata)
    }

    fn decode(json: &str) -> anyhow::Result<Self> {
        let stored: StoredComments =
            serde_json::from_str(json).context("stored table comments are not valid")?;
        anyhow::ensure!(
            stored.version == COMMENTS_VERSION,
            "stored table comments have version {}, but this server reads version {COMMENTS_VERSION}",
            stored.version
        );
        Ok(Self {
            table: stored.table,
            columns: stored.columns,
        })
    }

    fn encode(&self) -> anyhow::Result<String> {
        let stored = StoredComments {
            version: COMMENTS_VERSION,
            table: self.table.clone(),
            columns: self.columns.clone(),
        };
        Ok(serde_json::to_string_pretty(&stored)?)
    }
}

fn ensure_column(schema: &Schema, column: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        schema.column_with_name(column).is_some(),
        "column '{column}' does not exist in the table schema"
    );
    Ok(())
}

fn persistence(ctx: &Arc<SessionContext>) -> SchemaPersistenceService {
    SchemaPersistenceService::new(ctx.clone(), DEFAULT_DB_STORE_URL_OBJECT_URL.clone())
}

/// The live schema of the provider of `table`, without comments.
async fn provider_schema(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
) -> anyhow::Result<SchemaRef> {
    let provider = ctx
        .table_provider(table.clone())
        .await
        .map_err(|_| anyhow::anyhow!("table '{table}' not found"))?;
    Ok(provider.schema())
}

/// The sidecar key of `table`, or `None` when the table is not in the default
/// schema. Only that schema persists its tables, so only its tables have comments.
fn sidecar_key<'a>(ctx: &SessionContext, table: &'a TableReference) -> Option<&'a str> {
    let config = ctx.copied_config();
    let options = &config.options().catalog;
    let in_catalog = table
        .catalog()
        .is_none_or(|catalog| catalog == options.default_catalog);
    let in_schema = table
        .schema()
        .is_none_or(|schema| schema == options.default_schema);
    (in_catalog && in_schema).then(|| table.table())
}

/// The comments stored for `table`, or none. Does not check that the table exists.
pub(crate) async fn load_comments(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
) -> anyhow::Result<TableComments> {
    let Some(key) = sidecar_key(ctx, table) else {
        return Ok(TableComments::default());
    };
    match persistence(ctx).load_table_comments_json(key).await? {
        Some(json) => {
            TableComments::decode(&json).with_context(|| format!("comments of table '{table}'"))
        }
        None => Ok(TableComments::default()),
    }
}

async fn store_comments(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
    comments: &TableComments,
) -> anyhow::Result<()> {
    let key = sidecar_key(ctx, table).ok_or_else(|| {
        anyhow::anyhow!("table '{table}' is not in the default schema, so it cannot have comments")
    })?;
    let service = persistence(ctx);
    if comments.is_empty() {
        service.remove_table_comments_json(key).await?;
    } else {
        service
            .persist_table_comments_json(key, comments.encode()?)
            .await?;
    }
    Ok(())
}

/// The comments of a registered table.
pub async fn get_table_comments(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
) -> anyhow::Result<TableComments> {
    anyhow::ensure!(ctx.table_exist(table.clone())?, "table '{table}' not found");
    load_comments(ctx, table).await
}

/// Replace all comments of a table, after a check against its live schema.
pub async fn set_table_comments(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
    comments: TableComments,
) -> anyhow::Result<()> {
    let schema = provider_schema(ctx, table).await?;
    let mut normalized = TableComments::default();
    normalized.set_table(comments.table);
    for (column, text) in comments.columns {
        normalized.set_column(&column, Some(text));
    }
    normalized.validate(&schema)?;
    store_comments(ctx, table, &normalized).await
}

/// Remove all comments of a registered table.
pub async fn delete_table_comments(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
) -> anyhow::Result<()> {
    anyhow::ensure!(ctx.table_exist(table.clone())?, "table '{table}' not found");
    store_comments(ctx, table, &TableComments::default()).await
}

/// `COMMENT ON TABLE <table> IS '<text>' | NULL`.
pub(crate) async fn comment_on_table(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
    comment: Option<String>,
    if_exists: bool,
) -> anyhow::Result<()> {
    if !ctx.table_exist(table.clone())? {
        anyhow::ensure!(if_exists, "table '{table}' not found");
        return Ok(());
    }
    let mut comments = load_comments(ctx, table).await?;
    comments.set_table(comment);
    store_comments(ctx, table, &comments).await
}

/// `COMMENT ON COLUMN <table>.<column> IS '<text>' | NULL`.
///
/// A `NULL` comment skips the column check, so it also removes the comment of a
/// column that no longer exists.
pub(crate) async fn comment_on_column(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
    column: &str,
    comment: Option<String>,
    if_exists: bool,
) -> anyhow::Result<()> {
    if !ctx.table_exist(table.clone())? {
        anyhow::ensure!(if_exists, "table '{table}' not found");
        return Ok(());
    }
    if comment.is_some() {
        let schema = provider_schema(ctx, table).await?;
        ensure_column(&schema, column)?;
    }
    let mut comments = load_comments(ctx, table).await?;
    comments.set_column(column, comment);
    store_comments(ctx, table, &comments).await
}

/// A column change of an `ALTER TABLE` that affects the comments.
pub(crate) enum ColumnChange {
    Rename { from: String, to: String },
    Drop(String),
}

/// Apply the column changes of an `ALTER TABLE` to the stored comments, in order.
pub(crate) async fn alter_columns(
    ctx: &Arc<SessionContext>,
    table: &TableReference,
    changes: &[ColumnChange],
) -> anyhow::Result<()> {
    if changes.is_empty() {
        return Ok(());
    }
    let mut comments = load_comments(ctx, table).await?;
    for change in changes {
        match change {
            ColumnChange::Rename { from, to } => comments.rename_column(from, to),
            ColumnChange::Drop(column) => comments.set_column(column, None),
        }
    }
    store_comments(ctx, table, &comments).await
}

/// One row of `beacon.system.comments`.
pub(crate) struct CommentRow {
    pub(crate) table: String,
    pub(crate) column: Option<String>,
    pub(crate) comment: String,
}

/// Every stored comment, sorted by table and then column (table comment first).
pub(crate) async fn all_comments(ctx: &Arc<SessionContext>) -> anyhow::Result<Vec<CommentRow>> {
    let mut rows = Vec::new();
    for (table, json) in persistence(ctx).list_table_comments_json().await? {
        let comments = match TableComments::decode(&json) {
            Ok(comments) => comments,
            Err(error) => {
                tracing::warn!(table, %error, "skipped unreadable table comments");
                continue;
            }
        };
        if let Some(comment) = comments.table {
            rows.push(CommentRow {
                table: table.clone(),
                column: None,
                comment,
            });
        }
        for (column, comment) in comments.columns {
            rows.push(CommentRow {
                table: table.clone(),
                column: Some(column),
                comment,
            });
        }
    }
    rows.sort_by(|a, b| (&a.table, &a.column).cmp(&(&b.table, &b.column)));
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("depth", DataType::Float64, true),
            Field::new("temp", DataType::Float64, true),
        ])
    }

    fn comments(table: Option<&str>, columns: &[(&str, &str)]) -> TableComments {
        TableComments {
            table: table.map(String::from),
            columns: columns
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn apply_puts_comments_into_schema_and_field_metadata() {
        let applied = comments(Some("profiles"), &[("depth", "meters")]).apply(&schema());

        assert_eq!(
            applied
                .metadata()
                .get(COMMENT_METADATA_KEY)
                .map(String::as_str),
            Some("profiles")
        );
        let depth = applied.field_with_name("depth").unwrap();
        assert_eq!(
            depth
                .metadata()
                .get(COMMENT_METADATA_KEY)
                .map(String::as_str),
            Some("meters")
        );
        assert!(applied
            .field_with_name("temp")
            .unwrap()
            .metadata()
            .is_empty());
    }

    #[test]
    fn apply_replaces_native_comment_and_keeps_other_metadata() {
        let native = Schema::new(vec![Field::new("depth", DataType::Float64, true)
            .with_metadata(HashMap::from([
                ("comment".to_string(), "native".to_string()),
                ("units".to_string(), "m".to_string()),
            ]))]);

        let applied = comments(None, &[("depth", "beacon")]).apply(&native);

        let metadata = applied.field_with_name("depth").unwrap().metadata();
        assert_eq!(metadata.get("comment").map(String::as_str), Some("beacon"));
        assert_eq!(metadata.get("units").map(String::as_str), Some("m"));
    }

    #[test]
    fn apply_ignores_a_comment_on_a_missing_column() {
        let applied = comments(None, &[("ghost", "stale")]).apply(&schema());

        assert_eq!(applied, schema());
    }

    #[test]
    fn an_empty_comment_removes_it() {
        let mut c = comments(Some("t"), &[("depth", "meters")]);

        c.set_table(Some(String::new()));
        c.set_column("depth", Some(String::new()));

        assert!(c.is_empty());
    }

    #[test]
    fn rename_moves_the_comment() {
        let mut c = comments(None, &[("depth", "meters")]);

        c.rename_column("depth", "pressure");
        c.rename_column("absent", "other");

        assert_eq!(c, comments(None, &[("pressure", "meters")]));
    }

    #[test]
    fn validate_rejects_an_unknown_column() {
        let error = comments(None, &[("ghost", "x")])
            .validate(&schema())
            .unwrap_err();

        assert!(error.to_string().contains("ghost"), "{error}");
    }

    #[test]
    fn stored_form_round_trips_with_a_version() {
        let original = comments(Some("t"), &[("depth", "meters")]);

        let json = original.encode().unwrap();

        assert!(json.contains("\"version\": 1"), "{json}");
        assert_eq!(TableComments::decode(&json).unwrap(), original);
    }

    #[test]
    fn decode_rejects_an_unknown_version() {
        let error = TableComments::decode(r#"{"version": 2, "table": "t"}"#).unwrap_err();

        assert!(error.to_string().contains("version 2"), "{error}");
    }
}
