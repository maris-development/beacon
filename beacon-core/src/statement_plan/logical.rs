//! Beacon's custom statement [`LogicalPlan::Extension`] nodes.
//!
//! Each node carries just enough to perform its side effect; they produce no
//! rows, so they share an empty output schema. The matching physical nodes live
//! in [`super::physical`].
//!
//! [`LogicalPlan::Extension`]: datafusion::logical_expr::LogicalPlan::Extension

use std::{
    cmp::Ordering,
    hash::{Hash, Hasher},
    sync::{Arc, OnceLock},
};

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::{
    common::{DFSchema, DFSchemaRef},
    error::Result,
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    sql::{
        sqlparser::ast::{AlterTableOperation, ObjectName},
        TableReference,
    },
};

use crate::extensions::show_extensions_arrow_schema;

/// Shared empty schema returned by beacon's side-effecting statement nodes,
/// which produce no rows. `schema()` must return a reference, so the schema is
/// stored once rather than rebuilt per node.
fn empty_schema() -> &'static DFSchemaRef {
    static EMPTY: OnceLock<DFSchemaRef> = OnceLock::new();
    EMPTY.get_or_init(|| Arc::new(DFSchema::empty()))
}

/// The `count: UInt64` schema produced by row-mutating physical execs (INSERT,
/// CTAS), matching DataFusion's DML output schema.
pub(crate) fn count_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

/// Wraps a payload that lacks `PartialOrd`/`Hash` (the sqlparser `ALTER TABLE`
/// AST) so it can live inside a [`UserDefinedLogicalNodeCore`], which requires
/// `Eq + PartialOrd + Hash`. Ordering, equality, and hashing use only the stable
/// string `key`.
#[derive(Debug, Clone)]
pub(crate) struct Keyed<T> {
    key: String,
    pub(crate) payload: T,
}

impl<T> Keyed<T> {
    pub(crate) fn new(key: String, payload: T) -> Self {
        Self { key, payload }
    }
}

impl<T> PartialEq for Keyed<T> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl<T> Eq for Keyed<T> {}
impl<T> PartialOrd for Keyed<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<T> Ord for Keyed<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}
impl<T> Hash for Keyed<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key.hash(state);
    }
}

/// The `name` + `operations` of an `ALTER TABLE`, carried as an opaque payload
/// (sqlparser AST does not implement the ordering/hashing the node trait needs).
#[derive(Debug, Clone)]
pub(crate) struct AlterTableSpec {
    pub(crate) name: ObjectName,
    pub(crate) operations: Vec<AlterTableOperation>,
}

/// Logical node for `CREATE MATERIALIZED VIEW <name> AS <query>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct CreateMaterializedViewNode {
    pub(crate) view_name: String,
    pub(crate) query_sql: String,
}

impl CreateMaterializedViewNode {
    pub(crate) fn new(view_name: String, query_sql: String) -> Self {
        Self {
            view_name,
            query_sql,
        }
    }
}

impl UserDefinedLogicalNodeCore for CreateMaterializedViewNode {
    fn name(&self) -> &str {
        "CreateMaterializedView"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateMaterializedView: name={}", self.view_name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            view_name: self.view_name.clone(),
            query_sql: self.query_sql.clone(),
        })
    }
}

/// Logical node for the auth-management statements (CREATE/DROP USER/ROLE, GRANT/DENY/REVOKE).
///
/// The [`AuthStatement`] AST lacks `PartialOrd`/`Hash`, so it is carried in a [`Keyed`] wrapper
/// whose stable key is the statement's SQL rendering. Produces no rows.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct AuthNode {
    pub(crate) statement: Keyed<crate::parser::statement::AuthStatement>,
}

impl UserDefinedLogicalNodeCore for AuthNode {
    fn name(&self) -> &str {
        "Auth"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Auth: {}", self.statement.payload)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            statement: self.statement.clone(),
        })
    }
}

/// Logical node for `REFRESH [TABLE] <name>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct RefreshNode {
    pub(crate) name: String,
}

impl RefreshNode {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl UserDefinedLogicalNodeCore for RefreshNode {
    fn name(&self) -> &str {
        "Refresh"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Refresh: name={}", self.name)
    }

    fn with_exprs_and_inputs(
        &self,
        _exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
        })
    }
}

/// Arrow schema produced by `SHOW CRAWLERS`.
pub(crate) fn show_crawlers_arrow_schema() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("name", DataType::Utf8, false),
                Field::new("target_prefix", DataType::Utf8, false),
                Field::new("format_filter", DataType::Utf8, true),
                Field::new("detect_partitions", DataType::Boolean, false),
                Field::new("schedule_secs", DataType::UInt64, true),
                Field::new("event_driven", DataType::Boolean, false),
                Field::new("table_naming", DataType::Utf8, false),
            ]))
        })
        .clone()
}

fn show_crawlers_df_schema() -> &'static DFSchemaRef {
    static SCHEMA: OnceLock<DFSchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Arc::new(
            DFSchema::try_from(show_crawlers_arrow_schema().as_ref().clone())
                .expect("SHOW CRAWLERS schema is valid"),
        )
    })
}

/// Logical node for `CREATE CRAWLER <name> [ON '<prefix>'] [WITH (...)]`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct CreateCrawlerNode {
    pub(crate) name: String,
    pub(crate) target_prefix: Option<String>,
    /// Options as a sorted `(key, value)` list (the node trait needs `Ord`/`Hash`,
    /// which `HashMap` is not).
    pub(crate) options: Vec<(String, String)>,
}

impl CreateCrawlerNode {
    pub(crate) fn new(
        name: String,
        target_prefix: Option<String>,
        mut options: Vec<(String, String)>,
    ) -> Self {
        options.sort();
        Self {
            name,
            target_prefix,
            options,
        }
    }
}

impl UserDefinedLogicalNodeCore for CreateCrawlerNode {
    fn name(&self) -> &str {
        "CreateCrawler"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateCrawler: name={}", self.name)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
            target_prefix: self.target_prefix.clone(),
            options: self.options.clone(),
        })
    }
}

/// Logical node for `RUN CRAWLER <name>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct RunCrawlerNode {
    pub(crate) name: String,
}

impl RunCrawlerNode {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl UserDefinedLogicalNodeCore for RunCrawlerNode {
    fn name(&self) -> &str {
        "RunCrawler"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RunCrawler: name={}", self.name)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
        })
    }
}

/// Logical node for `DROP CRAWLER <name>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct DropCrawlerNode {
    pub(crate) name: String,
}

impl DropCrawlerNode {
    pub(crate) fn new(name: String) -> Self {
        Self { name }
    }
}

impl UserDefinedLogicalNodeCore for DropCrawlerNode {
    fn name(&self) -> &str {
        "DropCrawler"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DropCrawler: name={}", self.name)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            name: self.name.clone(),
        })
    }
}

/// Logical node for `SHOW CRAWLERS`. Unlike the other crawler nodes it produces
/// rows, so it carries the listing schema.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct ShowCrawlersNode;

impl UserDefinedLogicalNodeCore for ShowCrawlersNode {
    fn name(&self) -> &str {
        "ShowCrawlers"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        show_crawlers_df_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ShowCrawlers")
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self)
    }
}

/// Arrow schema produced by `SHOW INDEXES`.
pub(crate) fn show_indexes_arrow_schema() -> Arc<Schema> {
    static SCHEMA: OnceLock<Arc<Schema>> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            Arc::new(Schema::new(vec![
                Field::new("index_name", DataType::Utf8, false),
                Field::new("columns", DataType::Utf8, false),
            ]))
        })
        .clone()
}

fn show_indexes_df_schema() -> &'static DFSchemaRef {
    static SCHEMA: OnceLock<DFSchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Arc::new(
            DFSchema::try_from(show_indexes_arrow_schema().as_ref().clone())
                .expect("SHOW INDEXES schema is valid"),
        )
    })
}

/// Logical node for `CREATE INDEX [<name>] ON <table> (<column>) [USING <type>]`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct CreateIndexNode {
    pub(crate) table: String,
    pub(crate) column: String,
    pub(crate) name: Option<String>,
    pub(crate) using: Option<String>,
}

impl UserDefinedLogicalNodeCore for CreateIndexNode {
    fn name(&self) -> &str {
        "CreateIndex"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CreateIndex: table={} column={}", self.table, self.column)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            table: self.table.clone(),
            column: self.column.clone(),
            name: self.name.clone(),
            using: self.using.clone(),
        })
    }
}

/// Logical node for `DROP INDEX <name> ON <table>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct DropIndexNode {
    pub(crate) table: String,
    pub(crate) name: String,
}

impl UserDefinedLogicalNodeCore for DropIndexNode {
    fn name(&self) -> &str {
        "DropIndex"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DropIndex: table={} name={}", self.table, self.name)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            table: self.table.clone(),
            name: self.name.clone(),
        })
    }
}

/// Logical node for `SHOW INDEXES ON <table>`. Produces rows, so it carries the
/// listing schema.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct ShowIndexesNode {
    pub(crate) table: String,
}

impl UserDefinedLogicalNodeCore for ShowIndexesNode {
    fn name(&self) -> &str {
        "ShowIndexes"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        show_indexes_df_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ShowIndexes: table={}", self.table)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            table: self.table.clone(),
        })
    }
}

/// A native row mutation, derived best-effort at lowering time. When present and
/// the target is a Lance table, it is applied via Lance's native `delete`/update
/// (deletion vectors / fragment rewrite) instead of the copy-on-write `input`.
/// Iceberg always uses the copy-on-write `input`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) enum Mutation {
    /// `DELETE FROM t [WHERE predicate]`; `None` predicate deletes every row.
    Delete { predicate: Option<String> },
    /// `UPDATE t SET <assignments> [WHERE predicate]`, assignments as
    /// `(column, value_sql)`.
    Update {
        predicate: Option<String>,
        assignments: Vec<(String, String)>,
    },
}

/// Logical node for the copy-on-write replacement that backs `DELETE` and
/// `UPDATE`: `input` computes the table's full post-mutation contents, which
/// atomically replace the table's data files (used for Iceberg, and as the
/// fallback when no native [`Mutation`] could be derived). Produces no rows.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct ReplaceTableContentsNode {
    pub(crate) table: TableReference,
    pub(crate) input: LogicalPlan,
    /// Native mutation spec; `None` means copy-on-write only.
    pub(crate) mutation: Option<Mutation>,
}

impl UserDefinedLogicalNodeCore for ReplaceTableContentsNode {
    fn name(&self) -> &str {
        "ReplaceTableContents"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.input]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ReplaceTableContents: table={}", self.table)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, mut inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            table: self.table.clone(),
            input: inputs.swap_remove(0),
            mutation: self.mutation.clone(),
        })
    }
}

/// Logical node for `ALTER TABLE <name> <operations>`. DataFusion cannot plan
/// `ALTER TABLE`, so it is lowered straight from the parsed AST.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct AlterTableNode {
    pub(crate) spec: Keyed<AlterTableSpec>,
}

impl UserDefinedLogicalNodeCore for AlterTableNode {
    fn name(&self) -> &str {
        "AlterTable"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AlterTable: name={}", self.spec.payload.name)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            spec: self.spec.clone(),
        })
    }
}

fn show_extensions_df_schema() -> &'static DFSchemaRef {
    static SCHEMA: OnceLock<DFSchemaRef> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        Arc::new(
            DFSchema::try_from(show_extensions_arrow_schema().as_ref().clone())
                .expect("SHOW EXTENSIONS schema is valid"),
        )
    })
}

/// Logical node for `SET EXTENSION '<kind>' FOR <table> TO '<json>'`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct SetExtensionNode {
    pub(crate) kind: String,
    pub(crate) table: String,
    pub(crate) json: String,
}

impl SetExtensionNode {
    pub(crate) fn new(kind: String, table: String, json: String) -> Self {
        Self { kind, table, json }
    }
}

impl UserDefinedLogicalNodeCore for SetExtensionNode {
    fn name(&self) -> &str {
        "SetExtension"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SetExtension: table={} kind={}", self.table, self.kind)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            kind: self.kind.clone(),
            table: self.table.clone(),
            json: self.json.clone(),
        })
    }
}

/// Logical node for `DROP EXTENSION '<kind>' FOR <table>`.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct DropExtensionNode {
    pub(crate) kind: String,
    pub(crate) table: String,
}

impl DropExtensionNode {
    pub(crate) fn new(kind: String, table: String) -> Self {
        Self { kind, table }
    }
}

impl UserDefinedLogicalNodeCore for DropExtensionNode {
    fn name(&self) -> &str {
        "DropExtension"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        empty_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "DropExtension: table={} kind={}", self.table, self.kind)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            kind: self.kind.clone(),
            table: self.table.clone(),
        })
    }
}

/// Logical node for `SHOW EXTENSIONS FOR <table>`. Produces one JSON row.
#[derive(Debug, PartialEq, Eq, PartialOrd, Hash)]
pub(crate) struct ShowExtensionsNode {
    pub(crate) table: String,
}

impl ShowExtensionsNode {
    pub(crate) fn new(table: String) -> Self {
        Self { table }
    }
}

impl UserDefinedLogicalNodeCore for ShowExtensionsNode {
    fn name(&self) -> &str {
        "ShowExtensions"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }
    fn schema(&self) -> &DFSchemaRef {
        show_extensions_df_schema()
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ShowExtensions: table={}", self.table)
    }
    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, _inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            table: self.table.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::Extension;

    /// Wrapping a node in `LogicalPlan::Extension` and displaying the plan
    /// exercises `fmt_for_explain`; this is what `EXPLAIN` would render once the
    /// statement reaches the logical-plan layer.
    #[test]
    fn create_materialized_view_node_explains() {
        let node = CreateMaterializedViewNode::new("mv1".to_string(), "SELECT 1".to_string());
        assert_eq!(node.name(), "CreateMaterializedView");
        assert!(node.inputs().is_empty());
        assert_eq!(node.schema().fields().len(), 0);

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let display = format!("{}", plan.display_indent());
        assert!(
            display.contains("CreateMaterializedView: name=mv1"),
            "unexpected explain output: {display}"
        );
    }

    #[test]
    fn refresh_node_explains() {
        let node = RefreshNode::new("my_table".to_string());
        assert_eq!(node.name(), "Refresh");
        assert!(node.inputs().is_empty());
        assert_eq!(node.schema().fields().len(), 0);

        let plan = LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        });
        let display = format!("{}", plan.display_indent());
        assert!(
            display.contains("Refresh: name=my_table"),
            "unexpected explain output: {display}"
        );
    }
}
