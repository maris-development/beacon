use std::collections::HashMap;
use std::fmt::Display;

use datafusion::sql::{parser::Statement, sqlparser::ast::ObjectName};

#[derive(Debug, Clone)]
pub enum BeaconStatement {
    DFStatement(Box<Statement>),
    CreateMaterializedView(CreateMaterializedViewStatement),
    Refresh(RefreshStatement),
    CreateCrawler(CreateCrawlerStatement),
    RunCrawler(RunCrawlerStatement),
    DropCrawler(DropCrawlerStatement),
    ShowCrawlers,
    SetExtension(SetExtensionStatement),
    DropExtension(DropExtensionStatement),
    ShowExtensions(ShowExtensionsStatement),
}

/// SET EXTENSION '<kind>' FOR <table> TO '<json>'
#[derive(Debug, Clone)]
pub struct SetExtensionStatement {
    /// Extension kind (e.g. `mcp`, `preset`).
    pub kind: String,
    /// Target table.
    pub table: ObjectName,
    /// The extension payload as a JSON string literal.
    pub json: String,
}

impl Display for SetExtensionStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SET EXTENSION '{}' FOR {} TO '{}'",
            escape_sql_literal(&self.kind),
            self.table,
            escape_sql_literal(&self.json)
        )
    }
}

/// DROP EXTENSION '<kind>' FOR <table>
#[derive(Debug, Clone)]
pub struct DropExtensionStatement {
    /// Extension kind to remove.
    pub kind: String,
    /// Target table.
    pub table: ObjectName,
}

impl Display for DropExtensionStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DROP EXTENSION '{}' FOR {}",
            escape_sql_literal(&self.kind),
            self.table
        )
    }
}

/// SHOW EXTENSIONS FOR <table>
#[derive(Debug, Clone)]
pub struct ShowExtensionsStatement {
    /// Target table.
    pub table: ObjectName,
}

impl Display for ShowExtensionsStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW EXTENSIONS FOR {}", self.table)
    }
}

/// Escape a value for embedding in a single-quoted SQL string literal so the
/// `Display` form re-parses to the same value (the tokenizer turns `''` back into
/// `'`).
fn escape_sql_literal(value: &str) -> String {
    value.replace('\'', "''")
}

/// CREATE CRAWLER <name> [ON '<prefix>'] [WITH (k 'v', ...)]
#[derive(Debug, Clone)]
pub struct CreateCrawlerStatement {
    pub name: ObjectName,
    /// The `ON '<prefix>'` target prefix, if given.
    pub target_prefix: Option<String>,
    /// The `WITH (...)` options.
    pub options: HashMap<String, String>,
}

impl Display for CreateCrawlerStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE CRAWLER {}", self.name)?;
        if let Some(prefix) = &self.target_prefix {
            write!(f, " ON '{prefix}'")?;
        }
        if !self.options.is_empty() {
            let mut opts: Vec<_> = self.options.iter().collect();
            opts.sort_by(|a, b| a.0.cmp(b.0));
            let rendered = opts
                .iter()
                .map(|(k, v)| format!("'{k}' '{v}'"))
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, " WITH ({rendered})")?;
        }
        Ok(())
    }
}

/// RUN CRAWLER <name>
#[derive(Debug, Clone)]
pub struct RunCrawlerStatement {
    pub name: ObjectName,
}

impl Display for RunCrawlerStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RUN CRAWLER {}", self.name)
    }
}

/// DROP CRAWLER <name>
#[derive(Debug, Clone)]
pub struct DropCrawlerStatement {
    pub name: ObjectName,
}

impl Display for DropCrawlerStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP CRAWLER {}", self.name)
    }
}

/// CREATE MATERIALIZED VIEW <view_name> AS <query>
#[derive(Debug, Clone)]
pub struct CreateMaterializedViewStatement {
    pub view_name: ObjectName,
    /// The SQL text of the defining query (everything after `AS`).
    pub query_sql: String,
}

impl Display for CreateMaterializedViewStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE MATERIALIZED VIEW {} AS {}",
            self.view_name, self.query_sql
        )
    }
}

/// REFRESH [TABLE] <name> — applies to external tables and materialized views.
#[derive(Debug, Clone)]
pub struct RefreshStatement {
    pub name: ObjectName,
}

impl Display for RefreshStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "REFRESH {}", self.name)
    }
}

impl Display for BeaconStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DFStatement(s) => write!(f, "{s}"),
            Self::CreateMaterializedView(s) => write!(f, "{s}"),
            Self::Refresh(s) => write!(f, "{s}"),
            Self::CreateCrawler(s) => write!(f, "{s}"),
            Self::RunCrawler(s) => write!(f, "{s}"),
            Self::DropCrawler(s) => write!(f, "{s}"),
            Self::ShowCrawlers => write!(f, "SHOW CRAWLERS"),
            Self::SetExtension(s) => write!(f, "{s}"),
            Self::DropExtension(s) => write!(f, "{s}"),
            Self::ShowExtensions(s) => write!(f, "{s}"),
        }
    }
}
