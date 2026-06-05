use std::fmt::Display;

use datafusion::sql::{parser::Statement, sqlparser::ast::ObjectName};

#[derive(Debug, Clone)]
pub enum BeaconStatement {
    DFStatement(Box<Statement>),
    CreateMaterializedView(CreateMaterializedViewStatement),
    Refresh(RefreshStatement),
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
        }
    }
}
