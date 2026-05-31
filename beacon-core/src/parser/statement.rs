use std::fmt::Display;

use datafusion::sql::{parser::Statement, sqlparser::ast::ObjectName};

#[derive(Debug, Clone)]
pub enum BeaconStatement {
    DFStatement(Box<Statement>),
    Ingest(IngestStatement),
    DeleteAtlasDatasets(DeleteAtlasDatasetsStatement),
    CreateAtlasTable(CreateAtlasTableStatement),
    AlterAtlas(AlterAtlasTableStatement),
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

/// ALTER ATLAS TABLE <table_name> ON PARTITION <partition_name> ALTER COLUMN <column_name> SET DATA TYPE <data_type>
#[derive(Debug, Clone)]
pub struct AlterAtlasTableStatement {
    pub table_name: ObjectName,
    pub partition_name: String,
    pub op: AtlasOp,
}

#[derive(Debug, Clone)]
pub enum AtlasOp {
    CastColumn {
        column_name: String,
        data_type: String,
    },
}

impl Display for AlterAtlasTableStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.op {
            AtlasOp::CastColumn {
                column_name,
                data_type,
            } => write!(
                f,
                "ALTER ATLAS TABLE {} ON PARTITION {} ALTER COLUMN {} SET DATA TYPE {}",
                self.table_name, self.partition_name, column_name, data_type
            ),
        }
    }
}

/// CREATE ATLAS TABLE <table_name> LOCATION '<path>'
#[derive(Debug, Clone)]
pub struct CreateAtlasTableStatement {
    pub table_name: ObjectName,
    pub location: String,
}

impl Display for CreateAtlasTableStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CREATE ATLAS TABLE {} LOCATION '{}'",
            self.table_name, self.location
        )
    }
}

/// DELETE ATLAS DATASETS [dataset_name1, dataset_name2, ...] FROM <table_name> ON PARTITION <partition_name>
#[derive(Debug, Clone)]
pub struct DeleteAtlasDatasetsStatement {
    pub dataset_names: Option<Vec<String>>,
    pub table_name: ObjectName,
    pub partition_name: String,
}

impl Display for DeleteAtlasDatasetsStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DELETE ATLAS DATASETS ")?;
        if let Some(names) = &self.dataset_names {
            let quoted: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
            write!(f, "{} ", quoted.join(", "))?;
        }
        write!(
            f,
            "FROM {} ON PARTITION {}",
            self.table_name, self.partition_name
        )
    }
}

/// INGEST INTO ATLAS <table_name> ON PARTITION <partition_name> FROM '<glob_pattern>' WITH <format>
#[derive(Debug, Clone)]
pub struct IngestStatement {
    pub glob_pattern: String,
    pub format: String,
    pub table_name: ObjectName,
    pub partition_name: String,
}

impl Display for BeaconStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DFStatement(s) => write!(f, "{s}"),
            Self::Ingest(s) => write!(f, "{s}"),
            Self::DeleteAtlasDatasets(s) => write!(f, "{s}"),
            Self::CreateAtlasTable(s) => write!(f, "{s}"),
            Self::AlterAtlas(s) => write!(f, "{s}"),
            Self::CreateMaterializedView(s) => write!(f, "{s}"),
            Self::Refresh(s) => write!(f, "{s}"),
        }
    }
}

impl Display for IngestStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "INGEST INTO ATLAS {} ON PARTITION {} FROM '{}' WITH {}",
            self.table_name, self.partition_name, self.glob_pattern, self.format
        )
    }
}
