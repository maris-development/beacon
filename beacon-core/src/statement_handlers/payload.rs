use crate::parser::statement::{
    BeaconStatement, CreateMaterializedViewStatement, RefreshStatement,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum StatementKind {
    DFStatement,
    CreateTable,
    CreateMaterializedView,
    Refresh,
}

pub(crate) enum StatementPayload {
    DFStatement(datafusion::sql::parser::Statement),
    CreateMaterializedView(CreateMaterializedViewStatement),
    Refresh(RefreshStatement),
}

impl StatementPayload {
    pub(crate) fn kind(&self) -> StatementKind {
        match self {
            Self::DFStatement(_) => StatementKind::DFStatement,
            Self::CreateMaterializedView(_) => StatementKind::CreateMaterializedView,
            Self::Refresh(_) => StatementKind::Refresh,
        }
    }

    pub(crate) fn into_df_statement(self) -> anyhow::Result<datafusion::sql::parser::Statement> {
        match self {
            Self::DFStatement(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected DFStatement payload")),
        }
    }

    pub(crate) fn into_create_materialized_view(
        self,
    ) -> anyhow::Result<CreateMaterializedViewStatement> {
        match self {
            Self::CreateMaterializedView(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected CreateMaterializedView payload")),
        }
    }

    pub(crate) fn into_refresh(self) -> anyhow::Result<RefreshStatement> {
        match self {
            Self::Refresh(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected Refresh payload")),
        }
    }
}

impl From<BeaconStatement> for StatementPayload {
    fn from(statement: BeaconStatement) -> Self {
        match statement {
            BeaconStatement::DFStatement(statement) => Self::DFStatement(*statement),
            BeaconStatement::CreateMaterializedView(statement) => {
                Self::CreateMaterializedView(statement)
            }
            BeaconStatement::Refresh(statement) => Self::Refresh(statement),
        }
    }
}
