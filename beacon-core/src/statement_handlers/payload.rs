use crate::parser::statement::BeaconStatement;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum StatementKind {
    DFStatement,
}

pub(crate) enum StatementPayload {
    DFStatement(datafusion::sql::parser::Statement),
}

impl StatementPayload {
    pub(crate) fn kind(&self) -> StatementKind {
        match self {
            Self::DFStatement(_) => StatementKind::DFStatement,
        }
    }

    pub(crate) fn into_df_statement(self) -> anyhow::Result<datafusion::sql::parser::Statement> {
        match self {
            Self::DFStatement(statement) => Ok(statement),
        }
    }
}

impl From<BeaconStatement> for StatementPayload {
    fn from(statement: BeaconStatement) -> Self {
        match statement {
            BeaconStatement::DFStatement(statement) => Self::DFStatement(*statement),
            // `CreateMaterializedView` and `Refresh` are lowered to physical-plan
            // nodes in `Runtime::run_sql` and never reach the statement registry.
            other => {
                unreachable!("non-DFStatement reached the statement registry: {other}")
            }
        }
    }
}
