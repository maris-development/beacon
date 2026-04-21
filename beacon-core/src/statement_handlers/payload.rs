use crate::parser::statement::{
    AlterAtlasTableStatement, BeaconStatement, CreateAtlasTableStatement,
    DeleteAtlasDatasetsStatement, IngestStatement,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum StatementKind {
    DFStatement,
    Ingest,
    DeleteAtlasDatasets,
    CreateAtlasTable,
    AlterAtlas,
    CreateTable,
}

pub(crate) enum StatementPayload {
    DFStatement(datafusion::sql::parser::Statement),
    Ingest(IngestStatement),
    DeleteAtlasDatasets(DeleteAtlasDatasetsStatement),
    CreateAtlasTable(CreateAtlasTableStatement),
    AlterAtlas(AlterAtlasTableStatement),
}

impl StatementPayload {
    pub(crate) fn kind(&self) -> StatementKind {
        match self {
            Self::DFStatement(_) => StatementKind::DFStatement,
            Self::Ingest(_) => StatementKind::Ingest,
            Self::DeleteAtlasDatasets(_) => StatementKind::DeleteAtlasDatasets,
            Self::CreateAtlasTable(_) => StatementKind::CreateAtlasTable,
            Self::AlterAtlas(_) => StatementKind::AlterAtlas,
        }
    }

    pub(crate) fn into_df_statement(self) -> anyhow::Result<datafusion::sql::parser::Statement> {
        match self {
            Self::DFStatement(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected DFStatement payload")),
        }
    }

    pub(crate) fn into_ingest(self) -> anyhow::Result<IngestStatement> {
        match self {
            Self::Ingest(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected Ingest payload")),
        }
    }

    pub(crate) fn into_delete_atlas_datasets(self) -> anyhow::Result<DeleteAtlasDatasetsStatement> {
        match self {
            Self::DeleteAtlasDatasets(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected DeleteAtlasDatasets payload")),
        }
    }

    pub(crate) fn into_create_atlas_table(self) -> anyhow::Result<CreateAtlasTableStatement> {
        match self {
            Self::CreateAtlasTable(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected CreateAtlasTable payload")),
        }
    }

    pub(crate) fn into_alter_atlas(self) -> anyhow::Result<AlterAtlasTableStatement> {
        match self {
            Self::AlterAtlas(statement) => Ok(statement),
            _ => Err(anyhow::anyhow!("Expected AlterAtlas payload")),
        }
    }
}

impl From<BeaconStatement> for StatementPayload {
    fn from(statement: BeaconStatement) -> Self {
        match statement {
            BeaconStatement::DFStatement(statement) => Self::DFStatement(*statement),
            BeaconStatement::Ingest(statement) => Self::Ingest(statement),
            BeaconStatement::DeleteAtlasDatasets(statement) => Self::DeleteAtlasDatasets(statement),
            BeaconStatement::CreateAtlasTable(statement) => Self::CreateAtlasTable(statement),
            BeaconStatement::AlterAtlas(statement) => Self::AlterAtlas(statement),
        }
    }
}
