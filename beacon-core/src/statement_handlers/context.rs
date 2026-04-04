use std::sync::Arc;

use arrow::datatypes::Schema;
use beacon_atlas::datafusion::table::AtlasTableDefinition;
use beacon_data_lake::DataLake;
use datafusion::{
    datasource::TableProvider,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
    sql::{sqlparser::ast::ObjectName, TableReference},
};

use crate::statement_handlers::{registry::IngestFormatLoaderRegistry, traits::IngestFormatLoader};

pub(crate) struct HandlerContext {
    session_ctx: Arc<SessionContext>,
    data_lake: Arc<DataLake>,
    loader_registry: IngestFormatLoaderRegistry,
}

impl HandlerContext {
    pub(crate) fn new(
        session_ctx: Arc<SessionContext>,
        data_lake: Arc<DataLake>,
        loader_registry: IngestFormatLoaderRegistry,
    ) -> Self {
        Self {
            session_ctx,
            data_lake,
            loader_registry,
        }
    }

    pub(crate) fn session_ctx(&self) -> Arc<SessionContext> {
        self.session_ctx.clone()
    }

    pub(crate) fn data_lake(&self) -> Arc<DataLake> {
        self.data_lake.clone()
    }

    pub(crate) fn ingest_loader(&self, format: &str) -> Option<Arc<dyn IngestFormatLoader>> {
        self.loader_registry.get_loader(format)
    }

    pub(crate) async fn resolve_table_provider(
        &self,
        table_name: &ObjectName,
    ) -> anyhow::Result<Arc<dyn TableProvider>> {
        let table_ref = TableReference::parse_str(&table_name.to_string());
        self.session_ctx
            .table_provider(table_ref)
            .await
            .map_err(Into::into)
    }

    pub(crate) fn as_atlas_table<'a>(
        &self,
        table: &'a dyn TableProvider,
    ) -> anyhow::Result<&'a AtlasTableDefinition> {
        table
            .as_any()
            .downcast_ref::<AtlasTableDefinition>()
            .ok_or_else(|| anyhow::anyhow!("Table is not an AtlasTable"))
    }

    pub(crate) fn empty_record_batch_stream(&self) -> SendableRecordBatchStream {
        let stream = RecordBatchStreamAdapter::new(
            Schema::empty().into(),
            futures::stream::empty::<datafusion::error::Result<arrow::record_batch::RecordBatch>>(),
        );

        Box::pin(stream) as SendableRecordBatchStream
    }
}
