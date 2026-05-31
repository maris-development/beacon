use std::sync::Arc;

use arrow::datatypes::Schema;
use beacon_data_lake::FileManager;
use beacon_datafusion_ext::listing_table_factory_ext::ListingTableFactoryExt;
use datafusion::{
    datasource::{listing, TableProvider},
    execution::{object_store::ObjectStoreUrl, SendableRecordBatchStream},
    physical_plan::stream::RecordBatchStreamAdapter,
    prelude::SessionContext,
    sql::{sqlparser::ast::ObjectName, TableReference},
};

use crate::statement_handlers::{registry::IngestFormatLoaderRegistry, traits::IngestFormatLoader};

pub(crate) struct HandlerContext {
    session_ctx: Arc<SessionContext>,
    file_manager: Arc<FileManager>,
    loader_registry: IngestFormatLoaderRegistry,
    listing_table_factory: Arc<ListingTableFactoryExt>,
    auth: Arc<beacon_auth::AuthContext>,
    identity: beacon_auth::AuthIdentity,
}

impl HandlerContext {
    pub(crate) fn new(
        session_ctx: Arc<SessionContext>,
        file_manager: Arc<FileManager>,
        loader_registry: IngestFormatLoaderRegistry,
        listing_table_factory: Arc<ListingTableFactoryExt>,
        auth: Arc<beacon_auth::AuthContext>,
        identity: beacon_auth::AuthIdentity,
    ) -> Self {
        Self {
            session_ctx,
            file_manager,
            loader_registry,
            listing_table_factory,
            auth,
            identity,
        }
    }

    pub(crate) fn session_ctx(&self) -> Arc<SessionContext> {
        self.session_ctx.clone()
    }

    pub(crate) fn auth_context(&self) -> &beacon_auth::AuthContext {
        &self.auth
    }

    pub(crate) fn identity(&self) -> &beacon_auth::AuthIdentity {
        &self.identity
    }

    #[cfg(test)]
    pub(crate) fn file_manager(&self) -> Arc<FileManager> {
        self.file_manager.clone()
    }

    pub(crate) fn data_object_store_url(&self) -> ObjectStoreUrl {
        self.file_manager.data_object_store_url()
    }

    pub(crate) fn ingest_loader(&self, format: &str) -> Option<Arc<dyn IngestFormatLoader>> {
        self.loader_registry.get_loader(format)
    }

    pub(crate) fn listing_table_factory(&self) -> Arc<ListingTableFactoryExt> {
        self.listing_table_factory.clone()
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

    pub(crate) fn empty_record_batch_stream(&self) -> SendableRecordBatchStream {
        let stream = RecordBatchStreamAdapter::new(
            Schema::empty().into(),
            futures::stream::empty::<datafusion::error::Result<arrow::record_batch::RecordBatch>>(),
        );

        Box::pin(stream) as SendableRecordBatchStream
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_data_lake::FileManager;
    use beacon_datafusion_ext::listing_table_factory_ext::ListingTableFactoryExt;
    use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};
    use futures::StreamExt;

    use crate::statement_handlers::registry::IngestFormatLoaderRegistry;

    use super::HandlerContext;

    fn test_identity() -> beacon_auth::AuthIdentity {
        beacon_auth::AuthIdentity {
            username: "test".to_string(),
            roles: vec![],
            is_super_user: true,
        }
    }

    #[tokio::test]
    async fn handler_context_exposes_manager_references() {
        let session_ctx = Arc::new(SessionContext::new());
        let data_store_url =
            ObjectStoreUrl::parse("datasets://").expect("datasets url should parse");
        let file_manager = Arc::new(FileManager::new(
            session_ctx.clone(),
            data_store_url,
            vec![],
        ));

        let context = HandlerContext::new(
            session_ctx,
            file_manager.clone(),
            IngestFormatLoaderRegistry::new(),
            Arc::new(ListingTableFactoryExt::new(
                file_manager.data_object_store_url(),
            )),
            Arc::new(beacon_auth::AuthContext::new(Arc::new(
                beacon_auth::BasicAuthProvider::new(),
            ))),
            test_identity(),
        );

        assert!(Arc::ptr_eq(&context.file_manager(), &file_manager));
        assert_eq!(
            context.data_object_store_url().as_str(),
            file_manager.data_object_store_url().as_str()
        );
    }

    #[tokio::test]
    async fn empty_record_batch_stream_is_empty() {
        let session_ctx = Arc::new(SessionContext::new());
        let data_store_url =
            ObjectStoreUrl::parse("datasets://").expect("datasets url should parse");
        let file_manager = Arc::new(FileManager::new(
            session_ctx.clone(),
            data_store_url,
            vec![],
        ));
        let table_factory = Arc::new(ListingTableFactoryExt::new(
            file_manager.data_object_store_url(),
        ));

        let context = HandlerContext::new(
            session_ctx,
            file_manager,
            IngestFormatLoaderRegistry::new(),
            table_factory,
            Arc::new(beacon_auth::AuthContext::new(Arc::new(
                beacon_auth::BasicAuthProvider::new(),
            ))),
            test_identity(),
        );

        let mut stream = context.empty_record_batch_stream();
        assert!(stream.next().await.is_none());
    }
}
