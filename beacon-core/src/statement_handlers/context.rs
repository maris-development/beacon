use std::sync::Arc;

use beacon_data_lake::FileManager;
use beacon_datafusion_ext::listing_table_factory_ext::ListingTableFactoryExt;
use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

pub(crate) struct HandlerContext {
    session_ctx: Arc<SessionContext>,
    file_manager: Arc<FileManager>,
    listing_table_factory: Arc<ListingTableFactoryExt>,
}

impl HandlerContext {
    pub(crate) fn new(
        session_ctx: Arc<SessionContext>,
        file_manager: Arc<FileManager>,
        listing_table_factory: Arc<ListingTableFactoryExt>,
    ) -> Self {
        Self {
            session_ctx,
            file_manager,
            listing_table_factory,
        }
    }

    pub(crate) fn session_ctx(&self) -> Arc<SessionContext> {
        self.session_ctx.clone()
    }

    #[cfg(test)]
    pub(crate) fn file_manager(&self) -> Arc<FileManager> {
        self.file_manager.clone()
    }

    pub(crate) fn data_object_store_url(&self) -> ObjectStoreUrl {
        self.file_manager.data_object_store_url()
    }

    pub(crate) fn listing_table_factory(&self) -> Arc<ListingTableFactoryExt> {
        self.listing_table_factory.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use beacon_data_lake::FileManager;
    use beacon_datafusion_ext::listing_table_factory_ext::ListingTableFactoryExt;
    use datafusion::{execution::object_store::ObjectStoreUrl, prelude::SessionContext};

    use super::HandlerContext;

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

        let table_factory = Arc::new(ListingTableFactoryExt::new(
            file_manager.data_object_store_url(),
            Arc::downgrade(&session_ctx),
        ));
        let context = HandlerContext::new(session_ctx, file_manager.clone(), table_factory);

        assert!(Arc::ptr_eq(&context.file_manager(), &file_manager));
        assert_eq!(
            context.data_object_store_url().as_str(),
            file_manager.data_object_store_url().as_str()
        );
    }
}
