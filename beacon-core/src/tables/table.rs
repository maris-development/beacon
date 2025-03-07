use std::{fmt::Debug, sync::Arc};

use datafusion::{catalog::TableProvider, execution::SessionState, prelude::SessionContext};

#[async_trait::async_trait]
#[typetag::serde]
pub trait BeaconTable: Debug + Send + Sync {
    fn table_name(&self) -> &str;
    async fn as_table(&self, session_state: Arc<SessionState>) -> Arc<dyn TableProvider>;

    async fn rebuild(&self, session_state: Arc<SessionContext>) -> anyhow::Result<()> {
        Ok(())
    }
}
