use std::{fmt::Debug, sync::Arc};

use datafusion::{catalog::TableProvider, prelude::SessionContext};
use serde::{Deserialize, Serialize};

use crate::engine::IndexUpdate;

pub mod glob;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    name: String,
    provider: Arc<dyn IndexProvider>,
}

#[typetag::serde]
pub trait IndexProvider: Debug {
    fn as_table(&self, session_ctx: SessionContext) -> Arc<dyn TableProvider>;
    fn update(&self, update: IndexUpdate) -> anyhow::Result<()>;
}
