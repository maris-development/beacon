use std::{collections::HashMap, sync::Arc};

use crate::statement_handlers::{
    payload::StatementKind,
    traits::{IngestFormatLoader, StatementHandler},
};

pub(crate) struct StatementRegistry {
    handlers: HashMap<StatementKind, Arc<dyn StatementHandler>>,
}

impl StatementRegistry {
    pub(crate) fn new() -> Self {
        Self {
            handlers: HashMap::new(),
        }
    }

    pub(crate) fn register_handler(&mut self, handler: Arc<dyn StatementHandler>) {
        self.handlers.insert(handler.kind(), handler);
    }

    pub(crate) fn get_handler(
        &self,
        kind: StatementKind,
    ) -> anyhow::Result<Arc<dyn StatementHandler>> {
        self.handlers
            .get(&kind)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("No handler registered for statement kind: {kind:?}"))
    }
}

pub(crate) struct IngestFormatLoaderRegistry {
    loaders: HashMap<String, Arc<dyn IngestFormatLoader>>,
}

impl IngestFormatLoaderRegistry {
    pub(crate) fn new() -> Self {
        Self {
            loaders: HashMap::new(),
        }
    }

    pub(crate) fn register_loader(&mut self, loader: Arc<dyn IngestFormatLoader>) {
        self.loaders
            .insert(loader.format_name().to_ascii_lowercase(), loader);
    }

    pub(crate) fn get_loader(&self, format: &str) -> Option<Arc<dyn IngestFormatLoader>> {
        self.loaders.get(&format.to_ascii_lowercase()).cloned()
    }
}