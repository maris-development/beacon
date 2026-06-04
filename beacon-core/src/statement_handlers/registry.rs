use std::{collections::HashMap, sync::Arc};

use crate::statement_handlers::{payload::StatementKind, traits::StatementHandler};

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