//! Gives table code the stores of the Beacon host through the DataFusion session.
//!
//! `datafusion-ffi` 54.1 gives table code a session and a task context with a default
//! `RuntimeEnv`. The wrappers here put the stores of the host in that environment.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::common::config::TableOptions;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::registry::ExtensionTypeRegistryRef;
use datafusion::logical_expr::{
    AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};

use crate::host::{Host, HostApi, HostStores};

/// The host of one export of table types, and its stores for DataFusion code.
#[derive(Debug)]
pub(crate) struct HostBinding {
    host: Host,
    stores: Arc<dyn ObjectStoreRegistry>,
}

impl HostBinding {
    pub(crate) fn new(api: Arc<dyn HostApi>) -> Self {
        Self {
            stores: Arc::new(HostStores::new(Arc::clone(&api))),
            host: Host::new(api),
        }
    }

    /// `env` with the host object stores. The pools and caches of `env` stay.
    fn runtime_env(&self, env: &RuntimeEnv) -> Arc<RuntimeEnv> {
        let mut env = env.clone();
        env.object_store_registry = Arc::clone(&self.stores);
        Arc::new(env)
    }

    /// `context` with the host object stores.
    pub(crate) fn task_ctx(&self, context: &TaskContext) -> Arc<TaskContext> {
        Arc::new(TaskContext::new(
            context.task_id(),
            context.session_id(),
            context.session_config().clone(),
            context.scalar_functions().clone(),
            context.higher_order_functions().clone(),
            context.aggregate_functions().clone(),
            context.window_functions().clone(),
            self.runtime_env(&context.runtime_env()),
        ))
    }
}

/// The session that table code gets: `inner` with the host object stores.
pub(crate) struct HostSession<'a> {
    inner: &'a dyn Session,
    binding: &'a HostBinding,
    runtime_env: Arc<RuntimeEnv>,
    table_options: TableOptions,
}

impl<'a> HostSession<'a> {
    pub(crate) fn new(inner: &'a dyn Session, binding: &'a HostBinding) -> Self {
        Self {
            inner,
            binding,
            runtime_env: binding.runtime_env(inner.runtime_env()),
            table_options: inner.table_options().clone(),
        }
    }
}

#[async_trait]
impl Session for HostSession<'_> {
    fn session_id(&self) -> &str {
        self.inner.session_id()
    }

    fn config(&self) -> &SessionConfig {
        self.inner.config()
    }

    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.inner.create_physical_plan(logical_plan).await
    }

    fn create_physical_expr(
        &self,
        expr: Expr,
        df_schema: &DFSchema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.inner.create_physical_expr(expr, df_schema)
    }

    fn scalar_functions(&self) -> &HashMap<String, Arc<ScalarUDF>> {
        self.inner.scalar_functions()
    }

    fn higher_order_functions(&self) -> &HashMap<String, Arc<HigherOrderUDF>> {
        self.inner.higher_order_functions()
    }

    fn aggregate_functions(&self) -> &HashMap<String, Arc<AggregateUDF>> {
        self.inner.aggregate_functions()
    }

    fn window_functions(&self) -> &HashMap<String, Arc<WindowUDF>> {
        self.inner.window_functions()
    }

    fn extension_type_registry(&self) -> &ExtensionTypeRegistryRef {
        self.inner.extension_type_registry()
    }

    fn runtime_env(&self) -> &Arc<RuntimeEnv> {
        &self.runtime_env
    }

    fn execution_props(&self) -> &ExecutionProps {
        self.inner.execution_props()
    }

    /// Returns the [`Host`], for `Host::of`. A borrowed session cannot be an `Any`.
    fn as_any(&self) -> &dyn Any {
        &self.binding.host
    }

    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        self.binding.task_ctx(&self.inner.task_ctx())
    }
}
