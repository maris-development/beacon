//! The host value of a table type export: the Beacon object stores and the datasets URLs.
//!
//! `datafusion-ffi` 54.1 gives table code a session and a task context with a default
//! `RuntimeEnv`. The wrappers here replace its object store registry with the host registry.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::TableOptions;
use datafusion::common::{DFSchema, plan_err};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionConfig;
use datafusion::execution::object_store::{ObjectStoreRegistry, ObjectStoreUrl};
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::logical_expr::registry::ExtensionTypeRegistryRef;
use datafusion::logical_expr::{
    AggregateUDF, Expr, HigherOrderUDF, LogicalPlan, ScalarUDF, WindowUDF,
};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_ffi::ffi_option::FFI_Option;
use object_store::ObjectStore;
use stabby::string::String as SString;
use tokio::runtime::Handle;
use url::Url;

use super::location::{FFI_LocationResolver, LocationResolver, ResolvedLocation};
use crate::object_store::FFI_ObjectStoreRegistry;
use crate::{ABI_VERSION, sdk_abi_version};

/// What Beacon gives to the table types of a plugin, as FFI data.
#[repr(C)]
pub struct FFI_PluginHost {
    /// Returns [`crate::BEACON_PLUGIN_SDK_ABI`] of the host. It stays the first field.
    pub version: unsafe extern "C" fn() -> u64,
    /// The object stores of Beacon, read-only.
    pub registry: FFI_ObjectStoreRegistry,
    /// The rule of Beacon for the `LOCATION` text of a table.
    pub resolver: FFI_LocationResolver,
    /// The URL of the datasets store, such as `datasets://`. Empty without a datasets store.
    pub datasets_url: FFI_Option<SString>,
    /// The location of the datasets store, such as `file:///beacon/data/datasets/`.
    pub datasets_base_url: FFI_Option<SString>,
}

impl FFI_PluginHost {
    /// Exports `registry`, `resolver` and the datasets URLs. The store I/O runs inside `handle`.
    pub fn new(
        registry: Arc<dyn ObjectStoreRegistry>,
        handle: Handle,
        resolver: Arc<dyn LocationResolver>,
        datasets_url: Option<&ObjectStoreUrl>,
        datasets_base_url: Option<&Url>,
    ) -> Self {
        Self {
            version: sdk_abi_version,
            resolver: FFI_LocationResolver::new(resolver, handle.clone()),
            registry: FFI_ObjectStoreRegistry::new(registry, handle),
            datasets_url: datasets_url.map(|url| SString::from(url.as_str())).into(),
            datasets_base_url: datasets_base_url
                .map(|url| SString::from(url.as_str()))
                .into(),
        }
    }
}

/// The imported host value. Each plugin context owns one.
#[derive(Debug)]
pub(crate) struct PluginHost {
    registry: Arc<dyn ObjectStoreRegistry>,
    resolver: Arc<dyn LocationResolver>,
    pub(crate) datasets_url: Option<ObjectStoreUrl>,
    pub(crate) datasets_base_url: Option<Url>,
}

impl TryFrom<FFI_PluginHost> for PluginHost {
    type Error = DataFusionError;

    /// Fails when the host has another [`crate::BEACON_PLUGIN_SDK_ABI`].
    fn try_from(host: FFI_PluginHost) -> Result<Self> {
        // SAFETY: `version` has the same place in every host version.
        let version = unsafe { (host.version)() };
        if version != ABI_VERSION {
            return Err(DataFusionError::Plan(format!(
                "plugin host FFI version {version} is not supported (want {ABI_VERSION})"
            )));
        }
        let FFI_PluginHost {
            registry,
            resolver,
            datasets_url,
            datasets_base_url,
            ..
        } = host;
        let datasets_url = datasets_url
            .into_option()
            .map(|url| ObjectStoreUrl::parse(url.as_str()))
            .transpose()?;
        let datasets_base_url = datasets_base_url
            .into_option()
            .map(|url| {
                Url::parse(url.as_str()).map_err(|error| {
                    DataFusionError::Plan(format!("invalid datasets base URL `{url}`: {error}"))
                })
            })
            .transpose()?;
        Ok(Self {
            registry: Arc::<dyn ObjectStoreRegistry>::try_from(registry)?,
            resolver: Arc::<dyn LocationResolver>::try_from(resolver)?,
            datasets_url,
            datasets_base_url,
        })
    }
}

impl PluginHost {
    /// `env` with the host object stores. The pools and caches of `env` stay.
    fn runtime_env(&self, env: &RuntimeEnv) -> Arc<RuntimeEnv> {
        let mut env = env.clone();
        env.object_store_registry = Arc::clone(&self.registry);
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
    host: Arc<PluginHost>,
    runtime_env: Arc<RuntimeEnv>,
    table_options: TableOptions,
}

impl<'a> HostSession<'a> {
    pub(crate) fn new(inner: &'a dyn Session, host: &Arc<PluginHost>) -> Self {
        Self {
            inner,
            runtime_env: host.runtime_env(inner.runtime_env()),
            host: Arc::clone(host),
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

    /// Returns the host value. A borrowed session cannot be an `Any`.
    fn as_any(&self) -> &dyn Any {
        self.host.as_ref()
    }

    fn table_options(&self) -> &TableOptions {
        &self.table_options
    }

    fn table_options_mut(&mut self) -> &mut TableOptions {
        &mut self.table_options
    }

    fn task_ctx(&self) -> Arc<TaskContext> {
        self.host.task_ctx(&self.inner.task_ctx())
    }
}

fn host_of(state: &dyn Session) -> Option<&PluginHost> {
    state.as_any().downcast_ref::<PluginHost>()
}

/// The URL of the datasets store of Beacon, such as `datasets://`.
///
/// `state` is the session that the table code got. Returns `None` when Beacon has no
/// datasets store.
pub fn datasets_url(state: &dyn Session) -> Option<ObjectStoreUrl> {
    host_of(state)?.datasets_url.clone()
}

/// The location of the datasets store, such as `file:///beacon/data/datasets/` or
/// `s3://bucket/`. Use it for a reader that opens a path or URL by itself.
///
/// The object store of this URL is not the datasets store. Read the datasets store through
/// [`open_location`].
pub fn datasets_base_url(state: &dyn Session) -> Option<Url> {
    host_of(state)?.datasets_base_url.clone()
}

/// Asks Beacon for the object store and the path of the `LOCATION` text of a table.
///
/// Beacon owns the rule. A relative location is a path in the datasets store. A path of
/// the local machine has no store. A session that is not from Beacon gives no store.
pub fn resolve_location(state: &dyn Session, location: &str) -> Result<ResolvedLocation> {
    match host_of(state) {
        Some(host) => host.resolver.resolve(location),
        None => Ok(ResolvedLocation {
            store_url: None,
            path: location.to_string(),
        }),
    }
}

/// An open `LOCATION`: the object store and the path in it.
#[derive(Debug, Clone)]
pub struct OpenLocation {
    /// The object store of Beacon that holds the location.
    pub store: Arc<dyn ObjectStore>,
    /// The URL of the store.
    pub store_url: ObjectStoreUrl,
    /// The path in the store, with glob characters unchanged.
    pub path: String,
}

/// Opens the object store of the `LOCATION` text of a table.
///
/// Fails for a path of the local machine, which has no store.
pub fn open_location(state: &dyn Session, location: &str) -> Result<OpenLocation> {
    let found = resolve_location(state, location)?;
    let Some(store_url) = found.store_url else {
        return plan_err!("the location `{location}` is not in an object store");
    };
    Ok(OpenLocation {
        store: state.runtime_env().object_store(&store_url)?,
        store_url,
        path: found.path,
    })
}

/// The `LOCATION` text of a table as a DataFusion `ListingTableUrl`.
///
/// Use it for a `ListingTable`, or with `list_all_files` for a location with a glob.
pub fn listing_url(state: &dyn Session, location: &str) -> Result<ListingTableUrl> {
    let found = resolve_location(state, location)?;
    let Some(store_url) = found.store_url else {
        return ListingTableUrl::parse(&found.path);
    };
    // DataFusion reads a glob only in a path with no scheme, so the glob goes in apart.
    let (folder, glob) = match found.path.find(['?', '*', '[']) {
        Some(at) => {
            let split = found.path[..at].rfind('/').map_or(0, |slash| slash + 1);
            (&found.path[..split], Some(&found.path[split..]))
        }
        None => (found.path.as_str(), None),
    };
    // The store URL ends with `/`.
    let url = ListingTableUrl::parse(format!("{}{folder}", store_url.as_str()))?;
    match glob {
        Some(glob) => url.with_glob(glob),
        None => Ok(url),
    }
}

#[cfg(test)]
mod tests {
    use datafusion::prelude::SessionContext;
    use object_store::ObjectStoreExt;
    use object_store::path::Path;

    use super::*;
    use crate::table_type::tests::test_host;

    fn host() -> Arc<PluginHost> {
        Arc::new(PluginHost::try_from(test_host()).unwrap())
    }

    /// Runs `check` with a session that has the host of the tests.
    fn with_session<T>(check: impl FnOnce(&dyn Session) -> T) -> T {
        let host = host();
        let state = SessionContext::new().state();
        check(&HostSession::new(&state, &host))
    }

    #[test]
    fn resolve_location_asks_the_host() {
        let found = with_session(|state| resolve_location(state, "a/x.txt")).unwrap();
        assert_eq!(found.store_url.unwrap().as_str(), "datasets:///");
        assert_eq!(found.path, "a/x.txt");
        let error = with_session(|state| resolve_location(state, "bad/x")).unwrap_err();
        assert!(
            error.to_string().contains("the host refuses bad/x"),
            "{error}"
        );
    }

    #[test]
    fn the_urls_cross() {
        with_session(|state| {
            assert_eq!(datasets_url(state).unwrap().as_str(), "datasets:///");
            assert_eq!(
                datasets_base_url(state).unwrap().as_str(),
                "file:///beacon/data/datasets/"
            );
        });
    }

    #[test]
    fn a_session_of_another_kind_has_no_host() {
        let state = SessionContext::new().state();
        assert_eq!(datasets_url(&state), None);
        assert_eq!(datasets_base_url(&state), None);
        let found = resolve_location(&state, "a/b").unwrap();
        assert_eq!(found.store_url, None);
        assert_eq!(found.path, "a/b");
        assert!(open_location(&state, "a/b").is_err());
    }

    #[test]
    fn open_location_reads_the_host_store() {
        let open = with_session(|state| open_location(state, "a/x.txt")).unwrap();
        assert_eq!(open.store_url.as_str(), "datasets:///");
        let bytes = futures::executor::block_on(async {
            open.store.get(&Path::from(open.path)).await?.bytes().await
        })
        .unwrap();
        assert_eq!(bytes.as_ref(), b"hi");
    }

    #[test]
    fn open_location_refuses_a_local_path() {
        let error = with_session(|state| open_location(state, "/data/x")).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("the location `/data/x` is not in an object store"),
            "{error}"
        );
    }

    #[test]
    fn listing_url_keeps_the_store_and_the_glob() {
        let url = with_session(|state| listing_url(state, "a/*.txt")).unwrap();
        assert_eq!(url.object_store().as_str(), "datasets:///");
        assert_eq!(url.prefix().as_ref(), "a");
        let files: Vec<String> = with_session(|state| {
            let store = state
                .runtime_env()
                .object_store(url.object_store())
                .unwrap();
            futures::executor::block_on(async {
                use futures::TryStreamExt;
                let listed = url.list_all_files(state, store.as_ref(), "").await?;
                listed.try_collect::<Vec<_>>().await
            })
        })
        .unwrap()
        .into_iter()
        .map(|meta| meta.location.to_string())
        .collect();
        assert_eq!(files, vec!["a/x.txt".to_string()]);
    }

    #[test]
    fn listing_url_of_a_local_path_is_a_file_url() {
        let url = with_session(|state| listing_url(state, "/data/x.txt")).unwrap();
        assert_eq!(url.scheme(), "file");
    }
}
