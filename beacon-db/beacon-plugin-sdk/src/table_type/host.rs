//! The host value of a table type export: the Beacon object stores and the datasets URLs.
//!
//! `datafusion-ffi` 54.1 gives table code a session and a task context with a default
//! `RuntimeEnv`. The wrappers here replace its object store registry with the host registry.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::DFSchema;
use datafusion::common::config::TableOptions;
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
use stabby::string::String as SString;
use tokio::runtime::Handle;
use url::Url;

use crate::object_store::FFI_ObjectStoreRegistry;
use crate::{ABI_VERSION, sdk_abi_version};

/// What Beacon gives to the table types of a plugin, as FFI data.
#[repr(C)]
pub struct FFI_PluginHost {
    /// Returns [`crate::BEACON_PLUGIN_SDK_ABI`] of the host. It stays the first field.
    pub version: unsafe extern "C" fn() -> u64,
    /// The object stores of Beacon, read-only.
    pub registry: FFI_ObjectStoreRegistry,
    /// The URL of the datasets store, such as `datasets://`. Empty without a datasets store.
    pub datasets_url: FFI_Option<SString>,
    /// The location of the datasets store, such as `file:///beacon/data/datasets/`.
    pub datasets_base_url: FFI_Option<SString>,
}

impl FFI_PluginHost {
    /// Exports `registry` and the datasets URLs. The store I/O runs inside `handle`.
    pub fn new(
        registry: Arc<dyn ObjectStoreRegistry>,
        handle: Handle,
        datasets_url: Option<&ObjectStoreUrl>,
        datasets_base_url: Option<&Url>,
    ) -> Self {
        Self {
            version: sdk_abi_version,
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
/// [`datasets_url`].
pub fn datasets_base_url(state: &dyn Session) -> Option<Url> {
    host_of(state)?.datasets_base_url.clone()
}

/// A `LOCATION` as an object store and a path in that store.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLocation {
    /// The store, for `runtime_env().object_store`. `None` for a path of the local machine.
    pub store_url: Option<ObjectStoreUrl>,
    /// The path in the store, with glob characters unchanged. No leading `/` in a store.
    pub path: String,
}

/// True for `/x`, `\x`, `C:\x` and `C:/x`.
fn is_absolute_path(location: &str) -> bool {
    let bytes = location.as_bytes();
    location.starts_with(['/', '\\'])
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'/' | b'\\'))
}

/// Splits `scheme://rest`. A text with no valid scheme gives `None`.
fn split_scheme(location: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = location.split_once("://")?;
    // One letter is a Windows drive.
    let valid = scheme.len() > 1
        && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    valid.then_some((scheme, rest))
}

/// Resolves the `LOCATION` of a table against the object stores of Beacon.
///
/// - A relative location is a path in the datasets store.
/// - `datasets://a/b` and `datasets:///a/b` are the path `a/b` in the datasets store.
/// - A location with another scheme gives the store `scheme://authority` and its path.
///   The path keeps a query string and percent codes as text.
/// - An absolute local path stays, with no store.
/// - Without a datasets store, a relative location stays, with no store.
pub fn resolve_location(state: &dyn Session, location: &str) -> Result<ResolvedLocation> {
    let datasets = datasets_url(state);
    if let Some((scheme, rest)) = split_scheme(location) {
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let datasets_authority = datasets
            .as_ref()
            .and_then(|url| split_scheme(url.as_str()))
            .filter(|(found, _)| found.eq_ignore_ascii_case(scheme))
            .map(|(_, rest)| rest.trim_end_matches('/'));
        let in_datasets = match datasets_authority {
            // The registry keys a store by scheme and authority, so the first segment is path.
            Some("") => Some(rest.trim_start_matches('/')),
            Some(found) if found.eq_ignore_ascii_case(authority) => Some(path),
            _ => None,
        };
        if let Some(path) = in_datasets {
            return Ok(ResolvedLocation {
                store_url: datasets,
                path: path.to_string(),
            });
        }
        return Ok(ResolvedLocation {
            store_url: Some(ObjectStoreUrl::parse(format!("{scheme}://{authority}"))?),
            path: path.to_string(),
        });
    }
    let store_url = match is_absolute_path(location) {
        true => None,
        false => datasets,
    };
    Ok(ResolvedLocation {
        store_url,
        path: location.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use datafusion::execution::object_store::DefaultObjectStoreRegistry;
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::table_type::tests::{test_handle, test_host};

    fn host() -> Arc<PluginHost> {
        Arc::new(PluginHost::try_from(test_host()).unwrap())
    }

    /// A host of a Beacon without a datasets store.
    fn bare_host() -> Arc<PluginHost> {
        let registry = Arc::new(DefaultObjectStoreRegistry::new());
        let ffi = FFI_PluginHost::new(registry, test_handle(), None, None);
        Arc::new(PluginHost::try_from(ffi).unwrap())
    }

    fn resolved(host: &Arc<PluginHost>, location: &str) -> (Option<String>, String) {
        let state = SessionContext::new().state();
        let session = HostSession::new(&state, host);
        let found = resolve_location(&session, location).unwrap();
        (
            found.store_url.map(|url| url.as_str().to_string()),
            found.path,
        )
    }

    #[test]
    fn resolve_location_cases() {
        let host = host();
        let datasets = Some("datasets:///".to_string());
        for (location, store, path) in [
            (
                "example-shards/part_*.lance",
                datasets.clone(),
                "example-shards/part_*.lance",
            ),
            (
                "datasets://example-shards/part_*.lance",
                datasets.clone(),
                "example-shards/part_*.lance",
            ),
            ("datasets:///a/b", datasets.clone(), "a/b"),
            (
                "s3://bucket/a/b.parquet",
                Some("s3://bucket/".to_string()),
                "a/b.parquet",
            ),
            ("s3://bucket", Some("s3://bucket/".to_string()), ""),
            ("/beacon/data/x.lance", None, "/beacon/data/x.lance"),
            (r"C:\data\x.lance", None, r"C:\data\x.lance"),
            ("C:/data/x.lance", None, "C:/data/x.lance"),
            (
                "file:///beacon/data/x",
                Some("file:///".to_string()),
                "beacon/data/x",
            ),
        ] {
            assert_eq!(
                resolved(&host, location),
                (store, path.to_string()),
                "{location}"
            );
        }
    }

    /// A host whose datasets store has the URL `url`.
    fn host_at(url: &str) -> Arc<PluginHost> {
        let registry = Arc::new(DefaultObjectStoreRegistry::new());
        let url = ObjectStoreUrl::parse(url).unwrap();
        let ffi = FFI_PluginHost::new(registry, test_handle(), Some(&url), None);
        Arc::new(PluginHost::try_from(ffi).unwrap())
    }

    #[test]
    fn a_datasets_store_with_an_authority_matches_only_that_authority() {
        let host = host_at("s3://bucket");
        let datasets = Some("s3://bucket/".to_string());
        assert_eq!(
            resolved(&host, "obs/a"),
            (datasets.clone(), "obs/a".to_string())
        );
        assert_eq!(
            resolved(&host, "s3://bucket/obs/a"),
            (datasets, "obs/a".to_string())
        );
        assert_eq!(
            resolved(&host, "s3://other/obs/a"),
            (Some("s3://other/".to_string()), "obs/a".to_string())
        );
    }

    #[test]
    fn the_scheme_matches_without_case() {
        let host = host();
        assert_eq!(
            resolved(&host, "DATASETS://obs/a"),
            (Some("datasets:///".to_string()), "obs/a".to_string())
        );
    }

    #[test]
    fn a_one_letter_scheme_is_a_local_path() {
        let host = host();
        assert_eq!(
            resolved(&host, "C://data/x"),
            (None, "C://data/x".to_string())
        );
    }

    #[test]
    fn relative_location_stays_without_a_default_store() {
        let host = bare_host();
        assert_eq!(resolved(&host, "a/b"), (None, "a/b".to_string()));
        let state = SessionContext::new().state();
        let session = HostSession::new(&state, &host);
        assert_eq!(datasets_url(&session), None);
        assert_eq!(datasets_base_url(&session), None);
    }

    #[test]
    fn the_urls_cross() {
        let host = host();
        let state = SessionContext::new().state();
        let session = HostSession::new(&state, &host);
        assert_eq!(datasets_url(&session).unwrap().as_str(), "datasets:///");
        assert_eq!(
            datasets_base_url(&session).unwrap().as_str(),
            "file:///beacon/data/datasets/"
        );
    }

    #[test]
    fn a_session_of_another_kind_has_no_host() {
        let state = SessionContext::new().state();
        assert_eq!(datasets_url(&state), None);
        assert_eq!(datasets_base_url(&state), None);
        let found = resolve_location(&state, "a/b").unwrap();
        assert_eq!(found.store_url, None);
        assert_eq!(found.path, "a/b");
    }

    #[test]
    fn a_resolved_location_reads_through_the_session() {
        let host = host();
        let state = SessionContext::new().state();
        let session = HostSession::new(&state, &host);
        let found = resolve_location(&session, "datasets://a/x.txt").unwrap();
        let store = session
            .runtime_env()
            .object_store(found.store_url.unwrap())
            .unwrap();
        let path = object_store::path::Path::from(found.path);
        let bytes = futures::executor::block_on(async {
            use object_store::ObjectStoreExt;
            store.get(&path).await?.bytes().await
        })
        .unwrap();
        assert_eq!(bytes.as_ref(), b"hi");
    }
}
