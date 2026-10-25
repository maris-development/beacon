//! Checks and registers plugins: at startup from the plugins directory, and at install.

use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use anyhow::Context as _;
use bytes::Bytes;
use datafusion::{
    execution::{context::SessionState, FunctionRegistry as _},
    logical_expr::ScalarUDF,
    prelude::SessionContext,
    sql::{
        parser::{DFParserBuilder, Statement as DFStatement},
        sqlparser::ast::Statement as SqlAstStatement,
    },
};

use super::{
    archive::{self, sha256_hex},
    beacon_version,
    manifest::valid_name,
    native::{library_file_name, load_native},
    registry::{ClaimGuard, PluginRegistry},
    source::{check_allowed, fetch_archive, PluginSource},
    store::{self, InstallRecord, Listing, TEMP_PREFIX},
    PluginManifest, MANIFEST_FILE,
};
use crate::statement_plan::functions::{
    as_macro, is_built_in, plan_plugin_sql, stored_function_names,
};

/// A plugin that passed every check and has registered nothing yet.
pub(crate) struct LoadedPlugin {
    manifest: PluginManifest,
    udfs: Vec<ScalarUDF>,
    /// Names and aliases of the native functions.
    native_names: Vec<String>,
}

/// The name of `udf` and its aliases.
fn names_of(udf: &ScalarUDF) -> Vec<String> {
    std::iter::once(udf.name().to_string())
        .chain(udf.aliases().iter().cloned())
        .collect()
}

fn read_manifest(dir: &Path) -> anyhow::Result<PluginManifest> {
    let text = std::fs::read_to_string(dir.join(MANIFEST_FILE))
        .with_context(|| format!("cannot read {MANIFEST_FILE}"))?;
    PluginManifest::parse(&text)
}

/// Refuses an empty, absolute or `..` path.
fn check_manifest_path(path: &str) -> anyhow::Result<()> {
    let safe = !path.is_empty()
        && Path::new(path)
            .components()
            .all(|component| matches!(component, Component::Normal(_)));
    anyhow::ensure!(safe, "unsafe path in manifest: '{path}'");
    Ok(())
}

fn check_collision(
    state: &SessionState,
    reserved: &HashSet<String>,
    registry: &PluginRegistry,
    plugin: &str,
    function: &str,
    seen: &HashSet<String>,
) -> anyhow::Result<()> {
    let clash = if seen.contains(function) {
        format!("another function of plugin '{plugin}'")
    } else if let Some(owner) = registry.owner_of(function) {
        format!("a function of plugin '{owner}'")
    } else if reserved.contains(function) {
        "a user function".to_string()
    } else if is_built_in(state, function) {
        "a built-in function".to_string()
    } else if state.scalar_functions().contains_key(function) {
        "a user function".to_string()
    } else {
        return Ok(());
    };
    anyhow::bail!("function '{function}' of plugin '{plugin}' collides with {clash}")
}

/// Runs every check on the unpacked plugin in `dir`. Registers nothing.
pub(crate) fn prepare(
    dir: &Path,
    expected_name: &str,
    session: &SessionContext,
    reserved: &HashSet<String>,
    registry: &PluginRegistry,
) -> anyhow::Result<LoadedPlugin> {
    let manifest = read_manifest(dir)?;
    anyhow::ensure!(
        manifest.name == expected_name,
        "the manifest name '{}' does not match the plugin name '{expected_name}'",
        manifest.name
    );
    manifest.check_beacon(&beacon_version())?;
    if let Some(native) = &manifest.native {
        check_manifest_path(&native.library)?;
    }
    let files = manifest
        .sql
        .as_ref()
        .map(|sql| sql.files.clone())
        .unwrap_or_default();
    for file in &files {
        check_manifest_path(file)?;
    }

    let state = session.state();
    // Plans see built-ins and earlier functions of this plugin, the same at install and startup.
    let mut scratch = state.clone();
    let hidden: Vec<String> = state
        .scalar_functions()
        .iter()
        .filter(|(_, udf)| as_macro(udf).is_some())
        .map(|(name, _)| name.clone())
        .chain(
            registry
                .snapshot()
                .into_iter()
                .flat_map(|entry| entry.functions),
        )
        .collect();
    for function in &hidden {
        scratch.deregister_udf(function)?;
    }
    let mut seen = HashSet::new();
    let mut udfs = Vec::new();
    let mut native_names = Vec::new();
    // Native functions first, so the plugin SQL can call them.
    if let Some(native) = &manifest.native {
        let path = dir.join("lib").join(library_file_name(&native.library));
        for udf in load_native(&path)? {
            let names = names_of(&udf);
            for name in &names {
                check_collision(&state, reserved, registry, expected_name, name, &seen)?;
                seen.insert(name.clone());
            }
            scratch.register_udf(Arc::new(udf.clone()))?;
            native_names.extend(names);
            udfs.push(udf);
        }
    }
    for file in &files {
        let text = std::fs::read_to_string(dir.join(file))
            .with_context(|| format!("cannot read SQL file '{file}'"))?;
        let statements = DFParserBuilder::new(text.as_str())
            .build()?
            .parse_statements()
            .with_context(|| format!("cannot parse SQL file '{file}'"))?;
        for statement in statements {
            let DFStatement::Statement(inner) = statement else {
                anyhow::bail!("plugin SQL files may hold only CREATE FUNCTION");
            };
            let SqlAstStatement::CreateFunction(create) = &*inner else {
                anyhow::bail!("plugin SQL files may hold only CREATE FUNCTION");
            };
            anyhow::ensure!(
                !create.or_replace,
                "plugin SQL files may hold only CREATE FUNCTION"
            );
            let definition = inner.to_string();
            let udf = plan_plugin_sql(&scratch, &definition)
                .with_context(|| format!("in SQL file '{file}'"))?;
            for name in names_of(&udf) {
                check_collision(&state, reserved, registry, expected_name, &name, &seen)?;
                seen.insert(name);
            }
            scratch.register_udf(Arc::new(udf.clone()))?;
            udfs.push(udf);
        }
    }
    Ok(LoadedPlugin {
        manifest,
        udfs,
        native_names,
    })
}

impl LoadedPlugin {
    /// The primary function names, as `beacon.system.plugins` lists them.
    fn function_names(&self) -> Vec<String> {
        self.udfs.iter().map(|udf| udf.name().to_string()).collect()
    }

    /// Every name the plugin owns, aliases included.
    fn claimed_names(&self) -> Vec<String> {
        self.udfs.iter().flat_map(names_of).collect()
    }
}

/// Claims the function names of `plugin` in the registry, then checks the session again.
/// The claim lasts until the returned guard drops or is disarmed.
fn claim<'a>(
    session: &SessionContext,
    registry: &'a PluginRegistry,
    plugin: &LoadedPlugin,
) -> anyhow::Result<ClaimGuard<'a>> {
    let names = plugin.claimed_names();
    registry.try_claim(&plugin.manifest.name, &names)?;
    let guard = registry.claim_guard(&plugin.manifest.name, names.clone());
    // A user function made after `prepare` must still win.
    let state = session.state();
    if let Some(name) = names
        .iter()
        .find(|name| state.scalar_functions().contains_key(*name) || is_built_in(&state, name))
    {
        anyhow::bail!(
            "function '{name}' of plugin '{}' collides with an existing function",
            plugin.manifest.name
        );
    }
    Ok(guard)
}

/// Registers the claimed functions and records the plugin as loaded.
pub(crate) fn commit(
    session: &SessionContext,
    registry: &PluginRegistry,
    record: InstallRecord,
    plugin: LoadedPlugin,
) {
    let names = plugin.function_names();
    // The federation rule must know a native function before a query can call it.
    registry.add_native_functions(plugin.native_names);
    for udf in plugin.udfs {
        session.register_udf(udf);
    }
    tracing::info!(
        "Loaded plugin '{}' {} with functions {names:?}",
        record.name,
        record.version
    );
    registry.add_loaded(record, plugin.manifest.kind(), names);
}

/// A new temp folder path under `plugins_dir`. Startup deletes what is left of it.
fn temp_path(plugins_dir: &Path) -> PathBuf {
    plugins_dir.join(format!("{TEMP_PREFIX}{}", uuid::Uuid::new_v4().simple()))
}

/// Unpacks into a new, empty temp folder under `plugins_dir`.
fn unpack_fresh(archive: &[u8], plugins_dir: &Path) -> anyhow::Result<PathBuf> {
    std::fs::create_dir_all(plugins_dir)
        .with_context(|| format!("cannot create {}", plugins_dir.display()))?;
    let temp = temp_path(plugins_dir);
    std::fs::create_dir(&temp)?;
    if let Err(error) = archive::unpack(archive, &temp) {
        let _ = std::fs::remove_dir_all(&temp);
        return Err(error);
    }
    Ok(temp)
}

/// Renames the checked `temp` folder to `<plugins_dir>/<name>/`. Never replaces a folder.
fn move_into_place(plugins_dir: &Path, name: &str, temp: &Path) -> anyhow::Result<()> {
    let dir = plugins_dir.join(name);
    if dir.exists() {
        anyhow::bail!(
            "the plugin folder {} already exists; delete it before you install '{name}'",
            dir.display()
        );
    }
    std::fs::rename(temp, &dir)
        .with_context(|| format!("cannot move the plugin to {}", dir.display()))?;
    sync_dir(plugins_dir)
}

/// Flushes every file under `dir` to disk, then `dir` itself.
fn sync_tree(dir: &Path) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            sync_tree(&path)?;
        } else {
            sync_file(&path)?;
        }
    }
    sync_dir(dir)
}

/// Flushes the file at `path` to disk.
fn sync_file(path: &Path) -> anyhow::Result<()> {
    // Windows flushes only a handle with write access.
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("cannot sync {}", path.display()))
}

/// Flushes the entries of `dir` to disk. Std cannot open a directory for this on Windows.
fn sync_dir(dir: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    std::fs::File::open(dir)
        .and_then(|file| file.sync_all())
        .with_context(|| format!("cannot sync {}", dir.display()))?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

/// Why an install failed.
#[derive(Debug)]
pub(crate) enum InstallError {
    /// Beacon did not read the archive: a bad or refused source, or a failed download.
    Fetch(anyhow::Error),
    /// Beacon read the archive, then the unpack, a check or the folder write failed.
    Check(anyhow::Error),
}

impl InstallError {
    pub(crate) fn into_inner(self) -> anyhow::Error {
        match self {
            InstallError::Fetch(error) | InstallError::Check(error) => error,
        }
    }
}

/// Reads the archive of `source_text`, then checks, stores and registers the plugin.
pub(crate) async fn install(
    session: &SessionContext,
    registry: &PluginRegistry,
    plugins_dir: &Path,
    name: &str,
    source_text: &str,
) -> anyhow::Result<()> {
    install_with_kind(session, registry, plugins_dir, name, source_text)
        .await
        .map_err(InstallError::into_inner)
}

/// [`install`], with the kind of failure.
pub(crate) async fn install_with_kind(
    session: &SessionContext,
    registry: &PluginRegistry,
    plugins_dir: &Path,
    name: &str,
    source_text: &str,
) -> Result<(), InstallError> {
    let fetched = async {
        anyhow::ensure!(valid_name(name), "invalid plugin name '{name}'");
        let source = PluginSource::parse(source_text)?;
        check_allowed(source_text, registry.allowed_sources())?;
        let guard = registry.begin_install(name)?;
        let (archive, sha256) = fetch_archive(&source, name).await?;
        Ok((guard, archive, sha256))
    };
    let (_guard, archive, sha256) = fetched.await.map_err(InstallError::Fetch)?;
    install_locked(
        session,
        registry,
        plugins_dir,
        name,
        archive,
        sha256,
        source_text,
    )
    .await
    .map_err(InstallError::Check)
}

/// Checks, stores and registers a plugin archive. The name must be free.
pub(crate) async fn install_archive(
    session: &SessionContext,
    registry: &PluginRegistry,
    plugins_dir: &Path,
    name: &str,
    archive: Bytes,
    source: &str,
) -> anyhow::Result<()> {
    anyhow::ensure!(valid_name(name), "invalid plugin name '{name}'");
    let _guard = registry.begin_install(name)?;
    let sha256 = sha256_hex(&archive);
    install_locked(
        session,
        registry,
        plugins_dir,
        name,
        archive,
        sha256,
        source,
    )
    .await
}

/// The install core. The caller holds the install guard of `name`.
async fn install_locked(
    session: &SessionContext,
    registry: &PluginRegistry,
    plugins_dir: &Path,
    name: &str,
    archive: Bytes,
    sha256: String,
    source: &str,
) -> anyhow::Result<()> {
    let temp = unpack_fresh(&archive, plugins_dir)?;
    let result = async {
        // A crash after the rename must not leave a half-written plugin folder.
        // The sync runs before the check: Windows cannot open a loaded library for write.
        sync_tree(&temp)?;
        let reserved = stored_function_names(session).await;
        let plugin = prepare(&temp, name, session, &reserved, registry)?;
        let record = InstallRecord {
            name: name.to_string(),
            version: plugin.manifest.version.to_string(),
            source: source.to_string(),
            sha256,
            installed_at: chrono::Utc::now().to_rfc3339(),
        };
        // A cancelled query drops this future; the guard then releases the claim.
        let claimed = claim(session, registry, &plugin)?;
        store::write_record(&temp, &record)?;
        sync_file(&temp.join(store::RECORD_FILE))?;
        sync_dir(&temp)?;
        // The rename commits the install on disk.
        move_into_place(plugins_dir, name, &temp)?;
        claimed.disarm();
        commit(session, registry, record, plugin);
        Ok(())
    }
    .await;
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&temp);
    }
    result
}

/// Deregisters a loaded or failed plugin and deletes its folder.
pub(crate) async fn uninstall(
    session: &SessionContext,
    registry: &PluginRegistry,
    plugins_dir: &Path,
    name: &str,
) -> anyhow::Result<()> {
    let _guard = registry.begin_uninstall(name)?;
    let dir = plugins_dir.join(name);
    // A temp name first: startup deletes what a failed delete leaves.
    let trash = temp_path(plugins_dir);
    let moved = match std::fs::rename(&dir, &trash) {
        Ok(()) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => {
            return Err(error)
                .with_context(|| format!("failed to delete the plugin folder {}", dir.display()))
        }
    };
    let names = registry.owned_names(name);
    for function in &names {
        session.deregister_udf(function);
    }
    registry.remove_native_functions(&names);
    registry.release_claim(name, &names);
    registry.set_uninstalled(name);
    // On Windows a loaded native library blocks the delete until the restart.
    if moved {
        if let Err(error) = std::fs::remove_dir_all(&trash) {
            tracing::warn!(
                "Cannot delete {} of plugin '{name}': {error}",
                trash.display()
            );
        }
    }
    tracing::info!("Uninstalled plugin '{name}' with functions {names:?}");
    Ok(())
}

/// Deletes the temp folders of unfinished installs and uninstalls.
fn clean_temp_dirs(plugins_dir: &Path) {
    let Ok(entries) = std::fs::read_dir(plugins_dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let is_dir = entry.file_type().is_ok_and(|kind| kind.is_dir());
        if is_dir && name.starts_with(TEMP_PREFIX) {
            if let Err(error) = std::fs::remove_dir_all(entry.path()) {
                tracing::warn!("Cannot delete {}: {error}", entry.path().display());
            }
        }
    }
}

/// Checks the plugin folder of `record` and claims its functions.
fn load_one(
    session: &SessionContext,
    registry: &PluginRegistry,
    dir: &Path,
    reserved: &HashSet<String>,
    record: &InstallRecord,
) -> anyhow::Result<LoadedPlugin> {
    let plugin = prepare(dir, &record.name, session, reserved, registry)?;
    // The caller commits at once, with no await in between.
    claim(session, registry, &plugin)?.disarm();
    Ok(plugin)
}

/// Loads every plugin folder in name order. A failed plugin is recorded and logged.
pub(crate) async fn load_installed(
    session: &SessionContext,
    registry: &PluginRegistry,
    plugins_dir: &Path,
) {
    clean_temp_dirs(plugins_dir);
    let Listing { records, broken } = match store::list(plugins_dir) {
        Ok(listing) => listing,
        Err(error) => {
            tracing::error!("Failed to load plugins: {error:#}");
            return;
        }
    };
    let kind_of = |name: &str| {
        read_manifest(&plugins_dir.join(name))
            .ok()
            .map(|manifest| manifest.kind())
    };
    for (name, reason) in broken {
        let kind = kind_of(&name);
        registry.add_failed(name, None, kind, reason);
    }
    // A stored user function wins over a plugin function.
    let reserved = stored_function_names(session).await;
    for record in records {
        let dir = plugins_dir.join(&record.name);
        match load_one(session, registry, &dir, &reserved, &record) {
            Ok(plugin) => commit(session, registry, record, plugin),
            Err(error) => {
                let reason = format!("{error:#}");
                tracing::error!("Failed to load plugin '{}': {reason}", record.name);
                let kind = kind_of(&record.name);
                registry.add_failed(record.name.clone(), Some(record), kind, reason);
            }
        }
    }
}
