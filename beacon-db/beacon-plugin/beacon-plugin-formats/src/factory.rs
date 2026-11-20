//! The DataFusion file format factory of one plugin format.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use beacon_plugin_sdk::file_format::FormatFactory;
use datafusion::catalog::Session;
use datafusion::common::{GetExt, plan_err};
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::error::Result;

use crate::format::{ErrorReader, PluginFileFormat, not_installed};
use crate::options::plugin_options;

/// Turns one plugin [`FormatFactory`] into a DataFusion and Beacon format factory.
#[derive(Clone)]
pub struct PluginFileFormatFactory {
    factory: Arc<dyn FormatFactory>,
    /// The file extension of the formats that this factory makes.
    ext: String,
    /// Shared by all clones and formats: `false` after the plugin is uninstalled.
    installed: Arc<AtomicBool>,
}

impl std::fmt::Debug for PluginFileFormatFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginFileFormatFactory")
            .field("name", &self.name())
            .field("ext", &self.ext)
            .finish()
    }
}

impl PluginFileFormatFactory {
    /// Wraps `factory`. Its first extension is the file extension.
    pub fn new(factory: Arc<dyn FormatFactory>) -> Self {
        let ext = factory
            .extensions()
            .first()
            .cloned()
            .unwrap_or_else(|| factory.name().to_string())
            .to_ascii_lowercase();
        Self {
            factory,
            ext,
            installed: Arc::new(AtomicBool::new(true)),
        }
    }

    /// `false` after the plugin is uninstalled.
    pub fn is_installed(&self) -> bool {
        self.installed.load(Ordering::SeqCst)
    }

    /// Marks the format as uninstalled: every table on it fails from now on.
    pub fn mark_uninstalled(&self) {
        self.installed.store(false, Ordering::SeqCst);
    }

    /// The format name.
    pub fn name(&self) -> &str {
        self.factory.name()
    }

    /// The plugin factory.
    pub fn plugin(&self) -> &Arc<dyn FormatFactory> {
        &self.factory
    }

    /// The key the format answers to in `STORED AS`: the name, lower case.
    pub fn key(&self) -> String {
        self.name().to_ascii_lowercase()
    }

    fn format(&self, raw: &HashMap<String, String>) -> Result<PluginFileFormat> {
        if !self.is_installed() {
            return plan_err!("{}", not_installed(self.name()));
        }
        let (options, single_file_output) = plugin_options(raw)?;
        let reader = self
            .factory
            .create(&options)
            .map_err(|e| e.context(format!("file format {}", self.name())))?;
        Ok(PluginFileFormat {
            factory: Arc::clone(&self.factory),
            reader,
            ext: self.ext.clone(),
            options,
            single_file_output,
            // The file analyzer does not see plugin formats yet.
            statistics: false,
            installed: Arc::clone(&self.installed),
        })
    }
}

impl GetExt for PluginFileFormatFactory {
    /// DataFusion registers the factory under this key, so it is the name and not an extension.
    fn get_ext(&self) -> String {
        self.key()
    }
}

impl FileFormatFactory for PluginFileFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(self.format(format_options)?))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        match self.format(&HashMap::new()) {
            Ok(format) => Arc::new(format),
            Err(error) => Arc::new(PluginFileFormat {
                factory: Arc::clone(&self.factory),
                reader: Arc::new(ErrorReader(error.to_string())),
                ext: self.ext.clone(),
                options: HashMap::new(),
                single_file_output: None,
                statistics: false,
                installed: Arc::clone(&self.installed),
            }),
        }
    }
}
