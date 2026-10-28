//! The DataFusion file format factory of one plugin format.

use std::collections::HashMap;
use std::sync::Arc;

use beacon_plugin_sdk::file_format::FormatFactory;
use datafusion::catalog::Session;
use datafusion::common::GetExt;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::error::Result;

use crate::format::{ErrorReader, PluginFileFormat};
use crate::options::plugin_options;

/// Turns one plugin [`FormatFactory`] into a DataFusion and Beacon format factory.
#[derive(Clone)]
pub struct PluginFileFormatFactory {
    factory: Arc<dyn FormatFactory>,
    /// The key that DataFusion registers this factory under.
    ext: String,
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
    /// Wraps `factory`, under its first extension.
    pub fn new(factory: Arc<dyn FormatFactory>) -> Self {
        let ext = factory
            .extensions()
            .first()
            .cloned()
            .unwrap_or_else(|| factory.name().to_string())
            .to_ascii_lowercase();
        Self { factory, ext }
    }

    /// The same factory, registered under `key`.
    pub fn with_ext(&self, key: &str) -> Self {
        Self {
            ext: key.to_ascii_lowercase(),
            ..self.clone()
        }
    }

    /// The format name.
    pub fn name(&self) -> &str {
        self.factory.name()
    }

    /// The plugin factory.
    pub fn plugin(&self) -> &Arc<dyn FormatFactory> {
        &self.factory
    }

    /// The keys the format answers to: the name, then each extension, lower case.
    pub fn keys(&self) -> Vec<String> {
        let mut keys = vec![self.name().to_ascii_lowercase()];
        keys.extend(
            self.factory
                .extensions()
                .iter()
                .map(|ext| ext.to_ascii_lowercase()),
        );
        keys
    }

    fn format(&self, raw: &HashMap<String, String>) -> Result<PluginFileFormat> {
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
        })
    }
}

impl GetExt for PluginFileFormatFactory {
    fn get_ext(&self) -> String {
        self.ext.clone()
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
            }),
        }
    }
}
