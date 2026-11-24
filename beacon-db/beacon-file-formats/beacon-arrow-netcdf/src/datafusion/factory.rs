//! The NetCDF format factory: it turns the options of a table into a [`NetcdfFormat`].

use std::collections::HashMap;
use std::sync::Arc;

use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::format_options::{format_option, parse_bool_option};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::catalog::Session;
use datafusion::common::GetExt;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::Result;
use object_store::ObjectMeta;

use super::options::NetcdfOptions;
use super::{NetcdfFormat, NETCDF_EXTENSION, NETCDF_EXTENSIONS};

/// Runtime configuration for the NetCDF format.
///
/// The host fills it in. These are the defaults of a runtime; a table can
/// override them with `CREATE EXTERNAL TABLE ... OPTIONS (...)`.
#[derive(Debug, Clone)]
pub struct NetcdfConfig {
    /// Whether the file analyzer computes file statistics.
    pub enable_statistics: bool,
}

impl Default for NetcdfConfig {
    fn default() -> Self {
        Self {
            enable_statistics: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetCDFFormatFactory {
    pub listing_factory: Arc<ListingFactory>,
    pub options: NetcdfOptions,
    pub config: NetcdfConfig,
}

impl NetCDFFormatFactory {
    pub fn new(
        listing_factory: Arc<ListingFactory>,
        options: NetcdfOptions,
        config: NetcdfConfig,
    ) -> Self {
        Self {
            listing_factory,
            options,
            config,
        }
    }

    /// Whether this table wants file statistics at all.
    ///
    /// Only the file analyzer computes them, through
    /// [`FileFormatFactoryExt::create_for_analysis`]. This switch turns even
    /// that off, per table or per runtime.
    fn statistics_wanted(&self, format_options: &HashMap<String, String>) -> Result<bool> {
        match format_option(format_options, "enable_statistics") {
            Some(value) => parse_bool_option("enable_statistics", value),
            None => Ok(self.config.enable_statistics),
        }
    }

    /// The format of a table with `format_options`. It computes no statistics.
    fn format(&self, format_options: &HashMap<String, String>) -> Result<NetcdfFormat> {
        let mut options = self.options.clone();
        options.apply_format_options(format_options)?;
        // Checked here, so a bad value fails at `CREATE EXTERNAL TABLE` and not at the first analysis.
        self.statistics_wanted(format_options)?;
        Ok(NetcdfFormat::new(self.listing_factory.clone(), options))
    }
}

impl FileFormatFactory for NetCDFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(self.format(format_options)?))
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(NetcdfFormat::new(
            self.listing_factory.clone(),
            self.options.clone(),
        ))
    }
}

impl GetExt for NetCDFFormatFactory {
    fn get_ext(&self) -> String {
        NETCDF_EXTENSION.to_string()
    }
}

impl FileFormatFactoryExt for NetCDFFormatFactory {
    /// The same format, with statistics on when the table wants them.
    ///
    /// `infer_stats` opens the file and reads every coordinate array, so only
    /// the file analyzer asks for this.
    fn create_for_analysis(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
        _url: &ListingTableUrl,
        _listing: &ListingFactory,
    ) -> Result<Arc<dyn FileFormat>> {
        let wanted = self.statistics_wanted(format_options)?;
        Ok(Arc::new(
            self.format(format_options)?.with_enable_statistics(wanted),
        ))
    }

    fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location.extension().is_some_and(|ext| {
                    NETCDF_EXTENSIONS
                        .iter()
                        .any(|known| ext.eq_ignore_ascii_case(known))
                })
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }

    fn file_extensions(&self) -> Vec<String> {
        NETCDF_EXTENSIONS.map(String::from).to_vec()
    }

    /// Every spelling reads the same way, so the crawler can build a table on each.
    fn crawlable_extensions(&self) -> Vec<String> {
        self.file_extensions()
    }

    /// netCDF opts into the schema cache.
    ///
    /// The key holds the name of the reader, so the entries of a different
    /// reader are never an answer for this one.
    ///
    /// `skip_invalid_files` is in the key when it is set, because it turns an
    /// unreadable file into an empty schema. A table without it must still get
    /// the error. Without the option the key is the same as for older entries.
    ///
    /// Nothing else is. The statistics switch only decides whether ranges are
    /// computed. It changes no field and no type.
    ///
    /// TODO(#367): cache a dimension-projected read as well. `read_dimensions`
    /// decides which variables appear, so the same file has one schema per
    /// dimension set, and the key would have to carry the set in order. It is
    /// left out of this first pass to keep the key simple: a `read_netcdf` that
    /// names dimensions derives its schema, exactly as it did before the cache
    /// existed. The default read — every table, and every collector pass — is
    /// what the 9.2s was measured on, and that one is cached.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        let format = format.downcast_ref::<NetcdfFormat>()?;
        if format.options.read_dimensions.is_some() {
            return None;
        }
        let options = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf");
        let options = if format.options.skip_invalid_files {
            options.str("skip_invalid_files")
        } else {
            options
        };
        Some(options.finish())
    }
}
