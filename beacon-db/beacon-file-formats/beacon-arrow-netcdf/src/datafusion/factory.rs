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

#[cfg(test)]
mod tests {
    use object_store::path::Path;

    use super::*;
    use crate::attributes::CombineAttrs;
    use crate::datafusion::test_util::one_partition_session;

    fn factory() -> NetCDFFormatFactory {
        NetCDFFormatFactory::new(
            Arc::new(ListingFactory::dynamic()),
            NetcdfOptions::default(),
            NetcdfConfig::default(),
        )
    }

    /// The options of the format that the factory builds from `format_options`.
    fn options_of(pairs: &[(&str, &str)]) -> Result<NetcdfOptions> {
        let format_options: HashMap<String, String> = pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect();
        let format = factory().create(&one_partition_session().state(), &format_options)?;
        Ok(format
            .downcast_ref::<NetcdfFormat>()
            .expect("the factory builds a NetcdfFormat")
            .options
            .clone())
    }

    /// A stored table can still hold the option of the removed reader.
    #[test]
    fn the_removed_reader_option_does_not_change_the_format() {
        for value in ["false", "on"] {
            assert!(options_of(&[("use_rust_reader", value)]).is_ok());
        }

        let error = options_of(&[("use_rust_reader", "maybe")])
            .unwrap_err()
            .to_string();
        assert!(error.contains("use_rust_reader"), "{error}");
    }

    /// The key is the one that earlier releases wrote for this reader.
    #[test]
    fn the_schema_cache_key_does_not_change() {
        let factory = factory();
        let format = FileFormatFactory::default(&factory);
        let expected = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf").finish();
        assert_eq!(
            factory.schema_options_fingerprint(format.as_ref()),
            Some(expected)
        );
    }

    /// A skipped file caches an empty schema, so the option must change the key.
    /// Without the option the key stays the same as for older cache entries.
    #[test]
    fn skip_invalid_files_changes_the_schema_cache_key() {
        let factory = factory();
        let format = |skip_invalid_files| {
            NetcdfFormat::new(
                Arc::new(ListingFactory::dynamic()),
                NetcdfOptions {
                    skip_invalid_files,
                    ..NetcdfOptions::default()
                },
            )
        };
        let expected_default = SchemaOptions::new(NETCDF_EXTENSION).str("oxcdf").finish();

        let strict = factory.schema_options_fingerprint(&format(false));
        let lenient = factory.schema_options_fingerprint(&format(true));

        assert_eq!(strict, Some(expected_default));
        assert!(lenient.is_some());
        assert_ne!(strict, lenient);
    }

    /// The `OPTIONS` of `CREATE EXTERNAL TABLE` reach the format.
    #[test]
    fn the_factory_reads_the_skip_options() {
        let options = options_of(&[
            ("skip_unbroadcastable", "true"),
            ("skip_invalid_files", "true"),
        ])
        .unwrap();
        assert!(options.skip_unbroadcastable);
        assert!(options.skip_invalid_files);

        let default = options_of(&[]).unwrap();
        assert!(!default.skip_unbroadcastable);
        assert!(!default.skip_invalid_files);
    }

    #[test]
    fn combine_attrs_is_off_by_default() {
        assert_eq!(options_of(&[]).unwrap().writer.combine_attrs, None);
    }

    #[test]
    fn combine_attrs_reads_the_bare_and_the_prefixed_key() {
        let bare = options_of(&[("combine_attrs", "identical")]).unwrap();
        assert_eq!(bare.writer.combine_attrs, Some(CombineAttrs::Identical));

        let prefixed = options_of(&[("format.combine_attrs", "drop_conflicts")]).unwrap();
        assert_eq!(
            prefixed.writer.combine_attrs,
            Some(CombineAttrs::DropConflicts)
        );
    }

    #[test]
    fn a_bad_combine_attrs_is_an_error() {
        let error = options_of(&[("combine_attrs", "union")])
            .unwrap_err()
            .to_string();
        assert!(error.contains("combine_attrs"), "{error}");
    }

    /// The registry keys a format by these, so each spelling resolves to netCDF.
    #[test]
    fn the_factory_names_every_netcdf_extension() {
        let factory = factory();
        assert_eq!(factory.get_ext(), "nc");
        assert_eq!(
            factory.file_extensions(),
            vec!["nc", "nc3", "nc4", "netcdf"]
        );
    }

    /// Discovery keeps every spelling, in any case, and labels it `nc`.
    #[test]
    fn discovery_keeps_every_netcdf_extension() {
        let objects: Vec<ObjectMeta> = [
            "a.nc",
            "b.nc3",
            "c.nc4",
            "d.netcdf",
            "e.cdf",
            "f.NC4",
            "g.h5",
            "h.ncml",
            "i.parquet",
        ]
        .into_iter()
        .map(|path| ObjectMeta {
            location: Path::from(path),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        })
        .collect();

        let datasets = factory().discover_datasets(&objects).unwrap();

        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, ["a.nc", "b.nc3", "c.nc4", "d.netcdf", "f.NC4"]);
        assert!(datasets.iter().all(|d| d.format == "nc"));
    }
}
