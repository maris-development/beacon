//! [`Hdf5FormatFactory`]: the HDF5 `FileFormat` factory, delegating to the netCDF format.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use beacon_arrow_netcdf::datafusion::NetCDFFormatFactory;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::{
    catalog::Session,
    common::GetExt,
    datasource::{
        file_format::{FileFormat, FileFormatFactory},
        listing::ListingTableUrl,
    },
};
use object_store::ObjectMeta;

use crate::{HDF5_EXTENSIONS, HDF5_FORMAT_NAME};

/// A `FileFormat` factory for HDF5 files.
///
/// A NetCDF-4 file is HDF5, and netCDF-c opens plain HDF5 too, so the actual reading/writing is
/// the netCDF format's — this factory wraps a [`NetCDFFormatFactory`] and only supplies the HDF5
/// identity: the `STORED AS` name / `get_ext` it registers under and the `.h5`/`.hdf5` files it
/// recognizes during discovery.
#[derive(Debug, Clone)]
pub struct Hdf5FormatFactory {
    inner: NetCDFFormatFactory,
    /// The `get_ext` this instance registers under. DataFusion's native format registry keys a
    /// factory only by its single `get_ext`, so `h5` and `hdf5` need one instance each (built
    /// with [`Self::with_ext`]); beacon's own registry keys by [`Self::file_extensions`] and
    /// needs only one.
    ext: String,
}

impl Hdf5FormatFactory {
    /// Wrap a netCDF factory, registering under the canonical `hdf5` name.
    pub fn wrapping(inner: NetCDFFormatFactory) -> Self {
        Self {
            inner,
            ext: HDF5_FORMAT_NAME.to_string(),
        }
    }

    /// The same factory registered under a different `get_ext` (e.g. `h5`).
    pub fn with_ext(mut self, ext: impl Into<String>) -> Self {
        self.ext = ext.into();
        self
    }
}

impl GetExt for Hdf5FormatFactory {
    fn get_ext(&self) -> String {
        self.ext.clone()
    }
}

impl FileFormatFactory for Hdf5FormatFactory {
    fn create(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        self.inner.create(state, format_options)
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        self.inner.default()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for Hdf5FormatFactory {
    /// Delegates to the netCDF factory's native-root path, which handles both readers: netcdf-c
    /// opens a local path / http(s) URL, and the pure-Rust reader goes through the object store.
    /// A NetCDF-4 file is HDF5 and both readers parse plain HDF5 too, so neither needs a change
    /// here.
    fn create_with_native_root(
        &self,
        state: &dyn Session,
        format_options: &HashMap<String, String>,
        url: &ListingTableUrl,
        listing: &ListingFactory,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        self.inner
            .create_with_native_root(state, format_options, url, listing)
    }

    fn file_extensions(&self) -> Vec<String> {
        HDF5_EXTENSIONS.iter().map(|ext| ext.to_string()).collect()
    }

    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| HDF5_EXTENSIONS.contains(&ext))
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{path::Path, ObjectMeta};

    // A minimal netCDF factory to wrap. It never opens a file in these tests — they only exercise
    // the HDF5 identity (extensions, names, discovery filtering).
    fn factory(ext: &str) -> Hdf5FormatFactory {
        use beacon_arrow_netcdf::datafusion::{options::NetcdfOptions, NetcdfConfig};
        use beacon_datafusion_ext::listing_factory::ListingFactory;

        let listing = Arc::new(ListingFactory::new(None));
        let inner = NetCDFFormatFactory::new(
            listing,
            std::env::temp_dir(),
            NetcdfOptions::default(),
            NetcdfConfig::default(),
        );
        Hdf5FormatFactory::wrapping(inner).with_ext(ext)
    }

    fn object(name: &str) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(name),
            // A fixed epoch avoids a clock call; the value is irrelevant to discovery.
            last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
            size: 0,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn advertises_hdf5_extensions_and_name() {
        let f = factory("hdf5");
        assert_eq!(
            f.file_extensions(),
            vec!["h5".to_string(), "hdf5".to_string()]
        );
        assert_eq!(f.get_ext(), "hdf5");
        assert_eq!(f.file_format_name(), "hdf5");
    }

    #[test]
    fn with_ext_changes_only_the_registration_key() {
        let f = factory("h5");
        assert_eq!(f.get_ext(), "h5");
        // The recognized extensions are the same regardless of which key this instance answers to.
        assert_eq!(
            f.file_extensions(),
            vec!["h5".to_string(), "hdf5".to_string()]
        );
    }

    #[test]
    fn discovery_picks_up_h5_and_hdf5_only() {
        let f = factory("hdf5");
        let objects = [
            object("a.h5"),
            object("b.hdf5"),
            object("c.nc"),
            object("d.parquet"),
            object("no_extension"),
        ];
        let discovered = f.discover_datasets(&objects).unwrap();
        let paths: Vec<String> = discovered.iter().map(|d| d.file_path.clone()).collect();
        assert_eq!(paths, vec!["a.h5".to_string(), "b.hdf5".to_string()]);
        // Each discovered dataset is tagged with this factory's format name.
        assert!(discovered.iter().all(|d| d.format == "hdf5"));
    }
}
