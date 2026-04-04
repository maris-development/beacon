mod netcdf;

use std::sync::Arc;

use netcdf::NetcdfIngestFormatLoader;

use crate::statement_handlers::registry::IngestFormatLoaderRegistry;

pub(crate) fn register_default_ingest_loaders(registry: &mut IngestFormatLoaderRegistry) {
    registry.register_loader(Arc::new(NetcdfIngestFormatLoader));
}