use datafusion::catalog::TableFunction;

pub mod datasets;
pub mod netcdf;
pub mod util;

pub trait BeaconProvider: Send + Sync {
    fn function(&self) -> TableFunction;
    fn schema_function(&self) -> TableFunction;
}

inventory::collect!(&'static dyn BeaconProvider);
