use std::sync::Arc;

use datafusion::catalog::TableFunction;

use crate::providers::BeaconProvider;

pub mod data;
pub mod schema;

pub struct BeaconNetCDFProvider;

impl BeaconProvider for BeaconNetCDFProvider {
    fn function(&self) -> TableFunction {
        TableFunction::new("netcdf".to_string(), Arc::new(data::NetCDFDataFunction))
    }

    fn schema_function(&self) -> TableFunction {
        TableFunction::new(
            "netcdf_schema".to_string(),
            Arc::new(schema::NetCDFSchemaFunction),
        )
    }
}

inventory::submit!(&BeaconNetCDFProvider as &'static dyn BeaconProvider);
