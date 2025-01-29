use std::path::{Path, PathBuf};

use datafusion::{
    catalog::TableFunctionImpl, functions_table::create_udtf_function, logical_expr::Signature,
    scalar::ScalarValue,
};
use glob::glob;

pub mod default;
pub mod ragged;
pub(crate) mod util;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct NetCDF;

impl TableFunctionImpl for NetCDF {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        //Expect 1 argument, which is a string glob path
        if args.len() != 1 {
            return Err(datafusion::error::DataFusionError::Execution(
                "NetCDF provider expects 1 argument".to_string(),
            ));
        }

        //Get the string literal from the argument
        match &args[0] {
            datafusion::prelude::Expr::Literal(ScalarValue::Utf8(Some(path))) => {
                if let Ok(paths) = glob(path) {
                    // Get the directory where the datasets are stored
                    let datasets_dir =
                        PathBuf::from(&beacon_config::CONFIG.datasets_dir).canonicalize()?;
                    println!("{:?}", datasets_dir);
                    let mut files = vec![];
                    for path in paths.into_iter() {
                        //Canocalize the paths
                        let path = path.unwrap().canonicalize().unwrap();
                        //Ensure it starts with datasets directory defined in the config
                        if !path.starts_with(&datasets_dir) {
                            return Err(datafusion::error::DataFusionError::Execution(
                                "Path is not allowed".to_string(),
                            ));
                        }

                        files.push(path);
                    }

                    let provider = default::DefaultNetCDFProvider::new(files).unwrap();

                    Ok(std::sync::Arc::new(provider))
                } else {
                    return Err(datafusion::error::DataFusionError::Execution(
                        "Failed to glob path".to_string(),
                    ));
                }
            }
            value => {
                println!("{:?}", value);
                return Err(datafusion::error::DataFusionError::Execution(
                    "NetCDF provider expects a string literal".to_string(),
                ));
            }
        }
    }
}
