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
                    let mut files = vec![];
                    for path in paths.into_iter() {
                        let path = path.unwrap();
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
            _ => {
                return Err(datafusion::error::DataFusionError::Execution(
                    "NetCDF provider expects a string literal".to_string(),
                ));
            }
        }
    }
}
