use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::csv_format::SuperCsvFormat;

pub struct CsvTableFunction {
    session_ctx: Arc<SessionContext>,
}

impl CsvTableFunction {
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        Self { session_ctx }
    }
}
impl std::fmt::Debug for CsvTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvTableFunction").finish()
    }
}

impl datafusion::catalog::TableFunctionImpl for CsvTableFunction {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        //Get first argument as delimiter
        let delimiter = if let Some(datafusion::prelude::Expr::Literal(
            datafusion::common::ScalarValue::Utf8(Some(delimiter)),
        )) = args.get(0)
        {
            delimiter.clone()
        } else {
            return Err(datafusion::error::DataFusionError::Plan(
                "Delimiter not provided".to_string(),
            ));
        };
        let delimiter_char = delimiter.chars().next().ok_or_else(|| {
            datafusion::error::DataFusionError::Plan("Delimiter is empty".to_string())
        })?;
        //Get second argument as infer_records
        let infer_records_count = if let Some(datafusion::prelude::Expr::Literal(
            datafusion::common::ScalarValue::Int64(Some(infer_records)),
        )) = args.get(1)
        {
            *infer_records
        } else {
            return Err(datafusion::error::DataFusionError::Plan(
                "Infer records count not provided".to_string(),
            ));
        };

        let table_url_args = &args[2..];

        let session_state = self.session_ctx.state();
        let listing_table_urls = super::parse_exprs_to_urls(table_url_args)?;

        let datasource = super::create_datasource(
            &session_state,
            listing_table_urls,
            Arc::new(SuperCsvFormat::new(
                delimiter_char as u8,
                infer_records_count as usize,
            )),
        )?;

        // Create a new DataSource with the urls
        Ok(Arc::new(datasource))
    }
}
