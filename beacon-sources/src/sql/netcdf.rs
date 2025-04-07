use std::{fmt::Debug, sync::Arc};

use datafusion::{
    catalog::TableFunctionImpl, datasource::file_format::FileFormatFactory, prelude::SessionContext,
};

use crate::netcdf_format::NetCDFFileFormatFactory;

use super::{create_datasource, parse_exprs_to_urls};

pub struct NetCDFTableFunction {
    session_ctx: Arc<SessionContext>,
}

impl NetCDFTableFunction {
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        Self { session_ctx }
    }
}

impl Debug for NetCDFTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetCDFTableFunction").finish()
    }
}

impl TableFunctionImpl for NetCDFTableFunction {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let session_state = self.session_ctx.state();
        let listing_table_urls = parse_exprs_to_urls(args)?;

        let datasource = create_datasource(
            &session_state,
            listing_table_urls,
            NetCDFFileFormatFactory.default(),
        )?;

        // Create a new DataSource with the urls
        Ok(Arc::new(datasource))
    }
}
