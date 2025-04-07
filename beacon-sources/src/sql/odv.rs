use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::odv_format::OdvFormat;

pub struct OdvTableFunction {
    session_ctx: Arc<SessionContext>,
}

impl OdvTableFunction {
    pub fn new(session_ctx: Arc<SessionContext>) -> Self {
        Self { session_ctx }
    }
}

impl std::fmt::Debug for OdvTableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OdvTableFunction").finish()
    }
}

impl datafusion::catalog::TableFunctionImpl for OdvTableFunction {
    fn call(
        &self,
        args: &[datafusion::prelude::Expr],
    ) -> datafusion::error::Result<std::sync::Arc<dyn datafusion::catalog::TableProvider>> {
        let session_state = self.session_ctx.state();
        let listing_table_urls = super::parse_exprs_to_urls(args)?;

        let datasource =
            super::create_datasource(&session_state, listing_table_urls, Arc::new(OdvFormat))?;

        // Create a new DataSource with the urls
        Ok(Arc::new(datasource))
    }
}
