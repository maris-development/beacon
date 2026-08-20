//! `browse_datasets([prefix[, offset[, limit]]])`: one directory level.

use std::sync::Arc;

use beacon_datafusion_ext::format_ext::FileFormatFactoryExt;
use datafusion::{
    catalog::{TableFunctionImpl, TableProvider},
    prelude::Expr,
};

use super::provider::{DatasetsTable, Listing};
use crate::file_formats::BeaconTableFunctionImpl;
use crate::listing::args::{string_arg, usize_arg};

/// `browse_datasets([prefix[, offset[, limit]]])`: one directory level.
///
/// The same provider as `list_datasets`, stopped at one level. A folder view
/// wants this: its cost is one delimiter request, so it does not grow with the
/// size of the store below the prefix.
pub struct BrowseDatasetsFunc {
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl BrowseDatasetsFunc {
    pub fn new(file_formats: Vec<Arc<dyn FileFormatFactoryExt>>) -> Self {
        Self { file_formats }
    }
}

impl std::fmt::Debug for BrowseDatasetsFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrowseDatasetsFunc")
    }
}

impl BeaconTableFunctionImpl for BrowseDatasetsFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        "browse_datasets".to_string()
    }

    fn description(&self) -> Option<String> {
        Some(
            "Lists one directory level of the datasets store. Optional arguments: \
             browse_datasets(prefix, offset, limit) — a directory (default the root), \
             a row offset, and a row limit. Sub-directories come back as rows with \
             `is_directory` set. Unlike list_datasets it does not descend, so its \
             cost does not grow with the size of the store below the prefix."
                .to_string(),
        )
    }
}

impl TableFunctionImpl for BrowseDatasetsFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        let prefix = string_arg(args, 0).unwrap_or_default();
        let offset = usize_arg(args, 1).unwrap_or(0);
        let limit = usize_arg(args, 2);

        Ok(Arc::new(DatasetsTable::new(
            Listing::Level { prefix },
            offset,
            limit,
            self.file_formats.clone(),
        )))
    }
}
