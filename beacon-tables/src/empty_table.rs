use std::{path::PathBuf, sync::Arc};

use arrow::datatypes::Schema;
use datafusion::prelude::SessionContext;

use crate::LogicalTableProvider;

/// An implementation of a LogicalTableProvider that represents an empty table.
///
/// The EmptyTable struct provides a minimal implementation where the table does not store any data.
/// This implementation is useful for testing or placeholder scenarios.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct EmptyTable {
    /// The name of the table.
    table_name: String,
}

impl EmptyTable {
    /// Creates a new instance of an EmptyTable.
    ///
    /// # Arguments
    ///
    /// * `table_name` - A string representing the name of the table.
    ///
    /// # Returns
    ///
    /// A new instance of EmptyTable.
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

#[typetag::serde(name = "empty")]
#[async_trait::async_trait]
impl LogicalTableProvider for EmptyTable {
    /// Returns the name of the table.
    ///
    /// # Returns
    ///
    /// A string slice containing the table name.
    fn table_name(&self) -> &str {
        &self.table_name
    }

    /// Performs any necessary creation or initialization for the table.
    ///
    /// For the EmptyTable implementation, no creation logic is required.
    ///
    /// # Arguments
    ///
    /// * `_directory` - The directory where the table might store its data (unused in this implementation).
    /// * `_session_ctx` - The session context for DataFusion operations (unused in this implementation).
    ///
    /// # Returns
    ///
    /// A Result indicating success or failure. Always returns Ok(()).
    async fn create(
        &self,
        _directory: PathBuf,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<(), crate::LogicalTableError> {
        Ok(())
    }

    /// Provides a DataFusion TableProvider for query execution.
    ///
    /// This method returns a provider that represents an empty table.
    ///
    /// # Arguments
    ///
    /// * `_session_ctx` - The session context for DataFusion operations (unused in this implementation).
    ///
    /// # Returns
    ///
    /// A Result containing an Arc to a TableProvider, or a LogicalTableError if an error occurs.
    async fn table_provider(
        &self,
        _session_ctx: Arc<SessionContext>,
    ) -> Result<Arc<dyn datafusion::catalog::TableProvider>, crate::LogicalTableError> {
        Ok(Arc::new(datafusion::datasource::empty::EmptyTable::new(
            Arc::new(Schema::empty()),
        )))
    }
}
