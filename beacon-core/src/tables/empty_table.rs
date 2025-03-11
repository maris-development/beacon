use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{datasource::empty::EmptyTable, execution::SessionState};

use crate::tables::table::BeaconTable;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
/// A struct representing an empty default table with a name.
///
/// # Fields
///
/// * `table_name` - A `String` representing the name of the table.
pub struct EmptyDefaultTable {
    table_name: String,
}

impl EmptyDefaultTable {
    /// Creates a new `EmptyDefaultTable` with the given name.
    ///
    /// # Arguments
    ///
    /// * `table_name` - A `String` representing the name of the table.
    pub fn new(table_name: String) -> Self {
        Self { table_name }
    }
}

#[typetag::serde(name = "empty_table")]
#[async_trait::async_trait]
/// Implementation of the `BeaconTable` trait for the `EmptyDefaultTable` struct.
///
/// This implementation provides an asynchronous method to return an empty table
/// and a method to retrieve the table name.
///
/// # Methods
///
/// * `as_table` - Asynchronously returns an `Arc` containing a `TableProvider` for an empty table.
/// * `table_name` - Returns a reference to the name of the table.
impl BeaconTable for EmptyDefaultTable {
    async fn as_table(
        &self,
        _session_state: Arc<SessionState>,
    ) -> Arc<dyn datafusion::catalog::TableProvider> {
        Arc::new(EmptyTable::new(SchemaRef::from(Schema::empty())))
    }

    fn table_name(&self) -> &str {
        &self.table_name
    }
}
