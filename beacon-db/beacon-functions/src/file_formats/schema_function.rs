//! A generic `<read_fn>_schema` table function.
//!
//! Every `read_*` file-format function builds a [`TableProvider`] whose
//! [`schema`](TableProvider::schema) is the Arrow schema inferred from the
//! file(s). [`SchemaTableFunc`] wraps one of those readers and, instead of
//! returning the data, returns the *schema* as a table — one row per column
//! `(column_name, data_type, nullable)`. Reading only the schema is much cheaper
//! than scanning, and it is what dataset-inspection UIs need.
//!
//! The wrapper reuses the reader's argument parsing (globs / path lists) by
//! delegating [`call`](TableFunctionImpl::call) to it, so `read_parquet_schema`
//! accepts exactly what `read_parquet` does.

use std::sync::Arc;

use arrow::{
    array::{BooleanArray, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use beacon_common::table_function::BeaconTableFunctionImpl;
use datafusion::{
    catalog::{MemTable, TableFunctionImpl, TableProvider},
    prelude::Expr,
};

/// Wraps a `read_*` table function to expose the schema of the file(s) it would
/// read, under the name `<reader_name>_schema`.
pub struct SchemaTableFunc {
    name: String,
    description: String,
    reader: Arc<dyn BeaconTableFunctionImpl>,
}

impl SchemaTableFunc {
    /// Build the schema counterpart of `reader` (e.g. `read_parquet` ->
    /// `read_parquet_schema`).
    pub fn wrapping(reader: Arc<dyn BeaconTableFunctionImpl>) -> Self {
        let reader_name = reader.name();
        let description = format!(
            "Returns the Arrow schema (column_name, data_type, nullable) of the file(s) \
             `{reader_name}` would read, one row per column, without scanning their data."
        );
        Self {
            name: format!("{reader_name}_schema"),
            description,
            reader,
        }
    }

    /// The fixed output schema: one row per column of the inspected file(s).
    fn output_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("data_type", DataType::Utf8, false),
            Field::new("nullable", DataType::Boolean, false),
        ]))
    }
}

impl std::fmt::Debug for SchemaTableFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SchemaTableFunc({})", self.name)
    }
}

impl BeaconTableFunctionImpl for SchemaTableFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn description(&self) -> Option<String> {
        Some(self.description.clone())
    }

    /// The same arguments as the wrapped reader — schema inspection takes the
    /// same glob/path list a read would.
    fn arguments(&self) -> Option<Vec<Field>> {
        self.reader.arguments()
    }
}

impl TableFunctionImpl for SchemaTableFunc {
    fn call(&self, args: &[Expr]) -> datafusion::error::Result<Arc<dyn TableProvider>> {
        // Delegate to the reader to resolve the paths and infer the schema; take
        // its schema rather than its data.
        let provider = self.reader.call(args)?;
        let schema = provider.schema();

        let column_names: StringArray = schema
            .fields()
            .iter()
            .map(|field| Some(field.name().as_str()))
            .collect();
        let data_types: StringArray = schema
            .fields()
            .iter()
            .map(|field| Some(field.data_type().to_string()))
            .collect();
        let nullable = BooleanArray::from(
            schema
                .fields()
                .iter()
                .map(|field| field.is_nullable())
                .collect::<Vec<_>>(),
        );

        let out_schema = Self::output_schema();
        let batch = RecordBatch::try_new(
            out_schema.clone(),
            vec![
                Arc::new(column_names),
                Arc::new(data_types),
                Arc::new(nullable),
            ],
        )?;

        Ok(Arc::new(MemTable::try_new(out_schema, vec![vec![batch]])?))
    }
}
