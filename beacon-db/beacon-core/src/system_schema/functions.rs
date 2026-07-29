//! `beacon.system.functions` and `beacon.system.table_functions`.
//!
//! Both expose [`FunctionDoc`] rows, so they share a schema and a row builder.
//! They differ only in where the docs come from: scalar/aggregate functions are
//! read live off the session's function registry, while table-valued functions
//! are the docs captured at startup (DataFusion's UDTF registry is not
//! enumerable with metadata).

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_functions::function_doc::FunctionDoc;
use datafusion::common::Result as DFResult;

use crate::statement_plan::SessionCell;

use super::table::{Snapshot, SystemTable};

/// The shared schema of both function tables. `parameters` is a JSON array of
/// `{name, description, data_type}` objects — the parameter list is variable
/// length and purely descriptive, so it stays a single JSON column rather than a
/// nested Arrow type that would pin the table schema to `FunctionParameter`.
pub(super) fn function_doc_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("function_name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("return_type", DataType::Utf8, false),
        Field::new("parameters", DataType::Utf8, false),
    ]))
}

/// Sorts by name and drops duplicate names, then builds the batch. The sort makes
/// scans deterministic; the dedup mirrors the listing the HTTP endpoints used to
/// return (one row per function, not one per overload).
fn function_docs_batch(mut docs: Vec<FunctionDoc>) -> DFResult<RecordBatch> {
    docs.sort_by(|left, right| left.function_name.cmp(&right.function_name));
    docs.dedup_by(|left, right| left.function_name == right.function_name);

    let names: Vec<&str> = docs.iter().map(|doc| doc.function_name.as_str()).collect();
    let descriptions: Vec<&str> = docs.iter().map(|doc| doc.description.as_str()).collect();
    let return_types: Vec<&str> = docs.iter().map(|doc| doc.return_type.as_str()).collect();
    let parameters: Vec<String> = docs
        .iter()
        .map(|doc| {
            serde_json::to_string(&doc.params).unwrap_or_else(|_| "[]".to_string())
        })
        .collect();

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(descriptions)),
        Arc::new(StringArray::from(return_types)),
        Arc::new(StringArray::from(parameters)),
    ];

    Ok(RecordBatch::try_new(function_doc_schema(), columns)?)
}

/// `beacon.system.functions` — the scalar/aggregate functions registered on the
/// session, read live so functions registered after startup show up.
pub(super) fn functions_table(session: SessionCell) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        // A dropped session yields an empty table rather than an error: the
        // schema outlives the context only while the runtime is being torn down.
        let docs = session
            .get()
            .and_then(|weak| weak.upgrade())
            .map(|ctx| {
                ctx.state()
                    .scalar_functions()
                    .values()
                    .flat_map(|function| FunctionDoc::from_scalar(function))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Box::pin(async move { function_docs_batch(docs) })
    });

    SystemTable::new(function_doc_schema(), snapshot)
}

/// `beacon.system.table_functions` — the table-valued functions, from the docs
/// captured when they were registered at startup.
pub(super) fn table_functions_table(docs: Vec<FunctionDoc>) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let docs = docs.clone();
        Box::pin(async move { function_docs_batch(docs) })
    });
    SystemTable::new(function_doc_schema(), snapshot)
}
