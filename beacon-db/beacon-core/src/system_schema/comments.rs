//! `beacon.system.comments` — every table and column comment as SQL.
//!
//! One row for each comment. A table comment has a `NULL` `column_name`. Rows
//! come from the `comments.json` sidecars, so a comment on a column that no
//! longer exists still shows here and can be removed with `COMMENT ON ... IS NULL`.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::error::DataFusionError;

use super::table::{Snapshot, SystemTable};
use crate::statement_plan::{upgrade_session, SessionCell};

fn comments_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("column_name", DataType::Utf8, true),
        Field::new("comment", DataType::Utf8, false),
    ]))
}

/// `beacon.system.comments` — read from the sidecars at scan time.
pub(super) fn comments_table(session: SessionCell) -> SystemTable {
    let schema = comments_schema();
    let snapshot_schema = schema.clone();
    let snapshot: Snapshot = Arc::new(move || {
        let session = session.clone();
        let schema = snapshot_schema.clone();
        Box::pin(async move {
            let ctx = upgrade_session(&session, "beacon.system.comments")
                .map_err(|error| DataFusionError::External(error.into()))?;
            let rows = crate::comments::all_comments(&ctx)
                .await
                .map_err(|error| DataFusionError::External(error.into()))?;
            let tables: StringArray = rows.iter().map(|row| Some(row.table.as_str())).collect();
            let columns: StringArray = rows.iter().map(|row| row.column.as_deref()).collect();
            let comments: StringArray = rows.iter().map(|row| Some(row.comment.as_str())).collect();
            Ok(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(tables) as ArrayRef,
                    Arc::new(columns),
                    Arc::new(comments),
                ],
            )?)
        })
    });
    SystemTable::new(schema, snapshot)
}
