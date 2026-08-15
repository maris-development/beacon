//! `beacon.system.settings` — every runtime-settable setting, as SQL.
//!
//! The table form of `SHOW SETTINGS`, for a client that would rather filter and
//! join than parse a `SHOW`. Same rows, same columns.
//!
//! Unlike the statement, this table is super-user-only, because everything in
//! `beacon.system` is (see the module docs and
//! [`authorize_logical_plan`](crate::statement_plan::authorize_logical_plan)).
//! A regular user reads the same values through `SHOW SETTINGS`.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_datafusion_ext::settings::{BeaconOptions, BootSettings};
use datafusion::common::Result as DFResult;
use datafusion::common::config::ExtensionOptions as _;

use super::table::{Snapshot, SystemTable};
use crate::statement_plan::{SessionCell, upgrade_session};

fn settings_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
        // What the runtime booted with — the `BEACON_*` variable, or the compiled
        // default. This is what `RESET <name>` restores.
        Field::new("default", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, false),
    ]))
}

/// `beacon.system.settings` — one row per `beacon.*` setting.
///
/// Snapshotted per scan through the session, so a `SET` in one statement is
/// visible to a `SELECT` in the next.
pub(super) fn settings_table(session: SessionCell) -> SystemTable {
    let snapshot: Snapshot = Arc::new(move || {
        let session = session.clone();
        Box::pin(async move {
            let Ok(session) = upgrade_session(&session, "beacon.system.settings") else {
                return empty_batch();
            };
            let state = session.state();
            let config = state.config();
            let boot = BootSettings::from_config(config);

            let mut entries = BeaconOptions::from_config(config).entries();
            entries.sort_by(|left, right| left.key.cmp(&right.key));

            let names: Vec<&str> = entries.iter().map(|entry| entry.key.as_str()).collect();
            let values: Vec<Option<&str>> =
                entries.iter().map(|entry| entry.value.as_deref()).collect();
            let defaults: Vec<Option<&str>> =
                entries.iter().map(|entry| boot.get(&entry.key)).collect();
            let descriptions: Vec<&str> = entries.iter().map(|entry| entry.description).collect();

            let columns: Vec<ArrayRef> = vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(values)),
                Arc::new(StringArray::from(defaults)),
                Arc::new(StringArray::from(descriptions)),
            ];
            Ok(RecordBatch::try_new(settings_schema(), columns)?)
        })
    });
    SystemTable::new(settings_schema(), snapshot)
}

/// No rows, for a torn-down runtime — the same answer the other system tables
/// give when their source is unavailable.
fn empty_batch() -> DFResult<RecordBatch> {
    Ok(RecordBatch::new_empty(settings_schema()))
}
