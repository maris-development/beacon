//! `beacon.system.plugins` — one row for each plugin in the plugins directory, also failed ones.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, ListBuilder, StringArray, StringBuilder},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};

use super::table::{Snapshot, SystemTable};
use crate::plugins::{PluginRegistry, PluginStatus};

fn plugins_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("version", DataType::Utf8, true),
        Field::new("kind", DataType::Utf8, true),
        Field::new("status", DataType::Utf8, false),
        Field::new("error", DataType::Utf8, true),
        Field::new(
            "functions",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("source", DataType::Utf8, true),
        Field::new("sha256", DataType::Utf8, true),
    ]))
}

/// `beacon.system.plugins` — read from the registry at scan time.
pub(super) fn plugins_table(registry: Arc<PluginRegistry>) -> SystemTable {
    let schema = plugins_schema();
    let snapshot_schema = schema.clone();
    let snapshot: Snapshot = Arc::new(move || {
        let entries = registry.snapshot();
        let schema = snapshot_schema.clone();
        Box::pin(async move {
            let names: StringArray = entries.iter().map(|e| Some(e.name.as_str())).collect();
            let versions: StringArray = entries
                .iter()
                .map(|e| e.record.as_ref().map(|r| r.version.as_str()))
                .collect();
            let kinds: StringArray = entries.iter().map(|e| e.kind.map(|k| k.as_str())).collect();
            let statuses: StringArray = entries
                .iter()
                .map(|e| {
                    Some(match e.status {
                        PluginStatus::Loaded => "loaded",
                        PluginStatus::Failed(_) => "failed",
                        PluginStatus::Uninstalled => "uninstalled",
                    })
                })
                .collect();
            let errors: StringArray = entries
                .iter()
                .map(|e| match &e.status {
                    PluginStatus::Failed(reason) => Some(reason.as_str()),
                    _ => None,
                })
                .collect();
            let mut functions = ListBuilder::new(StringBuilder::new());
            for entry in &entries {
                for function in &entry.functions {
                    functions.values().append_value(function);
                }
                functions.append(true);
            }
            let sources: StringArray = entries
                .iter()
                .map(|e| e.record.as_ref().map(|r| r.source.as_str()))
                .collect();
            let hashes: StringArray = entries
                .iter()
                .map(|e| e.record.as_ref().map(|r| r.sha256.as_str()))
                .collect();
            Ok(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(names) as ArrayRef,
                    Arc::new(versions),
                    Arc::new(kinds),
                    Arc::new(statuses),
                    Arc::new(errors),
                    Arc::new(functions.finish()),
                    Arc::new(sources),
                    Arc::new(hashes),
                ],
            )?)
        })
    });
    SystemTable::new(schema, snapshot)
}
