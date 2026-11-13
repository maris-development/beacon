//! `beacon.system.table_sizes` — the size of each managed table as SQL.
//!
//! One row for each Lance table in the default schema, also the internal
//! `__beacon_*` tables: they share `beacon.db` with the tables of the user.
//! Each scan opens every table and lists its objects in the store.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray, UInt64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use beacon_lance::{LanceTable, LanceWarehouse, TableSize};
use datafusion::{catalog::SchemaProvider, error::DataFusionError, prelude::SessionContext};
use futures::{StreamExt, TryStreamExt};

use super::table::{Snapshot, SystemTable};
use crate::schema_persistence::PersistentSchemaProvider;
use crate::statement_plan::{upgrade_session, SessionCell};

/// The value of the `engine` column: Lance holds every managed table.
const LANCE_ENGINE: &str = "lance";

/// The number of tables that are measured at the same time.
const CONCURRENT_TABLES: usize = 8;

fn table_sizes_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("table_name", DataType::Utf8, false),
        Field::new("engine", DataType::Utf8, false),
        Field::new("row_count", DataType::UInt64, false),
        Field::new("fragment_count", DataType::UInt64, false),
        Field::new("version_count", DataType::UInt64, false),
        Field::new("bytes_on_disk", DataType::UInt64, false),
    ]))
}

/// The name and the `db://` location of each Lance table in the default schema,
/// in name order.
async fn lance_tables(ctx: &SessionContext) -> anyhow::Result<Vec<(String, String)>> {
    let config = ctx.copied_config();
    let catalog_options = &config.options().catalog;
    let schema = ctx
        .catalog(&catalog_options.default_catalog)
        .and_then(|catalog| catalog.schema(&catalog_options.default_schema))
        .ok_or_else(|| anyhow::anyhow!("the default schema is not registered"))?;
    let provider = schema
        .downcast_ref::<PersistentSchemaProvider>()
        .ok_or_else(|| anyhow::anyhow!("the default schema is not the persistent provider"))?;

    let mut names = provider.all_table_names();
    names.sort_unstable();
    let mut tables = Vec::with_capacity(names.len());
    for name in names {
        // A table that a concurrent DROP removed is `None` here.
        let location = provider.table(&name).await?.and_then(|table| {
            table
                .downcast_ref::<LanceTable>()
                .map(|lance| lance.definition().location.clone())
        });
        if let Some(location) = location {
            tables.push((name, location));
        }
    }
    Ok(tables)
}

/// The size of each managed table, in name order.
async fn table_sizes(ctx: &SessionContext) -> anyhow::Result<Vec<(String, TableSize)>> {
    let warehouse = ctx
        .state()
        .config()
        .get_extension::<LanceWarehouse>()
        .ok_or_else(|| anyhow::anyhow!("Lance warehouse is unavailable"))?;
    let tables = lance_tables(ctx).await?;
    futures::stream::iter(tables)
        .map(|(name, location)| {
            let warehouse = warehouse.clone();
            async move {
                let size = beacon_lance::table_size(&warehouse, &location).await?;
                anyhow::Ok((name, size))
            }
        })
        // `buffered` keeps the name order.
        .buffered(CONCURRENT_TABLES)
        .try_collect()
        .await
}

/// `beacon.system.table_sizes` — measured at scan time.
pub(super) fn table_sizes_table(session: SessionCell) -> SystemTable {
    let schema = table_sizes_schema();
    let snapshot_schema = schema.clone();
    let snapshot: Snapshot = Arc::new(move || {
        let session = session.clone();
        let schema = snapshot_schema.clone();
        Box::pin(async move {
            let ctx = upgrade_session(&session, "beacon.system.table_sizes")
                .map_err(|error| DataFusionError::External(error.into()))?;
            let rows = table_sizes(&ctx)
                .await
                .map_err(|error| DataFusionError::External(error.into()))?;
            let names: StringArray = rows.iter().map(|(name, _)| Some(name.as_str())).collect();
            let engines: StringArray = rows.iter().map(|_| Some(LANCE_ENGINE)).collect();
            let column = |value: fn(&TableSize) -> u64| -> ArrayRef {
                Arc::new(UInt64Array::from_iter_values(
                    rows.iter().map(|(_, size)| value(size)),
                ))
            };
            Ok(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(names) as ArrayRef,
                    Arc::new(engines),
                    column(|size| size.rows),
                    column(|size| size.fragments),
                    column(|size| size.versions),
                    column(|size| size.bytes),
                ],
            )?)
        })
    });
    SystemTable::new(schema, snapshot)
}
