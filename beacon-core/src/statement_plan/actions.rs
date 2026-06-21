//! The side effects performed by beacon's DDL/DML execution-plan nodes.
//!
//! Each function holds the logic that used to live in the `DFStatementHandler`
//! match arms; the exec nodes in [`super::physical`] recover the
//! [`SessionContext`] from the planner's weak cell and call these.

use std::sync::Arc;

use beacon_data_lake::DATASETS_OBJECT_STORE_URL;
use beacon_datafusion_ext::{
    listing_table_factory_ext::ListingTableFactoryExt,
    remote::{RemoteTableDefinition, unresolved_schema},
    table_ext::{MaterializedView, TableDefinition},
};
use datafusion::{
    catalog::TableProviderFactory,
    datasource::ViewTable,
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{dml::InsertOp, CreateExternalTable, LogicalPlan},
    physical_plan::{execute_stream, ExecutionPlan},
    prelude::SessionContext,
    sql::{
        sqlparser::ast::{
            AlterColumnOperation, AlterTableOperation, ColumnDef as SqlColumnDef, Ident as SqlIdent,
        },
        TableReference,
    },
};

use super::logical::AlterTableSpec;
use super::materialized_view::delete_datasets_prefix;

/// Drop a managed table, reclaiming its backing storage. Materialized views
/// persist Parquet under a data prefix; Iceberg tables own metadata+data in the
/// warehouse.
pub(crate) async fn drop_table(
    session: &Arc<SessionContext>,
    name: &TableReference,
    if_exists: bool,
) -> anyhow::Result<()> {
    if !if_exists && !session.table_exist(name.clone())? {
        return Err(anyhow::anyhow!("Table '{}' does not exist", name));
    }

    // Inspect the provider before deregistering so we can reclaim its storage.
    let provider = session.table_provider(name.clone()).await.ok();
    let materialized_prefix = provider.as_ref().and_then(|provider| {
        provider
            .as_any()
            .downcast_ref::<MaterializedView>()
            .map(|mv| mv.base_storage_prefix())
    });
    let iceberg_definition = provider.as_ref().and_then(|provider| {
        provider
            .as_any()
            .downcast_ref::<beacon_iceberg::IcebergTable>()
            .map(|table| table.definition().clone())
    });

    session.deregister_table(name.clone())?;

    if let Some(prefix) = materialized_prefix {
        delete_datasets_prefix(session, &prefix).await;
    }

    if let Some(definition) = iceberg_definition {
        let store = beacon_iceberg::get_warehouse_store()?;
        beacon_iceberg::drop_iceberg_table(&store, &definition.namespace, &definition.name).await?;
    }

    Ok(())
}

/// Register a `CREATE EXTERNAL TABLE` via the listing-table factory and persist
/// it to the catalog.
pub(crate) async fn create_external_table(
    session: &Arc<SessionContext>,
    cmd: &CreateExternalTable,
) -> anyhow::Result<()> {
    // `STORED AS REMOTE` registers a federated table pointing at another Beacon
    // instance, rather than a listing table over the datasets store.
    if cmd.file_type.eq_ignore_ascii_case("REMOTE") {
        return create_remote_table(session, cmd).await;
    }

    // `STORED AS DELTA` registers a Delta Lake table. A Delta table is a
    // directory with a `_delta_log` transaction log, not a file glob, so it
    // bypasses the listing-table factory and uses its own provider.
    if cmd.file_type.eq_ignore_ascii_case("DELTA") {
        return create_delta_table(session, cmd).await;
    }

    let factory =
        ListingTableFactoryExt::new(DATASETS_OBJECT_STORE_URL.clone(), Arc::downgrade(session));
    let state = session.state();
    let created = factory.create(&state, cmd).await?;
    session.register_table(cmd.name.clone(), created)?;
    Ok(())
}

/// Build and register a federated remote-Beacon table from
/// `CREATE EXTERNAL TABLE … STORED AS REMOTE LOCATION 'beacon://host:port/table'
/// OPTIONS ('username' '…', 'password' '…', 'tls' 'true')`.
async fn create_remote_table(
    session: &Arc<SessionContext>,
    cmd: &CreateExternalTable,
) -> anyhow::Result<()> {
    // DataFusion prefixes `OPTIONS` keys without a `.` with `format.`, so look up
    // both the raw and prefixed forms.
    let option = |key: &str| {
        cmd.options
            .get(key)
            .or_else(|| cmd.options.get(&format!("format.{key}")))
            .cloned()
    };

    let (url, remote_table) = parse_remote_location(&cmd.location, option("tls"))?;

    // Honor an explicit column list if given; otherwise fetch the schema from the
    // remote when the provider is built.
    let schema = if cmd.schema.fields().is_empty() {
        unresolved_schema()
    } else {
        Arc::clone(cmd.schema.inner())
    };

    // No credentials: remote tables connect anonymously (the remote must allow
    // anonymous Flight SQL access).
    let definition = RemoteTableDefinition {
        name: cmd.name.to_string(),
        url,
        remote_table,
        schema,
    };

    // `build_provider` pins the resolved schema; registration persists table.json.
    let provider = definition
        .build_provider(session.clone(), &DATASETS_OBJECT_STORE_URL)
        .await?;
    session.register_table(cmd.name.clone(), provider)?;
    Ok(())
}

/// Parse `beacon://host:port/table` into the tonic endpoint URL and the remote
/// table name. `OPTIONS ('tls' 'true')` selects `https` over the default `http`.
fn parse_remote_location(
    location: &str,
    tls_option: Option<String>,
) -> anyhow::Result<(String, String)> {
    let rest = location
        .strip_prefix("beacon://")
        .or_else(|| location.strip_prefix("grpc://"))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "remote table LOCATION must be 'beacon://host:port/table', got '{location}'"
            )
        })?;

    let (authority, table) = rest.split_once('/').ok_or_else(|| {
        anyhow::anyhow!(
            "remote table LOCATION must include a table name: 'beacon://host:port/table', got '{location}'"
        )
    })?;

    anyhow::ensure!(!authority.is_empty(), "remote table LOCATION missing host:port");
    anyhow::ensure!(!table.is_empty(), "remote table LOCATION missing table name");

    let tls = tls_option
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let scheme = if tls { "https" } else { "http" };

    Ok((format!("{scheme}://{authority}"), table.to_string()))
}

/// Build and register a Delta Lake table from
/// `CREATE EXTERNAL TABLE … STORED AS DELTA LOCATION 'datasets://path/to/table'
/// OPTIONS ('version' '12')`. The Delta table must already exist at the location;
/// its schema is read from the transaction log.
async fn create_delta_table(
    session: &Arc<SessionContext>,
    cmd: &CreateExternalTable,
) -> anyhow::Result<()> {
    let definition = beacon_delta::DeltaTableDefinition {
        name: cmd.name.to_string(),
        location: cmd.location.clone(),
        options: cmd.options.clone(),
        definition: None,
    };

    // `build_provider` resolves the schema from the Delta log; registration
    // persists `table.json` via the TableManager.
    let provider = definition
        .build_provider(session.clone(), &DATASETS_OBJECT_STORE_URL)
        .await?;
    session.register_table(cmd.name.clone(), provider)?;
    Ok(())
}

/// Register a `CREATE VIEW` as a `ViewTable` (re-plans its query on each scan).
pub(crate) fn create_view(
    session: &Arc<SessionContext>,
    name: &TableReference,
    input: &LogicalPlan,
    definition: &Option<String>,
) -> anyhow::Result<()> {
    let table = ViewTable::new(input.clone(), definition.clone());
    session.register_table(name.clone(), Arc::new(table))?;
    Ok(())
}

/// Create an Iceberg-backed managed table from `child`'s output schema and
/// register it. For `CREATE TABLE AS SELECT` (`is_ctas`), also populate it from
/// `child` and return the inserted-row count stream; otherwise return `None`.
pub(crate) async fn create_table(
    session: &Arc<SessionContext>,
    name: &TableReference,
    child: Arc<dyn ExecutionPlan>,
    is_ctas: bool,
    if_not_exists: bool,
) -> anyhow::Result<Option<SendableRecordBatchStream>> {
    let table_name = name.table().to_string();

    if session.table_exist(name.clone())? {
        if if_not_exists {
            return Ok(None);
        }
        return Err(anyhow::anyhow!("Table '{table_name}' already exists"));
    }

    let arrow_schema = child.schema();
    let catalog = beacon_iceberg::get_catalog()?;
    let namespace = beacon_iceberg::beacon_namespace();
    let table =
        beacon_iceberg::create_iceberg_table(&catalog, &namespace, &table_name, &arrow_schema)
            .await?;

    // Registration persists the table's `table.json` pointer via the PersistentSchemaProvider.
    session.register_table(name.clone(), Arc::new(table))?;

    if is_ctas {
        let stream = insert_into(session, &table_name, InsertOp::Append, child).await?;
        Ok(Some(stream))
    } else {
        Ok(None)
    }
}

/// Insert the rows produced by `child` into an existing table, returning the
/// inserted-row count stream.
pub(crate) async fn insert_into(
    session: &Arc<SessionContext>,
    table: &str,
    op: InsertOp,
    child: Arc<dyn ExecutionPlan>,
) -> anyhow::Result<SendableRecordBatchStream> {
    let provider = session.table_provider(table).await?;
    let state = session.state();
    let insert_plan = provider.insert_into(&state, child, op).await?;
    let stream = execute_stream(insert_plan, session.task_ctx())?;
    Ok(stream)
}

/// Replace **all** rows of an Iceberg table with the rows produced by `child`
/// (copy-on-write), then rebuild the registered provider so later scans see the
/// new data. Rejects non-Iceberg targets.
pub(crate) async fn replace_table_contents(
    session: &Arc<SessionContext>,
    table: &TableReference,
    child: Arc<dyn ExecutionPlan>,
    task_ctx: &Arc<TaskContext>,
) -> anyhow::Result<()> {
    let provider = session.table_provider(table.clone()).await?;
    let definition = provider
        .as_any()
        .downcast_ref::<beacon_iceberg::IcebergTable>()
        .map(|table| table.definition().clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Row mutations are only supported on Iceberg tables, but '{}' is not one",
                table.table()
            )
        })?;

    let stream = execute_stream(child, task_ctx.clone())?;

    let catalog = beacon_iceberg::get_catalog()?;
    beacon_iceberg::replace_table_contents(
        &catalog,
        &definition.namespace,
        &definition.name,
        stream,
        task_ctx,
    )
    .await?;

    // The registered provider caches its snapshot; rebuild it to see the replace.
    let fresh = definition
        .build_provider(session.clone(), &DATASETS_OBJECT_STORE_URL)
        .await?;
    session.register_table(table.clone(), fresh)?;

    Ok(())
}

/// Apply `ALTER TABLE` operations to an Iceberg table by mapping them to schema
/// changes and rebuilding the table under the new schema.
pub(crate) async fn alter_table(
    session: &Arc<SessionContext>,
    spec: &AlterTableSpec,
) -> anyhow::Result<()> {
    let table_ref = TableReference::parse_str(&spec.name.to_string());

    let provider = session.table_provider(table_ref.clone()).await?;
    let definition = provider
        .as_any()
        .downcast_ref::<beacon_iceberg::IcebergTable>()
        .map(|table| table.definition().clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ALTER TABLE is only supported on Iceberg tables, but '{}' is not one",
                table_ref.table()
            )
        })?;

    let mut changes = Vec::new();
    for operation in &spec.operations {
        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                changes.push(beacon_iceberg::SchemaChange::AddColumn {
                    name: column_def.name.value.clone(),
                    data_type: sql_column_type_to_arrow(column_def)?,
                });
            }
            AlterTableOperation::DropColumn { column_names, .. } => {
                for column_name in column_names {
                    changes.push(beacon_iceberg::SchemaChange::DropColumn {
                        name: column_name.value.clone(),
                    });
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                changes.push(beacon_iceberg::SchemaChange::RenameColumn {
                    from: old_column_name.value.clone(),
                    to: new_column_name.value.clone(),
                });
            }
            AlterTableOperation::AlterColumn { column_name, op } => match op {
                AlterColumnOperation::SetDataType { data_type, .. } => {
                    let column_def = SqlColumnDef {
                        name: SqlIdent::new("c"),
                        data_type: data_type.clone(),
                        options: Vec::new(),
                    };
                    changes.push(beacon_iceberg::SchemaChange::AlterColumnType {
                        name: column_name.value.clone(),
                        data_type: sql_column_type_to_arrow(&column_def)?,
                    });
                }
                other => {
                    return Err(anyhow::anyhow!("Unsupported ALTER COLUMN operation: {other}"));
                }
            },
            other => {
                return Err(anyhow::anyhow!("Unsupported ALTER TABLE operation: {other}"));
            }
        }
    }

    let catalog = beacon_iceberg::get_catalog()?;
    let store = beacon_iceberg::get_warehouse_store()?;
    beacon_iceberg::alter_table_schema(
        &catalog,
        &store,
        &definition.namespace,
        &definition.name,
        &changes,
    )
    .await?;

    // Rebuild the registered provider so subsequent queries see the new schema.
    let fresh = definition
        .build_provider(session.clone(), &DATASETS_OBJECT_STORE_URL)
        .await?;
    session.register_table(table_ref, fresh)?;

    Ok(())
}

/// Convert a SQL column definition's type to an Arrow type, reusing
/// DataFusion's full type support (no catalog access is needed).
fn sql_column_type_to_arrow(
    column_def: &SqlColumnDef,
) -> anyhow::Result<arrow::datatypes::DataType> {
    let provider = AlterTypeContextProvider::default();
    let planner = datafusion::sql::planner::SqlToRel::new(&provider);
    let schema = planner
        .build_schema(vec![column_def.clone()])
        .map_err(|error| anyhow::anyhow!("Unsupported column type: {error}"))?;
    Ok(schema.field(0).data_type().clone())
}

/// Minimal [`ContextProvider`] used only to convert SQL column types to Arrow
/// types (via `SqlToRel::build_schema`); type conversion never touches the
/// catalog, functions, or variables.
#[derive(Default)]
struct AlterTypeContextProvider {
    options: datafusion::config::ConfigOptions,
}

impl datafusion::sql::planner::ContextProvider for AlterTypeContextProvider {
    fn get_table_source(
        &self,
        name: TableReference,
    ) -> datafusion::error::Result<Arc<dyn datafusion::logical_expr::TableSource>> {
        datafusion::common::plan_err!("ALTER type conversion does not resolve table '{name}'")
    }

    fn get_function_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::ScalarUDF>> {
        None
    }

    fn get_aggregate_meta(
        &self,
        _name: &str,
    ) -> Option<Arc<datafusion::logical_expr::AggregateUDF>> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<datafusion::logical_expr::WindowUDF>> {
        None
    }

    fn get_variable_type(&self, _variable: &[String]) -> Option<arrow::datatypes::DataType> {
        None
    }

    fn options(&self) -> &datafusion::config::ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

#[cfg(test)]
mod remote_location_tests {
    use super::parse_remote_location;

    #[test]
    fn parses_beacon_scheme_into_http_endpoint_and_table() {
        let (url, table) = parse_remote_location("beacon://host:50051/obs", None).unwrap();
        assert_eq!(url, "http://host:50051");
        assert_eq!(table, "obs");
    }

    #[test]
    fn tls_option_selects_https() {
        let (url, _) =
            parse_remote_location("beacon://host:50051/obs", Some("true".to_string())).unwrap();
        assert_eq!(url, "https://host:50051");
    }

    #[test]
    fn rejects_missing_scheme_or_table() {
        assert!(parse_remote_location("host:50051/obs", None).is_err());
        assert!(parse_remote_location("beacon://host:50051", None).is_err());
    }
}
