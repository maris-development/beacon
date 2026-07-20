//! The side effects performed by beacon's DDL/DML execution-plan nodes.
//!
//! Each function holds the logic that used to live in the `DFStatementHandler`
//! match arms; the exec nodes in [`super::physical`] recover the
//! [`SessionContext`] from the planner's weak cell and call these.

use std::collections::BTreeMap;
use std::sync::Arc;

use beacon_datafusion_ext::{
    listing_table_factory_ext::ListingTableFactoryExt,
    remote::{unresolved_schema, RemoteTableDefinition},
    table_ext::{MaterializedView, TableDefinition},
};
use beacon_sql_databases::{EncryptedSecret, SqlDatabaseTableDefinition, SqlEngine};
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

use super::logical::{AlterTableSpec, Mutation};
use super::materialized_view::delete_tables_prefix;

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
    let lance_location = provider.as_ref().and_then(|provider| {
        provider
            .as_any()
            .downcast_ref::<beacon_lance::LanceTable>()
            .map(|table| table.definition().location.clone())
    });

    session.deregister_table(name.clone())?;

    if let Some(prefix) = materialized_prefix {
        delete_tables_prefix(session, &prefix).await;
    }

    if let Some(location) = lance_location {
        let warehouse = lance_warehouse(session)?;
        beacon_lance::drop_lance_table(&warehouse, &location).await?;
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

    // `STORED AS POSTGRES|MYSQL` registers an external database table backed by a
    // federated connection to the source database, rather than a listing table.
    if let Some(engine) = SqlEngine::from_stored_as(&cmd.file_type) {
        return create_sql_db_table(session, cmd, engine).await;
    }

    let state = session.state();
    let factory = state
        .config()
        .get_extension::<ListingTableFactoryExt>()
        .ok_or_else(|| anyhow::anyhow!("Listing-table factory is not registered on the session"))?;
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
    let provider = definition.build_provider(session.clone()).await?;
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

    anyhow::ensure!(
        !authority.is_empty(),
        "remote table LOCATION missing host:port"
    );
    anyhow::ensure!(
        !table.is_empty(),
        "remote table LOCATION missing table name"
    );

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
    let provider = definition.build_provider(session.clone()).await?;
    session.register_table(cmd.name.clone(), provider)?;
    Ok(())
}

/// Build and register an external PostgreSQL/MySQL table from
/// `CREATE EXTERNAL TABLE … STORED AS POSTGRES LOCATION 'public.orders'
/// OPTIONS ('host' '…', 'port' '5432', 'user' '…', 'password' '…',
/// 'database' '…', 'sslmode' 'require')`.
///
/// `LOCATION` is the table name on the remote database. The password is
/// encrypted at rest with the deployment master key (`BEACON_SECRETS_KEY`);
/// creation fails closed if a password is supplied without a configured key.
async fn create_sql_db_table(
    session: &Arc<SessionContext>,
    cmd: &CreateExternalTable,
    engine: SqlEngine,
) -> anyhow::Result<()> {
    // DataFusion prefixes `OPTIONS` keys without a `.` with `format.`; strip it
    // so users write plain option names. The password is pulled out separately
    // so it is never persisted in the plaintext `options` map.
    let mut options: BTreeMap<String, String> = BTreeMap::new();
    let mut password: Option<String> = None;
    for (key, value) in &cmd.options {
        let key = key.strip_prefix("format.").unwrap_or(key);
        match key {
            "password" | "pass" => password = Some(value.clone()),
            other => {
                options.insert(other.to_string(), value.clone());
            }
        }
    }

    let remote_table = cmd.location.clone();
    anyhow::ensure!(
        !remote_table.is_empty(),
        "{} table LOCATION must be the remote table name, e.g. 'public.orders'",
        engine.as_str()
    );

    // Encrypt the password eagerly so the persisted definition holds ciphertext.
    let secret = match password {
        Some(password) => {
            let secret_store = session
                .state()
                .config()
                .get_extension::<beacon_datafusion_ext::secrets::SecretStore>()
                .ok_or_else(|| anyhow::anyhow!("secret store is unavailable"))?;
            let key = secret_store.master_key().ok_or_else(|| {
                anyhow::anyhow!(
                    "Cannot store credentials for '{}': no secrets encryption key is configured \
                     (set BEACON_SECRETS_KEY to a base64 32-byte key) to enable encrypted \
                     external-database credentials",
                    cmd.name
                )
            })?;
            Some(EncryptedSecret::encrypt(&password, key)?)
        }
        None => None,
    };

    // Honor an explicit column list if given; otherwise the schema is resolved
    // from the database when the provider is built.
    let schema = if cmd.schema.fields().is_empty() {
        beacon_sql_databases::unresolved_schema()
    } else {
        Arc::clone(cmd.schema.inner())
    };

    let definition = SqlDatabaseTableDefinition {
        name: cmd.name.to_string(),
        engine,
        remote_table,
        schema,
        options,
        secret,
    };

    // `build_provider` resolves+pins the schema; registration persists table.json.
    let provider = definition.build_provider(session.clone()).await?;
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

/// Create a managed table from `child`'s output schema and register it, using
/// the engine resolved from session/global config (Lance by default, or
/// Iceberg). For `CREATE TABLE AS SELECT` (`is_ctas`), also populate it from
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

    // Beacon-managed tables are Lance. They live in the redb (`db://`) store
    // alongside their `table.json`, so this works regardless of the datasets
    // backend (S3 only ever applies to the datasets store).
    let warehouse = lance_warehouse(session)?;
    let namespace = beacon_lance::beacon_namespace();
    let table =
        beacon_lance::create_lance_table(warehouse, &namespace, &table_name, &arrow_schema).await?;
    let provider: Arc<dyn datafusion::catalog::TableProvider> = Arc::new(table);

    // Registration persists the table's `table.json` pointer via the PersistentSchemaProvider.
    session.register_table(name.clone(), provider)?;

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

/// Apply a `DELETE`/`UPDATE` to a managed (Lance) table: native row mutations
/// (deletion vectors / fragment rewrite) when a [`Mutation`] spec was derived,
/// else copy-on-write overwrite with `child`'s rows. Rejects non-managed targets.
pub(crate) async fn replace_table_contents(
    session: &Arc<SessionContext>,
    table: &TableReference,
    child: Arc<dyn ExecutionPlan>,
    mutation: Option<Mutation>,
    task_ctx: &Arc<TaskContext>,
) -> anyhow::Result<()> {
    let provider = session.table_provider(table.clone()).await?;

    // Lance: prefer native delete/update; fall back to overwrite with the
    // surviving/updated rows. The provider reopens the latest dataset version on
    // each scan, so no rebuild is needed.
    if let Some(lance) = provider.as_any().downcast_ref::<beacon_lance::LanceTable>() {
        let location = lance.definition().location.clone();
        let warehouse = lance_warehouse(session)?;
        match mutation {
            Some(Mutation::Delete { predicate }) => {
                beacon_lance::delete_rows(&warehouse, &location, predicate.as_deref()).await?;
            }
            Some(Mutation::Update {
                predicate,
                assignments,
            }) => {
                beacon_lance::update_rows(
                    &warehouse,
                    &location,
                    predicate.as_deref(),
                    &assignments,
                )
                .await?;
            }
            None => {
                let stream = execute_stream(child, task_ctx.clone())?;
                beacon_lance::replace_table_contents(&warehouse, &location, stream).await?;
            }
        }
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "Row mutations are only supported on managed (Lance) tables, but '{}' is not one",
        table.table()
    ))
}

/// Apply `ALTER TABLE` operations to a managed (Lance) table, mapping each to a
/// native Lance schema change (each its own atomic version), then rebuild the
/// provider so its cached schema reflects the evolution.
pub(crate) async fn alter_table(
    session: &Arc<SessionContext>,
    spec: &AlterTableSpec,
) -> anyhow::Result<()> {
    let table_ref = TableReference::parse_str(&spec.name.to_string());

    let provider = session.table_provider(table_ref.clone()).await?;
    let definition = provider
        .as_any()
        .downcast_ref::<beacon_lance::LanceTable>()
        .map(|table| table.definition().clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "ALTER TABLE is only supported on managed (Lance) tables, but '{}' is not one",
                table_ref.table()
            )
        })?;

    let mut changes = Vec::new();
    for operation in &spec.operations {
        match operation {
            AlterTableOperation::AddColumn { column_def, .. } => {
                changes.push(beacon_lance::SchemaChange::AddColumn {
                    name: column_def.name.value.clone(),
                    data_type: sql_column_type_to_arrow(column_def)?,
                });
            }
            AlterTableOperation::DropColumn { column_names, .. } => {
                for column_name in column_names {
                    changes.push(beacon_lance::SchemaChange::DropColumn {
                        name: column_name.value.clone(),
                    });
                }
            }
            AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => {
                changes.push(beacon_lance::SchemaChange::RenameColumn {
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
                    changes.push(beacon_lance::SchemaChange::AlterColumnType {
                        name: column_name.value.clone(),
                        data_type: sql_column_type_to_arrow(&column_def)?,
                    });
                }
                other => {
                    return Err(anyhow::anyhow!(
                        "Unsupported ALTER COLUMN operation: {other}"
                    ));
                }
            },
            other => {
                return Err(anyhow::anyhow!(
                    "Unsupported ALTER TABLE operation: {other}"
                ));
            }
        }
    }

    let warehouse = lance_warehouse(session)?;
    beacon_lance::alter_table(&warehouse, &definition.location, &changes).await?;

    let fresh = definition.build_provider(session.clone()).await?;
    session.register_table(table_ref, fresh)?;

    Ok(())
}

/// Fetch the runtime-scoped Lance warehouse from the session config extensions.
fn lance_warehouse(
    session: &Arc<SessionContext>,
) -> anyhow::Result<Arc<beacon_lance::LanceWarehouse>> {
    session
        .state()
        .config()
        .get_extension::<beacon_lance::LanceWarehouse>()
        .ok_or_else(|| anyhow::anyhow!("Lance warehouse is unavailable"))
}

/// Resolve a managed table to the on-disk location of its Lance dataset, erroring
/// if the table is not Lance-backed (indexes are a Lance-only feature).
async fn lance_table_location(
    session: &Arc<SessionContext>,
    table: &str,
) -> anyhow::Result<String> {
    let provider = session.table_provider(table).await?;
    provider
        .as_any()
        .downcast_ref::<beacon_lance::LanceTable>()
        .map(|t| t.definition().location.clone())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Indexes are only supported on Lance-backed tables, but '{table}' is not one"
            )
        })
}

/// `CREATE INDEX [<name>] ON <table> (<column>) [USING <type>]`: build a scalar
/// index on a Lance table. `using` defaults to `btree`; `name` defaults to
/// `<table>_<column>_idx`.
pub(crate) async fn create_index(
    session: &Arc<SessionContext>,
    table: &str,
    column: &str,
    name: Option<String>,
    using: Option<String>,
) -> anyhow::Result<()> {
    let location = lance_table_location(session, table).await?;
    let kind = match using {
        Some(using) => using
            .parse::<beacon_lance::ScalarIndexKind>()
            .map_err(|e| anyhow::anyhow!(e))?,
        None => beacon_lance::ScalarIndexKind::BTree,
    };
    let index_name = name.unwrap_or_else(|| format!("{table}_{column}_idx"));
    let warehouse = lance_warehouse(session)?;
    beacon_lance::create_index(&warehouse, &location, column, &index_name, kind).await
}

/// `DROP INDEX <name> ON <table>`: drop a Lance table's index by name.
pub(crate) async fn drop_index(
    session: &Arc<SessionContext>,
    table: &str,
    name: &str,
) -> anyhow::Result<()> {
    let location = lance_table_location(session, table).await?;
    let warehouse = lance_warehouse(session)?;
    beacon_lance::drop_index(&warehouse, &location, name).await
}

/// `SHOW INDEXES ON <table>`: list a Lance table's indexes as rows of
/// `(index_name, columns)`.
pub(crate) async fn list_indexes(
    session: &Arc<SessionContext>,
    table: &str,
) -> anyhow::Result<arrow::record_batch::RecordBatch> {
    let location = lance_table_location(session, table).await?;
    let warehouse = lance_warehouse(session)?;
    let indices = beacon_lance::list_indices(&warehouse, &location).await?;

    let names = indices.iter().map(|i| i.name.clone()).collect::<Vec<_>>();
    let columns = indices
        .iter()
        .map(|i| i.columns.join(", "))
        .collect::<Vec<_>>();

    let batch = arrow::record_batch::RecordBatch::try_new(
        super::logical::show_indexes_arrow_schema(),
        vec![
            Arc::new(arrow::array::StringArray::from_iter_values(names)),
            Arc::new(arrow::array::StringArray::from_iter_values(columns)),
        ],
    )?;
    Ok(batch)
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
