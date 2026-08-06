//! Read-only table discovery endpoints (listing, schema, configuration).

use std::sync::Arc;

use ::axum::{
    extract::{Query, State},
    http::StatusCode,
    Extension, Json,
};
use beacon_core::extensions::TableExtensions;
use beacon_core::AuthIdentity;
use crate::server::{catalog, Server};
use utoipa::{IntoParams, ToSchema};

/// Returns the names of all tables registered in the runtime catalog.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/tables",
    responses((status = 200, description = "List of registered table names", body = Vec<String>)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_tables(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<Vec<String>> {
    Json(
        catalog::list_table_names(&state, identity)
            .await
            .unwrap_or_default(),
    )
}

/// A table as it appears in a catalog listing.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub(crate) struct SchemaTableView {
    /// Table name, unqualified.
    name: String,
    /// `BASE TABLE`, `VIEW`, … as `information_schema` reports it.
    table_type: String,
}

/// A schema and the tables it contains.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub(crate) struct CatalogSchemaView {
    /// Schema name (e.g. `public`, `system`, `information_schema`).
    name: String,
    /// The schema's tables, sorted by name.
    tables: Vec<SchemaTableView>,
}

/// A catalog and the schemas it contains.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub(crate) struct CatalogView {
    /// Catalog name (e.g. `beacon`, or the name a remote was attached under).
    name: String,
    /// The catalog's schemas, sorted by name.
    schemas: Vec<CatalogSchemaView>,
}

/// The full catalog tree, plus the catalog and schema an unqualified table name
/// resolves against.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub(crate) struct CatalogsView {
    /// The catalog an unqualified table name resolves against.
    default_catalog: String,
    /// The schema an unqualified table name resolves against.
    default_schema: String,
    /// Every catalog visible to the caller, sorted by name.
    catalogs: Vec<CatalogView>,
}

/// Returns every catalog, schema, and table visible to the caller.
///
/// Unlike `/api/tables` — which lists only the tables in the default schema —
/// this covers the whole namespace, so a client can browse it: for the
/// super-user that includes beacon's `system` schema, `information_schema`, and
/// any attached remote catalog. Other callers see neither metadata schema, and
/// only the tables their roles grant `SELECT` on.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/catalogs",
    responses((status = 200, description = "The catalog/schema/table tree", body = CatalogsView)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_catalogs(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<CatalogsView> {
    let entries = catalog::list_catalog_tables(&state, identity)
        .await
        .unwrap_or_default();
    let (default_catalog, default_schema) = catalog::default_catalog_and_schema(&state);

    // The rows arrive ordered by catalog, then schema, then table, so grouping is
    // a single pass that appends to the last group whenever the key repeats.
    let mut catalogs: Vec<CatalogView> = Vec::new();
    for entry in entries {
        if catalogs.last().map(|c| c.name.as_str()) != Some(entry.catalog.as_str()) {
            catalogs.push(CatalogView {
                name: entry.catalog,
                schemas: Vec::new(),
            });
        }
        let schemas = &mut catalogs.last_mut().expect("just pushed").schemas;
        if schemas.last().map(|s| s.name.as_str()) != Some(entry.schema.as_str()) {
            schemas.push(CatalogSchemaView {
                name: entry.schema,
                tables: Vec::new(),
            });
        }
        schemas
            .last_mut()
            .expect("just pushed")
            .tables
            .push(SchemaTableView {
                name: entry.table,
                table_type: entry.table_type,
            });
    }

    Json(CatalogsView {
        default_catalog,
        default_schema,
        catalogs,
    })
}

/// Response entry pairing a table name with its Arrow schema fields.
#[derive(Debug, Clone, serde::Serialize, ToSchema)]
pub(crate) struct TableWithSchema {
    /// Registered table name.
    table_name: String,
    /// The table's Arrow fields, as Arrow serializes them: `name`, `data_type`,
    /// `nullable`, `metadata` (see [`crate::api::SCHEMA_RESPONSE`]).
    #[schema(value_type = Vec<Object>)]
    columns: Vec<arrow::datatypes::FieldRef>,
}

/// Returns every registered table along with its Arrow schema fields.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/tables-with-schema",
    responses((status = 200, description = "Registered tables with their Arrow schemas", body = Vec<TableWithSchema>)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_tables_with_schema(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<Vec<TableWithSchema>> {
    let table_names = catalog::list_table_names(&state, identity.clone())
        .await
        .unwrap_or_default();
    let mut result = Vec::new();
    for table_name in table_names {
        let table = catalog::table_reference(None, None, &table_name);
        if let Ok(Some(schema)) = catalog::table_schema(&state, table, identity.clone()).await {
            result.push(TableWithSchema {
                table_name,
                columns: schema.fields().to_vec(),
            });
        }
    }

    Json(result)
}

/// Query parameters for [`list_table_schema`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableSchemaQuery {
    /// Name of the registered table to inspect.
    pub table_name: String,
    /// Catalog the table lives in. Defaults to the session's default catalog.
    /// Only used together with `schema`.
    pub catalog: Option<String>,
    /// Schema the table lives in. Defaults to the session's default schema.
    pub schema: Option<String>,
}

/// Returns the Arrow schema of the named table, or 404 if the table is not registered.
///
/// The table is resolved in the session's default catalog and schema unless
/// `catalog`/`schema` name another one.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get, 
    path = "/api/table-schema",
    params(ListTableSchemaQuery),
    responses(
        (status = 200, description = "The Arrow schema of the table", body = Object),
        (status = 404, description = "Table not found"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_schema(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(query): Query<ListTableSchemaQuery>,
) -> Result<Json<arrow::datatypes::SchemaRef>, (StatusCode, String)> {
    let table = catalog::table_reference(
        query.catalog.as_deref(),
        query.schema.as_deref(),
        &query.table_name,
    );
    let result = catalog::table_schema(&state, table.clone(), identity)
        .await
        .unwrap_or(None);

    match result {
        Some(schema) => Ok(Json(schema)),
        None => {
            tracing::error!("Error listing table schema: table not found");
            Err((StatusCode::NOT_FOUND, format!("Table {table} not found")))
        }
    }
}

/// Query parameters for [`list_table_extensions`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct ListTableExtensionsQuery {
    /// Name of the registered table whose extensions to return.
    pub table_name: String,
}

/// Returns the downstream extensions (MCP descriptor, query presets) attached to
/// the named table, or 404 if the table is not registered. A table with no
/// extensions returns an empty object.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/table-extensions",
    params(ListTableExtensionsQuery),
    responses(
        (status = 200, description = "The table's extensions", body = TableExtensions),
        (status = 404, description = "Table not found"),
    ),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn list_table_extensions(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
    Query(query): Query<ListTableExtensionsQuery>,
) -> Result<Json<TableExtensions>, (StatusCode, String)> {
    match catalog::table_extensions(&state, &query.table_name, identity).await {
        Ok(extensions) => Ok(Json(extensions)),
        Err(error) => {
            tracing::error!(?error, "error listing table extensions");
            Err((
                StatusCode::NOT_FOUND,
                format!("Table {} not found", query.table_name),
            ))
        }
    }
}

/// Returns the Arrow schema of the runtime's default table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/default-table-schema",
    responses((status = 200, description = "The Arrow schema of the default table", body = Object)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn default_table_schema(
    State(state): State<Arc<Server>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<arrow::datatypes::SchemaRef> {
    let table = catalog::table_reference(None, None, &catalog::default_table(&state));
    let schema = catalog::table_schema(&state, table, identity)
        .await
        .unwrap_or(None)
        .unwrap_or_else(|| Arc::new(arrow::datatypes::Schema::empty()));
    Json(schema)
}

/// Returns the name of the runtime's default table.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    get,
    path = "/api/default-table",
    responses((status = 200, description = "Name of the default table", body = String)),
    security(
        (),
        ("basic-auth" = []),
        ("bearer" = [])
    )
)]
pub(crate) async fn default_table(State(state): State<Arc<Server>>) -> Json<String> {
    Json(catalog::default_table(&state))
}