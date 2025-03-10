use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use beacon_core::{runtime::Runtime, tables::table::BeaconTable};
use utoipa::{IntoParams, ToSchema};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CreateTable {
    #[schema(value_type = Object)]
    inner: Arc<dyn BeaconTable>,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    post, 
    path = "/api/admin/create-table", 
    responses((status = 200, description = "Create a new table")),
    security(
        ("basic-auth" = [])
    ))
]
pub(crate) async fn create_table(
    State(state): State<Arc<Runtime>>,
    Json(create_table): Json<CreateTable>,
) -> Json<Vec<String>> {
    todo!()
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct DeleteTable {
    table_name: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    delete, 
    path = "/api/admin/delete-table", 
    params(DeleteTable),
    responses((status = 200, description = "Delete a table")),
    security(
        ("basic-auth" = [])
    ))
]
pub(crate) async fn delete_table(
    State(state): State<Arc<Runtime>>,
    Query(delete_table): Query<DeleteTable>,
) -> Json<Vec<String>> {
    Json(vec!["Table deleted".to_string()])
}