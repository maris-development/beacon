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
    #[serde(flatten)]
    inner: Arc<dyn BeaconTable>,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    post, 
    path = "/api/admin/create-table", 
    responses((status = 200, description = "Create a new table")),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    ))
]
pub(crate) async fn create_table(
    State(state): State<Arc<Runtime>>,
    Json(create_table): Json<CreateTable>,
) -> Result<(StatusCode,String), Json<String>> {
    let result = state.add_table(create_table.inner.clone()).await;
    match result {
        Ok(_) => Ok((StatusCode::OK, format!("Table: {} was created", create_table.inner.table_name()))),
        Err(err) => {
            tracing::error!("Error creating table: {:?}", err);
            Err(Json(format!("Error creating table: {:?}", err)))
        }
    }
}


#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema, IntoParams)]
pub struct DeleteTable {
    table_name: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "tables",
    delete, 
    path = "/api/admin/delete-table", 
    params(DeleteTable),
    responses((status = 200, description = "Delete a table")),
    security(
        ("basic-auth" = []),
        ("bearer" = [])
    ))
]
pub(crate) async fn delete_table(
    State(state): State<Arc<Runtime>>,
    Query(delete_table): Query<DeleteTable>,
) -> Json<Vec<String>> {
    Json(vec!["Table deleted".to_string()])
}