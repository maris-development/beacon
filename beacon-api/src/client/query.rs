use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::runtime::Runtime;
use beacon_query::Query;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct Args {
    inner: String,
    output: String,
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/query",
    responses(
        (status=200, description="Response containing the query results in the format specified by the query"),
    ),
    security(
        (),
        ("basic-auth" = [])
    )
)]
pub(crate) async fn query(
    State(state): State<Arc<Runtime>>,
    Json(query_obj): Json<Query>,
) -> Result<Response<Body>, Json<String>> {
    let result = state.run_client_query(query_obj).await;

    match result {
        Ok(output) => match output.output_method {
            beacon_output::OutputMethod::Stream(stream) => {
                let inner_stream = Body::from_stream(stream);
                Ok((
                    [
                        (header::CONTENT_TYPE, output.content_type),
                        (header::CONTENT_DISPOSITION, output.content_disposition),
                    ],
                    inner_stream,
                )
                    .into_response())
            }
            beacon_output::OutputMethod::File(named_temp_file) => {
                let file = tokio::fs::File::open(named_temp_file.path()).await.unwrap();
                let stream = tokio_util::io::ReaderStream::new(file);
                let inner_stream = Body::from_stream(stream);
                Ok((
                    [
                        (header::CONTENT_TYPE, output.content_type),
                        (header::CONTENT_DISPOSITION, output.content_disposition),
                    ],
                    inner_stream,
                )
                    .into_response())
            }
        },
        Err(err) => {
            tracing::error!("Error querying beacon: {}", err);

            Err(Json(err.to_string()))
        }
    }
}

#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "query",
    post,
    path = "/api/query/available_columns",
    responses(
        (status=200, description="Response containing the available columns in the default table schema"),
    ),
    security(
        (),
        ("basic-auth" = [])
    )
)]
#[deprecated = "Use /api/default-table-schema instead"]
pub(crate) async fn available_columns(State(state): State<Arc<Runtime>>) -> Json<Vec<String>> {
    Json(
        state
            .list_default_table_schema()
            .await
            .fields()
            .iter()
            .map(|f| f.name().to_string())
            .collect(),
    )
}
