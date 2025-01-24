use axum::{
    body::Body,
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use beacon_core::output;

#[tracing::instrument]
#[utoipa::path(
    post,
    path = "/query",
    responses(
        (status=200, description="Response containing the query results in the format specified by the query"),
    )
)]
pub(crate) async fn query() -> Result<Response<Body>, Json<String>> {
    let result = beacon_core::query().await;

    match result {
        Ok(output) => match output.output_method {
            output::OutputMethod::Stream(stream) => {
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
            output::OutputMethod::File(named_temp_file) => {
                let file = named_temp_file.into_file();
                let file = tokio::fs::File::from_std(file);
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
