//! Removes the server's own data paths from error responses.
//!
//! A storage error names the absolute path it failed on, for example
//! `Unable to open file C:\srv\beacon\data\datasets\grid.zarr`. That path tells an
//! anonymous client how the server is laid out. The middleware here rewrites every
//! error body so a dataset path is relative to the datasets store, as the client wrote
//! it, and any other data path is relative to the data directory.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use ::axum::{
    body::{to_bytes, Body},
    extract::{Request, State},
    http::header::CONTENT_LENGTH,
    middleware::Next,
    response::Response,
};

/// Rewrites the paths under the configured data directories in a text.
#[derive(Debug, Default)]
pub struct PathRedactor {
    /// `(needle, replacement)`, longest needle first.
    rules: Vec<(String, String)>,
}

/// The largest error body the middleware reads. Error bodies are a message or two.
const MAX_ERROR_BODY_BYTES: usize = 1024 * 1024;

impl PathRedactor {
    /// A redactor for the datasets store `datasets` and the data directory `data`.
    ///
    /// A path under `datasets` loses that prefix; a path elsewhere under `data` loses the
    /// `data` prefix. The bare directories become `<datasets>` and `<data>`.
    pub fn new(datasets: &Path, data: &Path) -> Self {
        let mut rules = Vec::new();
        for (root, label) in [(datasets, "<datasets>"), (data, "<data>")] {
            for (form, separator) in spellings(root) {
                rules.push((format!("{form}{separator}"), String::new()));
                rules.push((form, label.to_string()));
            }
        }
        // The longest needle first, so the datasets store wins over the data directory
        // that holds it, and a prefix with its separator wins over the bare directory.
        rules.sort_by(|a, b| b.0.len().cmp(&a.0.len()));
        rules.dedup();
        Self { rules }
    }

    /// A redactor for the data paths of `config`.
    pub fn from_config(config: &beacon_server_config::Config) -> Self {
        let datasets = &config.data.datasets;
        let data = datasets.parent().unwrap_or(datasets);
        Self::new(datasets, data)
    }

    /// The text with every configured path rewritten.
    pub fn redact(&self, text: &str) -> String {
        let mut text = text.to_string();
        for (needle, replacement) in &self.rules {
            if text.contains(needle.as_str()) {
                text = text.replace(needle.as_str(), replacement);
            }
        }
        text
    }
}

/// Each spelling of `root` a message can carry, with the separator that follows it.
///
/// A message holds the path as configured or as the filesystem resolved it, with either
/// separator on Windows, and with doubled backslashes inside a JSON string.
fn spellings(root: &Path) -> Vec<(String, &'static str)> {
    let mut bases: Vec<PathBuf> = Vec::new();
    if let Ok(absolute) = std::path::absolute(root) {
        bases.push(absolute);
    }
    if let Ok(canonical) = std::fs::canonicalize(root) {
        bases.push(canonical);
    }

    let mut spellings = Vec::new();
    for base in bases {
        let base = base.to_string_lossy();
        // `canonicalize` gives a verbatim `\\?\C:\...` path on Windows.
        let base = base.strip_prefix(r"\\?\").unwrap_or(&base);
        let base = base.trim_end_matches(['/', '\\']);
        // The filesystem root trims to nothing, and an empty needle matches everywhere.
        if base.is_empty() {
            continue;
        }
        let backslashed = base.replace('/', "\\");
        spellings.push((base.replace('\\', "/"), "/"));
        spellings.push((backslashed.replace('\\', "\\\\"), "\\\\"));
        spellings.push((backslashed, "\\"));
    }
    spellings
}

/// Rewrites the data paths in the body of every 4xx and 5xx response.
pub async fn redact_error_paths(
    State(redactor): State<Arc<PathRedactor>>,
    request: Request,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    let status = response.status();
    if !(status.is_client_error() || status.is_server_error()) {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, MAX_ERROR_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(error) => {
            tracing::warn!(%error, "could not read an error body to redact it");
            parts.headers.remove(CONTENT_LENGTH);
            return Response::from_parts(parts, Body::from("the error response could not be read"));
        }
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        return Response::from_parts(parts, Body::from(bytes));
    };

    let redacted = redactor.redact(text);
    if redacted == text {
        return Response::from_parts(parts, Body::from(bytes));
    }
    parts.headers.remove(CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(redacted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::axum::{http::StatusCode, routing::get, Router};
    use tower::ServiceExt;

    fn redactor(data_dir: &Path) -> PathRedactor {
        PathRedactor::new(&data_dir.join("datasets"), data_dir)
    }

    #[test]
    fn a_dataset_path_becomes_relative_to_the_datasets_store() {
        let data = tempfile::tempdir().unwrap();
        let file = data.path().join("datasets").join("grid.zarr");
        let text = format!("Unable to open file {}: denied", file.display());

        assert_eq!(
            redactor(data.path()).redact(&text),
            "Unable to open file grid.zarr: denied"
        );
    }

    #[test]
    fn another_data_path_becomes_relative_to_the_data_directory() {
        let data = tempfile::tempdir().unwrap();
        let file = data.path().join("tmp").join("out.csv");
        let text = format!("cannot write {}", file.display());

        let redacted = redactor(data.path()).redact(&text);

        assert!(
            !redacted.contains(&data.path().display().to_string()),
            "{redacted}"
        );
        assert!(redacted.contains("out.csv"), "{redacted}");
    }

    #[test]
    fn a_json_escaped_path_is_rewritten() {
        let data = tempfile::tempdir().unwrap();
        let file = data.path().join("datasets").join("a").join("b.nc");
        let json = serde_json::to_string(&format!("failed on {}", file.display())).unwrap();

        let redacted = redactor(data.path()).redact(&json);

        let message: String = serde_json::from_str(&redacted).expect("still valid JSON");
        assert!(message.starts_with("failed on a"), "{message}");
        assert!(message.ends_with("b.nc"), "{message}");
    }

    #[test]
    fn the_bare_directories_get_a_label() {
        let data = tempfile::tempdir().unwrap();
        let text = format!("walk failed at {}", data.path().join("datasets").display());

        assert_eq!(
            redactor(data.path()).redact(&text),
            "walk failed at <datasets>"
        );
    }

    /// The filesystem root trims to an empty needle, which would match between every character.
    #[test]
    fn a_root_data_directory_leaves_the_text_readable() {
        let root = Path::new("/");
        let text = "Schema error: a/b is not a column";

        assert_eq!(PathRedactor::new(root, root).redact(text), text);
    }

    #[test]
    fn a_text_without_data_paths_is_unchanged() {
        let data = tempfile::tempdir().unwrap();
        let text = "Schema error: No field named nope";

        assert_eq!(redactor(data.path()).redact(text), text);
    }

    #[tokio::test]
    async fn the_middleware_rewrites_an_error_body_and_leaves_a_success_alone() {
        let data = tempfile::tempdir().unwrap();
        let path = data.path().join("datasets").join("grid.zarr");
        let error_body = format!("Unable to open file {}", path.display());
        let ok_body = error_body.clone();
        let router = Router::new()
            .route(
                "/error",
                get(move || async move { (StatusCode::BAD_REQUEST, error_body) }),
            )
            .route("/ok", get(move || async move { ok_body }))
            .layer(::axum::middleware::from_fn_with_state(
                Arc::new(redactor(data.path())),
                redact_error_paths,
            ));

        let body = |response: Response| async move {
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            String::from_utf8(bytes.to_vec()).unwrap()
        };
        let error = router
            .clone()
            .oneshot(Request::get("/error").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body(error).await, "Unable to open file grid.zarr");

        let ok = router
            .oneshot(Request::get("/ok").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(body(ok).await.contains(&data.path().display().to_string()));
    }
}
