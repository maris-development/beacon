//! Admin endpoints for managing crawlers (create, list, get, run, drop).

use std::sync::Arc;

use ::axum::{
    extract::{Path, State},
    http::StatusCode,
    Extension, Json,
};
use beacon_core::api::{
    CrawlReportView, CrawlerView, CreateCrawlerRequest, TableNamingView,
};
use beacon_core::AuthIdentity;
use serde_json::Value;
use crate::datalake::{
    sql::{query_rows, quote_ident, quote_literal, str_field},
    DataLake,
};

use super::bad_request;

/// Render a create request as the `CREATE CRAWLER` statement it wraps.
///
/// The typed fields map onto the statement's `WITH (...)` option keys, which is
/// the same translation the runtime used to do internally.
fn create_crawler_sql(req: &CreateCrawlerRequest) -> String {
    let mut options: Vec<(String, String)> = Vec::new();
    if let Some(formats) = &req.format_filter {
        options.push(("format".to_string(), formats.join(",")));
    }
    options.push((
        "detect_partitions".to_string(),
        req.detect_partitions.to_string(),
    ));
    options.push(("event_driven".to_string(), req.event_driven.to_string()));
    if let Some(secs) = req.schedule_secs {
        // The parser accepts a duration; seconds is the unambiguous spelling.
        options.push(("schedule".to_string(), format!("{secs}s")));
    }
    options.push((
        "table_naming".to_string(),
        match req.table_naming {
            TableNamingView::LeafPrefix => "leaf_prefix".to_string(),
            TableNamingView::CrawlerPrefixed => "crawler_prefixed".to_string(),
        },
    ));
    // Caller-supplied format options last, sorted so the statement is stable.
    let mut extra: Vec<(&String, &String)> = req.options.iter().collect();
    extra.sort();
    options.extend(extra.into_iter().map(|(k, v)| (k.clone(), v.clone())));

    let rendered: Vec<String> = options
        .iter()
        .map(|(k, v)| format!("{} {}", quote_literal(k), quote_literal(v)))
        .collect();

    format!(
        "CREATE CRAWLER {} ON {} WITH ({})",
        quote_ident(&req.name),
        quote_literal(&req.target_prefix),
        rendered.join(", ")
    )
}

/// Map one `SHOW CRAWLERS` row back to the API view.
fn crawler_view(row: &Value) -> CrawlerView {
    let format_filter = match str_field(row, "format_filter") {
        "" => None,
        list => Some(list.split(',').map(|s| s.trim().to_string()).collect()),
    };
    CrawlerView {
        name: str_field(row, "name").to_string(),
        target_prefix: str_field(row, "target_prefix").to_string(),
        format_filter,
        table_naming: match str_field(row, "table_naming") {
            "crawler_prefixed" => TableNamingView::CrawlerPrefixed,
            _ => TableNamingView::LeafPrefix,
        },
        detect_partitions: row
            .get("detect_partitions")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        schedule_secs: row.get("schedule_secs").and_then(Value::as_u64),
        event_driven: row
            .get("event_driven")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        options: serde_json::from_str(str_field(row, "options")).unwrap_or_default(),
    }
}

/// Every defined crawler, via `SHOW CRAWLERS`.
async fn show_crawlers(
    state: &Arc<DataLake>,
    identity: AuthIdentity,
) -> anyhow::Result<Vec<CrawlerView>> {
    let rows = query_rows(state, "SHOW CRAWLERS", identity).await?;
    Ok(rows.iter().map(crawler_view).collect())
}

/// Defines (or replaces) a crawler and starts its triggers.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/crawlers",
    request_body = CreateCrawlerRequest,
    responses(
        (status = 200, description = "Crawler created"),
        (status = 400, description = "Invalid crawler definition")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn create_crawler(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(req): Json<CreateCrawlerRequest>,
) -> Result<(), (StatusCode, String)> {
    crate::datalake::sql::execute(&state, create_crawler_sql(&req), identity)
        .await
        .map_err(bad_request)
}

/// Lists all defined crawlers.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/crawlers",
    responses((status = 200, description = "List of crawlers", body = [CrawlerView])),
    security(("basic-auth" = []))
)]
pub(crate) async fn list_crawlers(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
) -> Json<Vec<CrawlerView>> {
    Json(show_crawlers(&state, identity).await.unwrap_or_default())
}

/// Returns a single crawler by name, or 404 if it is not defined.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    get,
    path = "/api/admin/crawlers/{name}",
    params(("name" = String, Path, description = "Crawler name")),
    responses(
        (status = 200, description = "Crawler definition", body = CrawlerView),
        (status = 404, description = "Crawler not found")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn get_crawler(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(name): Path<String>,
) -> Result<Json<CrawlerView>, (StatusCode, String)> {
    // `SHOW CRAWLERS` has no name filter, so the single-crawler lookup selects
    // from the full listing. The set is small and admin-only.
    let crawlers = show_crawlers(&state, identity).await.map_err(bad_request)?;
    match crawlers.into_iter().find(|c| c.name == name) {
        Some(crawler) => Ok(Json(crawler)),
        None => Err((StatusCode::NOT_FOUND, format!("crawler '{name}' not found"))),
    }
}

/// Runs a crawler once on demand and returns its report.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/crawlers/{name}/run",
    params(("name" = String, Path, description = "Crawler name")),
    responses(
        (status = 200, description = "Crawl report", body = CrawlReportView),
        (status = 400, description = "Crawler does not exist or failed to run")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn run_crawler(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(name): Path<String>,
) -> Result<Json<CrawlReportView>, (StatusCode, String)> {
    let rows = query_rows(
        &state,
        format!("RUN CRAWLER {}", quote_ident(&name)),
        identity,
    )
    .await
    .map_err(bad_request)?;

    let row = rows
        .first()
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "crawl produced no report".to_string()))?;

    let list = |key: &str| -> Vec<String> {
        serde_json::from_str(str_field(row, key)).unwrap_or_default()
    };

    Ok(Json(CrawlReportView {
        crawler: str_field(row, "crawler").to_string(),
        discovered: row.get("discovered").and_then(Value::as_u64).unwrap_or(0) as usize,
        created: list("created"),
        updated: list("updated"),
        skipped: list("skipped"),
        failed: serde_json::from_str(str_field(row, "failed")).unwrap_or_default(),
        skipped_files: row
            .get("skipped_files")
            .and_then(Value::as_u64)
            .unwrap_or(0) as usize,
    }))
}

/// Removes a crawler definition and stops its triggers. Crawled tables are left in place.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    delete,
    path = "/api/admin/crawlers/{name}",
    params(("name" = String, Path, description = "Crawler name")),
    responses(
        (status = 200, description = "Crawler dropped"),
        (status = 400, description = "Crawler does not exist")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn drop_crawler(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Path(name): Path<String>,
) -> Result<(), (StatusCode, String)> {
    crate::datalake::sql::execute(
        &state,
        format!("DROP CRAWLER {}", quote_ident(&name)),
        identity,
    )
    .await
    .map_err(bad_request)
}
