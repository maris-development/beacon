//! Admin endpoint for creating external tables from structured fields.

use std::sync::Arc;

use ::axum::{extract::State, http::StatusCode, Extension, Json};
use beacon_core::api::CreateExternalTableRequest;
use beacon_core::AuthIdentity;
use crate::datalake::{
    sql::{execute, quote_ident, quote_literal},
    DataLake,
};

use super::bad_request;

/// Render the request as the `CREATE EXTERNAL TABLE` statement it has always been
/// equivalent to. Identifiers and literals are quoted, so a name or location
/// containing a quote cannot break out of the statement.
fn create_external_table_sql(req: &CreateExternalTableRequest) -> String {
    let mut sql = String::from("CREATE EXTERNAL TABLE ");
    if req.if_not_exists {
        sql.push_str("IF NOT EXISTS ");
    }
    sql.push_str(&quote_ident(&req.name));
    sql.push_str(" STORED AS ");
    // The storage type is a bare SQL keyword (PARQUET, CSV, DELTA, …), not an
    // identifier, so it is constrained to alphanumerics rather than quoted.
    sql.push_str(
        &req.file_type
            .chars()
            .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect::<String>(),
    );

    if !req.partition_cols.is_empty() {
        let cols: Vec<String> = req.partition_cols.iter().map(|c| quote_ident(c)).collect();
        sql.push_str(&format!(" PARTITIONED BY ({})", cols.join(", ")));
    }

    sql.push_str(&format!(" LOCATION {}", quote_literal(&req.location)));

    if !req.options.is_empty() {
        // Sorted so the generated statement is deterministic.
        let mut options: Vec<(&String, &String)> = req.options.iter().collect();
        options.sort();
        let rendered: Vec<String> = options
            .iter()
            .map(|(k, v)| format!("{} {}", quote_literal(k), quote_literal(v)))
            .collect();
        sql.push_str(&format!(" OPTIONS ({})", rendered.join(", ")));
    }

    sql
}

/// Creates an external table over files in the datasets store. The runtime
/// assembles and runs the equivalent `CREATE EXTERNAL TABLE` statement, so all
/// `STORED AS` variants and catalog persistence behave exactly as the SQL form.
#[tracing::instrument(level = "info", skip(state))]
#[utoipa::path(
    tag = "admin",
    post,
    path = "/api/admin/external-tables",
    request_body = CreateExternalTableRequest,
    responses(
        (status = 200, description = "External table created"),
        (status = 400, description = "Invalid request or registration failed")
    ),
    security(("basic-auth" = []))
)]
pub(crate) async fn create_external_table(
    State(state): State<Arc<DataLake>>,
    Extension(identity): Extension<AuthIdentity>,
    Json(req): Json<CreateExternalTableRequest>,
) -> Result<(), (StatusCode, String)> {
    execute(&state, create_external_table_sql(&req), identity)
        .await
        .map_err(bad_request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn request() -> CreateExternalTableRequest {
        CreateExternalTableRequest {
            name: "obs".to_string(),
            location: "obs/".to_string(),
            file_type: "PARQUET".to_string(),
            partition_cols: Vec::new(),
            options: HashMap::new(),
            if_not_exists: false,
        }
    }

    #[test]
    fn renders_the_minimal_statement() {
        assert_eq!(
            create_external_table_sql(&request()),
            r#"CREATE EXTERNAL TABLE "obs" STORED AS PARQUET LOCATION 'obs/'"#
        );
    }

    #[test]
    fn renders_partitions_options_and_if_not_exists() {
        let mut req = request();
        req.if_not_exists = true;
        req.partition_cols = vec!["year".to_string(), "month".to_string()];
        req.options = HashMap::from([("compression".to_string(), "zstd".to_string())]);
        assert_eq!(
            create_external_table_sql(&req),
            r#"CREATE EXTERNAL TABLE IF NOT EXISTS "obs" STORED AS PARQUET PARTITIONED BY ("year", "month") LOCATION 'obs/' OPTIONS ('compression' 'zstd')"#
        );
    }

    /// A quote in a user-supplied name or location is escaped rather than
    /// terminating the identifier/literal.
    #[test]
    fn quotes_are_escaped_not_injected() {
        let mut req = request();
        req.name = r#"ev"il"#.to_string();
        req.location = "it's/".to_string();
        let sql = create_external_table_sql(&req);
        assert!(sql.contains(r#""ev""il""#), "{sql}");
        assert!(sql.contains("'it''s/'"), "{sql}");
    }

    /// The storage type is interpolated as a bare keyword, so anything that is
    /// not alphanumeric is stripped instead of reaching the parser.
    #[test]
    fn file_type_is_reduced_to_a_bare_keyword() {
        let mut req = request();
        req.file_type = "PARQUET; DROP TABLE t".to_string();
        assert!(create_external_table_sql(&req).contains("STORED AS PARQUETDROPTABLEt"));
    }
}
