//! The MCP tool catalog and the dispatch of tool calls.
//!
//! The tool set is fixed: `list_tables`, `describe_table`, `run_sql` and
//! `export_query`. A table needs no MCP configuration. The agent sees every table
//! that the caller may read, and it learns what each table and column means from
//! their comments (`COMMENT ON`), which arrive as Arrow schema metadata.

use std::sync::Arc;

use arrow::array::AsArray;
use arrow::datatypes::{Field, Schema, SchemaRef};
use beacon_core::comments::COMMENT_METADATA_KEY;
use beacon_core::runtime::Runtime;
use beacon_core::{AuthIdentity, TableReference};
use rmcp::model::{Tool, ToolAnnotations};
use serde_json::{json, Map, Value};

use crate::result::run_sql_to_json;

/// The tables in beacon's own schema that `identity` is entitled to see, sorted.
///
/// The enumeration cannot be run as `identity` — `information_schema` is
/// super-user-only — so it goes through `Runtime::visible_tables`, which reads
/// the catalog as the engine and returns only the tables that identity's roles
/// grant `Select` on.
async fn list_table_names(
    runtime: &Arc<Runtime>,
    identity: &AuthIdentity,
) -> anyhow::Result<Vec<String>> {
    let (default_catalog, default_schema) = runtime.default_catalog_and_schema();
    let batches = runtime.visible_tables(identity).await?;

    let mut names = Vec::new();
    for batch in &batches {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .and_then(|array| array.as_string_opt::<i32>())
        };
        let (Some(catalogs), Some(schemas), Some(tables)) = (
            column("table_catalog"),
            column("table_schema"),
            column("table_name"),
        ) else {
            anyhow::bail!("the catalog listing is missing an expected column");
        };
        for row in 0..batch.num_rows() {
            if catalogs.value(row) == default_catalog && schemas.value(row) == default_schema {
                names.push(tables.value(row).to_string());
            }
        }
    }
    Ok(names)
}

/// A table's Arrow schema with its comments, or `None` when the table is
/// unknown (or `identity` may not read it).
async fn table_schema(
    runtime: &Arc<Runtime>,
    table: &str,
    identity: &AuthIdentity,
) -> Option<SchemaRef> {
    runtime
        .table_arrow_schema(TableReference::bare(table.to_string()), identity)
        .await
        .ok()
}

/// The fixed tool list.
pub fn tools() -> Vec<Tool> {
    vec![
        list_tables_tool(),
        describe_table_tool(),
        run_sql_tool(),
        export_query_tool(),
    ]
}

/// Route a tool call to its handler.
pub async fn dispatch(
    runtime: &Arc<Runtime>,
    name: &str,
    args: Map<String, Value>,
    identity: AuthIdentity,
) -> anyhow::Result<String> {
    match name {
        "list_tables" => list_tables_json(runtime, &identity).await,
        "describe_table" => describe_table_json(runtime, &args, &identity).await,
        "run_sql" => {
            let sql = args
                .get("sql")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing required 'sql' argument"))?;
            run_sql_to_json(runtime, sql.to_string(), identity).await
        }
        "export_query" => export_query_recipe(&args),
        other => anyhow::bail!("unknown tool '{other}'"),
    }
}

fn object_schema(props: Value, required: &[&str]) -> Map<String, Value> {
    let mut schema = Map::new();
    schema.insert("type".into(), json!("object"));
    schema.insert("properties".into(), props);
    if !required.is_empty() {
        schema.insert("required".into(), json!(required));
    }
    schema
}

/// Mark a tool as read-only via the MCP `Tool.annotations.readOnlyHint`, so
/// clients know it never mutates state. Every beacon MCP tool is read-only.
fn read_only(tool: Tool) -> Tool {
    tool.with_annotations(ToolAnnotations::new().read_only(true))
}

fn list_tables_tool() -> Tool {
    read_only(Tool::new(
        "list_tables",
        "List the tables you can read, each with its description.",
        object_schema(json!({}), &[]),
    ))
}

fn describe_table_tool() -> Tool {
    read_only(Tool::new(
        "describe_table",
        "Return a table's description and its columns, each with name, data type, \
         nullability and description. Call it before you write SQL for a table.",
        object_schema(
            json!({ "table_name": { "type": "string", "description": "Name of the table." } }),
            &["table_name"],
        ),
    ))
}

fn run_sql_tool() -> Tool {
    read_only(Tool::new(
        "run_sql",
        "Run a read-only SQL query (SELECT only) and return JSON rows. This is a bounded \
         PREVIEW: at most 1000 rows are returned; if the result is larger it is truncated \
         (the response sets \"truncated\": true). For complete or large results — anything you \
         intend to analyze in full or hand to a script — use export_query to fetch a \
         Parquet/Arrow/CSV file instead of run_sql.",
        object_schema(
            json!({ "sql": { "type": "string", "description": "A read-only SELECT statement." } }),
            &["sql"],
        ),
    ))
}

fn export_query_tool() -> Tool {
    read_only(Tool::new(
        "export_query",
        "Build a recipe to export a large read-only SELECT as a Parquet/Arrow/CSV file for use \
         in a Python script. Returns the exact /api/query request plus a ready-to-run Python \
         snippet; it does NOT run the query or return rows. Prefer this over run_sql when the \
         result is large.",
        object_schema(
            json!({
                "sql": { "type": "string", "description": "A read-only SELECT statement to export." },
                "format": {
                    "type": "string",
                    "enum": ["parquet", "arrow", "csv"],
                    "description": "Output file format (default parquet)."
                }
            }),
            &["sql"],
        ),
    ))
}

/// Build a "fetch recipe" for exporting a query as a file. MCP tool results are
/// model-context text, so we never stream the (potentially huge) file through the
/// model: instead we return the exact `/api/query` request and a Python snippet
/// the agent can drop into a script, which fetches the Parquet/Arrow/CSV directly.
fn export_query_recipe(args: &Map<String, Value>) -> anyhow::Result<String> {
    let sql = args
        .get("sql")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing required 'sql' argument"))?
        .trim();
    // MCP is read-only: only allow SELECT / WITH (CTE) exports.
    let head = sql.split_whitespace().next().unwrap_or("").to_ascii_uppercase();
    anyhow::ensure!(
        matches!(head.as_str(), "SELECT" | "WITH"),
        "export_query only supports read-only SELECT queries"
    );
    let format = args.get("format").and_then(Value::as_str).unwrap_or("parquet");
    anyhow::ensure!(
        matches!(format, "parquet" | "arrow" | "csv"),
        "unsupported format '{format}'; expected one of: parquet, arrow, csv"
    );

    let body = json!({ "sql": sql, "output": { "format": format } });
    let body_py = serde_json::to_string(&body)?;
    let (imports, reader) = match format {
        "parquet" => ("import io, requests, pandas as pd", "df = pd.read_parquet(io.BytesIO(resp.content))"),
        "csv" => ("import io, requests, pandas as pd", "df = pd.read_csv(io.BytesIO(resp.content))"),
        "arrow" => (
            "import io, requests, pyarrow.ipc as pa_ipc",
            "df = pa_ipc.open_file(io.BytesIO(resp.content)).read_all().to_pandas()",
        ),
        _ => unreachable!(),
    };
    let python = [
        imports.to_string(),
        "BEACON_URL = \"http://localhost:5001\"  # your beacon host".to_string(),
        "AUTH = \"Bearer <token>\"  # or \"Basic <base64 user:pass>\"; omit header if anonymous".to_string(),
        format!(
            "resp = requests.post(f\"{{BEACON_URL}}/api/query\", headers={{\"Authorization\": AUTH}}, json={body_py})"
        ),
        "resp.raise_for_status()".to_string(),
        reader.to_string(),
        "print(df.shape)".to_string(),
    ]
    .join("\n");

    let recipe = json!({
        "note": "This does not run the query. POST `request.body` to <BEACON_URL>/api/query; the response body IS the file. Send the same Authorization you use for MCP (Basic/Bearer), or omit it for anonymous access.",
        "format": format,
        "request": {
            "method": "POST",
            "path": "/api/query",
            "headers": {
                "Content-Type": "application/json",
                "Authorization": "<same credential as MCP; omit if anonymous>"
            },
            "body": body
        },
        "python": python
    });
    Ok(serde_json::to_string_pretty(&recipe)?)
}

async fn list_tables_json(
    runtime: &Arc<Runtime>,
    identity: &AuthIdentity,
) -> anyhow::Result<String> {
    let mut out = Vec::new();
    for table in list_table_names(runtime, identity).await? {
        let description = table_schema(runtime, &table, identity)
            .await
            .and_then(|schema| table_description(&schema));
        out.push(json!({ "name": table, "description": description }));
    }
    Ok(serde_json::to_string_pretty(&out)?)
}

async fn describe_table_json(
    runtime: &Arc<Runtime>,
    args: &Map<String, Value>,
    identity: &AuthIdentity,
) -> anyhow::Result<String> {
    let table = args
        .get("table_name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing required 'table_name' argument"))?;
    let schema = table_schema(runtime, table, identity)
        .await
        .ok_or_else(|| anyhow::anyhow!("table '{table}' not found"))?;
    let columns: Vec<Value> = schema
        .fields()
        .iter()
        .map(|field| {
            json!({
                "name": field.name(),
                // Arrow renders its own types (`Float64`, `Timestamp(Nanosecond, None)`).
                "data_type": field.data_type().to_string(),
                "nullable": field.is_nullable(),
                "description": column_description(field),
            })
        })
        .collect();
    Ok(serde_json::to_string_pretty(&json!({
        "name": table,
        "description": table_description(&schema),
        "columns": columns,
    }))?)
}

/// The table comment, from the schema metadata.
fn table_description(schema: &Schema) -> Option<String> {
    schema.metadata().get(COMMENT_METADATA_KEY).cloned()
}

/// The column comment, with a fallback to native `description` field metadata.
fn column_description(field: &Field) -> Option<String> {
    field
        .metadata()
        .get(COMMENT_METADATA_KEY)
        .or_else(|| field.metadata().get("description"))
        .cloned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use std::collections::HashMap;

    #[test]
    fn export_query_recipe_builds_fetch_and_guards_writes() {
        let mut args = Map::new();
        args.insert("sql".into(), Value::String("SELECT * FROM obs".into()));
        args.insert("format".into(), Value::String("parquet".into()));
        let out = export_query_recipe(&args).unwrap();
        assert!(out.contains("/api/query"), "recipe should reference the query endpoint");
        assert!(out.contains("read_parquet"), "parquet snippet should use read_parquet");
        assert!(out.contains("\"format\": \"parquet\""));

        // WITH (CTE) is allowed; default format is parquet.
        let mut cte = Map::new();
        cte.insert("sql".into(), Value::String("WITH x AS (SELECT 1) SELECT * FROM x".into()));
        assert!(export_query_recipe(&cte).unwrap().contains("read_parquet"));

        // Non-SELECT is rejected (MCP is read-only).
        let mut bad = Map::new();
        bad.insert("sql".into(), Value::String("DELETE FROM obs".into()));
        assert!(export_query_recipe(&bad).is_err());
    }

    #[test]
    fn the_tool_list_is_fixed_and_read_only() {
        let tools = tools();

        let names: Vec<&str> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(names, ["list_tables", "describe_table", "run_sql", "export_query"]);
        for tool in &tools {
            let hint = tool.annotations.as_ref().and_then(|a| a.read_only_hint);
            assert_eq!(hint, Some(true), "{} must be read-only", tool.name);
        }
    }

    fn field_with(metadata: &[(&str, &str)]) -> Field {
        Field::new("depth", DataType::Float64, true).with_metadata(
            metadata
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<HashMap<_, _>>(),
        )
    }

    #[test]
    fn a_column_comment_wins_over_native_description() {
        let both = field_with(&[("comment", "beacon"), ("description", "native")]);
        let native = field_with(&[("description", "native")]);
        let none = field_with(&[]);

        assert_eq!(column_description(&both).as_deref(), Some("beacon"));
        assert_eq!(column_description(&native).as_deref(), Some("native"));
        assert_eq!(column_description(&none), None);
    }

    #[test]
    fn the_table_description_is_the_schema_comment() {
        let schema = Schema::new(vec![field_with(&[])]).with_metadata(HashMap::from([(
            "comment".to_string(),
            "profiles".to_string(),
        )]));

        assert_eq!(table_description(&schema).as_deref(), Some("profiles"));
        assert_eq!(table_description(&Schema::empty()), None);
    }
}
