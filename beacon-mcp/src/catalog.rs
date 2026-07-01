//! Generates the MCP tool catalog from the runtime and dispatches tool calls.
//!
//! Three generic tools are always present (`list_tables`, `describe_table`,
//! `run_sql`). In addition, every table whose `mcp` extension is enabled becomes
//! its own tool whose input schema is derived from the extension metadata
//! (exposed columns + named presets).

use std::sync::Arc;

use beacon_core::extensions::{McpExtension, PresetExtension, PresetFilter, PresetOp};
use beacon_core::api::{SchemaFieldView, SchemaView};
use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use rmcp::model::{Tool, ToolAnnotations};
use serde_json::{json, Map, Value};

use crate::result::{run_sql_to_json, MAX_ROWS};

/// Build the full tool list: generic tools + per-table tools from extensions.
pub async fn build_tools(runtime: &Arc<Runtime>) -> anyhow::Result<Vec<Tool>> {
    let mut tools = vec![
        list_tables_tool(),
        describe_table_tool(),
        run_sql_tool(),
        export_query_tool(),
    ];
    for table in runtime.list_tables() {
        let ext = runtime
            .get_table_extensions(table.clone())
            .await
            .unwrap_or_default();
        if ext.mcp.as_ref().is_some_and(|mcp| mcp.enabled) {
            tools.push(table_tool(runtime, &table, &ext.mcp.unwrap(), ext.preset.as_ref()).await);
        }
    }
    Ok(tools)
}

/// Route a tool call to its handler.
pub async fn dispatch(
    runtime: &Arc<Runtime>,
    name: &str,
    args: Map<String, Value>,
    identity: AuthIdentity,
) -> anyhow::Result<String> {
    match name {
        "list_tables" => list_tables_json(runtime).await,
        "describe_table" => describe_table_json(runtime, &args).await,
        "run_sql" => {
            let sql = args
                .get("sql")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!("missing required 'sql' argument"))?;
            run_sql_to_json(runtime, sql.to_string(), identity).await
        }
        "export_query" => export_query_recipe(&args),
        other => run_table_tool(runtime, other, &args, identity).await,
    }
}

// ---- generic tools -------------------------------------------------------

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
        "List the tables registered in beacon, with their MCP exposure status.",
        object_schema(json!({}), &[]),
    ))
}

fn describe_table_tool() -> Tool {
    read_only(Tool::new(
        "describe_table",
        "Return a table's column schema and its attached extensions (MCP descriptor, presets).",
        object_schema(
            json!({ "table_name": { "type": "string", "description": "Name of the table." } }),
            &["table_name"],
        ),
    ))
}

fn run_sql_tool() -> Tool {
    read_only(Tool::new(
        "run_sql",
        "Run a read-only SQL query (SELECT only) against beacon and return JSON rows.",
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

async fn list_tables_json(runtime: &Arc<Runtime>) -> anyhow::Result<String> {
    let mut out = Vec::new();
    for table in runtime.list_tables() {
        let ext = runtime
            .get_table_extensions(table.clone())
            .await
            .unwrap_or_default();
        out.push(json!({
            "name": table,
            "mcp_enabled": ext.mcp.as_ref().map(|m| m.enabled).unwrap_or(false),
            "description": ext.mcp.as_ref().and_then(|m| m.description.clone()),
        }));
    }
    Ok(serde_json::to_string_pretty(&out)?)
}

async fn describe_table_json(
    runtime: &Arc<Runtime>,
    args: &Map<String, Value>,
) -> anyhow::Result<String> {
    let table = args
        .get("table_name")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("missing required 'table_name' argument"))?;
    let schema = runtime
        .list_table_schema_view(table.to_string())
        .await
        .ok_or_else(|| anyhow::anyhow!("table '{table}' not found"))?;
    let ext = runtime
        .get_table_extensions(table.to_string())
        .await
        .unwrap_or_default();
    // Merge schema types with per-column descriptions, scoped to the exposed
    // columns (or all columns when none are curated).
    let columns: Vec<Value> = resolve_columns(&schema, ext.mcp.as_ref())
        .iter()
        .map(|c| {
            json!({
                "name": c.name,
                "data_type": c.data_type,
                "nullable": c.nullable,
                "description": c.description,
            })
        })
        .collect();
    Ok(serde_json::to_string_pretty(
        &json!({ "name": table, "columns": columns, "extensions": ext }),
    )?)
}

/// A column resolved for the model: its schema type merged with any description.
struct ResolvedColumn {
    name: String,
    data_type: String,
    nullable: bool,
    description: Option<String>,
}

/// Merge the table schema with the mcp extension's per-column descriptions.
/// Scoped to `exposed_columns` (in that order) when set, otherwise every column.
/// A column's description comes from the extension entry, falling back to the
/// Arrow field's `description`/`comment` metadata when present.
fn resolve_columns(schema: &SchemaView, mcp: Option<&McpExtension>) -> Vec<ResolvedColumn> {
    match mcp.and_then(|m| m.exposed_columns.as_ref()) {
        Some(cols) => {
            let by_name: std::collections::HashMap<&str, &SchemaFieldView> =
                schema.fields.iter().map(|f| (f.name.as_str(), f)).collect();
            cols.iter()
                .filter_map(|c| {
                    let field = by_name.get(c.name())?;
                    Some(ResolvedColumn {
                        name: c.name().to_string(),
                        data_type: field.data_type.clone(),
                        nullable: field.nullable,
                        description: c
                            .description()
                            .map(String::from)
                            .or_else(|| field_description(field)),
                    })
                })
                .collect()
        }
        None => schema
            .fields
            .iter()
            .map(|f| ResolvedColumn {
                name: f.name.clone(),
                data_type: f.data_type.clone(),
                nullable: f.nullable,
                description: field_description(f),
            })
            .collect(),
    }
}

/// A column description carried in the Arrow field metadata, if any.
fn field_description(field: &SchemaFieldView) -> Option<String> {
    field
        .metadata
        .get("description")
        .or_else(|| field.metadata.get("comment"))
        .cloned()
}

// ---- per-table tools -----------------------------------------------------

fn default_tool_name(table: &str) -> String {
    // Sanitize to MCP-safe characters so a table name with dots/spaces still
    // yields a valid tool name (see `beacon_core::extensions::is_valid_tool_name`).
    let sanitized: String = table
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();
    let mut name = format!("query_{sanitized}");
    name.truncate(64);
    name
}

async fn table_tool(
    runtime: &Arc<Runtime>,
    table: &str,
    mcp: &McpExtension,
    preset: Option<&PresetExtension>,
) -> Tool {
    let name = mcp
        .tool_name
        .clone()
        .unwrap_or_else(|| default_tool_name(table));
    let description = mcp
        .description
        .clone()
        .unwrap_or_else(|| format!("Query the '{table}' table."));

    // Merge the table schema (types) with the extension's per-column descriptions,
    // scoped to `exposed_columns` when set, or all columns otherwise, so the model
    // sees name + data type + meaning for every queryable column.
    let resolved = match runtime.list_table_schema_view(table.to_string()).await {
        Some(schema) => resolve_columns(&schema, Some(mcp)),
        None => Vec::new(),
    };
    let column_names: Vec<String> = resolved.iter().map(|c| c.name.clone()).collect();
    let glossary: Vec<String> = resolved
        .iter()
        .map(|c| match &c.description {
            Some(desc) => format!("{} ({}): {}", c.name, c.data_type, desc),
            None => format!("{} ({})", c.name, c.data_type),
        })
        .collect();
    let select_description = if glossary.is_empty() {
        "Columns to return. Omit for all columns.".to_string()
    } else {
        format!(
            "Columns to return, each shown as name (type): meaning. Omit for all. {}.",
            glossary.join("; ")
        )
    };
    let preset_names: Vec<String> = preset
        .map(|p| p.presets.iter().map(|x| x.name.clone()).collect())
        .unwrap_or_default();

    let mut props = Map::new();
    if !column_names.is_empty() {
        props.insert(
            "select".into(),
            json!({
                "type": "array",
                "items": { "type": "string", "enum": column_names },
                "description": select_description
            }),
        );
    }
    if !preset_names.is_empty() {
        props.insert(
            "preset".into(),
            json!({
                "type": "string",
                "enum": preset_names,
                "description": "Apply a predefined, named filter set."
            }),
        );
    }
    props.insert(
        "limit".into(),
        json!({ "type": "integer", "description": "Maximum rows to return (default 100)." }),
    );

    let mut tool = read_only(Tool::new(
        name,
        description,
        object_schema(Value::Object(props), &[]),
    ));
    if let Some(title) = mcp.title.clone() {
        tool = tool.with_title(title);
    }
    tool
}

async fn run_table_tool(
    runtime: &Arc<Runtime>,
    tool_name: &str,
    args: &Map<String, Value>,
    identity: AuthIdentity,
) -> anyhow::Result<String> {
    for table in runtime.list_tables() {
        let ext = runtime
            .get_table_extensions(table.clone())
            .await
            .unwrap_or_default();
        let Some(mcp) = ext.mcp.as_ref().filter(|m| m.enabled) else {
            continue;
        };
        let name = mcp
            .tool_name
            .clone()
            .unwrap_or_else(|| default_tool_name(&table));
        if name != tool_name {
            continue;
        }
        let sql = build_table_sql(&table, mcp, ext.preset.as_ref(), args)?;
        return run_sql_to_json(runtime, sql, identity).await;
    }
    anyhow::bail!("unknown tool '{tool_name}'")
}

/// Build a `SELECT` for a per-table tool from its args, expanding a chosen preset
/// into `WHERE` clauses. Identifiers are quoted and values rendered as literals.
fn build_table_sql(
    table: &str,
    mcp: &McpExtension,
    preset: Option<&PresetExtension>,
    args: &Map<String, Value>,
) -> anyhow::Result<String> {
    let exposed = mcp.exposed_column_names();

    let select = match args.get("select").and_then(Value::as_array) {
        Some(arr) if !arr.is_empty() => {
            let cols: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if let Some(exp) = &exposed {
                for col in &cols {
                    anyhow::ensure!(
                        exp.contains(&col.as_str()),
                        "column '{col}' is not exposed by this tool"
                    );
                }
            }
            cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        }
        _ => default_select(exposed.as_deref()),
    };

    let mut clauses = Vec::new();
    if let Some(name) = args.get("preset").and_then(Value::as_str) {
        let preset = preset
            .and_then(|p| p.presets.iter().find(|x| x.name == name))
            .ok_or_else(|| anyhow::anyhow!("unknown preset '{name}'"))?;
        for filter in &preset.filters {
            clauses.push(render_filter(filter)?);
        }
    }

    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(100)
        .min(MAX_ROWS as u64);

    let mut sql = format!("SELECT {select} FROM {}", quote_ident(table));
    if !clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&clauses.join(" AND "));
    }
    sql.push_str(&format!(" LIMIT {limit}"));
    Ok(sql)
}

fn default_select(exposed: Option<&[&str]>) -> String {
    match exposed {
        Some(cols) if !cols.is_empty() => {
            cols.iter().map(|&c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        }
        _ => "*".to_string(),
    }
}

/// Render a stored preset filter into a SQL boolean expression.
fn render_filter(filter: &PresetFilter) -> anyhow::Result<String> {
    let col = quote_ident(&filter.column);
    match filter.op {
        PresetOp::Between => {
            let arr = filter
                .value
                .as_array()
                .filter(|a| a.len() == 2)
                .ok_or_else(|| anyhow::anyhow!("'between' requires a two-element array"))?;
            Ok(format!(
                "{col} BETWEEN {} AND {}",
                render_scalar(&arr[0])?,
                render_scalar(&arr[1])?
            ))
        }
        PresetOp::In => {
            let arr = filter
                .value
                .as_array()
                .filter(|a| !a.is_empty())
                .ok_or_else(|| anyhow::anyhow!("'in' requires a non-empty array"))?;
            let vals = arr.iter().map(render_scalar).collect::<anyhow::Result<Vec<_>>>()?;
            Ok(format!("{col} IN ({})", vals.join(", ")))
        }
        op => Ok(format!("{col} {} {}", op.as_sql(), render_scalar(&filter.value)?)),
    }
}

fn render_scalar(value: &Value) -> anyhow::Result<String> {
    Ok(match value {
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Bool(b) => if *b { "TRUE".into() } else { "FALSE".into() },
        Value::Null => "NULL".into(),
        other => anyhow::bail!("unsupported filter value: {other}"),
    })
}

/// Quote a SQL identifier, escaping embedded double quotes.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn preset(name: &str, filters: Vec<PresetFilter>) -> PresetExtension {
        PresetExtension {
            presets: vec![beacon_core::extensions::Preset {
                name: name.to_string(),
                description: None,
                filters,
            }],
        }
    }

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

    fn field(name: &str, data_type: &str) -> SchemaFieldView {
        SchemaFieldView {
            name: name.into(),
            data_type: data_type.into(),
            nullable: true,
            metadata: Default::default(),
        }
    }

    #[test]
    fn resolve_columns_merges_types_and_descriptions() {
        use beacon_core::extensions::{ColumnDoc, ExposedColumn};
        let schema = SchemaView {
            fields: vec![field("lat", "Float64"), field("depth", "Float64"), field("x", "Int64")],
            metadata: Default::default(),
        };

        // No exposed_columns -> all columns, types included, no descriptions.
        let all = resolve_columns(&schema, Some(&mcp(None)));
        assert_eq!(all.len(), 3);
        assert_eq!((all[0].name.as_str(), all[0].data_type.as_str()), ("lat", "Float64"));

        // Exposed subset (in order), merging schema type + entry description.
        let ext = McpExtension {
            enabled: true,
            tool_name: None,
            title: None,
            description: None,
            exposed_columns: Some(vec![
                ExposedColumn::Documented(ColumnDoc {
                    name: "depth".into(),
                    description: Some("meters".into()),
                }),
                ExposedColumn::Name("lat".into()),
            ]),
        };
        let cols = resolve_columns(&schema, Some(&ext));
        assert_eq!(cols.len(), 2);
        assert_eq!(
            (cols[0].name.as_str(), cols[0].data_type.as_str(), cols[0].description.as_deref()),
            ("depth", "Float64", Some("meters"))
        );
        assert_eq!((cols[1].name.as_str(), cols[1].description.as_deref()), ("lat", None));
    }

    fn mcp(cols: Option<Vec<&str>>) -> McpExtension {
        McpExtension {
            enabled: true,
            tool_name: None,
            description: None,
            title: None,
            exposed_columns: cols.map(|c| {
                c.into_iter()
                    .map(|s| beacon_core::extensions::ExposedColumn::Name(s.to_string()))
                    .collect()
            }),
        }
    }

    #[test]
    fn builds_select_with_preset_between() {
        let p = preset(
            "shallow",
            vec![PresetFilter {
                column: "depth".into(),
                op: PresetOp::Between,
                value: serde_json::json!([0, 10]),
            }],
        );
        let mut args = Map::new();
        args.insert("preset".into(), Value::String("shallow".into()));
        let sql = build_table_sql("obs", &mcp(Some(vec!["lat", "depth"])), Some(&p), &args).unwrap();
        assert_eq!(
            sql,
            r#"SELECT "lat", "depth" FROM "obs" WHERE "depth" BETWEEN 0 AND 10 LIMIT 100"#
        );
    }

    #[test]
    fn rejects_unexposed_select_column() {
        let mut args = Map::new();
        args.insert("select".into(), serde_json::json!(["ghost"]));
        let err = build_table_sql("obs", &mcp(Some(vec!["lat"])), None, &args).unwrap_err();
        assert!(err.to_string().contains("not exposed"), "{err}");
    }

    #[test]
    fn unknown_preset_errors() {
        let mut args = Map::new();
        args.insert("preset".into(), Value::String("nope".into()));
        let err = build_table_sql("obs", &mcp(None), None, &args).unwrap_err();
        assert!(err.to_string().contains("unknown preset"), "{err}");
    }

    #[test]
    fn in_and_string_values_are_escaped() {
        let f = PresetFilter {
            column: "basin".into(),
            op: PresetOp::In,
            value: serde_json::json!(["a'b", "c"]),
        };
        assert_eq!(render_filter(&f).unwrap(), r#""basin" IN ('a''b', 'c')"#);
    }

    #[test]
    fn default_select_is_star_without_exposed() {
        let sql = build_table_sql("obs", &mcp(None), None, &Map::new()).unwrap();
        assert_eq!(sql, r#"SELECT * FROM "obs" LIMIT 100"#);
    }
}
