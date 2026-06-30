//! Generates the MCP tool catalog from the runtime and dispatches tool calls.
//!
//! Three generic tools are always present (`list_tables`, `describe_table`,
//! `run_sql`). In addition, every table whose `mcp` extension is enabled becomes
//! its own tool whose input schema is derived from the extension metadata
//! (exposed columns + named presets).

use std::sync::Arc;

use beacon_core::extensions::{McpExtension, PresetExtension, PresetFilter};
use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use rmcp::model::Tool;
use serde_json::{json, Map, Value};

use crate::result::{run_sql_to_json, MAX_ROWS};

/// Build the full tool list: generic tools + per-table tools from extensions.
pub async fn build_tools(runtime: &Arc<Runtime>) -> anyhow::Result<Vec<Tool>> {
    let mut tools = vec![list_tables_tool(), describe_table_tool(), run_sql_tool()];
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

fn list_tables_tool() -> Tool {
    Tool::new(
        "list_tables",
        "List the tables registered in beacon, with their MCP exposure status.",
        object_schema(json!({}), &[]),
    )
}

fn describe_table_tool() -> Tool {
    Tool::new(
        "describe_table",
        "Return a table's column schema and its attached extensions (MCP descriptor, presets).",
        object_schema(
            json!({ "table_name": { "type": "string", "description": "Name of the table." } }),
            &["table_name"],
        ),
    )
}

fn run_sql_tool() -> Tool {
    Tool::new(
        "run_sql",
        "Run a read-only SQL query (SELECT only) against beacon and return JSON rows.",
        object_schema(
            json!({ "sql": { "type": "string", "description": "A read-only SELECT statement." } }),
            &["sql"],
        ),
    )
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
    let columns: Vec<Value> = schema
        .fields
        .iter()
        .map(|f| json!({ "name": f.name, "data_type": f.data_type, "nullable": f.nullable }))
        .collect();
    Ok(serde_json::to_string_pretty(
        &json!({ "name": table, "columns": columns, "extensions": ext }),
    )?)
}

// ---- per-table tools -----------------------------------------------------

fn default_tool_name(table: &str) -> String {
    format!("query_{table}")
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

    // Columns offered to the model: the curated `exposed_columns` if set,
    // otherwise the table's full schema.
    let columns: Vec<String> = match &mcp.exposed_columns {
        Some(cols) => cols.clone(),
        None => runtime
            .list_table_schema_view(table.to_string())
            .await
            .map(|s| s.fields.into_iter().map(|f| f.name).collect())
            .unwrap_or_default(),
    };
    let preset_names: Vec<String> = preset
        .map(|p| p.presets.iter().map(|x| x.name.clone()).collect())
        .unwrap_or_default();

    let mut props = Map::new();
    if !columns.is_empty() {
        props.insert(
            "select".into(),
            json!({
                "type": "array",
                "items": { "type": "string", "enum": columns },
                "description": "Columns to return. Omit for all exposed columns."
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

    Tool::new(name, description, object_schema(Value::Object(props), &[]))
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
    let exposed = mcp.exposed_columns.as_ref();

    let select = match args.get("select").and_then(Value::as_array) {
        Some(arr) if !arr.is_empty() => {
            let cols: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if let Some(exp) = exposed {
                for col in &cols {
                    anyhow::ensure!(exp.contains(col), "column '{col}' is not exposed by this tool");
                }
            }
            cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        }
        _ => default_select(exposed),
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

fn default_select(exposed: Option<&Vec<String>>) -> String {
    match exposed {
        Some(cols) if !cols.is_empty() => {
            cols.iter().map(|c| quote_ident(c)).collect::<Vec<_>>().join(", ")
        }
        _ => "*".to_string(),
    }
}

/// Render a stored preset filter into a SQL boolean expression.
fn render_filter(filter: &PresetFilter) -> anyhow::Result<String> {
    let col = quote_ident(&filter.column);
    match filter.op.as_str() {
        "=" | "!=" | "<" | "<=" | ">" | ">=" => {
            Ok(format!("{col} {} {}", filter.op, render_scalar(&filter.value)?))
        }
        "between" => {
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
        "in" => {
            let arr = filter
                .value
                .as_array()
                .filter(|a| !a.is_empty())
                .ok_or_else(|| anyhow::anyhow!("'in' requires a non-empty array"))?;
            let vals = arr.iter().map(render_scalar).collect::<anyhow::Result<Vec<_>>>()?;
            Ok(format!("{col} IN ({})", vals.join(", ")))
        }
        other => anyhow::bail!("unsupported operator '{other}'"),
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

    fn mcp(cols: Option<Vec<&str>>) -> McpExtension {
        McpExtension {
            enabled: true,
            tool_name: None,
            description: None,
            exposed_columns: cols.map(|c| c.into_iter().map(String::from).collect()),
        }
    }

    #[test]
    fn builds_select_with_preset_between() {
        let p = preset(
            "shallow",
            vec![PresetFilter {
                column: "depth".into(),
                op: "between".into(),
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
            op: "in".into(),
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
