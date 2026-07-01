//! Query execution and result serialization for MCP tools.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use beacon_core::query::Query;
use beacon_core::runtime::Runtime;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;

/// Hard cap on rows returned to the model, to keep tool output bounded.
pub const MAX_ROWS: usize = 1000;

/// Run a read-only SQL query and return the rows as a JSON string. Executes as a
/// non-super-user, so DDL/DML is rejected by the planner.
pub async fn run_sql_to_json(
    runtime: &Arc<Runtime>,
    sql: String,
    identity: AuthIdentity,
) -> anyhow::Result<String> {
    let result = runtime.run_query(Query::sql(sql), identity).await?;
    let mut stream = result.into_record_stream()?;

    let mut batches: Vec<RecordBatch> = Vec::new();
    let mut total = 0usize;
    let mut truncated = false;
    while let Some(batch) = stream.try_next().await? {
        let remaining = MAX_ROWS - total;
        if batch.num_rows() > remaining {
            batches.push(batch.slice(0, remaining));
            truncated = true;
            break;
        }
        total += batch.num_rows();
        batches.push(batch);
        if total >= MAX_ROWS {
            // Peek no further; assume more may exist.
            truncated = stream.try_next().await?.is_some();
            break;
        }
    }

    let rows = batches_to_json(&batches)?;
    if truncated {
        Ok(format!(
            "{{\"truncated\":true,\"max_rows\":{MAX_ROWS},\"rows\":{rows}}}"
        ))
    } else {
        Ok(rows)
    }
}

/// Serialize record batches to a JSON array of row objects.
fn batches_to_json(batches: &[RecordBatch]) -> anyhow::Result<String> {
    if batches.iter().all(|b| b.num_rows() == 0) {
        return Ok("[]".to_string());
    }
    let mut buf = Vec::new();
    let mut writer = arrow_json::ArrayWriter::new(&mut buf);
    for batch in batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    Ok(String::from_utf8(buf)?)
}
