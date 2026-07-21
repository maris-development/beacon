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
        // Guard rail: the result is larger than the preview cap. Return the
        // preview but steer the model to `export_query` for the complete data
        // rather than letting it treat the truncated rows as the full result.
        Ok(format!(
            "{{\"truncated\":true,\"returned_rows\":{MAX_ROWS},\"max_rows\":{MAX_ROWS},\
             \"guidance\":\"This is a truncated preview ({MAX_ROWS} rows) because the result is \
             large. Do NOT treat these rows as complete. To get the full result, call \
             export_query with the same SQL to fetch it as a Parquet/Arrow/CSV file.\",\
             \"rows\":{rows}}}"
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names)),
            ],
        )
        .unwrap()
    }

    /// A zero-row result serializes to an empty JSON array (not `null` or an
    /// error), which is the shape MCP clients expect for "no rows".
    #[test]
    fn empty_result_is_an_empty_json_array() {
        let empty = batch(vec![], vec![]);
        assert_eq!(batches_to_json(&[empty]).unwrap(), "[]");
        assert_eq!(batches_to_json(&[]).unwrap(), "[]");
    }

    /// Rows serialize as an array of column-keyed objects, and multiple batches
    /// are concatenated into a single array.
    #[test]
    fn rows_serialize_as_objects_across_batches() {
        let json = batches_to_json(&[batch(vec![1], vec!["a"]), batch(vec![2], vec!["b"])]).unwrap();
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], 1);
        assert_eq!(parsed[0]["name"], "a");
        assert_eq!(parsed[1]["id"], 2);
    }
}
