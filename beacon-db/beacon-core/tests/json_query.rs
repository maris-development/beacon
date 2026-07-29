//! The structured JSON query body (`QueryBody`): the non-SQL half of
//! `Runtime::run_query`, used by clients that build queries programmatically.
//!
//! Each test deserializes a JSON value exactly as an HTTP client would post it,
//! so the serde surface (untagged variants, aliases) is under test too.

mod common;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use beacon_core::query::Query;
use common::{column_strings, runtime, total_rows, TestRuntime};
use futures::TryStreamExt;
use serde_json::json;

async fn seeded(tag: &str) -> TestRuntime {
    let rt = runtime(tag).await;
    rt.sql("CREATE TABLE jq (a BIGINT, name VARCHAR)").await;
    rt.sql("INSERT INTO jq VALUES (1, 'x'), (2, 'y'), (3, 'y'), (4, 'z')")
        .await;
    rt
}

/// Runs a JSON query body (as a client would post it) and collects the rows.
async fn run_json(rt: &TestRuntime, body: serde_json::Value) -> Vec<RecordBatch> {
    let query: Query = serde_json::from_value(body).expect("query body should deserialize");
    rt.runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("JSON query should run")
        .into_record_stream()
        .expect("JSON query should produce a row stream")
        .try_collect()
        .await
        .expect("JSON query stream should drain")
}

fn i64_column(batches: &[RecordBatch], col: usize) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let column = b
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("expected Int64");
            (0..column.len()).map(|i| column.value(i)).collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn select_filter_sort_and_limit() {
    let rt = seeded("json-select").await;

    let batches = run_json(
        &rt,
        json!({
            "select": ["a", "name"],
            "from": "jq",
            "filter": { "column": "a", "gt_eq": 2 },
            "sort_by": [ { "Desc": "a" } ],
            "limit": 2
        }),
    )
    .await;

    assert_eq!(
        i64_column(&batches, 0),
        vec![4, 3],
        "filter a >= 2, descending, limited to 2"
    );
    assert_eq!(column_strings(&batches, 1), vec!["z", "y"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn offset_skips_rows() {
    let rt = seeded("json-offset").await;

    let batches = run_json(
        &rt,
        json!({
            "select": ["a"],
            "from": "jq",
            "sort_by": [ { "Asc": "a" } ],
            "offset": 3
        }),
    )
    .await;

    assert_eq!(i64_column(&batches, 0), vec![4], "offset 3 of 4 sorted rows");
}

/// The JSON `function` select resolves against the session's **scalar**
/// functions. A scalar call projects row-wise and honours its alias.
#[tokio::test(flavor = "multi_thread")]
async fn scalar_functions_and_aliases_project() {
    let rt = seeded("json-functions").await;

    let batches = run_json(
        &rt,
        json!({
            "select": [
                { "function": "upper", "args": ["name"], "alias": "upper_name" },
            ],
            "from": "jq",
            "sort_by": [ { "Asc": "name" } ]
        }),
    )
    .await;

    assert_eq!(batches[0].schema().field(0).name(), "upper_name");
    assert_eq!(
        column_strings(&batches, 0),
        vec!["X", "Y", "Y", "Z"],
        "upper(name) should be applied row-wise"
    );
}

/// Aggregates are deliberately out of the JSON `function` surface: it looks up
/// only scalar functions, so `avg`/`sum`/`count` are not resolvable there. This
/// pins that contract so a future change to the compiler is a conscious one.
#[tokio::test(flavor = "multi_thread")]
async fn aggregate_functions_are_not_available_in_json_select() {
    let rt = seeded("json-aggregate").await;

    let query: Query = serde_json::from_value(json!({
        "select": [ { "function": "avg", "args": ["a"], "alias": "avg_a" } ],
        "from": "jq"
    }))
    .expect("body should deserialize");

    let err = match rt.runtime.run_query(query, rt.admin().await).await {
        Err(e) => e,
        Ok(_) => panic!("an aggregate in a JSON select should not resolve"),
    };
    assert!(
        err.to_string().contains("avg") && err.to_string().contains("registry"),
        "the error should say the function is not in the registry, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_filters_combine() {
    let rt = seeded("json-nested-filter").await;

    // (a >= 2 AND a <= 3) OR name = 'x'  =>  rows 1, 2, 3.
    // The compiler projects before it filters, so `name` must be projected for
    // the filter to reference it (see `filter_sees_the_projected_schema`).
    let batches = run_json(
        &rt,
        json!({
            "select": ["a", "name"],
            "from": "jq",
            "filter": {
                "or": [
                    { "and": [
                        { "column": "a", "gt_eq": 2 },
                        { "column": "a", "lt_eq": 3 }
                    ]},
                    { "column": "name", "eq": "x" }
                ]
            },
            "sort_by": [ { "Asc": "a" } ]
        }),
    )
    .await;

    assert_eq!(i64_column(&batches, 0), vec![1, 2, 3]);
}

/// The JSON compiler applies `select` (projection) before `filter`, so a filter
/// can only reference projected columns — unlike SQL, where `WHERE` sees the
/// source. This pins that ordering: filtering on an unprojected column errors.
#[tokio::test(flavor = "multi_thread")]
async fn filter_sees_the_projected_schema() {
    let rt = seeded("json-filter-scope").await;

    let query: Query = serde_json::from_value(json!({
        "select": ["a"],
        "from": "jq",
        "filter": { "column": "name", "eq": "x" }
    }))
    .expect("body should deserialize");

    let err = match rt.runtime.run_query(query, rt.admin().await).await {
        Err(e) => e,
        Ok(_) => panic!("filtering on an unprojected column should fail"),
    };
    assert!(
        err.to_string().contains("name"),
        "the error should name the unprojected column, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn from_a_csv_file_instead_of_a_table() {
    let rt = seeded("json-from-csv").await;
    common::write_file(&rt.datasets_dir().join("j/data.csv"), "v,name\n10,q\n20,r\n");

    let batches = run_json(
        &rt,
        json!({
            "select": ["v"],
            "from": { "csv": { "paths": ["j/data.csv"] } },
            "filter": { "column": "v", "gt_eq": 15 }
        }),
    )
    .await;

    assert_eq!(total_rows(&batches), 1, "only the 20 row passes the filter");
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_column_is_a_clear_error() {
    let rt = seeded("json-unknown-column").await;

    let query: Query = serde_json::from_value(json!({
        "select": ["no_such_column"],
        "from": "jq"
    }))
    .expect("body should deserialize");

    let err = match rt.runtime.run_query(query, rt.admin().await).await {
        Err(e) => e,
        Ok(_) => panic!("projecting an unknown column should fail"),
    };
    assert!(
        err.to_string().contains("no_such_column"),
        "the error should name the missing column, got: {err}"
    );
}
