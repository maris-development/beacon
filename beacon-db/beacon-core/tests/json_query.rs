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

/// An integer literal stays an integer, so a function that wants an integer argument, such
/// as the decimal places of `round`, takes it.
#[tokio::test(flavor = "multi_thread")]
async fn an_integer_literal_is_an_integer_argument() {
    let rt = seeded("json-integer-literal").await;

    let batches = run_json(
        &rt,
        json!({
            "select": [
                { "function": "round", "args": ["a", { "value": 0 }], "alias": "rounded" }
            ],
            "from": "jq",
            "sort_by": [ { "Asc": "rounded" } ]
        }),
    )
    .await;

    assert_eq!(total_rows(&batches), 4);
}

/// `distinct` selects its own columns, so it needs no top-level `select`, and it reads the
/// source columns.
#[tokio::test(flavor = "multi_thread")]
async fn distinct_on_without_a_top_level_select() {
    let rt = seeded("json-distinct").await;

    let batches = run_json(
        &rt,
        json!({
            "from": "jq",
            "distinct": { "on": ["name"], "select": ["name", "a"] },
            "sort_by": [ { "Asc": "name" } ]
        }),
    )
    .await;

    assert_eq!(batches[0].schema().fields().len(), 2);
    assert_eq!(column_strings(&batches, 0), vec!["x", "y", "z"]);
}

/// A filter can name a column that the `select` leaves out.
#[tokio::test(flavor = "multi_thread")]
async fn filter_on_a_column_outside_the_select() {
    let rt = seeded("json-filter-unselected").await;

    let batches = run_json(
        &rt,
        json!({
            "select": ["name"],
            "from": "jq",
            "filters": [ { "column": "a", "gt": 2 } ],
            "sort_by": [ { "Asc": "name" } ]
        }),
    )
    .await;

    assert_eq!(batches[0].schema().fields().len(), 1, "only the selected column");
    assert_eq!(column_strings(&batches, 0), vec!["y", "z"]);
}

/// An Arrow IPC file with the column names of a World Ocean Database export.
fn write_wod_arrow(path: &std::path::Path) {
    use arrow::array::{Float32Array, Float64Array, Int32Array, StringArray, TimestampSecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::ipc::writer::FileWriter;
    use std::sync::Arc;

    // Row 1 and row 7 pass every filter. Each other row fails one filter.
    let days = [
        "2022-06-01",
        "2021-06-01",
        "2022-07-01",
        "2022-08-01",
        "2022-09-01",
        "2022-10-01",
        "2022-11-01",
    ];
    let time: Vec<i64> = days
        .iter()
        .map(|day| {
            format!("{day}T00:00:00")
                .parse::<chrono::NaiveDateTime>()
                .unwrap()
                .and_utc()
                .timestamp()
        })
        .collect();
    let text = |value: &str| Arc::new(StringArray::from(vec![value; 7]));
    let schema = Arc::new(Schema::new(vec![
        Field::new("wod_unique_cast", DataType::Int64, false),
        Field::new("Temperature", DataType::Float64, true),
        Field::new("Temperature_WODflag", DataType::Int32, true),
        Field::new("Temperature.units", DataType::Utf8, true),
        Field::new("z", DataType::Float32, true),
        Field::new("z.units", DataType::Utf8, true),
        Field::new("time", DataType::Timestamp(TimeUnit::Second, None), true),
        Field::new("lon", DataType::Float64, true),
        Field::new("lat", DataType::Float64, true),
        Field::new("Platform", DataType::Utf8, true),
        Field::new("Institute", DataType::Utf8, true),
        Field::new(".featureType", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7])),
            Arc::new(Float64Array::from(vec![
                Some(15.0),
                Some(14.0),
                Some(-10000000000.0),
                None,
                Some(13.0),
                Some(11.0),
                Some(12.0),
            ])),
            Arc::new(Int32Array::from(vec![0, 0, 0, 0, 1, 0, 0])),
            text("degrees_C"),
            Arc::new(Float32Array::from(vec![
                5.0, 5.0, 5.0, 5.0, 5.0, 50.0, 10.0,
            ])),
            text("m"),
            Arc::new(TimestampSecondArray::from(time)),
            Arc::new(Float64Array::from(vec![4.0; 7])),
            Arc::new(Float64Array::from(vec![52.0; 7])),
            text("SHIP"),
            text("NIOZ"),
            text("profile"),
        ],
    )
    .unwrap();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut writer = FileWriter::try_new(std::fs::File::create(path).unwrap(), &schema).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
}

/// A filter names a select alias, and the compiler reads the column behind it.
#[tokio::test(flavor = "multi_thread")]
async fn filters_name_select_aliases() {
    let rt = seeded("json-filter-alias").await;
    write_wod_arrow(&rt.datasets_dir().join("wod/data.arrow"));

    let batches = run_json(
        &rt,
        json!({
            "output": null,
            "select": [
                {"column": "wod_unique_cast", "alias": null},
                {"column": "Temperature", "alias": "TEMPERATURE"},
                {"column": "Temperature_WODflag", "alias": "TEMPERATURE_QC"},
                {"column": "Temperature.units", "alias": "TEMPERATURE_UNIT"},
                {"column": "z", "alias": "DEPTH"},
                {"column": "z.units", "alias": "DEPTH_UNIT"},
                {"column": "time", "alias": "TIME"},
                {"column": "lon", "alias": "LONGITUDE"},
                {"column": "lat", "alias": "LATITUDE"},
                {"column": "Platform", "alias": "PLATFORM"},
                {"column": "Institute", "alias": "INSTITUTE"},
                {"column": ".featureType", "alias": "FEATURE_TYPE"}
            ],
            "filters": [
                {"column": "TIME", "gt_eq": "2022-01-01T00:00:00", "lt_eq": "2023-01-01T00:00:00"},
                {"is_not_null": {"column": "TEMPERATURE"}},
                {"column": "TEMPERATURE", "neq": -10000000000.0},
                {"column": "TEMPERATURE_QC", "eq": 0.0},
                {"column": "DEPTH", "gt_eq": 0, "lt_eq": 10}
            ],
            "distinct": null,
            "sort_by": [{"Asc": "wod_unique_cast"}],
            "limit": null,
            "offset": null,
            "from": {"arrow": {"paths": ["wod/data.arrow"]}}
        }),
    )
    .await;

    assert_eq!(batches[0].schema().field(1).name(), "TEMPERATURE");
    assert_eq!(i64_column(&batches, 0), vec![1, 7]);
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
