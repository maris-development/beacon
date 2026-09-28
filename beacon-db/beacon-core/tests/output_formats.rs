//! Output formats beyond CSV/NetCDF (which `query_output_file` covers): the
//! result-file path for Parquet and Arrow IPC, verified down to the file magic
//! so a mislabeled writer cannot pass.

mod common;

use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use common::runtime;
use serde_json::json;

/// Runs `SELECT a FROM t` with the given output format and returns the bytes of
/// the produced file.
async fn output_bytes(tag: &str, output: serde_json::Value) -> Vec<u8> {
    let rt = runtime(tag).await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2), (3)").await;

    let mut query = Query::sql("SELECT a FROM t".to_string());
    query.output = Some(serde_json::from_value(output).expect("valid output spec"));

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("query with an output format should run");

    match result.query_output {
        QueryOutput::File(file) => {
            std::fs::read(file.path()).expect("the output file should be readable")
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_output_is_a_real_parquet_file() {
    let bytes = output_bytes("output-parquet", json!({ "format": "parquet" })).await;
    assert!(
        bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"),
        "parquet files start and end with the PAR1 magic"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn arrow_output_is_a_real_ipc_file() {
    // "arrow" is the serde alias for the IPC variant — the name clients use.
    let bytes = output_bytes("output-arrow", json!({ "format": "arrow" })).await;
    assert!(
        bytes.starts_with(b"ARROW1"),
        "IPC files start with the ARROW1 magic"
    );
}

/// The ND NetCDF output writes each dimension column as a NetCDF dimension, not one flat `obs`.
#[tokio::test(flavor = "multi_thread")]
async fn nd_netcdf_output_writes_the_dimension_columns_as_dimensions() {
    let rt = runtime("output-nd-netcdf").await;
    rt.sql("CREATE TABLE grid (lat DOUBLE, lon DOUBLE, v DOUBLE)").await;
    rt.sql(
        "INSERT INTO grid VALUES (1.0, 10.0, 0.1), (1.0, 20.0, 0.2), (1.0, 30.0, 0.3), \
         (2.0, 10.0, 0.4), (2.0, 20.0, 0.5), (2.0, 30.0, 0.6)",
    )
    .await;

    let mut query = Query::sql("SELECT lat, lon, v FROM grid".to_string());
    query.output = Some(
        serde_json::from_value(json!({
            "format": { "ndnetcdf": { "dimension_columns": ["lat", "lon"] } }
        }))
        .expect("valid nd netcdf output spec"),
    );

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("an ND NetCDF query should run");
    let QueryOutput::File(file) = result.query_output else {
        panic!("an output format should yield a file, not a stream");
    };

    let nc = beacon_arrow_netcdf::netcdf::open(file.path()).expect("a readable NetCDF file");
    let mut dims: Vec<(String, usize)> = nc.dimensions().map(|d| (d.name(), d.len())).collect();
    dims.sort();
    assert_eq!(dims, vec![("lat".to_string(), 2), ("lon".to_string(), 3)]);

    let v = nc.variable("v").expect("the data column is a variable");
    let v_dims: Vec<String> = v.dimensions().iter().map(|d| d.name()).collect();
    assert_eq!(v_dims, vec!["lat", "lon"]);
}

/// Runs `sql` with `output` and returns the path of the NetCDF file it writes.
async fn netcdf_output(
    rt: &common::TestRuntime,
    sql: &str,
    output: serde_json::Value,
) -> beacon_core::query_result::QueryOutputFile {
    let mut query = Query::sql(sql.to_string());
    query.output = Some(serde_json::from_value(output).expect("valid output spec"));
    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("the NetCDF query should run");
    let QueryOutput::File(file) = result.query_output else {
        panic!("an output format should yield a file, not a stream");
    };
    file
}

/// Parquet written by pandas or pyarrow holds `LargeUtf8` strings, and a flag column is
/// `Boolean`. The flat NetCDF output writes both.
#[tokio::test(flavor = "multi_thread")]
async fn netcdf_output_writes_large_strings_and_booleans() {
    let rt = runtime("output-netcdf-large-utf8").await;
    let sql = "SELECT arrow_cast(name, 'LargeUtf8') AS name, flag \
               FROM (VALUES ('a', true), ('bb', false)) AS t(name, flag)";

    let file = netcdf_output(&rt, sql, json!({ "format": "netcdf" })).await;

    let nc = beacon_arrow_netcdf::netcdf::open(file.path()).expect("a readable NetCDF file");
    assert!(nc.variable("name").is_some(), "the string column is a variable");
    let flag = nc.variable("flag").expect("the boolean column is a variable");
    let values: Vec<u8> = flag.get_values(..).expect("flag values");
    assert_eq!(values, vec![1, 0]);
}

/// The GeoParquet output takes its point from the columns the request names, also when the
/// names are not ones the column detection knows.
#[tokio::test(flavor = "multi_thread")]
async fn geoparquet_output_uses_the_named_coordinate_columns() {
    let rt = runtime("output-geoparquet-named").await;
    rt.sql("CREATE TABLE stations (x_coord DOUBLE, y_coord DOUBLE)").await;
    rt.sql("INSERT INTO stations VALUES (4.5, 52.0)").await;

    let mut query = Query::sql("SELECT x_coord, y_coord FROM stations".to_string());
    query.output = Some(
        serde_json::from_value(json!({
            "format": {
                "geoparquet": { "longitude_column": "x_coord", "latitude_column": "y_coord" }
            }
        }))
        .expect("valid geoparquet output spec"),
    );

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("the named coordinate columns should build the geometry");
    assert!(matches!(result.query_output, QueryOutput::File(_)));
}

/// Regression guard for the GeoParquet COPY sink.
///
/// The sink appends a `geometry` column, so its output schema is one wider than its input.
/// It used to advertise that wider schema from `DataSink::schema()`, which tripped DataFusion's
/// `execute_input_stream` assertion (`sink_schema.len() == input.schema().len()`) and panicked
/// on every COPY. This runs the full `run_query` → COPY → `DataSinkExec::execute` path — the one
/// that panicked — and asserts a real Parquet file comes out. The geoparquet crate's own sink
/// test drives `write_all` directly and never hits `DataSinkExec`, so this is the guard that
/// covers the actual failure.
#[tokio::test(flavor = "multi_thread")]
async fn geoparquet_output_does_not_panic_and_is_a_real_parquet_file() {
    let rt = runtime("output-geoparquet").await;
    rt.sql("CREATE TABLE points (lon DOUBLE, lat DOUBLE, name VARCHAR)")
        .await;
    rt.sql("INSERT INTO points VALUES (4.5, 52.0, 'a'), (5.5, 53.0, 'b')")
        .await;

    let mut query = Query::sql("SELECT lon, lat, name FROM points".to_string());
    query.output = Some(
        serde_json::from_value(json!({
            "format": { "geoparquet": { "longitude_column": "lon", "latitude_column": "lat" } }
        }))
        .expect("valid geoparquet output spec"),
    );

    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("a GeoParquet COPY must not panic or error");

    let bytes = match result.query_output {
        QueryOutput::File(file) => {
            std::fs::read(file.path()).expect("the output file should be readable")
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file, not a stream"),
    };
    assert!(
        bytes.starts_with(b"PAR1") && bytes.ends_with(b"PAR1"),
        "GeoParquet is Parquet underneath — the PAR1 magic must be present"
    );
}
