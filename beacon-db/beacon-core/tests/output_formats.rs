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

/// Unpacks a zip archive into `dir` and returns the sorted entry names.
fn unzip(archive: &std::path::Path, dir: &std::path::Path) -> Vec<String> {
    let file = std::fs::File::open(archive).expect("the output file should be readable");
    let mut archive = zip::ZipArchive::new(file).expect("zarr output should be a zip archive");
    let mut names: Vec<String> = archive.file_names().map(str::to_string).collect();
    names.sort();
    archive.extract(dir).expect("the archive should unpack");
    names
}

/// The zarr output is a zipped flat store: one 1-D array per column over `obs`.
/// Unpacked, it reads back through `read_zarr` with the same rows and nulls.
#[tokio::test(flavor = "multi_thread")]
async fn zarr_output_is_a_zipped_flat_store_that_reads_back() {
    let rt = runtime("output-zarr").await;
    rt.sql("CREATE TABLE obs (id BIGINT, temp DOUBLE, site VARCHAR)")
        .await;
    rt.sql("INSERT INTO obs VALUES (1, 10.5, 'a'), (2, NULL, 'b'), (3, 12.5, NULL)")
        .await;

    let mut query = Query::sql("SELECT id, temp, site FROM obs".to_string());
    query.output = Some(serde_json::from_value(json!({ "format": "zarr" })).unwrap());
    let result = rt
        .runtime
        .run_query(query, rt.admin().await)
        .await
        .expect("a zarr output query should run");
    let QueryOutput::File(file) = result.query_output else {
        panic!("an output format should yield a file, not a stream");
    };
    assert!(
        file.path().to_string_lossy().ends_with(".zarr.zip"),
        "{:?}",
        file.path()
    );

    let store = rt.datasets_dir().join("back.zarr");
    let names = unzip(file.path(), &store);
    for expected in ["zarr.json", "id/zarr.json", "temp/zarr.json", "site/zarr.json"] {
        assert!(names.iter().any(|n| n == expected), "missing {expected} in {names:?}");
    }
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(store.join("temp/zarr.json")).unwrap()).unwrap();
    assert_eq!(meta["shape"], json!([3]));
    assert_eq!(meta["dimension_names"], json!(["obs"]));

    // The scratch store directory is gone; only the zip itself remains.
    let leftovers: Vec<_> = std::fs::read_dir(rt.tmp_dir())
        .unwrap()
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("beacon_out_"))
        .map(|entry| entry.file_name())
        .collect();
    assert!(leftovers.is_empty(), "scratch directories left: {leftovers:?}");

    let rows = rt
        .sql("SELECT id, temp, site FROM read_zarr(['back.zarr/']) ORDER BY id")
        .await;
    assert_eq!(common::total_rows(&rows), 3);
    let batch = arrow::compute::concat_batches(&rows[0].schema(), &rows).unwrap();
    let temp = arrow::compute::cast(batch.column(1), &arrow::datatypes::DataType::Float64).unwrap();
    let temp = temp
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .unwrap();
    assert_eq!(temp.iter().collect::<Vec<_>>(), vec![Some(10.5), None, Some(12.5)]);
    let site = arrow::compute::cast(batch.column(2), &arrow::datatypes::DataType::Utf8).unwrap();
    let site = site
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    assert_eq!(site.iter().collect::<Vec<_>>(), vec![Some("a"), Some("b"), None]);
}

/// `COPY TO ... STORED AS ZARR` writes a plain directory store, not a zip.
#[tokio::test(flavor = "multi_thread")]
async fn copy_to_zarr_writes_a_directory_store() {
    let rt = runtime("copy-zarr").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;

    rt.sql("COPY (SELECT a FROM t) TO 'tmp:///copy.zarr' STORED AS ZARR")
        .await;

    let store = rt.tmp_dir().join("copy.zarr");
    assert!(store.is_dir(), "expected a directory store at {store:?}");
    assert!(store.join("zarr.json").is_file(), "root group metadata");
    assert!(store.join("a/zarr.json").is_file(), "array metadata");

    // A second COPY must not overwrite the store.
    let err = rt
        .try_sql("COPY (SELECT a FROM t) TO 'tmp:///copy.zarr' STORED AS ZARR")
        .await
        .expect_err("an existing store should not be replaced");
    assert!(err.to_string().contains("already exists"), "{err}");
}
