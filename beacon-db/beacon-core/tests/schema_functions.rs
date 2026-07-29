//! The `<read_fn>_schema` table functions: read a file's Arrow schema (one row
//! per column) without scanning its data. One is registered for every `read_*`
//! file-format reader (`read_parquet` -> `read_parquet_schema`, etc.).

mod common;

use common::{column_strings, runtime, scalar_i64, total_rows, write_file, TestRuntime};

async fn seeded(tag: &str) -> TestRuntime {
    let rt = runtime(tag).await;
    write_file(&rt.datasets_dir().join("obs/a.csv"), "id,name,depth\n1,a,10\n2,b,20\n");
    write_file(&rt.datasets_dir().join("obs/b.csv"), "id,name,depth\n3,c,30\n");
    std::fs::copy(parquet_fixture(), rt.datasets_dir().join("data.parquet"))
        .expect("copy parquet fixture");
    rt
}

fn parquet_fixture() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

/// The schema functions are registered and discoverable, one per reader.
#[tokio::test(flavor = "multi_thread")]
async fn schema_functions_are_registered_for_each_reader() {
    let rt = runtime("schema-fn-registered").await;

    // Every `read_*` reader has a `<name>_schema` counterpart, and both return TABLE.
    for name in [
        "read_parquet_schema",
        "read_csv_schema",
        "read_netcdf_schema",
        "read_arrow_schema",
        "read_delta_schema",
    ] {
        assert_eq!(
            scalar_i64(
                &rt.sql(&format!(
                    "SELECT count(*) FROM beacon.system.table_functions \
                     WHERE function_name = '{name}' AND return_type = 'TABLE'"
                ))
                .await
            ),
            1,
            "`{name}` should be a registered table function"
        );
    }

    // There are exactly as many `*_schema` functions as `read_*` readers.
    let readers = scalar_i64(
        &rt.sql(
            "SELECT count(*) FROM beacon.system.table_functions \
             WHERE function_name LIKE 'read\\_%' ESCAPE '\\' \
               AND function_name NOT LIKE '%\\_schema' ESCAPE '\\'",
        )
        .await,
    );
    let schemas = scalar_i64(
        &rt.sql(
            "SELECT count(*) FROM beacon.system.table_functions \
             WHERE function_name LIKE 'read\\_%\\_schema' ESCAPE '\\'",
        )
        .await,
    );
    assert_eq!(readers, schemas, "one schema function per reader");
    assert!(readers >= 5, "sanity: several readers are registered");
}

#[tokio::test(flavor = "multi_thread")]
async fn csv_schema_reports_the_columns() {
    let rt = seeded("schema-fn-csv").await;

    let batches = rt
        .sql("SELECT column_name, data_type, nullable FROM read_csv_schema('obs/a.csv') ORDER BY column_name")
        .await;
    // One row per column of the CSV.
    assert_eq!(total_rows(&batches), 3, "the CSV has three columns");

    let names = column_strings(&batches, 0);
    assert_eq!(names, vec!["depth", "id", "name"]);

    // The output shape is exactly (column_name, data_type, nullable).
    let schema = batches[0].schema();
    let out_cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(out_cols, vec!["column_name", "data_type", "nullable"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_schema_matches_a_zero_row_scan() {
    let rt = seeded("schema-fn-parquet").await;

    // The columns the schema function reports must match the columns a real read
    // produces — same names, same order.
    let from_schema_fn = column_strings(
        &rt.sql("SELECT column_name FROM read_parquet_schema('data.parquet')")
            .await,
        0,
    );
    // `LIMIT 1` guarantees at least one batch to read the schema from (the
    // fixture is non-empty); the column set is what we compare.
    let scanned = rt.sql("SELECT * FROM read_parquet('data.parquet') LIMIT 1").await;
    let from_scan: Vec<String> = scanned[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();

    assert!(!from_schema_fn.is_empty(), "the parquet fixture has columns");
    assert_eq!(
        from_schema_fn, from_scan,
        "the schema function should report exactly the reader's columns, in order"
    );
}

/// A glob covering several files resolves to one unified schema (not one schema
/// per file).
#[tokio::test(flavor = "multi_thread")]
async fn schema_over_a_glob_is_the_unified_schema() {
    let rt = seeded("schema-fn-glob").await;

    let names = column_strings(
        &rt.sql("SELECT column_name FROM read_csv_schema('obs/*.csv') ORDER BY column_name")
            .await,
        0,
    );
    assert_eq!(
        names,
        vec!["depth", "id", "name"],
        "the glob's files share one schema, reported once"
    );
}

/// The schema functions compose with SQL like any table.
#[tokio::test(flavor = "multi_thread")]
async fn schema_function_composes_with_sql() {
    let rt = seeded("schema-fn-compose").await;

    // Count the columns of a file with a scalar SQL query over the function.
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM read_csv_schema('obs/a.csv')").await),
        3,
    );
    // Filter to a single column's type.
    let ty = rt
        .sql("SELECT data_type FROM read_csv_schema('obs/a.csv') WHERE column_name = 'name'")
        .await;
    assert_eq!(total_rows(&ty), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_of_a_missing_file_errors() {
    let rt = seeded("schema-fn-missing").await;

    let err = rt
        .try_sql("SELECT * FROM read_parquet_schema('no/such/file.parquet')")
        .await
        .err()
        .expect("reading the schema of a missing file should error");
    assert!(!err.to_string().is_empty());
}
