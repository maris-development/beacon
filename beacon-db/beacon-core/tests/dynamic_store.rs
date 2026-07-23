//! A bare `RuntimeBuilder` — no `with_default_store` / `with_default_object_store`
//! — puts the runtime in *dynamic* storage mode: there is no datasets root to
//! resolve bare paths against, so every path is resolved on the fly by its own
//! form (DuckDB-style). A schemeless path is a local filesystem path; a
//! `file://` URL is local; object-store schemes (`s3://`, …) route to their own
//! store.
//!
//! The URL-resolution rules themselves are unit-tested in
//! `beacon-datafusion-ext::listing_url_resolver`. This suite is the *integration*
//! proof: that those resolutions actually read real bytes end to end through the
//! `Runtime`, across formats and statement kinds, with no default store set.
//!
//! Everything here uses **absolute** paths (or `file://` URLs), which do not
//! depend on the process working directory, so these tests are safe to run in
//! parallel. The working-directory (relative-path) behaviour lives in
//! `default_store_cwd.rs`, which must serialize around the process-wide cwd.

use std::path::{Path, PathBuf};

use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use beacon_core::query::Query;
use beacon_core::query_result::QueryOutput;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;
use tempfile::TempDir;

/// A bare runtime in dynamic storage mode: no default store, in-memory tables,
/// a default tmp dir. Auth is off, so queries run as the system identity.
async fn dynamic_runtime() -> Runtime {
    RuntimeBuilder::new()
        .build()
        .await
        .expect("a bare builder should build")
}

async fn try_sql(rt: &Runtime, sql: &str) -> anyhow::Result<Vec<RecordBatch>> {
    let batches = rt
        .run_query(Query::sql(sql.to_string()), AuthIdentity::system())
        .await?
        .into_record_stream()?
        .try_collect::<Vec<_>>()
        .await?;
    Ok(batches)
}

async fn sql(rt: &Runtime, sql: &str) -> Vec<RecordBatch> {
    try_sql(rt, sql)
        .await
        .unwrap_or_else(|e| panic!("SQL failed: {sql}\n{e}"))
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 column")
        .value(0)
}

fn scalar_string(batches: &[RecordBatch]) -> String {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("a Utf8 column")
        .value(0)
        .to_string()
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// A canonicalized temp dir, standing in for some absolute location on disk.
/// Canonicalized because macOS reports `/var/...` as `/private/var/...`, and the
/// object store lists the canonical form.
fn temp_dir() -> (TempDir, PathBuf) {
    let root = tempfile::Builder::new()
        .prefix("beacon-dynamic-store-")
        .tempdir()
        .expect("create temp dir");
    let path = std::fs::canonicalize(root.path()).expect("canonical temp dir");
    (root, path)
}

fn write(path: &Path, contents: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

fn parquet_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root")
        .join("test-datasets/test_file.parquet")
}

fn netcdf_fixture() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("beacon-file-formats/beacon-arrow-netcdf/test_files/wod_ctd_1964.nc")
}

// --------------------------------------------------------------------------
// External tables over absolute local paths.
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn absolute_directory_location_reads_every_file() {
    let (_root, dir) = temp_dir();
    write(&dir.join("obs/a.csv"), "v,name\n1,a\n2,b\n");
    write(&dir.join("obs/b.csv"), "v,name\n3,c\n");

    let rt = dynamic_runtime().await;
    let location = format!("{}/", dir.join("obs").display());
    sql(
        &rt,
        &format!("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION '{location}'"),
    )
    .await;

    assert_eq!(
        scalar_i64(&sql(&rt, "SELECT count(*) FROM obs").await),
        3,
        "an absolute directory LOCATION should list all its files"
    );
    // The resolved path is genuinely the absolute one, independent of anything
    // else: a predicate returns the right row.
    assert_eq!(
        scalar_i64(&sql(&rt, "SELECT v FROM obs WHERE name = 'c'").await),
        3
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn absolute_single_file_location_reads_one_file() {
    let (_root, dir) = temp_dir();
    let file = dir.join("solo.csv");
    write(&file, "v,name\n7,x\n8,y\n");

    let rt = dynamic_runtime().await;
    // No trailing slash → a single file, opened directly (not listed as a dir).
    sql(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE solo STORED AS CSV LOCATION '{}'",
            file.display()
        ),
    )
    .await;

    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM solo").await), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn file_url_directory_location_reads() {
    let (_root, dir) = temp_dir();
    write(&dir.join("d/a.csv"), "v\n1\n");
    write(&dir.join("d/b.csv"), "v\n2\n3\n");

    let rt = dynamic_runtime().await;
    // An explicit `file://` URL is resolved as a local directory.
    let location = format!("file://{}/", dir.join("d").display());
    sql(
        &rt,
        &format!("CREATE EXTERNAL TABLE viafile STORED AS CSV LOCATION '{location}'"),
    )
    .await;

    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM viafile").await), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn parquet_external_table_over_absolute_directory() {
    let (_root, dir) = temp_dir();
    std::fs::create_dir_all(dir.join("pq")).unwrap();
    std::fs::copy(parquet_fixture(), dir.join("pq/data.parquet")).expect("copy parquet fixture");

    let rt = dynamic_runtime().await;
    let location = format!("{}/", dir.join("pq").display());
    sql(
        &rt,
        &format!("CREATE EXTERNAL TABLE pq STORED AS PARQUET LOCATION '{location}'"),
    )
    .await;

    assert!(
        scalar_i64(&sql(&rt, "SELECT count(*) FROM pq").await) > 0,
        "the parquet fixture should read rows over an absolute path"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn two_absolute_locations_resolve_to_their_own_stores() {
    let (_root_a, dir_a) = temp_dir();
    let (_root_b, dir_b) = temp_dir();
    write(&dir_a.join("x.csv"), "v\n1\n");
    write(&dir_b.join("y.csv"), "v\n1\n2\n3\n");

    let rt = dynamic_runtime().await;
    sql(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE ta STORED AS CSV LOCATION '{}'",
            dir_a.join("x.csv").display()
        ),
    )
    .await;
    sql(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE tb STORED AS CSV LOCATION '{}'",
            dir_b.join("y.csv").display()
        ),
    )
    .await;

    // Each table reads from its own absolute location — dynamic mode resolves
    // each path independently, so two roots coexist in one runtime.
    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM ta").await), 1);
    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM tb").await), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn dropped_external_table_can_be_recreated_at_a_new_absolute_path() {
    let (_root, dir) = temp_dir();
    write(&dir.join("one/a.csv"), "v\n1\n");
    write(&dir.join("two/a.csv"), "v\n1\n2\n");

    let rt = dynamic_runtime().await;
    sql(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '{}/'",
            dir.join("one").display()
        ),
    )
    .await;
    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM t").await), 1);

    sql(&rt, "DROP TABLE t").await;
    sql(
        &rt,
        &format!(
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION '{}/'",
            dir.join("two").display()
        ),
    )
    .await;
    assert_eq!(
        scalar_i64(&sql(&rt, "SELECT count(*) FROM t").await),
        2,
        "the re-created table should resolve the new absolute path"
    );
}

// --------------------------------------------------------------------------
// `read_*` functions over absolute local paths and globs.
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn read_csv_over_absolute_path_and_globs() {
    let (_root, dir) = temp_dir();
    write(&dir.join("r/one.csv"), "v,name\n1,a\n2,b\n");
    write(&dir.join("r/two.csv"), "v,name\n3,c\n");
    write(&dir.join("r/nested/three.csv"), "v,name\n4,d\n");

    let rt = dynamic_runtime().await;

    // A single absolute file.
    assert_eq!(
        scalar_i64(
            &sql(
                &rt,
                &format!(
                    "SELECT count(*) FROM read_csv('{}')",
                    dir.join("r/one.csv").display()
                )
            )
            .await
        ),
        2
    );

    // Data-listing globs are NOT segment-aware: `*` crosses path separators
    // (DataFusion's listing semantics), so `*.csv` reaches the nested file too.
    // This is deliberately different from auth PATH grants, which ARE
    // segment-aware — see the auth suite's `enforced_path_glob_is_segment_aware`.
    assert_eq!(
        scalar_i64(
            &sql(
                &rt,
                &format!(
                    "SELECT count(*) FROM read_csv('{}/*.csv')",
                    dir.join("r").display()
                )
            )
            .await
        ),
        4,
        "a listing `*.csv` glob crosses separators and matches every csv under the root"
    );

    // A recursive glob likewise reaches every file.
    assert_eq!(
        scalar_i64(
            &sql(
                &rt,
                &format!(
                    "SELECT count(*) FROM read_csv('{}/**/*.csv')",
                    dir.join("r").display()
                )
            )
            .await
        ),
        4,
        "a `**/*.csv` glob should match every csv under the root"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_parquet_and_netcdf_over_absolute_paths() {
    let (_root, dir) = temp_dir();
    std::fs::copy(parquet_fixture(), dir.join("data.parquet")).expect("copy parquet fixture");
    std::fs::copy(netcdf_fixture(), dir.join("wod.nc")).expect("copy netcdf fixture");

    let rt = dynamic_runtime().await;

    assert!(
        scalar_i64(
            &sql(
                &rt,
                &format!(
                    "SELECT count(*) FROM read_parquet('{}')",
                    dir.join("data.parquet").display()
                )
            )
            .await
        ) > 0
    );

    // NetCDF is read *natively* by path; in dynamic mode the `file` scheme maps
    // to a filesystem root of `/`, so the absolute object path opens correctly.
    let rows = sql(
        &rt,
        &format!(
            "SELECT * FROM read_netcdf('{}') LIMIT 5",
            dir.join("wod.nc").display()
        ),
    )
    .await;
    assert!(total_rows(&rows) > 0, "the netcdf fixture should read rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn reading_a_missing_absolute_file_errors() {
    let (_root, dir) = temp_dir();
    let rt = dynamic_runtime().await;

    let missing = dir.join("nope/absent.parquet");
    let err = try_sql(
        &rt,
        &format!("SELECT count(*) FROM read_parquet('{}')", missing.display()),
    )
    .await
    .err()
    .expect("reading a missing absolute file should error, not return empty");
    assert!(!err.to_string().is_empty());
}

// --------------------------------------------------------------------------
// Storage that does NOT depend on the datasets store still works in dynamic
// mode: managed tables live in the tables store, query output in the tmp store.
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn managed_tables_work_without_a_default_store() {
    let rt = dynamic_runtime().await;

    // Managed (Lance) tables live in the tables store, which is independent of
    // the (absent) datasets default store.
    sql(&rt, "CREATE TABLE m (id BIGINT, name VARCHAR)").await;
    sql(&rt, "INSERT INTO m VALUES (1, 'a'), (2, 'b'), (3, 'b')").await;
    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM m").await), 3);

    sql(&rt, "UPDATE m SET name = 'z' WHERE id = 1").await;
    assert_eq!(
        scalar_string(&sql(&rt, "SELECT name FROM m WHERE id = 1").await),
        "z"
    );

    sql(&rt, "DELETE FROM m WHERE name = 'b'").await;
    assert_eq!(scalar_i64(&sql(&rt, "SELECT count(*) FROM m").await), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn ctas_from_an_absolute_external_source_without_a_default_store() {
    let (_root, dir) = temp_dir();
    write(&dir.join("src.csv"), "v\n10\n20\n30\n");

    let rt = dynamic_runtime().await;
    sql(
        &rt,
        &format!(
            "CREATE TABLE materialized AS SELECT * FROM read_csv('{}')",
            dir.join("src.csv").display()
        ),
    )
    .await;

    assert_eq!(
        scalar_i64(&sql(&rt, "SELECT count(*) FROM materialized").await),
        3
    );
    assert_eq!(
        scalar_i64(&sql(&rt, "SELECT sum(v) FROM materialized").await),
        60,
        "CTAS should read every row from the absolute source"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn query_output_file_works_without_a_default_store() {
    let rt = dynamic_runtime().await;
    sql(&rt, "CREATE TABLE t (a BIGINT)").await;
    sql(&rt, "INSERT INTO t VALUES (1), (2)").await;

    let mut query = Query::sql("SELECT a FROM t".to_string());
    query.output =
        Some(serde_json::from_value(serde_json::json!({ "format": "csv" })).expect("valid output"));

    let result = rt
        .run_query(query, AuthIdentity::system())
        .await
        .expect("query with output should run in dynamic mode");
    match result.query_output {
        QueryOutput::File(file) => {
            // Output files go to the tmp store, which is independent of the
            // (absent) datasets store.
            assert!(file.path().exists());
            assert!(file.size().expect("output size") > 0);
        }
        QueryOutput::Stream(_) => panic!("an output format should yield a file"),
    }
}

// --------------------------------------------------------------------------
// The dataset-listing UDTF over an absolute pattern (no default store to glob).
// --------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn list_datasets_over_an_absolute_pattern() {
    let (_root, dir) = temp_dir();
    write(&dir.join("ds/a.csv"), "v\n1\n");
    write(&dir.join("ds/sub/b.csv"), "v\n2\n");
    std::fs::copy(parquet_fixture(), dir.join("ds/c.parquet")).expect("copy parquet fixture");

    let rt = dynamic_runtime().await;

    // In dynamic mode there is no default store to list, so the pattern must be
    // an absolute path; discovery then walks that subtree.
    let pattern = format!("{}/**", dir.join("ds").display());
    let count = scalar_i64(
        &sql(
            &rt,
            &format!("SELECT count(*) FROM list_datasets('{pattern}')"),
        )
        .await,
    );
    assert_eq!(
        count, 3,
        "discovery should find the two CSVs and the parquet under the absolute pattern"
    );

    let csvs = scalar_i64(
        &sql(
            &rt,
            &format!(
                "SELECT count(*) FROM list_datasets('{pattern}') WHERE file_name LIKE '%.csv'"
            ),
        )
        .await,
    );
    assert_eq!(csvs, 2);
}
