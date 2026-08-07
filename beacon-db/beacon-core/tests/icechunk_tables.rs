//! End-to-end tests for Icechunk support driven through the SQL endpoint
//! (`Runtime::run_query`): the `read_icechunk()` table function, `CREATE
//! EXTERNAL TABLE ... STORED AS ICECHUNK`, version selection, and `DROP TABLE`.

mod common;

use std::path::Path;

use arrow::array::{Array, Float64Array};
use arrow::record_batch::RecordBatch;
use beacon_icechunk::fixture;
use common::{TestRuntime, scalar_i64};

/// Write the two-commit fixture repository under the datasets dir and return
/// its datasets-relative location.
async fn write_fixture(datasets_dir: &Path, rel: &str) -> (String, fixture::FixtureSnapshots) {
    let snapshots = fixture::write_gridded_repository(&datasets_dir.join(rel))
        .await
        .expect("the fixture repository should be written");
    (rel.to_string(), snapshots)
}

fn scalar_f64(batches: &[RecordBatch]) -> f64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("a Float64 column")
        .value(0)
}

#[tokio::test(flavor = "multi_thread")]
async fn icechunk_table_function_and_external_table() {
    let rt: TestRuntime = common::runtime("icechunk-e2e").await;

    let rel = format!("icechunk_e2e_{}", std::process::id());
    let (location, snapshots) = write_fixture(rt.datasets_dir(), &rel).await;
    let table = format!("ic_{}", std::process::id());

    // read_icechunk(): the tip of `main` is the second commit.
    let count = scalar_i64(
        &rt.sql(&format!(
            "SELECT count(*) FROM read_icechunk('{location}')"
        ))
        .await,
    );
    assert_eq!(count, fixture::ROWS as i64);
    let sst = scalar_f64(
        &rt.sql(&format!(
            "SELECT max(sst) FROM read_icechunk('{location}')"
        ))
        .await,
    );
    assert_eq!(sst, fixture::SECOND_SST);

    // read_icechunk(location, NULL, snapshot): a pinned, older version.
    let pinned = scalar_f64(
        &rt.sql(&format!(
            "SELECT max(sst) FROM read_icechunk('{location}', NULL, '{}')",
            snapshots.first
        ))
        .await,
    );
    assert_eq!(pinned, fixture::FIRST_SST);

    // read_icechunk_schema(): the columns, without a scan.
    let columns = rt
        .sql(&format!(
            "SELECT column_name FROM read_icechunk_schema('{location}')"
        ))
        .await;
    let names: Vec<String> = columns
        .iter()
        .flat_map(|batch| {
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap();
            (0..column.len())
                .map(|i| column.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    for expected in ["lat", "lon", "time", "sst"] {
        assert!(
            names.iter().any(|name| name == expected),
            "read_icechunk_schema should list {expected}: {names:?}"
        );
    }

    // CREATE EXTERNAL TABLE ... STORED AS ICECHUNK, then SELECT.
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE {table} STORED AS ICECHUNK LOCATION '{location}'"
    ))
    .await;
    let count = scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await);
    assert_eq!(count, fixture::ROWS as i64, "external table should expose the grid");

    // A predicate on a coordinate prunes, as it does for a plain zarr store.
    let pruned = scalar_i64(
        &rt.sql(&format!("SELECT count(*) FROM {table} WHERE lat > 100000"))
            .await,
    );
    assert_eq!(pruned, 0, "an impossible predicate should prune all rows");

    // DROP TABLE deregisters it (the repository stays on disk).
    rt.sql(&format!("DROP TABLE {table}")).await;
    assert!(
        rt.try_sql(&format!("SELECT count(*) FROM {table}"))
            .await
            .is_err(),
        "querying a dropped table should error"
    );
}

/// `OPTIONS ('snapshot' '…')` pins the version, and the pinned table keeps
/// returning that snapshot's data after a later commit lands on `main`.
#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_external_table_survives_a_later_commit() {
    let rt: TestRuntime = common::runtime("icechunk-pin").await;

    let rel = format!("icechunk_pin_{}", std::process::id());
    let (location, snapshots) = write_fixture(rt.datasets_dir(), &rel).await;

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE pinned STORED AS ICECHUNK LOCATION '{location}' \
         OPTIONS ('snapshot' '{}')",
        snapshots.first
    ))
    .await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE tip STORED AS ICECHUNK LOCATION '{location}'"
    ))
    .await;

    assert_eq!(
        scalar_f64(&rt.sql("SELECT max(sst) FROM pinned").await),
        fixture::FIRST_SST
    );

    fixture::append_commit(&rt.datasets_dir().join(&rel), 30.0)
        .await
        .unwrap();

    assert_eq!(
        scalar_f64(&rt.sql("SELECT max(sst) FROM pinned").await),
        fixture::FIRST_SST,
        "a pinned snapshot must not see a later commit"
    );
    assert_eq!(
        scalar_f64(&rt.sql("SELECT max(sst) FROM tip").await),
        30.0,
        "a branch-backed table must see the new tip"
    );
}

/// An Icechunk external table is persisted like every other table definition,
/// so it comes back after a restart.
#[tokio::test(flavor = "multi_thread")]
async fn an_external_table_survives_a_restart() {
    let rt: TestRuntime = common::restartable_runtime("icechunk-restart", |b| b).await;

    let rel = format!("icechunk_restart_{}", std::process::id());
    let (location, _) = write_fixture(rt.datasets_dir(), &rel).await;

    rt.sql(&format!(
        "CREATE EXTERNAL TABLE persisted STORED AS ICECHUNK LOCATION '{location}'"
    ))
    .await;

    let rt = rt.restart().await;
    let count = scalar_i64(&rt.sql("SELECT count(*) FROM persisted").await);
    assert_eq!(
        count,
        fixture::ROWS as i64,
        "the table definition should reload from table.json"
    );
}

/// A plain Zarr store still reads through `read_zarr` — Icechunk support does
/// not change how a listed store is discovered.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_zarr_store_still_reads() {
    let rt: TestRuntime = common::runtime("icechunk-zarr-unchanged").await;

    // The bundled zarr fixture, copied into the datasets store.
    let source = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../beacon-file-formats/beacon-arrow-zarr/test_files/gridded-example.zarr");
    let target = rt.datasets_dir().join("gridded-example.zarr");
    copy_dir(&source, &target);

    let count = scalar_i64(
        &rt.sql("SELECT count(*) FROM read_zarr('gridded-example.zarr') LIMIT 1")
            .await,
    );
    assert!(count > 0, "a plain zarr store must still read");
}

/// Recursively copy a directory tree.
fn copy_dir(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), &to).unwrap();
        }
    }
}
