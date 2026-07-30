//! An external table is catalog state, so it must survive a restart the way a
//! managed table does — `CREATE EXTERNAL TABLE` records a definition, and
//! reopening the same tables store must bring the table back.
//!
//! Managed tables are covered by `storage_configurability::managed_table_persists_across_restart`;
//! external tables had no equivalent, which is the gap these fill.

mod common;

use beacon_core::AuthIdentity;
use common::write_file;

/// `CREATE EXTERNAL TABLE` over a CSV, then reopen the same `beacon.db`: the
/// table is still registered and still reads its rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_table_persists_across_restart() {
    let rt = common::restartable_runtime("persist-external-table", |b| b).await;
    write_file(
        &rt.datasets_dir().join("obs/a.csv"),
        "v,name\n1,a\n2,b\n3,c\n",
    );

    rt.sql("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'")
        .await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM obs").await),
        3,
        "sanity: the external table reads before the restart"
    );

    let rt = rt.restart().await;

    // Registered in the catalog again...
    let listed = rt
        .sql("SELECT table_name FROM information_schema.tables WHERE table_name = 'obs'")
        .await;
    assert_eq!(
        common::total_rows(&listed),
        1,
        "the external table should still be listed after a restart"
    );

    // ...and still usable, not just a name in the catalog.
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM obs").await),
        3,
        "the restored external table should still read its rows"
    );
}

/// The same, for an external table whose `LOCATION` is a single file rather than
/// a directory — a different provider shape, so persisted separately.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn external_table_over_a_single_file_persists_across_restart() {
    let rt = common::restartable_runtime("persist-external-file", |b| b).await;
    write_file(&rt.datasets_dir().join("one/b.csv"), "v\n7\n8\n");

    rt.sql("CREATE EXTERNAL TABLE one STORED AS CSV LOCATION 'one/b.csv'")
        .await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM one").await),
        2,
    );

    let rt = rt.restart().await;
    assert_eq!(
        common::scalar_i64(&rt.sql("SELECT count(*) FROM one").await),
        2,
        "a single-file external table should survive a restart"
    );
}

/// A dropped external table must stay dropped — the persisted definition is
/// removed, not merely deregistered from the live catalog.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_external_table_does_not_come_back() {
    let rt = common::restartable_runtime("drop-external-table", |b| b).await;
    write_file(&rt.datasets_dir().join("obs/a.csv"), "v\n1\n");

    rt.sql("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'")
        .await;
    rt.sql("DROP TABLE obs").await;

    let rt = rt.restart().await;
    let listed = rt
        .sql("SELECT table_name FROM information_schema.tables WHERE table_name = 'obs'")
        .await;
    assert_eq!(
        common::total_rows(&listed),
        0,
        "a dropped external table must not reappear after a restart"
    );
    let _ = AuthIdentity::system(); // keep the import meaningful if assertions change
}
