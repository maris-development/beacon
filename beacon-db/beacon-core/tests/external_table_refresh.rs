//! External tables track files that change underneath them, but only on an explicit
//! `REFRESH` now that event-driven refresh has been removed with `DatasetsStore`.
//!
//! The table itself is a passive provider (it holds no session, so it cannot form a
//! cycle back into the catalog); `REFRESH` re-lists it from a session the runtime owns.

mod common;

use common::{runtime_with, scalar_i64, write_file};

#[tokio::test(flavor = "multi_thread")]
async fn manual_refresh_works_without_storage_events() {
    // No events: the table only becomes current on an explicit REFRESH.
    let rt = runtime_with("ext-manual-refresh", |b| b).await;

    write_file(&rt.datasets_dir().join("obs/a.csv"), "v,name\n1,a\n");
    rt.sql("CREATE EXTERNAL TABLE obs STORED AS CSV LOCATION 'obs/'")
        .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM obs").await), 1);

    write_file(&rt.datasets_dir().join("obs/b.csv"), "v,name\n2,b\n3,c\n");
    rt.sql("REFRESH obs").await;
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM obs").await),
        3,
        "REFRESH should re-list the table's files"
    );
}
