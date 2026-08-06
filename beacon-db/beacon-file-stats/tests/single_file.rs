//! The whole statistics store inside one `beacon.db`.
//!
//! Two kinds of state, one file. The registry is a B-tree, because a point
//! lookup over a million paths does not belong in an object store. The segments
//! are objects, because they are read by byte range and never updated. Both live
//! in the same `RedbStore`: the registry through its redb database, the segments
//! through its `ObjectStore` face.
//!
//! What these tests pin down is that the combination survives the two events
//! that would otherwise lose data quietly: a restart, and a vacuum.

use std::sync::Arc;

use arrow::datatypes::DataType;
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{FileStatsStore, ObservedFile, Registry, SegmentBuilder, StatScalar};
use beacon_redb_store::{RedbStore, VacuumMode};
use object_store::{ObjectStore, path::Path};

/// Where the statistics live inside the database's object namespace.
const STATS_PREFIX: &str = "__file_stats__";

fn stat(min: f64, max: f64) -> ColumnStat {
    ColumnStat {
        min: StatScalar::F64(min),
        max: StatScalar::F64(max),
        null_count: Some(0),
        row_count: Some(100),
        data_type: DataType::Float64,
    }
}

/// Build a store whose registry and segments both sit in `redb`.
async fn open(redb: &RedbStore) -> FileStatsStore {
    let registry = Arc::new(Registry::from_database(redb.database()).unwrap());
    let object_store: Arc<dyn ObjectStore> = Arc::new(redb.clone());
    FileStatsStore::open(registry, object_store, Path::from(STATS_PREFIX))
        .await
        .unwrap()
}

/// Write three files' statistics through one database handle.
async fn seed(store: &FileStatsStore) {
    let files: Vec<ObservedFile> = (0..3)
        .map(|i| ObservedFile::new(format!("argo/{i}.nc"), 4096, 1_700_000_000_000))
        .collect();
    let file_ids = store.registry().intern_files(&files).unwrap();
    let columns = store.registry().intern_columns(&["TEMP", "PSAL"]).unwrap();

    let mut builder = SegmentBuilder::new();
    builder.push_file(file_ids[0], [(columns[0], stat(0.0, 10.0))]);
    builder.push_file(
        file_ids[1],
        [(columns[0], stat(20.0, 30.0)), (columns[1], stat(34.0, 35.0))],
    );
    builder.push_file(file_ids[2], [(columns[1], stat(30.0, 31.0))]);
    store.commit_segment(builder).await.unwrap().unwrap();

    for id in &file_ids {
        store
            .registry()
            .mark_analyzed(*id, "netcdf", Some(1_000), Some(4096), 1)
            .unwrap();
    }
}

/// Both halves of the store answer, from one file.
#[tokio::test]
async fn registry_and_segments_share_one_database() {
    let dir = tempfile::tempdir().unwrap();
    let redb = RedbStore::open(dir.path().join("beacon.db")).unwrap();
    let store = open(&redb).await;
    seed(&store).await;

    // The registry half.
    let id = store.registry().file_id("argo/1.nc").unwrap().unwrap();
    let record = store.registry().record(id).unwrap().unwrap();
    assert_eq!(record.num_rows, Some(1_000));
    assert_eq!(record.format, "netcdf");

    // The segment half.
    let temp = store.column_stats_by_name("TEMP", (0, 2)).await.unwrap();
    assert_eq!(temp[0].file_ids, vec![0, 1]);

    // And the segment really is an object in this database, under the prefix.
    let object_store: Arc<dyn ObjectStore> = Arc::new(redb.clone());
    let listed: Vec<String> = futures::StreamExt::collect::<Vec<_>>(
        object_store.list(Some(&Path::from(STATS_PREFIX))),
    )
    .await
    .into_iter()
    .map(|meta| meta.unwrap().location.to_string())
    .collect();
    assert!(listed.iter().any(|p| p.ends_with("segment-00000000.bfs")));
    assert!(listed.iter().any(|p| p.ends_with("manifest.bin")));
}

/// Copy the one file and you carry the statistics with it. That is the whole
/// point of putting them here rather than beside it.
#[tokio::test]
async fn everything_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("beacon.db");

    {
        let redb = RedbStore::open(&file).unwrap();
        let store = open(&redb).await;
        seed(&store).await;
    } // every handle dropped, so redb releases its exclusive lock

    let redb = RedbStore::open(&file).unwrap();
    let store = open(&redb).await;

    let id = store.registry().file_id("argo/2.nc").unwrap().unwrap();
    assert_eq!(store.registry().record(id).unwrap().unwrap().num_rows, Some(1_000));
    assert_eq!(store.registry().num_files().unwrap(), 3);
    assert_eq!(store.num_segments().await, 1);

    let psal = store.column_stats_by_name("PSAL", (0, 2)).await.unwrap();
    assert_eq!(psal[0].file_ids, vec![1, 2]);
}

/// A vacuum rewrites the file. The registry tables are not objects, so nothing
/// in the object copy path would carry them: `RedbStore::vacuum` copies tenant
/// tables verbatim, and this is the test that says so end to end.
#[tokio::test]
async fn everything_survives_a_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("beacon.db");

    let redb = RedbStore::open(&file).unwrap();
    {
        let store = open(&redb).await;
        seed(&store).await;
    } // drop the tenant's database handle, which vacuum insists on

    let redb = redb.vacuum(VacuumMode::Rewrite).await.unwrap();
    let store = open(&redb).await;

    // The registry survived.
    assert_eq!(store.registry().num_files().unwrap(), 3);
    assert_eq!(store.registry().num_columns().unwrap(), 2);
    let id = store.registry().file_id("argo/0.nc").unwrap().unwrap();
    assert_eq!(store.registry().record(id).unwrap().unwrap().num_rows, Some(1_000));

    // The column ids still mean what the segment says they mean. A vacuum that
    // kept the segments but renumbered the registry would read as silent
    // corruption, so this is the assertion that matters most.
    let temp = store.column_stats_by_name("TEMP", (0, 2)).await.unwrap();
    assert_eq!(temp.len(), 1);
    assert_eq!(temp[0].file_ids, vec![0, 1]);
}

/// A file that changed under us must stop serving its old statistics, and the
/// segment it was written into must stay readable for the files that did not.
#[tokio::test]
async fn a_changed_file_goes_stale_without_disturbing_the_segment() {
    let dir = tempfile::tempdir().unwrap();
    let redb = RedbStore::open(dir.path().join("beacon.db")).unwrap();
    let store = open(&redb).await;
    seed(&store).await;

    let changed = ObservedFile::new("argo/1.nc", 999_999, 1_800_000_000_000);
    let id = store.registry().intern_files(&[changed]).unwrap()[0];

    assert_eq!(id, 1, "the id survives the change");
    assert_eq!(
        store.registry().record(id).unwrap().unwrap().state,
        beacon_file_stats::FileState::Stale
    );
    assert_eq!(store.registry().num_pending().unwrap(), 1);

    // The segment is immutable, so it still reports what it recorded. Acting on
    // the stale flag is the reader's job, not the segment's.
    let temp = store.column_stats_by_name("TEMP", (0, 2)).await.unwrap();
    assert_eq!(temp[0].file_ids, vec![0, 1]);
}
