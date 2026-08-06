//! Pruning, and the one invariant it must never break.
//!
//! Dropping a file that could hold a matching row is a silently wrong answer.
//! Keeping a file that could not is a scan the optimizer would have skipped. The
//! two failures are not comparable, so most of what follows checks the first
//! kind cannot happen, and only then that the second is actually avoided.

#![cfg(feature = "datafusion")]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{
    FileId, FileStatsStore, ObservedFile, Registry, SegmentBuilder, StatScalar, prune_files,
};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{binary, col, lit};
use object_store::{ObjectStore, memory::InMemory, path::Path};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, true),
        Field::new("PSAL", DataType::Float64, true),
    ]))
}

fn float_stat(min: f64, max: f64) -> ColumnStat {
    ColumnStat {
        min: StatScalar::F64(min),
        max: StatScalar::F64(max),
        null_count: 0,
        row_count: 100,
        data_type: DataType::Float64,
    }
}

fn greater_than(column: &str, value: f64) -> Arc<dyn PhysicalExpr> {
    let schema = schema();
    binary(
        col(column, &schema).unwrap(),
        Operator::Gt,
        lit(value),
        &schema,
    )
    .unwrap()
}

async fn store() -> (FileStatsStore, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::open(dir.path().join("registry.redb")).unwrap());
    let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = FileStatsStore::open(registry, object_store, Path::from("stats"))
        .await
        .unwrap();
    (store, dir)
}

/// Register `ranges` as one segment of TEMP statistics, and return the file ids.
async fn seed_temp(store: &FileStatsStore, ranges: &[(f64, f64)]) -> Vec<FileId> {
    let files: Vec<ObservedFile> = (0..ranges.len())
        .map(|i| ObservedFile::new(format!("argo/2024/{i:03}.nc"), 1, 1))
        .collect();
    let ids = store.registry().intern_files(&files).unwrap();
    let column = store.registry().intern_columns(&["TEMP"]).unwrap()[0];

    let mut builder = SegmentBuilder::new();
    for (id, (min, max)) in ids.iter().zip(ranges) {
        builder.push_file(*id, [(column, float_stat(*min, *max))]);
    }
    store.commit_segment(builder).await.unwrap();
    ids
}

/// A selective predicate drops the files whose range rules them out.
#[tokio::test]
async fn a_selective_predicate_drops_the_files_that_cannot_match() {
    let (store, _dir) = store().await;
    let ids = seed_temp(&store, &[(0.0, 5.0), (10.0, 20.0), (30.0, 40.0)]).await;

    let kept = prune_files(&store, &greater_than("TEMP", 25.0), &schema(), &ids).await;
    assert_eq!(kept, vec![2], "only the file whose max exceeds 25 survives");
}

/// The invariant, swept across every threshold that matters: a file whose range
/// could satisfy the predicate is never dropped.
#[tokio::test]
async fn pruning_never_drops_a_file_that_could_match() {
    let ranges: Vec<(f64, f64)> = vec![
        (-10.0, -1.0),
        (0.0, 0.0),
        (0.0, 10.0),
        (5.0, 5.5),
        (9.9, 10.1),
        (10.0, 100.0),
        (50.0, 50.0),
        (-3.0, 80.0),
    ];
    let (store, _dir) = store().await;
    let ids = seed_temp(&store, &ranges).await;

    for step in -20..=110 {
        let threshold = step as f64 / 2.0;
        let kept = prune_files(&store, &greater_than("TEMP", threshold), &schema(), &ids).await;

        for (index, (_, max)) in ranges.iter().enumerate() {
            let could_match = *max > threshold;
            let was_kept = kept.contains(&ids[index]);
            assert!(
                !could_match || was_kept,
                "file {index} with max {max} was dropped at threshold {threshold}"
            );
        }
    }
}

/// Pruning must actually prune, or it is just overhead.
#[tokio::test]
async fn pruning_removes_most_files_for_a_narrow_predicate() {
    let ranges: Vec<(f64, f64)> = (0..100).map(|i| (i as f64, i as f64 + 0.5)).collect();
    let (store, _dir) = store().await;
    let ids = seed_temp(&store, &ranges).await;

    let kept = prune_files(&store, &greater_than("TEMP", 90.0), &schema(), &ids).await;
    assert!(
        kept.len() <= 10,
        "expected a narrow predicate to leave at most 10 of 100, got {}",
        kept.len()
    );
    assert!(!kept.is_empty(), "and not to leave nothing");
}

/// A column no file ever declared carries no statistics, so nothing may be
/// dropped on it.
#[tokio::test]
async fn an_unknown_column_keeps_every_file() {
    let (store, _dir) = store().await;
    let ids = seed_temp(&store, &[(0.0, 5.0), (30.0, 40.0)]).await;

    let kept = prune_files(&store, &greater_than("PSAL", 1_000.0), &schema(), &ids).await;
    assert_eq!(kept, ids, "PSAL has no statistics, so nothing is prunable");
}

/// A file with no statistics inside an otherwise-populated column must survive.
/// This is the case a sparse layout creates constantly.
#[tokio::test]
async fn a_file_without_statistics_survives() {
    let (store, _dir) = store().await;

    let files: Vec<ObservedFile> = (0..3)
        .map(|i| ObservedFile::new(format!("argo/2024/{i}.nc"), 1, 1))
        .collect();
    let ids = store.registry().intern_files(&files).unwrap();
    let column = store.registry().intern_columns(&["TEMP"]).unwrap()[0];

    // File 1 declares no TEMP at all.
    let mut builder = SegmentBuilder::new();
    builder.push_file(ids[0], [(column, float_stat(0.0, 1.0))]);
    builder.push_file(ids[2], [(column, float_stat(0.0, 1.0))]);
    store.commit_segment(builder).await.unwrap();

    let kept = prune_files(&store, &greater_than("TEMP", 100.0), &schema(), &ids).await;
    assert_eq!(
        kept,
        vec![ids[1]],
        "the two files with ranges are prunable; the one without is not"
    );
}

/// A re-analyzed file appears in two segments. The newest range is the true one,
/// and the stale one must not resurrect a file the new range rules out.
#[tokio::test]
async fn the_newest_segment_wins_for_a_re_analyzed_file() {
    let (store, _dir) = store().await;
    let ids = seed_temp(&store, &[(90.0, 100.0)]).await;
    let column = store.registry().column_id("TEMP").unwrap().unwrap();

    // The file is rewritten and now holds a much colder range.
    let mut builder = SegmentBuilder::new();
    builder.push_file(ids[0], [(column, float_stat(0.0, 1.0))]);
    store.commit_segment(builder).await.unwrap();
    assert_eq!(store.num_segments().await, 2);

    let kept = prune_files(&store, &greater_than("TEMP", 50.0), &schema(), &ids).await;
    assert!(
        kept.is_empty(),
        "the newest range rules the file out; the stale one must not save it"
    );
}

/// Files that disagree about a column's type still prune correctly, because each
/// segment is cast to the type the predicate compares against.
#[tokio::test]
async fn segments_with_different_types_still_prune() {
    let (store, _dir) = store().await;
    let files: Vec<ObservedFile> = (0..2)
        .map(|i| ObservedFile::new(format!("argo/2024/{i}.nc"), 1, 1))
        .collect();
    let ids = store.registry().intern_files(&files).unwrap();
    let column = store.registry().intern_columns(&["TEMP"]).unwrap()[0];

    // One segment stores Int16, the other Float64.
    let mut narrow = SegmentBuilder::new();
    narrow.push_file(
        ids[0],
        [(
            column,
            ColumnStat {
                min: StatScalar::I64(1),
                max: StatScalar::I64(2),
                null_count: 0,
                row_count: 10,
                data_type: DataType::Int16,
            },
        )],
    );
    store.commit_segment(narrow).await.unwrap();

    let mut wide = SegmentBuilder::new();
    wide.push_file(ids[1], [(column, float_stat(80.0, 90.0))]);
    store.commit_segment(wide).await.unwrap();

    let kept = prune_files(&store, &greater_than("TEMP", 50.0), &schema(), &ids).await;
    assert_eq!(kept, vec![ids[1]], "the Int16 file is ruled out, the Float64 one is not");
}

/// An empty candidate list is not an error, and pruning an empty store keeps
/// everything.
#[tokio::test]
async fn degenerate_inputs_fail_open() {
    let (store, _dir) = store().await;

    let kept = prune_files(&store, &greater_than("TEMP", 0.0), &schema(), &[]).await;
    assert!(kept.is_empty());

    // A store with no segments at all.
    let ids = store
        .registry()
        .intern_files(&[ObservedFile::new("argo/2024/0.nc", 1, 1)])
        .unwrap();
    let kept = prune_files(&store, &greater_than("TEMP", 0.0), &schema(), &ids).await;
    assert_eq!(kept, ids, "no statistics means nothing is prunable");
}
