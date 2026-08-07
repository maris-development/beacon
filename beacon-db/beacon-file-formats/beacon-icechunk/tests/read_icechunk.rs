//! End-to-end reads of a local Icechunk repository through the zarr reader:
//! schema, versions, predicate pushdown, and the virtual-chunk scope.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Float32Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_icechunk::fixture::{self, LATS, ROWS};
use beacon_icechunk::{IcechunkTable, IcechunkTableDefinition};
use datafusion::catalog::TableProvider;
use datafusion::prelude::{SessionConfig, SessionContext};
use tempfile::TempDir;

/// A session wired the way beacon-core wires one for local paths: a dynamic
/// listing factory, so an absolute path resolves against the local filesystem.
/// Single-partition so row counts are easy to reason about.
fn session() -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_extension(Arc::new(ListingFactory::dynamic()));
    SessionContext::new_with_config(config)
}

fn definition(location: &Path, options: &[(&str, &str)]) -> IcechunkTableDefinition {
    IcechunkTableDefinition {
        name: "repo".to_string(),
        location: location.to_string_lossy().into_owned(),
        options: options
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect(),
        definition: None,
    }
}

/// Register the repository at `location` as table `repo` and return the session.
async fn register(location: &Path, options: &[(&str, &str)]) -> SessionContext {
    let ctx = session();
    let table = IcechunkTable::try_new(&ctx.state(), definition(location, options))
        .await
        .expect("the repository should open");
    ctx.register_table("repo", Arc::new(table)).unwrap();
    ctx
}

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

fn scalar_i64(batches: &[RecordBatch]) -> i64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("an Int64 column")
        .value(0)
}

fn scalar_f64(batches: &[RecordBatch]) -> f64 {
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("a Float64 column")
        .value(0)
}

async fn max_sst(ctx: &SessionContext) -> f64 {
    scalar_f64(
        &ctx.sql("SELECT max(sst) FROM repo")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap(),
    )
}

/// Build the fixture repository in a fresh temp dir.
async fn repository() -> (TempDir, fixture::FixtureSnapshots) {
    let dir = TempDir::new().unwrap();
    let snapshots = fixture::write_gridded_repository(&dir.path().join("repo"))
        .await
        .expect("the fixture repository should be written");
    (dir, snapshots)
}

#[tokio::test(flavor = "multi_thread")]
async fn reads_a_local_repository_at_the_tip_of_main() {
    let (dir, _) = repository().await;
    let ctx = register(&dir.path().join("repo"), &[]).await;

    let batches = ctx
        .sql("SELECT * FROM repo")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&batches), ROWS, "the full grid should be returned");

    // The default version is the tip of `main`, i.e. the second commit.
    assert_eq!(max_sst(&ctx).await, fixture::SECOND_SST);

    // A LIMIT reaches the scan without cutting the grid short: the scan carries
    // whole arrays per row, so a row limit must be applied after the broadcast.
    let limited = ctx
        .sql("SELECT lat FROM repo LIMIT 3")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(rows(&limited), 3, "LIMIT 3 should yield exactly 3 rows");
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_carries_coordinates_cf_time_and_attributes() {
    let (dir, _) = repository().await;
    let ctx = register(&dir.path().join("repo"), &[]).await;

    let schema = ctx.table_provider("repo").await.unwrap().schema();
    let dtype = |name: &str| {
        schema
            .field_with_name(name)
            .unwrap_or_else(|_| panic!("missing field '{name}'"))
            .data_type()
            .clone()
    };

    assert_eq!(dtype("lat"), DataType::Float32);
    assert_eq!(dtype("lon"), DataType::Float32);
    assert_eq!(dtype("sst"), DataType::Float64);
    // `time` is int32 "seconds since 1981-01-01" → decoded as a CF time.
    assert_eq!(
        dtype("time"),
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    // Group and array attributes surface as columns, as for a plain zarr store.
    assert_eq!(dtype(".Conventions"), DataType::Utf8);
    assert_eq!(dtype("sst.units"), DataType::Utf8);
}

/// A snapshot is fixed: a commit landing after the table was created does not
/// change what the pinned version returns, while the branch tip does move.
#[tokio::test(flavor = "multi_thread")]
async fn a_pinned_snapshot_survives_a_later_commit() {
    let (dir, snapshots) = repository().await;
    let location = dir.path().join("repo");

    let pinned = register(&location, &[("snapshot", &snapshots.first)]).await;
    let branch = register(&location, &[]).await;

    assert_eq!(max_sst(&pinned).await, fixture::FIRST_SST);
    assert_eq!(max_sst(&branch).await, fixture::SECOND_SST);

    // Land a third commit, then ask both tables again.
    fixture::append_commit(&location, 30.0).await.unwrap();

    assert_eq!(
        max_sst(&pinned).await,
        fixture::FIRST_SST,
        "a pinned snapshot must not see a later commit"
    );
    assert_eq!(
        max_sst(&branch).await,
        30.0,
        "a branch-backed table must see the new tip"
    );
}

/// A predicate on a coordinate reaches the scan and prunes, exactly as it does
/// for a plain zarr store.
#[tokio::test(flavor = "multi_thread")]
async fn a_coordinate_predicate_pushes_down() {
    use datafusion::physical_plan::displayable;

    let (dir, _) = repository().await;
    let ctx = register(&dir.path().join("repo"), &[]).await;

    // The plan is the nd spine over a file scan — the same shape the zarr
    // listing table produces, which is what carries the pushdown.
    let plan = ctx
        .sql("SELECT lat FROM repo WHERE lat > 41")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let rendered = displayable(plan.as_ref()).indent(true).to_string();
    let broadcast = rendered.find("NdBroadcastExec");
    let source = rendered.find("NdSourceExec");
    let scan = rendered.find("DataSourceExec");
    assert!(
        scan.is_some() && broadcast < source && source < scan,
        "expected NdBroadcastExec → NdSourceExec → DataSourceExec:\n{rendered}"
    );

    // An impossible predicate prunes every chunk.
    let none = ctx
        .sql("SELECT lat FROM repo WHERE lat > 100000")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(
        rows(&none),
        0,
        "an impossible predicate must prune all rows"
    );

    // A selective one keeps a strict subset, and every row satisfies it.
    let some = ctx
        .sql("SELECT lat FROM repo WHERE lat > 41.5")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert!(rows(&some) > 0 && rows(&some) < ROWS, "expected a subset");
    for batch in &some {
        let lat = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        for index in 0..lat.len() {
            assert!(lat.value(index) > 41.5);
        }
    }
}

/// `read_dimensions` narrows the table to the variables on those dimensions,
/// the same option the zarr reader takes.
#[tokio::test(flavor = "multi_thread")]
async fn read_dimensions_narrows_the_schema() {
    let (dir, _) = repository().await;
    let ctx = register(&dir.path().join("repo"), &[("read_dimensions", "lat")]).await;

    let names: Vec<String> = ctx
        .table_provider("repo")
        .await
        .unwrap()
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert!(names.contains(&"lat".to_string()), "{names:?}");
    assert!(
        !names.contains(&"sst".to_string()),
        "sst spans time/lat/lon and must be excluded: {names:?}"
    );

    let count = ctx
        .sql("SELECT count(*) FROM repo")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(scalar_i64(&count), LATS as i64);
}

/// Detection: a repository is recognised as one, and a plain zarr store is not,
/// so zarr discovery keeps owning it.
#[tokio::test(flavor = "multi_thread")]
async fn an_icechunk_repository_is_told_apart_from_a_plain_zarr_store() {
    use beacon_icechunk::{RepositoryBackend, is_icechunk_repository};

    let (dir, _) = repository().await;

    let repo = RepositoryBackend::LocalFileSystem(dir.path().join("repo"));
    assert!(is_icechunk_repository(&repo).await.unwrap());

    let plain = dir.path().join("plain.zarr");
    std::fs::create_dir_all(&plain).unwrap();
    std::fs::write(
        plain.join("zarr.json"),
        r#"{"zarr_format":3,"node_type":"group"}"#,
    )
    .unwrap();
    let plain = RepositoryBackend::LocalFileSystem(plain);
    assert!(!is_icechunk_repository(&plain).await.unwrap());
}

/// Opening a repository that is not there fails with a message naming it,
/// rather than producing an empty table.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_repository_is_an_error() {
    let dir = TempDir::new().unwrap();
    let ctx = session();
    let err = IcechunkTable::try_new(&ctx.state(), definition(&dir.path().join("absent"), &[]))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Icechunk"), "{err:#}");
}

/// Pointing an Icechunk table at a plain zarr store says so, and names the
/// function that does read it.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_zarr_store_is_not_a_repository() {
    let dir = TempDir::new().unwrap();
    let store = dir.path().join("plain.zarr");
    std::fs::create_dir_all(&store).unwrap();
    std::fs::write(
        store.join("zarr.json"),
        r#"{"zarr_format":3,"node_type":"group"}"#,
    )
    .unwrap();

    let ctx = session();
    let err = IcechunkTable::try_new(&ctx.state(), definition(&store, &[]))
        .await
        .unwrap_err();
    let message = format!("{err:#}");
    assert!(message.contains("no Icechunk repository"), "{message}");
    assert!(message.contains("read_zarr"), "{message}");
}

/// The stated scope for virtual chunk references: Beacon authorizes none, so a
/// chunk that lives in a file outside the repository does not read.
#[tokio::test(flavor = "multi_thread")]
async fn virtual_chunk_references_are_not_read() {
    let dir = TempDir::new().unwrap();
    let location = dir.path().join("virtual-repo");
    let referenced = dir.path().join("outside.bin");
    fixture::write_virtual_chunk_repository(&location, &referenced)
        .await
        .expect("the fixture repository should be written");

    // No authorized virtual chunk containers: the repository can name the file,
    // but Beacon will not follow the reference.
    let backend = beacon_icechunk::RepositoryBackend::LocalFileSystem(location.clone());
    let repository = beacon_icechunk::open_repository(&backend).await.unwrap();
    assert!(
        repository
            .authorized_virtual_container_prefixes()
            .is_empty(),
        "Beacon must authorize no virtual chunk containers"
    );

    // The metadata still reads — only the virtual chunk's bytes are refused.
    let ctx = register(&location, &[]).await;
    let err = ctx
        .sql("SELECT sum(values) FROM repo")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("reading a virtual chunk must fail");
    let message = format!("{err:#}").to_lowercase();
    assert!(
        message.contains("virtual"),
        "the error should name the virtual reference: {message}"
    );
}

/// The location resolves through the same `ListingFactory` every other reader
/// uses, so a `file://` URL and a bare path name the same repository.
#[tokio::test(flavor = "multi_thread")]
async fn a_file_url_and_a_bare_path_name_the_same_repository() {
    let (dir, _) = repository().await;
    let path = dir.path().join("repo");
    let ctx = session();

    let bare = IcechunkTable::try_new(&ctx.state(), definition(&path, &[]))
        .await
        .unwrap();
    let mut as_url = definition(&path, &[]);
    as_url.location = url::Url::from_directory_path(&path).unwrap().to_string();
    let url = IcechunkTable::try_new(&ctx.state(), as_url).await.unwrap();

    assert_eq!(bare.schema(), url.schema());
}

/// A branch that does not exist is a clear error, not an empty table.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_branch_is_an_error() {
    let (dir, _) = repository().await;
    let ctx = session();
    let err = IcechunkTable::try_new(
        &ctx.state(),
        definition(&dir.path().join("repo"), &[("branch", "no-such-branch")]),
    )
    .await
    .unwrap_err();
    assert!(format!("{err:#}").contains("no-such-branch"), "{err:#}");
}

/// Two version selectors are rejected before anything is opened.
#[tokio::test(flavor = "multi_thread")]
async fn a_branch_and_a_snapshot_together_are_rejected() {
    let (dir, snapshots) = repository().await;
    let ctx = session();
    let err = IcechunkTable::try_new(
        &ctx.state(),
        definition(
            &dir.path().join("repo"),
            &[("branch", "main"), ("snapshot", &snapshots.first)],
        ),
    )
    .await
    .unwrap_err();
    assert!(err.to_string().contains("at most one"), "{err:#}");
}

/// The options a definition carries are exactly what the provider reports back,
/// so `table.json` round-trips.
#[tokio::test(flavor = "multi_thread")]
async fn the_provider_carries_its_definition() {
    let (dir, snapshots) = repository().await;
    let ctx = session();
    let table = IcechunkTable::try_new(
        &ctx.state(),
        definition(&dir.path().join("repo"), &[("snapshot", &snapshots.first)]),
    )
    .await
    .unwrap();

    assert_eq!(
        table.definition().options.get("snapshot"),
        Some(&snapshots.first)
    );
    assert_eq!(
        table.version(),
        &beacon_icechunk::IcechunkVersion::Snapshot(snapshots.first.clone())
    );
}

/// Sanity check that the fixture is what the other tests assume.
#[tokio::test(flavor = "multi_thread")]
async fn the_fixture_commits_two_distinct_snapshots() {
    let (_dir, snapshots) = repository().await;
    assert_ne!(snapshots.first, snapshots.second);
}
