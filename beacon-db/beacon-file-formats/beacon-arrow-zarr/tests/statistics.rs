//! Per-file statistics for zarr stores, against real stores.
//!
//! The unit tests next to the code cover the attribute rules in isolation. These
//! open an actual store and check the two things that decide whether a query
//! answer stays correct: which columns get a range, and which bytes that costs.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow::datatypes::Schema;
use beacon_arrow_zarr::datafusion::statistics::generate_statistics;
use beacon_arrow_zarr::datafusion::ZarrFormat;
use beacon_arrow_zarr::reader::schema_from_group_path;
use beacon_arrow_zarr::util::ZarrStorage;
use bytes::Bytes;
use datafusion::common::stats::Precision;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as StorePath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use zarrs::array::{data_type, ArrayBuilder};
use zarrs::group::GroupBuilder;
use zarrs_object_store::AsyncObjectStore;

// ─── A store that records what it fetched ───────────────────────────────────

/// A read-only [`ObjectStore`] decorator that remembers every object fetched.
///
/// The point of the metadata shortcut is the bytes it does *not* move, and a
/// range that looks right proves nothing about that. This is how a test sees the
/// difference.
#[derive(Debug)]
struct RecordingStore {
    inner: LocalFileSystem,
    fetched: Mutex<Vec<String>>,
}

impl RecordingStore {
    fn new(inner: LocalFileSystem) -> Self {
        Self {
            inner,
            fetched: Mutex::new(Vec::new()),
        }
    }

    /// Whether any fetched object lives under the array node `array`'s chunks.
    fn read_chunks_of(&self, array: &str) -> bool {
        let prefix = format!("{array}/c");
        self.fetched
            .lock()
            .unwrap()
            .iter()
            .any(|path| path.starts_with(&prefix))
    }
}

impl std::fmt::Display for RecordingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RecordingStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for RecordingStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &StorePath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.fetched.lock().unwrap().push(location.to_string());
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.fetched.lock().unwrap().push(location.to_string());
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<StorePath>>,
    ) -> BoxStream<'static, object_store::Result<StorePath>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&StorePath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&StorePath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

// ─── The fixture ────────────────────────────────────────────────────────────

const LATS: usize = 4;
const LONS: usize = 5;

fn attributes(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

/// Write a small CF-style store at `dir`, covering every path statistics takes.
///
/// - `lat` — rank 1, states its own `actual_range`.
/// - `lon` — rank 1, states only `valid_min`/`valid_max`, which are not a bound.
/// - `sst` — rank 2, states only `valid_min`/`valid_max`: a data grid with
///   nothing usable, so it must report unknown.
/// - `sst_bounded` — rank 2, states its `actual_range`: a data grid that *is*
///   bounded, for free, from metadata.
async fn write_store(dir: &std::path::Path) -> anyhow::Result<()> {
    let store = Arc::new(AsyncObjectStore::new(LocalFileSystem::new_with_prefix(dir)?));

    GroupBuilder::new()
        .attributes(attributes(&[("Conventions", serde_json::json!("CF-1.8"))]))
        .build(store.clone(), "/")?
        .async_store_metadata()
        .await?;

    // A coordinate that states its range: 40.0 .. 43.0.
    let lat: Vec<f32> = (0..LATS).map(|i| 40.0 + i as f32).collect();
    let array = ArrayBuilder::new(
        vec![LATS as u64],
        vec![LATS as u64],
        data_type::float32(),
        0.0f32,
    )
    .dimension_names(Some(["lat"]))
    .attributes(attributes(&[
        ("units", serde_json::json!("degrees_north")),
        ("actual_range", serde_json::json!([40.0, 43.0])),
        ("valid_min", serde_json::json!(-90.0)),
        ("valid_max", serde_json::json!(90.0)),
    ]))
    .build(store.clone(), "/lat")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0], lat.as_slice()).await?;

    // A coordinate that states only *valid* bounds: 10.0 .. 14.0 in the data,
    // -180 .. 180 in the attributes.
    let lon: Vec<f32> = (0..LONS).map(|i| 10.0 + i as f32).collect();
    let array = ArrayBuilder::new(
        vec![LONS as u64],
        vec![LONS as u64],
        data_type::float32(),
        0.0f32,
    )
    .dimension_names(Some(["lon"]))
    .attributes(attributes(&[
        ("units", serde_json::json!("degrees_east")),
        ("valid_min", serde_json::json!(-180.0)),
        ("valid_max", serde_json::json!(180.0)),
    ]))
    .build(store.clone(), "/lon")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0], lon.as_slice()).await?;

    let grid: Vec<f64> = (0..LATS * LONS).map(|i| i as f64).collect();
    let array = ArrayBuilder::new(
        vec![LATS as u64, LONS as u64],
        vec![LATS as u64, LONS as u64],
        data_type::float64(),
        0.0f64,
    )
    .dimension_names(Some(["lat", "lon"]))
    .attributes(attributes(&[
        ("valid_min", serde_json::json!(-5.0)),
        ("valid_max", serde_json::json!(40.0)),
    ]))
    .build(store.clone(), "/sst")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0, 0], grid.as_slice()).await?;

    let array = ArrayBuilder::new(
        vec![LATS as u64, LONS as u64],
        vec![LATS as u64, LONS as u64],
        data_type::float64(),
        0.0f64,
    )
    .dimension_names(Some(["lat", "lon"]))
    .attributes(attributes(&[(
        "actual_range",
        serde_json::json!([0.0, 19.0]),
    )]))
    .build(store.clone(), "/sst_bounded")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0, 0], grid.as_slice()).await?;

    Ok(())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn column<'a>(schema: &Schema, stats: &'a Statistics, name: &str) -> &'a ColumnStatistics {
    let index = schema.index_of(name).unwrap_or_else(|_| {
        panic!(
            "column '{name}' missing; schema has {:?}",
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        )
    });
    &stats.column_statistics[index]
}

fn f32_bounds(stats: &ColumnStatistics) -> (f32, f32) {
    let (Some(ScalarValue::Float32(Some(min))), Some(ScalarValue::Float32(Some(max)))) =
        (stats.min_value.get_value(), stats.max_value.get_value())
    else {
        panic!("expected an f32 range, got {stats:?}");
    };
    (*min, *max)
}

fn f64_bounds(stats: &ColumnStatistics) -> (f64, f64) {
    let (Some(ScalarValue::Float64(Some(min))), Some(ScalarValue::Float64(Some(max)))) =
        (stats.min_value.get_value(), stats.max_value.get_value())
    else {
        panic!("expected an f64 range, got {stats:?}");
    };
    (*min, *max)
}

fn assert_unknown(stats: &ColumnStatistics, what: &str) {
    assert_eq!(stats.min_value, Precision::Absent, "{what} must have no min");
    assert_eq!(stats.max_value, Precision::Absent, "{what} must have no max");
}

/// Infer the root group's schema through `store`, then measure it.
async fn statistics_over(store: Arc<dyn ObjectStore>) -> (Schema, Statistics) {
    let storage = ZarrStorage::from_object_store(store);
    let schema = schema_from_group_path(storage.inner(), "/", None, None)
        .await
        .unwrap();
    let statistics = generate_statistics(storage.inner(), "/", None, &schema)
        .await
        .unwrap();
    (schema, statistics)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

/// A coordinate array gets a range; a data grid without usable metadata does
/// not. That split is what keeps the scan cost where it was.
#[tokio::test]
async fn coordinates_are_bounded_and_bare_grids_are_not() {
    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path()).await.unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let (schema, stats) = statistics_over(store).await;

    let (min, max) = f32_bounds(column(&schema, &stats, "lat"));
    assert!(min <= 40.0 && max >= 43.0, "lat range {min}..{max} must contain 40..43");

    let (min, max) = f32_bounds(column(&schema, &stats, "lon"));
    assert!(min <= 10.0 && max >= 14.0, "lon range {min}..{max} must contain 10..14");

    assert_unknown(column(&schema, &stats, "sst"), "a grid with no actual_range");
}

/// `valid_min`/`valid_max` state which values are *valid*, not which values are
/// stored, so they may never become a range. `lon` declares -180..180 and holds
/// 10..14; a bound taken from the attributes would be four times too wide here
/// and, on another file, too narrow — which drops matching rows.
#[tokio::test]
async fn valid_min_and_valid_max_are_not_the_range() {
    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path()).await.unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let (schema, stats) = statistics_over(store).await;

    let (min, max) = f32_bounds(column(&schema, &stats, "lon"));
    assert!(
        min > -180.0 && max < 180.0,
        "lon range {min}..{max} came from valid_min/valid_max, not the data"
    );
}

/// An `actual_range` costs no chunk read, and an array without one still gets
/// measured. Both halves matter: the first is the optimization, the second is
/// the coverage it must not cost.
#[tokio::test]
async fn actual_range_needs_no_chunk_read() {
    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path()).await.unwrap();

    let recording = Arc::new(RecordingStore::new(
        LocalFileSystem::new_with_prefix(dir.path()).unwrap(),
    ));
    let (schema, stats) = statistics_over(recording.clone()).await;

    // `lat` and `sst_bounded` state their own ranges.
    assert!(
        !recording.read_chunks_of("lat"),
        "lat states an actual_range; its chunks must stay untouched"
    );
    assert!(
        !recording.read_chunks_of("sst_bounded"),
        "sst_bounded states an actual_range; its chunks must stay untouched"
    );
    // `lon` states none, so the rank-1 fallback reads it.
    assert!(
        recording.read_chunks_of("lon"),
        "lon has no actual_range, so its values must be read"
    );
    // And a grid with no usable metadata is never read at all.
    assert!(
        !recording.read_chunks_of("sst"),
        "a rank-2 grid must never be read for statistics"
    );

    let (min, max) = f64_bounds(column(&schema, &stats, "sst_bounded"));
    assert_eq!((min, max), (0.0, 19.0));
}

/// The whole subsystem, through the file format the session registers: the
/// bundled GHRSST-style store bounds its coordinates and leaves its grids alone.
#[tokio::test]
async fn infer_stats_bounds_the_bundled_store() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files");
    let ctx = SessionContext::new();
    let state = ctx.state();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let object = ObjectMeta {
        location: StorePath::from("gridded-example.zarr/zarr.json"),
        last_modified: Default::default(),
        size: 0,
        e_tag: None,
        version: None,
    };

    let format = ZarrFormat::default();
    let schema = format
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .unwrap();
    let stats = format
        .infer_stats(&state, &store, schema.clone(), &object)
        .await
        .unwrap();

    // The coordinates a WHERE clause names.
    let (min, max) = f32_bounds(column(&schema, &stats, "lat"));
    assert!(min < max && min > 38.0 && max < 49.0, "lat range {min}..{max}");
    let (min, max) = f32_bounds(column(&schema, &stats, "lon"));
    assert!(min < max && min > 26.0 && max < 43.0, "lon range {min}..{max}");
    // `time` is CF-encoded, so its range arrives as a timestamp.
    let time = column(&schema, &stats, "time");
    assert!(
        matches!(
            time.min_value.get_value(),
            Some(ScalarValue::TimestampNanosecond(Some(_), _))
        ),
        "time must carry a timestamp range, got {time:?}"
    );

    // The grids: packed int16 with only valid_min/valid_max, so nothing to
    // report and nothing read.
    assert_unknown(column(&schema, &stats, "analysed_sst"), "analysed_sst");
    assert_unknown(column(&schema, &stats, "analysis_error"), "analysis_error");
}

/// The switch an operator needs: off means unknown, whatever the store holds.
#[tokio::test]
async fn disabled_statistics_report_unknown() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files");
    let ctx = SessionContext::new();
    let state = ctx.state();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let object = ObjectMeta {
        location: StorePath::from("gridded-example.zarr/zarr.json"),
        last_modified: Default::default(),
        size: 0,
        e_tag: None,
        version: None,
    };

    let format = ZarrFormat::default().with_enable_statistics(false);
    let schema = format
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .unwrap();
    let stats = format
        .infer_stats(&state, &store, schema.clone(), &object)
        .await
        .unwrap();

    for (field, column) in schema.fields().iter().zip(&stats.column_statistics) {
        assert_unknown(column, field.name());
    }
}

/// An object that is not a zarr group — a chunk, or an array's own metadata —
/// yields unknown rather than an error. Statistics may never fail a plan.
#[tokio::test]
async fn a_non_group_object_reports_unknown() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files");
    let ctx = SessionContext::new();
    let state = ctx.state();
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
        "lat",
        arrow::datatypes::DataType::Float32,
        true,
    )]));

    let format = ZarrFormat::default();
    for location in [
        "gridded-example.zarr/lat/c/0",
        "gridded-example.zarr/lat/zarr.json",
    ] {
        let object = ObjectMeta {
            location: StorePath::from(location),
            last_modified: Default::default(),
            size: 0,
            e_tag: None,
            version: None,
        };
        let stats = format
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .unwrap();
        assert_unknown(&stats.column_statistics[0], location);
    }
}

// ─── The table provider path ────────────────────────────────────────────────

/// A zarr store read through [`FastObjectTable`], the way `read_zarr` reads it.
///
/// A zarr store is a directory, but the reader never opens one: it takes the
/// `zarr.json` marker at the root and resolves the store from there. So the
/// table hands it that object like any other file, and no part of the plan has
/// to know the shape of the directory.
///
/// Every other test here drives `ZarrFormat` directly, so nothing else would
/// notice if the handover changed. This pins the rows it produces.
#[tokio::test]
async fn a_store_read_through_the_table_provider_returns_rows() {
    use beacon_datafusion_ext::fast_object::FastObjectTable;
    use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
    use datafusion::catalog::TableProvider;
    use datafusion::datasource::listing::ListingTableUrl;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::{collect, ExecutionPlanProperties};
    use datafusion::prelude::SessionConfig;

    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path()).await.unwrap();

    let state = SessionStateBuilder::new()
        .with_config(
            SessionConfig::new()
                .with_target_partitions(4)
                .with_extension(ArrowTypeWidening::default_extension()),
        )
        .with_default_features()
        .build();
    let ctx = SessionContext::new_with_state(state);

    let url = ListingTableUrl::parse(dir.path().to_string_lossy()).unwrap();
    let table = FastObjectTable::try_new(&ctx.state(), Arc::new(ZarrFormat::default()), vec![url])
        .await
        .expect("a zarr store registers as a table");

    let plan = table.scan(&ctx.state(), None, &[], None).await.unwrap();
    assert_eq!(plan.output_partitioning().partition_count(), 1, "one group, one partition");
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
    assert_eq!(rows, 20, "the 4x5 grid reads back whole");
    assert_eq!(
        batches[0].num_columns(),
        13,
        "every array and coordinate reaches the output"
    );
}
