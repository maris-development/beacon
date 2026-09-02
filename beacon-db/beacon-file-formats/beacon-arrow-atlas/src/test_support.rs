//! Collections the tests of this crate read.
//!
//! Every fixture is written with the real [`AtlasWriter`], so what the tests
//! read is a real container: a footer, its segments, and the statistics the
//! writer recorded while it staged them.

use std::path::Path;
use std::sync::Arc;

use atlas::{Atlas, AtlasWriter, Attr, FillValue, TimestampNs, WriterConfig};
use chrono::{DateTime, Utc};
use ndarray::{ArrayD, IxDyn, arr1};
use object_store::{ObjectMeta, ObjectStore, local::LocalFileSystem, path::Path as OsPath};

/// 2024-01-01T00:00:00Z, the epoch the `time` arrays count from.
pub const EPOCH_NANOS: i64 = 1_704_067_200_000_000_000;

/// One day, in nanoseconds.
pub const DAY_NANOS: i64 = 86_400_000_000_000;

/// A store rooted on `dir`, and the marker of the collection in it.
///
/// The marker carries the container's real size and modification time, which
/// is what the reader cache keys on.
pub fn store_and_marker(dir: &Path) -> (Arc<dyn ObjectStore>, ObjectMeta) {
    let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap());
    let container = dir.join(crate::store::ATLAS_MARKER);
    let (size, last_modified) = match std::fs::metadata(&container) {
        Ok(meta) => (
            meta.len(),
            meta.modified()
                .map(DateTime::<Utc>::from)
                .unwrap_or(DateTime::UNIX_EPOCH),
        ),
        // A test that has not written a collection still needs a marker to
        // hand to the code that will refuse it.
        Err(_) => (0, DateTime::UNIX_EPOCH),
    };
    let marker = ObjectMeta {
        location: OsPath::from(crate::store::ATLAS_MARKER),
        last_modified,
        size,
        e_tag: None,
        version: None,
    };
    (store, marker)
}

/// Open a fixture collection from its directory.
pub async fn open(dir: &Path) -> Arc<Atlas> {
    Arc::new(Atlas::open_path(dir).await.expect("open the collection"))
}

/// Two datasets that do not share a schema.
///
/// - `winter`: `temperature: Float32[4]`, `cycle: Int32[4]` with a fill of
///   `-1`, and `time: TimestampNs[4]`. Dataset attributes `season` and `year`;
///   `temperature` carries `units`.
/// - `summer`: `temperature: Float32[3]` alone, and the attribute `season`.
///
/// The differing lengths make the two datasets separable in a result, and the
/// fill on `cycle` carries an unwritten cell through to a null.
pub async fn two_datasets(dir: &Path) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    {
        let mut winter = writer.add_dataset("winter").await.expect("add winter");
        winter
            .define_array::<f32>("temperature", vec!["obs".into()], vec![4], None, None)
            .await
            .expect("define temperature");
        winter
            .define_array::<i32>(
                "cycle",
                vec!["obs".into()],
                vec![4],
                None,
                Some(FillValue::Int(-1)),
            )
            .await
            .expect("define cycle");
        winter
            .define_array::<TimestampNs>("time", vec!["obs".into()], vec![4], None, None)
            .await
            .expect("define time");

        winter
            .write_array(
                "temperature",
                vec![0],
                arr1(&[1.0f32, 2.0, 3.0, 4.0]).into_dyn().view(),
            )
            .await
            .expect("write temperature");
        winter
            .write_array(
                "cycle",
                vec![0],
                arr1(&[10i32, 20, 30, 40]).into_dyn().view(),
            )
            .await
            .expect("write cycle");
        let times: Vec<TimestampNs> = (0..4)
            .map(|day| TimestampNs(EPOCH_NANOS + day * DAY_NANOS))
            .collect();
        winter
            .write_array("time", vec![0], arr1(&times).into_dyn().view())
            .await
            .expect("write time");

        winter.set_attribute("season", Attr::String("winter".into()));
        winter.set_attribute("year", Attr::Int64(2024));
        winter
            .set_array_attribute("temperature", "units", Attr::String("celsius".into()))
            .expect("set units");
        winter.finish().await.expect("finish winter");
    }

    {
        let mut summer = writer.add_dataset("summer").await.expect("add summer");
        summer
            .define_array::<f32>("temperature", vec!["obs".into()], vec![3], None, None)
            .await
            .expect("define temperature");
        summer
            .write_array(
                "temperature",
                vec![0],
                arr1(&[20.0f32, 21.0, 22.0]).into_dyn().view(),
            )
            .await
            .expect("write temperature");
        summer.set_attribute("season", Attr::String("summer".into()));
        summer.finish().await.expect("finish summer");
    }

    writer.finish().await.expect("finish the collection");
}

/// One dataset, `grid`, holding two chunked 2-D arrays on `lat` and `lon`.
///
/// - `temperature: Float64[4, 6]`, chunked `[2, 3]`, written whole. Cell
///   `(row, col)` holds `row * 6 + col`, so a window states its own position.
/// - `sparse: Float64[4, 6]`, the same shape and chunking, with a fill of
///   `-999`. Only the first two rows are written, so the rest is a hole that
///   costs no bytes.
pub async fn chunked_grid(dir: &Path) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    let mut grid = writer.add_dataset("grid").await.expect("add grid");
    let dims = vec!["lat".to_string(), "lon".to_string()];
    grid.define_array::<f64>(
        "temperature",
        dims.clone(),
        vec![4, 6],
        Some(vec![2, 3]),
        None,
    )
    .await
    .expect("define temperature");
    grid.define_array::<f64>(
        "sparse",
        dims,
        vec![4, 6],
        Some(vec![2, 3]),
        Some(FillValue::Float(-999.0)),
    )
    .await
    .expect("define sparse");

    let values = ArrayD::from_shape_fn(IxDyn(&[4, 6]), |i| (i[0] * 6 + i[1]) as f64);
    grid.write_array("temperature", vec![0, 0], values.view())
        .await
        .expect("write temperature");

    let written = ArrayD::from_shape_fn(IxDyn(&[2, 6]), |i| (i[0] * 6 + i[1]) as f64);
    grid.write_array("sparse", vec![0, 0], written.view())
        .await
        .expect("write sparse");

    grid.finish().await.expect("finish grid");
    writer.finish().await.expect("finish the collection");
}

/// Two datasets whose shared array has two numeric types.
///
/// - `a`: `value: Int16[2] = [1, 2]`, `flag: Int32[2] = [7, 8]`.
/// - `b`: `value: Float32[2] = [3.5, 4.5]`.
///
/// The merged `value` widens past either dataset's own type, and `flag` is a
/// column only one dataset declares.
pub async fn widening(dir: &Path) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    {
        let mut a = writer.add_dataset("a").await.expect("add a");
        a.define_array::<i16>("value", vec!["obs".into()], vec![2], None, None)
            .await
            .expect("define value");
        a.define_array::<i32>("flag", vec!["obs".into()], vec![2], None, None)
            .await
            .expect("define flag");
        a.write_array("value", vec![0], arr1(&[1i16, 2]).into_dyn().view())
            .await
            .expect("write value");
        a.write_array("flag", vec![0], arr1(&[7i32, 8]).into_dyn().view())
            .await
            .expect("write flag");
        a.finish().await.expect("finish a");
    }

    {
        let mut b = writer.add_dataset("b").await.expect("add b");
        b.define_array::<f32>("value", vec!["obs".into()], vec![2], None, None)
            .await
            .expect("define value");
        b.write_array("value", vec![0], arr1(&[3.5f32, 4.5]).into_dyn().view())
            .await
            .expect("write value");
        b.finish().await.expect("finish b");
    }

    writer.finish().await.expect("finish the collection");
}

/// Two datasets whose shared array has no common numeric type.
///
/// - `a`: `value: String[2] = ["x", "y"]`, `only_a: Int32[2] = [7, 8]`.
/// - `b`: `value: Int64[2] = [1, 2]`.
///
/// There is no numeric super-type here, so this pins what the merge resolves
/// to and whether both datasets stay readable.
pub async fn incompatible(dir: &Path) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    {
        let mut a = writer.add_dataset("a").await.expect("add a");
        a.define_array::<String>("value", vec!["obs".into()], vec![2], None, None)
            .await
            .expect("define value");
        a.define_array::<i32>("only_a", vec!["obs".into()], vec![2], None, None)
            .await
            .expect("define only_a");
        a.write_array(
            "value",
            vec![0],
            arr1(&["x".to_string(), "y".to_string()]).into_dyn().view(),
        )
        .await
        .expect("write value");
        a.write_array("only_a", vec![0], arr1(&[7i32, 8]).into_dyn().view())
            .await
            .expect("write only_a");
        a.finish().await.expect("finish a");
    }

    {
        let mut b = writer.add_dataset("b").await.expect("add b");
        b.define_array::<i64>("value", vec!["obs".into()], vec![2], None, None)
            .await
            .expect("define value");
        b.write_array("value", vec![0], arr1(&[1i64, 2]).into_dyn().view())
            .await
            .expect("write value");
        b.finish().await.expect("finish b");
    }

    writer.finish().await.expect("finish the collection");
}

/// `n` datasets named `d0..d{n-1}`, each holding `temperature: Float32[4]`
/// over the disjoint range `[10i, 10i + 3]`.
///
/// A predicate such as `temperature > 45` then has a known answer: the
/// datasets whose range reaches past it, and no others.
pub async fn ranged(dir: &Path, n: usize) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    for i in 0..n {
        let mut ds = writer
            .add_dataset(&format!("d{i}"))
            .await
            .expect("add a dataset");
        ds.define_array::<f32>("temperature", vec!["obs".into()], vec![4], None, None)
            .await
            .expect("define temperature");
        let base = (10 * i) as f32;
        ds.write_array(
            "temperature",
            vec![0],
            arr1(&[base, base + 1.0, base + 2.0, base + 3.0])
                .into_dyn()
                .view(),
        )
        .await
        .expect("write temperature");
        ds.set_attribute("platform", Attr::String(format!("p{i}")));
        ds.finish().await.expect("finish a dataset");
    }

    writer.finish().await.expect("finish the collection");
}

/// One dataset carrying values Beacon cannot surface as columns.
///
/// `value: Float64[2]` is readable. The list attribute `range` has no rank-0
/// form and is dropped; the string attribute `units` beside it is kept, so a
/// test can tell "dropped" from "dropped everything".
///
/// A `Bool` or list *array* cannot appear here: `array-format` implements no
/// element type for either, so no writer can produce one.
pub async fn skips(dir: &Path) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");

    let mut ds = writer.add_dataset("s").await.expect("add s");
    ds.define_array::<f64>("value", vec!["obs".into()], vec![2], None, None)
        .await
        .expect("define value");
    ds.write_array("value", vec![0], arr1(&[1.0f64, 2.0]).into_dyn().view())
        .await
        .expect("write value");
    ds.set_array_attribute("value", "units", Attr::String("metres".into()))
        .expect("set units");
    ds.set_array_attribute("value", "range", Attr::Float64List(vec![0.0, 10.0]))
        .expect("set range");
    ds.set_attribute("tags", Attr::StringList(vec!["a".into(), "b".into()]));
    ds.set_attribute("title", Attr::String("skips".into()));
    ds.finish().await.expect("finish s");

    writer.finish().await.expect("finish the collection");
}

/// A collection that holds no dataset at all.
///
/// Legal, and the writer produces one whenever a job finds nothing to ingest.
pub async fn empty(dir: &Path) {
    let writer = AtlasWriter::create_path(dir, WriterConfig::default())
        .await
        .expect("create the collection");
    writer.finish().await.expect("finish the collection");
}
