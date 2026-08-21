//! The layout of an ASN OptoDAS acquisition file.
//!
//! # What the file holds
//!
//! One payload, `data(time, distance)`, of `int16` counts. Every axis of it is
//! anonymous, because the file attaches no dimension scale. Around it sit a few
//! hundred metadata variables in nested groups, and four of them describe the
//! payload:
//!
//! | The file says | This layer does |
//! |---|---|
//! | `header/dimensionNames`, `header/dimensionSizes` | names the axes of the payload |
//! | `header/time`, `header/dt` | builds the `time` coordinate |
//! | `header/dimensionRanges/dimension<n>` | builds the `distance` coordinate |
//! | `header/dataScale`, `header/unit` | decodes the payload to physical units |
//!
//! # Why the reader, and not a view
//!
//! A `CREATE VIEW` over the columns could do the arithmetic. It could not name
//! an axis, because the name has to exist before the broadcast joins the
//! payload to the description of each channel. It could not prune either: the
//! scan skips a chunk only when a predicate bounds a **1-D array of the axis**,
//! so `time` and `distance` have to be arrays of the dataset, not expressions
//! above it.
//!
//! # What it does not do
//!
//! `time` is the nominal clock: the start the file records, plus one `dt` per
//! sample. The file also carries `timing/ppses`, `timing/sampleDelayPPS` and
//! `timing/sampleSkew`, and this layer applies none of them. They stay readable
//! under their own names, so a query can correct the clock itself.
//!
//! A file that reports missing samples gets no `time` at all rather than a
//! wrong one.

use std::collections::HashMap;
use std::sync::Arc;

use beacon_arrow_netcdf::dimensions::PhonyDimensions;
use beacon_arrow_netcdf::oxcdf_reader::compat;
use beacon_nd_array::datatypes::TimestampNanosecond;
use beacon_nd_array::{NdArray, NdArrayD};
use indexmap::IndexMap;
use oxcdf::AsyncNetcdfFile;

/// The payload of an OptoDAS file, which every other path is relative to.
const PAYLOAD: &str = "data";
/// One nanosecond per second, for the epoch seconds the file records.
const NANOS_PER_SECOND: f64 = 1_000_000_000.0;

/// What one OptoDAS file describes about itself.
///
/// [`detect`] builds it. It holds no array: every value here is a number or a
/// name the check already read, so applying it costs no further read.
#[derive(Debug, Clone)]
pub struct OptoDas {
    /// The axis names, keyed by the name the axis carries today.
    axis_names: HashMap<String, String>,
    /// The start of the file, in epoch seconds, and the sample interval.
    ///
    /// `None` when the file records missing samples, because the clock below
    /// would then be wrong.
    time: Option<TimeAxis>,
    /// The distance axis, when the file describes one.
    distance: Option<DistanceAxis>,
    /// The scale of the payload, when the file records one.
    scale: Option<f64>,
}

/// The clock of one file.
#[derive(Debug, Clone)]
struct TimeAxis {
    /// The dimension the samples run along, after the naming above.
    dimension: String,
    /// Epoch seconds of the first sample.
    start: f64,
    /// Seconds between two samples.
    step: f64,
    /// How many samples there are.
    len: usize,
}

/// The positions of one file.
#[derive(Debug, Clone)]
struct DistanceAxis {
    /// The dimension the positions run along, after the naming above.
    dimension: String,
    /// The first position, in raw channels.
    first: f64,
    /// Raw channels between two positions.
    step: f64,
    /// Metres per raw channel.
    scale: f64,
    /// How many positions there are.
    len: usize,
}

/// Whether `file` follows this layout, and what it says.
///
/// `phony` is the naming the reader applies, so the axis names this returns are
/// keyed by the names the arrays will carry.
///
/// Returns `None` when the file does not follow the layout. That is not an
/// error: a scan over an archive holds files of every kind, and one that this
/// layer does not recognise reads plainly.
pub async fn detect(
    file: &Arc<AsyncNetcdfFile>,
    phony: &PhonyDimensions,
) -> anyhow::Result<Option<OptoDas>> {
    let Some(payload) = file.variable(PAYLOAD) else {
        tracing::debug!("no '{PAYLOAD}' dataset, so this is no OptoDAS file");
        return Ok(None);
    };
    let shape: Vec<usize> = payload.shape.iter().map(|&len| len as usize).collect();
    let axes = phony.apply(&payload.dimensions);

    // The names of the axes, and their lengths, as the file states them.
    let Some(names) = read_strings(file, "header/dimensionNames").await else {
        tracing::debug!("no 'header/dimensionNames', so this is no OptoDAS file");
        return Ok(None);
    };
    let sizes = read_i64s(file, "header/dimensionSizes")
        .await
        .unwrap_or_default();

    // The structural check. The file has to describe the payload it holds, or
    // this is another layout that happens to hold a dataset called `data`.
    if names.len() != shape.len() {
        tracing::warn!(
            "'header/dimensionNames' names {} axes and '{PAYLOAD}' holds {}; reading the file plainly",
            names.len(),
            shape.len()
        );
        return Ok(None);
    }
    if !sizes.is_empty()
        && sizes
            .iter()
            .map(|&len| len as usize)
            .ne(shape.iter().copied())
    {
        tracing::warn!(
            "'header/dimensionSizes' is {sizes:?} and '{PAYLOAD}' holds {shape:?}; reading the file plainly"
        );
        return Ok(None);
    }

    // ── The axis names ──────────────────────────────────────────────────
    let mut axis_names = HashMap::new();
    for (axis, name) in names.iter().enumerate() {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        // A name the file itself gives a dimension is not ours to take.
        if !phony.invented_names().contains(&axes[axis]) {
            tracing::debug!(
                "axis {axis} of '{PAYLOAD}' is named '{}' already",
                axes[axis]
            );
            continue;
        }
        axis_names.insert(axes[axis].clone(), name.to_string());
    }

    // ── The clock ───────────────────────────────────────────────────────
    let missing = read_i64s(file, "header/missingSamples")
        .await
        .unwrap_or_default();
    let time_axis = names.iter().position(|name| name.trim() == "time");
    let time = match (time_axis, missing.is_empty()) {
        (Some(axis), true) => {
            match (
                read_f64(file, "header/time").await,
                read_f64(file, "header/dt").await,
            ) {
                (Some(start), Some(step)) if step > 0.0 => Some(TimeAxis {
                    dimension: axis_names
                        .get(&axes[axis])
                        .cloned()
                        .unwrap_or_else(|| axes[axis].clone()),
                    start,
                    step,
                    len: shape[axis],
                }),
                _ => None,
            }
        }
        (Some(_), false) => {
            tracing::warn!(
                "the file reports {} missing samples, so its clock is not one step per sample; \
                 no 'time' column is built",
                missing.len()
            );
            None
        }
        (None, _) => None,
    };

    // ── The positions ───────────────────────────────────────────────────
    let distance_axis = names.iter().position(|name| name.trim() == "distance");
    let distance = match distance_axis {
        Some(axis) => distance_of(file, axis, shape[axis]).await.map(|mut found| {
            found.dimension = axis_names
                .get(&axes[axis])
                .cloned()
                .unwrap_or_else(|| axes[axis].clone());
            found
        }),
        None => None,
    };

    Ok(Some(OptoDas {
        axis_names,
        time,
        distance,
        scale: read_f64(file, "header/dataScale").await,
    }))
}

/// The positions of one axis, from the range the file records for it.
async fn distance_of(file: &Arc<AsyncNetcdfFile>, axis: usize, len: usize) -> Option<DistanceAxis> {
    let group = format!("header/dimensionRanges/dimension{axis}");
    let first = read_f64(file, &format!("{group}/min")).await?;
    let last = read_f64(file, &format!("{group}/max")).await?;
    let scale = read_f64(file, &format!("{group}/unitScale")).await?;
    if len < 2 {
        return None;
    }
    Some(DistanceAxis {
        dimension: String::new(),
        first,
        step: (last - first) / (len - 1) as f64,
        scale,
        len,
    })
}

impl OptoDas {
    /// The axis names this layer gives, keyed by the names the axes carry now.
    pub fn axis_names(&self) -> &HashMap<String, String> {
        &self.axis_names
    }

    /// Add to `arrays` what the file describes but does not store.
    ///
    /// Three things: the `time` coordinate, the `distance` coordinate, and the
    /// payload decoded through the scale the file records. Every column the
    /// file holds keeps its own name and its own values.
    pub async fn decorate(
        &self,
        file: &Arc<AsyncNetcdfFile>,
        phony: &PhonyDimensions,
        arrays: &mut IndexMap<String, Arc<dyn NdArrayD>>,
    ) -> anyhow::Result<()> {
        if let Some(time) = &self.time {
            // Count in nanoseconds, not in seconds. One `dt` of 0.008 s is
            // 8 ms exactly here, where `start + sample * dt` in doubles loses a
            // few hundred nanoseconds by the end of a file.
            let start = (time.start * NANOS_PER_SECOND).round() as i64;
            let step = (time.step * NANOS_PER_SECOND).round() as i64;
            let values: Vec<TimestampNanosecond> = (0..time.len)
                .map(|sample| TimestampNanosecond(start + sample as i64 * step))
                .collect();
            insert_coordinate(
                arrays,
                &time.dimension,
                NdArray::<TimestampNanosecond>::try_new_from_vec_in_mem(
                    values,
                    vec![time.len],
                    vec![time.dimension.clone()],
                    None,
                )
                .map(|array| Arc::new(array) as Arc<dyn NdArrayD>)?,
            );
        }

        if let Some(distance) = &self.distance {
            let values: Vec<f64> = (0..distance.len)
                .map(|position| (distance.first + position as f64 * distance.step) * distance.scale)
                .collect();
            insert_coordinate(
                arrays,
                &distance.dimension,
                NdArray::<f64>::try_new_from_vec_in_mem(
                    values,
                    vec![distance.len],
                    vec![distance.dimension.clone()],
                    None,
                )
                .map(|array| Arc::new(array) as Arc<dyn NdArrayD>)?,
            );
        }

        // The payload holds counts. One number turns them into the unit the
        // file names, and it lives in another group, so the CF path cannot find
        // it on its own.
        if let (Some(scale), Some(payload)) = (self.scale, file.variable(PAYLOAD)) {
            match compat::variable_to_nd_array_packed(
                file.clone(),
                &payload,
                phony,
                Some((scale, 0.0)),
            ) {
                Ok(decoded) => {
                    arrays.insert(PAYLOAD.to_string(), decoded);
                }
                Err(error) => tracing::warn!(
                    "the payload does not decode through 'header/dataScale', so it stays raw: {error}"
                ),
            }
        }

        arrays.sort_keys();
        Ok(())
    }
}

/// Add one coordinate under the name of its axis.
///
/// A column the file itself holds under that name wins. This layer describes a
/// file; it never overwrites one.
fn insert_coordinate(
    arrays: &mut IndexMap<String, Arc<dyn NdArrayD>>,
    name: &str,
    coordinate: Arc<dyn NdArrayD>,
) {
    if arrays.contains_key(name) {
        tracing::warn!("the file holds a dataset called '{name}' already, so it keeps it");
        return;
    }
    arrays.insert(name.to_string(), coordinate);
}

// ─── Reading the few values the check needs ────────────────────────────────

/// One variable as a lazy array, or `None` when the file holds no such thing.
fn array_of(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<Arc<dyn NdArrayD>> {
    let variable = file.variable(path)?;
    compat::variable_to_nd_array(file.clone(), &variable, &PhonyDimensions::none()).ok()
}

/// The strings of one variable.
async fn read_strings(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<Vec<String>> {
    let array = array_of(file, path)?;
    let typed = array.as_any().downcast_ref::<NdArray<String>>()?;
    Some(typed.clone_into_raw_vec().await)
}

/// The integers of one variable, whatever width the file wrote them at.
async fn read_i64s(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<Vec<i64>> {
    let array = array_of(file, path)?;
    if let Some(typed) = array.as_any().downcast_ref::<NdArray<i64>>() {
        return Some(typed.clone_into_raw_vec().await);
    }
    if let Some(typed) = array.as_any().downcast_ref::<NdArray<i32>>() {
        return Some(
            typed
                .clone_into_raw_vec()
                .await
                .into_iter()
                .map(i64::from)
                .collect(),
        );
    }
    None
}

/// The first value of one variable, as a double.
async fn read_f64(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<f64> {
    let array = array_of(file, path)?;
    if let Some(typed) = array.as_any().downcast_ref::<NdArray<f64>>() {
        return typed.clone_into_raw_vec().await.first().copied();
    }
    if let Some(typed) = array.as_any().downcast_ref::<NdArray<i64>>() {
        return typed
            .clone_into_raw_vec()
            .await
            .first()
            .map(|value| *value as f64);
    }
    if let Some(typed) = array.as_any().downcast_ref::<NdArray<i32>>() {
        return typed
            .clone_into_raw_vec()
            .await
            .first()
            .map(|value| *value as f64);
    }
    None
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::{local::LocalFileSystem, path::Path, ObjectStore};

    const OPTODAS_FILE: &str = "optodas.h5";
    /// The same shape, without the metadata that describes it.
    const PLAIN_FILE: &str = "instrument.h5";

    fn test_store() -> Arc<dyn ObjectStore> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        Arc::new(LocalFileSystem::new_with_prefix(root).expect("test_files store"))
    }

    async fn open(file: &str) -> (Arc<AsyncNetcdfFile>, PhonyDimensions) {
        let netcdf = Arc::new(
            AsyncNetcdfFile::open_store(test_store(), Path::from(file))
                .await
                .unwrap_or_else(|e| panic!("open {file}: {e}")),
        );
        let phony = PhonyDimensions::of_file(&netcdf);
        (netcdf, phony)
    }

    /// The layer names the axes of the payload from the file's own metadata.
    #[tokio::test]
    async fn it_names_the_axes_the_file_names() {
        let (file, phony) = open(OPTODAS_FILE).await;
        let found = detect(&file, &phony)
            .await
            .unwrap()
            .expect("the fixture follows the layout");

        // 6 samples and 4 channels, so the axes carry those lengths today.
        assert_eq!(
            found.axis_names().get("phony_len_6"),
            Some(&"time".to_string())
        );
        assert_eq!(
            found.axis_names().get("phony_len_4"),
            Some(&"distance".to_string())
        );
    }

    /// A file that does not carry the metadata is not this layout, and that is
    /// not an error.
    #[tokio::test]
    async fn it_stands_down_on_another_file() {
        let (file, phony) = open(PLAIN_FILE).await;
        assert!(detect(&file, &phony).await.unwrap().is_none());
    }

    /// The clock counts in nanoseconds, so every step is exact.
    #[tokio::test]
    async fn it_builds_the_clock_the_file_describes() {
        let (file, phony) = open(OPTODAS_FILE).await;
        let found = detect(&file, &phony).await.unwrap().unwrap();
        let mut arrays = IndexMap::new();
        found.decorate(&file, &phony, &mut arrays).await.unwrap();

        let time = arrays.get("time").expect("a time column");
        assert_eq!(time.dimensions(), vec!["time".to_string()]);
        let values = time
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .expect("nanosecond timestamps")
            .clone_into_raw_vec()
            .await;

        // 2026-03-28T12:00:00Z, then 8 ms per sample, six of them.
        let start = 1_774_699_200_000_000_000i64;
        assert_eq!(
            values.iter().map(|value| value.0).collect::<Vec<i64>>(),
            (0..6)
                .map(|step| start + step * 8_000_000)
                .collect::<Vec<i64>>()
        );
    }

    /// The positions come from the range the file records for the axis.
    #[tokio::test]
    async fn it_builds_the_positions_the_file_describes() {
        let (file, phony) = open(OPTODAS_FILE).await;
        let found = detect(&file, &phony).await.unwrap().unwrap();
        let mut arrays = IndexMap::new();
        found.decorate(&file, &phony, &mut arrays).await.unwrap();

        let distance = arrays.get("distance").expect("a distance column");
        assert_eq!(distance.dimensions(), vec!["distance".to_string()]);
        let values = distance
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .expect("metres")
            .clone_into_raw_vec()
            .await;
        // 4 raw channels apart, 1.25 m each.
        assert_eq!(values, vec![0.0, 5.0, 10.0, 15.0]);
    }

    /// The payload decodes through the scale the file records, which sits in
    /// another group and which no CF rule would find.
    #[tokio::test]
    async fn it_decodes_the_payload_through_the_scale_of_the_file() {
        let (file, phony) = open(OPTODAS_FILE).await;
        let found = detect(&file, &phony).await.unwrap().unwrap();
        let mut arrays = IndexMap::new();
        arrays.insert(
            PAYLOAD.to_string(),
            compat::variable_to_nd_array(file.clone(), &file.variable(PAYLOAD).unwrap(), &phony)
                .unwrap(),
        );
        found.decorate(&file, &phony, &mut arrays).await.unwrap();

        let payload = arrays.get(PAYLOAD).unwrap();
        assert_eq!(
            payload.datatype(),
            beacon_nd_array::datatypes::NdArrayDataType::F64
        );
        let values = payload
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .expect("decoded counts")
            .clone_into_raw_vec()
            .await;
        // The counts run 0, 1, 2 … and the scale is 0.5.
        assert_eq!(values[..4].to_vec(), vec![0.0, 0.5, 1.0, 1.5]);
    }

    /// A column the file holds itself is never overwritten.
    #[tokio::test]
    async fn a_column_of_the_file_wins_over_a_coordinate() {
        let (file, phony) = open(OPTODAS_FILE).await;
        let found = detect(&file, &phony).await.unwrap().unwrap();

        let mine = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![-1.0; 4],
            vec![4],
            vec!["distance".to_string()],
            None,
        )
        .unwrap();
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("distance".to_string(), Arc::new(mine));
        found.decorate(&file, &phony, &mut arrays).await.unwrap();

        let kept = arrays["distance"]
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .unwrap()
            .clone_into_raw_vec()
            .await;
        assert_eq!(kept, vec![-1.0; 4], "the file keeps its own column");
    }
}
