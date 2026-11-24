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

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, TimestampNanosecondArray};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Float64Type, Int64Type};
use beacon_arrow_netcdf::dimensions::PhonyDimensions;
use beacon_arrow_netcdf::nd::{stored_length, NetcdfVariable};
use beacon_nd_dataset::{Dimension, Dimensions, MemoryVariable, NdArrowArray, NdVariable, Region};
use indexmap::IndexMap;
use oxcdf::AsyncNetcdfFile;

/// The payload of an OptoDAS file, which every other path is relative to.
pub(crate) const PAYLOAD: &str = "data";
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

    /// The scale that turns the payload counts into the unit of the file.
    pub fn scale(&self) -> Option<f64> {
        self.scale
    }

    /// The coordinates the file describes but does not store: `time` and `distance`.
    ///
    /// `time` counts in nanoseconds, so one `dt` of 8 ms stays exact to the end of a file.
    pub fn coordinates(&self) -> beacon_nd_dataset::Result<Vec<Arc<dyn NdVariable>>> {
        let mut coordinates: Vec<Arc<dyn NdVariable>> = Vec::new();
        if let Some(time) = &self.time {
            let start = (time.start * NANOS_PER_SECOND).round() as i64;
            let step = (time.step * NANOS_PER_SECOND).round() as i64;
            let values = TimestampNanosecondArray::from_iter_values(
                (0..time.len).map(|sample| start + sample as i64 * step),
            );
            coordinates.push(coordinate(&time.dimension, time.len, Arc::new(values))?);
        }
        if let Some(distance) = &self.distance {
            let values = Float64Array::from_iter_values((0..distance.len).map(|position| {
                (distance.first + position as f64 * distance.step) * distance.scale
            }));
            coordinates.push(coordinate(
                &distance.dimension,
                distance.len,
                Arc::new(values),
            )?);
        }
        Ok(coordinates)
    }
}

/// One 1-D in-memory coordinate on the axis `dimension`.
fn coordinate(
    dimension: &str,
    len: usize,
    values: ArrayRef,
) -> beacon_nd_dataset::Result<Arc<dyn NdVariable>> {
    let dimensions = Dimensions::try_new([Dimension::new(dimension, len)])?;
    let array = NdArrowArray::try_new(values, dimensions)?;
    Ok(Arc::new(MemoryVariable::new(dimension, array)))
}

// ─── Reading the few values the check needs ────────────────────────────────

/// All values of one variable, or `None` when the file holds no such thing.
async fn read_column(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<ArrayRef> {
    let variable = file.variable(path)?;
    let phony = PhonyDimensions::none();
    let sizes: IndexMap<String, usize> = phony
        .apply(&variable.dimensions)
        .into_iter()
        .zip(stored_length(&variable))
        .collect();
    let column = NetcdfVariable::try_new(
        Arc::from(path),
        Arc::from(path),
        file.clone(),
        &variable,
        &phony,
        &sizes,
    )
    .ok()?;
    let read = column.read(&Region::full(column.dimensions())).await.ok()?;
    Some(read.values().clone())
}

/// The strings of one variable.
async fn read_strings(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<Vec<String>> {
    let values = read_column(file, path).await?;
    let strings = values.as_string_opt::<i32>()?;
    Some(
        strings
            .iter()
            .map(|value| value.unwrap_or_default().to_string())
            .collect(),
    )
}

/// The integers of one variable, whatever width the file wrote them at.
async fn read_i64s(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<Vec<i64>> {
    let values = read_column(file, path).await?;
    if !values.data_type().is_integer() {
        return None;
    }
    let values = cast(&values, &DataType::Int64).ok()?;
    Some(values.as_primitive::<Int64Type>().values().to_vec())
}

/// The first value of one variable, as a double.
async fn read_f64(file: &Arc<AsyncNetcdfFile>, path: &str) -> Option<f64> {
    let values = read_column(file, path).await?;
    if !(values.data_type().is_integer() || values.data_type().is_floating()) {
        return None;
    }
    let values = cast(&values, &DataType::Float64).ok()?;
    values
        .as_primitive::<Float64Type>()
        .values()
        .first()
        .copied()
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
}
