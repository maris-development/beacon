//! DataFusion sinks that materialize Arrow [`RecordBatch`]es into Zarr v3 stores.
//!
//! Two [`DataSink`] implementations mirror the NetCDF pair:
//!
//! * [`ZarrSink`] — writes each column as a 1-D array over a shared `obs`
//!   dimension, producing a *flat* store for table-shaped data. Batches stream
//!   straight through, so memory stays bounded.
//!
//! * [`ZarrNdSink`] — reshapes tabular rows into an N-dimensional grid: the
//!   named dimension columns become coordinate arrays and the remaining columns
//!   become arrays indexed by them (e.g. `lat × lon × time`).
//!
//! The write mode is chosen by [`ZarrOptions::write_dimensions`]; see
//! [`ZarrFormat::create_writer_physical_plan`](super::ZarrFormat).
//!
//! # Output layout
//!
//! A zarr store is a directory, but callers such as the HTTP query API can only
//! return one file. With [`ZarrOptions::zip_output`] set, the store is built in
//! a scratch directory and then packed into a zip archive at the output path.

use std::any::Any;
use std::fmt::Formatter;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::{physical_plan::FileSinkConfig, sink::DataSink},
    error::DataFusionError,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_plan::{DisplayAs, DisplayFormatType},
};
use futures::StreamExt;

use crate::datafusion::options::ZarrOptions;
use crate::writer::{OBS_DIMENSION, ZarrStoreWriter, archive_store, is_supported_field};

fn exec_err(e: impl std::fmt::Display) -> DataFusionError {
    DataFusionError::Execution(e.to_string())
}

/// Where a sink builds its store and where the finished artifact lands.
struct OutputPaths {
    /// Directory the zarr store is written into.
    store_dir: PathBuf,
    /// Zip archive to pack `store_dir` into, when zipping is enabled.
    zip_target: Option<PathBuf>,
}

impl OutputPaths {
    fn resolve(output_dir: &Path, prefix: &str, zip_output: bool) -> Self {
        let target = output_dir.join(prefix);
        if zip_output {
            // Build beside the target so the rename/zip stays on one filesystem.
            let mut scratch = target.clone().into_os_string();
            scratch.push(".zarr-build");
            Self {
                store_dir: PathBuf::from(scratch),
                zip_target: Some(target),
            }
        } else {
            Self {
                store_dir: target,
                zip_target: None,
            }
        }
    }

    /// Pack the store into its zip target, or leave the directory in place.
    fn finalize(self) -> datafusion::error::Result<()> {
        let Some(zip_target) = self.zip_target else {
            return Ok(());
        };
        // The caller pre-creates an empty temp file at the target path.
        let file = std::fs::File::create(&zip_target).map_err(exec_err)?;
        archive_store(&self.store_dir, std::io::BufWriter::new(file)).map_err(exec_err)?;
        if let Err(e) = std::fs::remove_dir_all(&self.store_dir) {
            tracing::warn!(error = %e, path = %self.store_dir.display(), "failed to clean up zarr scratch directory");
        }
        Ok(())
    }

    /// Clear anything already at the store path — `COPY TO` may have created an
    /// empty placeholder file where we need a directory.
    fn prepare(&self) -> datafusion::error::Result<()> {
        if self.store_dir.is_file() {
            std::fs::remove_file(&self.store_dir).map_err(exec_err)?;
        } else if self.store_dir.is_dir() {
            std::fs::remove_dir_all(&self.store_dir).map_err(exec_err)?;
        }
        Ok(())
    }
}

/// Rejects columns whose Arrow type has no zarr equivalent, naming all of them
/// at once rather than failing on the first.
fn check_supported(schema: &SchemaRef) -> datafusion::error::Result<()> {
    let unsupported: Vec<String> = schema
        .fields()
        .iter()
        .filter(|f| !is_supported_field(f))
        .map(|f| format!("{} ({})", f.name(), f.data_type()))
        .collect();
    if unsupported.is_empty() {
        Ok(())
    } else {
        Err(exec_err(format!(
            "Zarr output does not support these columns: {}",
            unsupported.join(", ")
        )))
    }
}

// ─── Flat sink ─────────────────────────────────────────────────────────────

/// [`DataSink`] that writes batches to a flat zarr store.
///
/// Every column becomes a 1-D array over the `obs` dimension. Batches are
/// appended as they arrive: the arrays grow with the stream, so nothing is
/// buffered beyond the current batch.
#[derive(Debug, Clone)]
pub struct ZarrSink {
    sink_config: FileSinkConfig,
    options: ZarrOptions,
    /// Directory the store is written to — the configured tmp store root.
    output_dir: PathBuf,
}

impl ZarrSink {
    pub fn new(sink_config: FileSinkConfig, options: ZarrOptions, output_dir: PathBuf) -> Self {
        Self {
            sink_config,
            options,
            output_dir,
        }
    }
}

impl DisplayAs for ZarrSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ZarrSink")
    }
}

#[async_trait::async_trait]
impl DataSink for ZarrSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let schema = self.sink_config.output_schema().clone();
        check_supported(&schema)?;

        let paths = OutputPaths::resolve(
            &self.output_dir,
            self.sink_config.table_paths[0].prefix().as_ref(),
            self.options.zip_output,
        );
        paths.prepare()?;
        tracing::info!("Writing flat Zarr store to path: {:?}", paths.store_dir);

        let mut writer = ZarrStoreWriter::new(paths.store_dir.clone(), self.options.clone())
            .map_err(exec_err)?;

        let dimensions = vec![OBS_DIMENSION.to_string()];
        for field in schema.fields() {
            // Shape 0 for now: `append` grows the `obs` extent as batches land.
            writer
                .create_array(field, vec![0], &dimensions)
                .map_err(exec_err)?;
        }

        let mut rows_written: u64 = 0;
        let mut stream = std::pin::pin!(data);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            for (index, field) in schema.fields().iter().enumerate() {
                writer
                    .append(field, rows_written, batch.column(index).as_ref())
                    .map_err(exec_err)?;
            }
            rows_written += batch.num_rows() as u64;
        }

        writer.finish().map_err(exec_err)?;
        paths.finalize()?;

        Ok(rows_written)
    }
}

// ─── N-dimensional sink ────────────────────────────────────────────────────

/// [`DataSink`] that reshapes tabular rows into a gridded zarr store.
///
/// The columns named in `dimension_columns` become the axes, in the order
/// given; each is written as a 1-D coordinate array of its sorted distinct
/// values. Every other column becomes an N-D array indexed by those axes, with
/// cells that no row covers left as the array's fill value.
///
/// Rows whose dimension values are null are dropped — they have no position in
/// the grid.
#[derive(Debug, Clone)]
pub struct ZarrNdSink {
    sink_config: FileSinkConfig,
    options: ZarrOptions,
    dimension_columns: Vec<String>,
    /// Directory the store is written to — the configured tmp store root.
    output_dir: PathBuf,
}

impl ZarrNdSink {
    /// Create a gridded sink.
    ///
    /// Fails if a named dimension column is missing from the output schema.
    pub fn new(
        sink_config: FileSinkConfig,
        options: ZarrOptions,
        dimension_columns: Vec<String>,
        output_dir: PathBuf,
    ) -> datafusion::error::Result<Self> {
        let schema = sink_config.output_schema();
        for column in &dimension_columns {
            if schema.index_of(column).is_err() {
                return Err(exec_err(format!(
                    "Zarr dimension column '{column}' is not in the query output"
                )));
            }
        }
        Ok(Self {
            sink_config,
            options,
            dimension_columns,
            output_dir,
        })
    }
}

impl DisplayAs for ZarrNdSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ZarrNdSink")
    }
}

#[async_trait::async_trait]
impl DataSink for ZarrNdSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.sink_config.output_schema()
    }

    fn metrics(&self) -> Option<datafusion::physical_plan::metrics::MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::error::Result<u64> {
        let schema = self.sink_config.output_schema().clone();
        check_supported(&schema)?;

        let paths = OutputPaths::resolve(
            &self.output_dir,
            self.sink_config.table_paths[0].prefix().as_ref(),
            self.options.zip_output,
        );
        paths.prepare()?;
        tracing::info!("Writing gridded Zarr store to path: {:?}", paths.store_dir);

        // Gridding needs every row at once: a cell's position depends on the
        // full set of distinct dimension values.
        let mut rows_written: u64 = 0;
        let mut batches = Vec::new();
        let mut stream = std::pin::pin!(data);
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            rows_written += batch.num_rows() as u64;
            batches.push(batch);
        }
        let batch = arrow::compute::concat_batches(&schema, batches.iter())?;

        let grid = Grid::build(&batch, &self.dimension_columns)?;

        let mut writer = ZarrStoreWriter::new(paths.store_dir.clone(), self.options.clone())
            .map_err(exec_err)?;

        // Coordinate arrays: 1-D, each indexed by its own dimension.
        for axis in &grid.axes {
            let field = schema.field_with_name(&axis.name)?;
            writer
                .create_array(
                    field,
                    vec![axis.values.len() as u64],
                    std::slice::from_ref(&axis.name),
                )
                .map_err(exec_err)?;
            writer
                .write_full(field, axis.values.as_ref())
                .map_err(exec_err)?;
        }

        // Data arrays: N-D, indexed by every axis.
        let shape: Vec<u64> = grid.axes.iter().map(|a| a.values.len() as u64).collect();
        let dimension_names: Vec<String> = grid.axes.iter().map(|a| a.name.clone()).collect();
        for field in schema.fields() {
            if self.dimension_columns.contains(field.name()) {
                continue;
            }
            let column = batch.column(schema.index_of(field.name())?);
            let slab = arrow::compute::take(column.as_ref(), &grid.cell_to_row, None)?;
            writer
                .create_array(field, shape.clone(), &dimension_names)
                .map_err(exec_err)?;
            writer.write_full(field, slab.as_ref()).map_err(exec_err)?;
        }

        writer.finish().map_err(exec_err)?;
        paths.finalize()?;

        Ok(rows_written)
    }
}

// ─── Gridding ──────────────────────────────────────────────────────────────

/// One dimension of the output grid.
struct Axis {
    name: String,
    /// The dimension's sorted distinct values — the coordinate array.
    values: ArrayRef,
    /// Position along this axis for each input row.
    row_positions: Vec<u32>,
}

/// The mapping from tabular rows onto grid cells.
struct Grid {
    axes: Vec<Axis>,
    /// For each cell of the flattened grid, the row that fills it (null where no
    /// row covers the cell). Feeds `arrow::compute::take`.
    cell_to_row: UInt32Array,
}

impl Grid {
    fn build(batch: &RecordBatch, dimension_columns: &[String]) -> datafusion::error::Result<Self> {
        let schema = batch.schema();

        // A row only has a grid position if all of its dimension values are
        // non-null; drop the rest.
        let mut usable = vec![true; batch.num_rows()];
        for column in dimension_columns {
            let array = batch.column(schema.index_of(column)?);
            if array.null_count() > 0 {
                for (row, keep) in usable.iter_mut().enumerate() {
                    *keep &= array.is_valid(row);
                }
            }
        }

        let mut axes = Vec::with_capacity(dimension_columns.len());
        for column in dimension_columns {
            let array = batch.column(schema.index_of(column)?);
            axes.push(dense_rank_axis(column.clone(), array, &usable)?);
        }

        let total_cells: usize = axes.iter().map(|a| a.values.len()).product();
        let mut cell_to_row: Vec<Option<u32>> = vec![None; total_cells];
        for row in 0..batch.num_rows() {
            if !usable[row] {
                continue;
            }
            // Row-major (C-order) flattening, matching the array shape.
            let mut cell = 0usize;
            for axis in &axes {
                cell = cell * axis.values.len() + axis.row_positions[row] as usize;
            }
            // Later rows win, mirroring the NetCDF sink's last-write-wins scatter.
            cell_to_row[cell] = Some(row as u32);
        }

        Ok(Self {
            axes,
            cell_to_row: UInt32Array::from(cell_to_row),
        })
    }
}

/// Build one grid axis from a dimension column.
///
/// Positions come from a dense rank over the column's sorted distinct values,
/// which works for any sortable Arrow type without per-type dispatch: sort the
/// column, mark where adjacent values differ, and accumulate. Unusable rows are
/// dropped up front so they cannot introduce a coordinate of their own.
fn dense_rank_axis(
    name: String,
    array: &ArrayRef,
    usable: &[bool],
) -> datafusion::error::Result<Axis> {
    let kept: UInt32Array = usable
        .iter()
        .enumerate()
        .filter(|(_, keep)| **keep)
        .map(|(row, _)| row as u32)
        .collect::<Vec<_>>()
        .into();

    let kept_values = arrow::compute::take(array.as_ref(), &kept, None)?;
    let num_kept = kept_values.len();

    let sorted_indices = arrow::compute::sort_to_indices(kept_values.as_ref(), None, None)?;
    let sorted = arrow::compute::take(kept_values.as_ref(), &sorted_indices, None)?;

    // `distinct` is null-aware equality: true wherever a sorted neighbour
    // differs, which is exactly where the dense rank advances.
    let changed: BooleanArray = if num_kept > 1 {
        arrow::compute::kernels::cmp::distinct(
            &sorted.slice(1, num_kept - 1),
            &sorted.slice(0, num_kept - 1),
        )?
    } else {
        BooleanArray::from(Vec::<bool>::new())
    };

    let mut row_positions = vec![0u32; array.len()];
    // First occurrence (in sorted order) of each distinct value, used to
    // materialize the coordinate array.
    let mut first_of_rank: Vec<u32> = Vec::new();

    let mut rank = 0u32;
    for position in 0..num_kept {
        if position > 0 && changed.value(position - 1) {
            rank += 1;
        }
        let kept_index = sorted_indices.value(position);
        if first_of_rank.len() as u32 == rank {
            first_of_rank.push(kept_index);
        }
        row_positions[kept.value(kept_index as usize) as usize] = rank;
    }

    // Coordinates are indices into the kept subset, so read them back from it.
    let values = arrow::compute::take(
        kept_values.as_ref(),
        &UInt32Array::from(first_of_rank),
        None,
    )?;
    Ok(Axis {
        name,
        values,
        row_positions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    /// Rows keyed by (time, depth), deliberately unsorted and with a gap so the
    /// scatter has to place values rather than copy them through.
    fn ungridded_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("depth", DataType::Int64, true),
            Field::new("temp", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 1])),
                Arc::new(Int64Array::from(vec![20, 10, 20])),
                Arc::new(Float64Array::from(vec![4.0, 1.0, 2.0])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn grid_axes_are_sorted_distinct_values() {
        let batch = ungridded_batch();
        let grid = Grid::build(&batch, &["time".to_string(), "depth".to_string()]).unwrap();

        assert_eq!(grid.axes.len(), 2);
        assert_eq!(
            grid.axes[0].values.as_ref().as_primitive::<arrow::datatypes::Int64Type>().values(),
            &[1, 2],
            "time axis should be the sorted distinct values"
        );
        assert_eq!(
            grid.axes[1].values.as_ref().as_primitive::<arrow::datatypes::Int64Type>().values(),
            &[10, 20],
            "depth axis should be the sorted distinct values"
        );
    }

    /// The cell mapping must place each row at `time × depth` in row-major
    /// order, and leave uncovered cells empty.
    #[test]
    fn grid_scatters_rows_into_cells() {
        let batch = ungridded_batch();
        let grid = Grid::build(&batch, &["time".to_string(), "depth".to_string()]).unwrap();

        let temp = batch.column(2);
        let slab = arrow::compute::take(temp.as_ref(), &grid.cell_to_row, None).unwrap();
        let slab = slab.as_ref().as_primitive::<arrow::datatypes::Float64Type>();

        assert_eq!(slab.len(), 4, "2 times x 2 depths");
        // (time=1, depth=10) -> 1.0, (time=1, depth=20) -> 2.0
        assert_eq!(slab.value(0), 1.0);
        assert_eq!(slab.value(1), 2.0);
        // (time=2, depth=10) has no row.
        assert!(slab.is_null(2), "uncovered cell should stay empty");
        // (time=2, depth=20) -> 4.0
        assert_eq!(slab.value(3), 4.0);
    }

    /// A row with a null dimension value has no grid position and must not
    /// introduce a coordinate of its own.
    #[test]
    fn grid_drops_rows_with_null_dimensions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Int64, true),
            Field::new("temp", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
                Arc::new(Float64Array::from(vec![1.0, 9.0, 2.0])),
            ],
        )
        .unwrap();

        let grid = Grid::build(&batch, &["time".to_string()]).unwrap();
        assert_eq!(
            grid.axes[0].values.as_ref().as_primitive::<arrow::datatypes::Int64Type>().values(),
            &[1, 2],
            "the null time must not become a coordinate"
        );

        let slab = arrow::compute::take(batch.column(1).as_ref(), &grid.cell_to_row, None).unwrap();
        let slab = slab.as_ref().as_primitive::<arrow::datatypes::Float64Type>();
        assert_eq!(slab.len(), 2);
        assert_eq!(slab.value(0), 1.0);
        assert_eq!(slab.value(1), 2.0);
    }

    /// Zipping puts the store under a scratch path so the archive can be
    /// written at the requested output path.
    #[test]
    fn zip_output_builds_in_a_scratch_directory() {
        let zipped = OutputPaths::resolve(Path::new("/tmp"), "out.tmp", true);
        assert_eq!(zipped.zip_target.as_deref(), Some(Path::new("/tmp/out.tmp")));
        assert_ne!(zipped.store_dir, PathBuf::from("/tmp/out.tmp"));

        let plain = OutputPaths::resolve(Path::new("/tmp"), "out.zarr", false);
        assert_eq!(plain.store_dir, PathBuf::from("/tmp/out.zarr"));
        assert!(plain.zip_target.is_none());
    }

    #[test]
    fn nd_sink_rejects_unknown_dimension_columns() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let config = FileSinkConfig {
            original_url: String::new(),
            object_store_url: datafusion::execution::object_store::ObjectStoreUrl::local_filesystem(),
            file_group: datafusion::datasource::physical_plan::FileGroup::default(),
            table_paths: vec![
                datafusion::datasource::listing::ListingTableUrl::parse("file:///tmp/out").unwrap(),
            ],
            output_schema: schema,
            table_partition_cols: vec![],
            insert_op: datafusion::logical_expr::dml::InsertOp::Overwrite,
            keep_partition_by_columns: false,
            file_extension: "zarr".to_string(),
            file_output_mode: datafusion::datasource::physical_plan::FileOutputMode::SingleFile,
        };

        let err = ZarrNdSink::new(
            config,
            ZarrOptions::default(),
            vec!["missing".to_string()],
            PathBuf::from("/tmp"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("missing"), "{err}");
    }
}
