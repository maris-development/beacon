//! The read of the chunks of one dataset.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use nd_arrow_array::{Dimension, Dimensions, NdRecordBatch};

use crate::dataset::variables;
use crate::{ChunkGrid, Error, NdDataset, NdVariable, Region, Result};

/// The variables of one dataset, bound to one chunk grid.
///
/// The reader finds each variable one time, and it checks one time that each
/// variable lies on the grid. Then it reads one chunk at a time.
#[derive(Debug)]
pub struct ChunkReader {
    /// The name of the dataset, for errors.
    dataset: String,
    grid: ChunkGrid,
    /// One nullable field for each variable.
    schema: SchemaRef,
    variables: Vec<BoundVariable>,
}

/// One variable, with the grid axis of each of its axes.
#[derive(Debug)]
struct BoundVariable {
    variable: Arc<dyn NdVariable>,
    axes: Vec<usize>,
}

impl ChunkReader {
    /// Bind the variables of `dataset` to `grid`.
    ///
    /// `Invalid` when a variable has a dimension that is not on the grid.
    pub fn try_new(dataset: &dyn NdDataset, grid: ChunkGrid) -> Result<Self> {
        let variables = variables(dataset)?
            .into_iter()
            .map(|variable| {
                let axes = variable
                    .dimensions()
                    .iter()
                    .map(|dimension| {
                        grid.dimensions()
                            .position(dimension.name())
                            .ok_or_else(|| Error::Invalid {
                                dataset: dataset.name().to_string(),
                                reason: format!(
                                    "the variable '{}' has the dimension '{}', which is not on the grid",
                                    variable.name(),
                                    dimension.name()
                                ),
                            })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(BoundVariable { variable, axes })
            })
            .collect::<Result<Vec<_>>>()?;
        let fields: Vec<Field> = variables
            .iter()
            .map(|bound| {
                let variable = &bound.variable;
                Field::new(variable.name(), variable.data_type().clone(), true)
            })
            .collect();
        Ok(Self {
            dataset: dataset.name().to_string(),
            grid,
            schema: Arc::new(Schema::new(fields)),
            variables,
        })
    }

    /// The grid of the reader.
    pub fn grid(&self) -> &ChunkGrid {
        &self.grid
    }

    /// The schema of each batch: one nullable field for each variable, in `variable_names()` order.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Read each variable in `chunk` into one un-broadcast batch.
    ///
    /// The target has the dimensions of the grid and the shape of `chunk`.
    /// The reader reads each variable on its own axes, so it reads a
    /// coordinate one time for each chunk and not one time for each row.
    /// `Read` when a variable cannot be read. `Invalid` when a variable
    /// returns an array that does not have the shape of its region.
    pub async fn read(&self, chunk: &Region) -> Result<NdRecordBatch> {
        let target = Dimensions::try_new(
            self.grid
                .dimensions()
                .iter()
                .zip(&chunk.shape)
                .map(|(dimension, &size)| Dimension::new(dimension.name(), size)),
        )?;

        let mut columns = Vec::with_capacity(self.variables.len());
        for BoundVariable { variable, axes } in &self.variables {
            let region = Region::new(
                axes.iter().map(|&axis| chunk.start[axis]).collect(),
                axes.iter().map(|&axis| chunk.shape[axis]).collect(),
            );
            let column = variable.read(&region).await.map_err(|error| match error {
                // This error names the variable.
                Error::InvalidRegion { .. } => error,
                error => Error::Read {
                    variable: variable.name().to_string(),
                    error: Box::new(error),
                },
            })?;
            // A wrong size of 1 would broadcast over the chunk with no error.
            if column.dims().shape() != region.shape
                || column.dims().names() != variable.dimensions().names()
            {
                return Err(Error::Invalid {
                    dataset: self.dataset.clone(),
                    reason: format!(
                        "the variable '{}' returned {:?} for the region {region:?}",
                        variable.name(),
                        column.dims()
                    ),
                });
            }
            columns.push(column);
        }

        Ok(NdRecordBatch::try_new(
            Arc::clone(&self.schema),
            columns,
            target,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{AsArray, Float64Array};
    use arrow::datatypes::{DataType, Float64Type};
    use async_trait::async_trait;
    use nd_arrow_array::{Attributes, NdArrowArray};

    use super::*;
    use crate::memory::testing::{dataset, dims, ramp};
    use crate::{DatasetSchema, MemoryDataset, VariableInfo, select_columns};

    fn sst() -> MemoryDataset {
        dataset(vec![
            ramp("analysed_sst", &[("time", 1), ("lat", 1208), ("lon", 1920)])
                .with_chunk_shape(vec![1, 604, 960]),
            ramp("lat", &[("lat", 1208)]),
        ])
    }

    /// A reader of `dataset` on its own grid at `batch_size`.
    fn reader(dataset: &dyn NdDataset, batch_size: usize) -> ChunkReader {
        let grid = ChunkGrid::try_new(dataset, batch_size).unwrap();
        ChunkReader::try_new(dataset, grid).unwrap()
    }

    /// The Float64 values of column `index` of `batch`.
    fn values(batch: &NdRecordBatch, index: usize) -> Vec<f64> {
        batch
            .column(index)
            .values()
            .as_primitive::<Float64Type>()
            .values()
            .to_vec()
    }

    /// A variable whose read gives the result of `read`.
    #[derive(Debug)]
    struct Stub {
        info: VariableInfo,
        read: fn() -> Result<NdArrowArray>,
    }

    impl Stub {
        /// The dataset `test.nd` with the one variable `v(n=3)`, whose read gives `read`.
        fn dataset(read: fn() -> Result<NdArrowArray>) -> DatasetSchema {
            let info = VariableInfo::new("v", DataType::Float64, dims(&[("n", 3)]));
            let v = Arc::new(Stub { info, read });
            DatasetSchema::try_new("test.nd", dims(&[("n", 3)]), vec![v]).unwrap()
        }
    }

    #[async_trait]
    impl NdVariable for Stub {
        fn name(&self) -> &str {
            &self.info.name
        }
        fn data_type(&self) -> &DataType {
            &self.info.data_type
        }
        fn dimensions(&self) -> &Dimensions {
            &self.info.dimensions
        }
        fn chunk_shape(&self) -> Option<&[usize]> {
            None
        }
        fn attributes(&self) -> &Attributes {
            &self.info.attributes
        }
        async fn read(&self, _: &Region) -> Result<NdArrowArray> {
            (self.read)()
        }
    }

    #[tokio::test]
    async fn a_read_error_names_the_variable_and_keeps_its_source() {
        let ds = Stub::dataset(|| Err(Error::format("disk gone")));
        let reader = reader(&ds, 8192);
        let error = reader.read(&reader.grid().chunk(0)).await.unwrap_err();
        assert_eq!(error.to_string(), "variable 'v': disk gone");
        let Error::Read { variable, error } = error else {
            panic!("{error:?}");
        };
        assert_eq!(variable, "v");
        assert!(matches!(*error, Error::Format(_)), "{error:?}");
    }

    #[tokio::test]
    async fn a_read_with_a_wrong_shape_is_invalid() {
        // One value for a region of three values.
        let ds = Stub::dataset(|| {
            let values = Arc::new(Float64Array::from(vec![1.0]));
            Ok(NdArrowArray::try_new(values, dims(&[("n", 1)]))?)
        });
        let reader = reader(&ds, 8192);
        let error = reader.read(&reader.grid().chunk(0)).await.unwrap_err();
        assert!(
            matches!(error, Error::Invalid { ref reason, .. } if reason.contains("'v'")),
            "{error:?}"
        );
    }

    #[test]
    fn a_variable_with_a_dimension_off_the_grid_is_invalid() {
        let ds = dataset(vec![ramp("v", &[("n", 6)]), ramp("w", &[("m", 2)])]);
        // The grid of `v` alone does not have the dimension `m`.
        let only_v = select_columns(Arc::new(ds.clone()), &["v"]);
        let grid = ChunkGrid::try_new(only_v.as_ref(), 4).unwrap();
        let error = ChunkReader::try_new(&ds, grid).unwrap_err();
        assert!(
            matches!(error, Error::Invalid { ref reason, .. } if reason.contains("'m'")),
            "{error:?}"
        );
    }

    #[test]
    fn the_schema_has_one_nullable_field_for_each_variable_in_dataset_order() {
        let reader = reader(&sst(), 8192);
        let fields: Vec<(&str, bool)> = reader
            .schema()
            .fields()
            .iter()
            .map(|field| (field.name().as_str(), field.is_nullable()))
            .collect();
        assert_eq!(fields, [("analysed_sst", true), ("lat", true)]);
    }

    #[tokio::test]
    async fn a_chunk_reads_each_variable_on_its_own_axes() {
        let ds = sst();
        let reader = reader(&ds, 8192);
        let batch = reader.read(&reader.grid().chunk(0)).await.unwrap();
        assert_eq!(batch.schema(), reader.schema());
        assert_eq!(batch.column(0).len(), 604 * 960);
        assert_eq!(batch.column(1).len(), 604);
        assert_eq!(batch.target().shape(), vec![1, 604, 960]);
        assert_eq!(ds.reads("analysed_sst"), 1);
        assert_eq!(ds.reads("lat"), 1);
    }

    #[tokio::test]
    async fn a_coordinate_is_read_on_the_range_of_the_chunk() {
        let reader = reader(&sst(), 8192);
        // Chunk 3 starts at lat 604.
        let batch = reader.read(&reader.grid().chunk(3)).await.unwrap();
        let lat = values(&batch, 1);
        assert_eq!(lat.len(), 604);
        assert_eq!(lat[0], 604.0);
    }

    #[tokio::test]
    async fn a_chunk_holds_the_values_of_its_region() {
        let ds = dataset(vec![ramp("v", &[("n", 6)])]);
        let reader = reader(&ds, 4);
        let batch = reader.read(&reader.grid().chunk(1)).await.unwrap();
        assert_eq!(values(&batch, 0), vec![4.0, 5.0]);
    }

    #[tokio::test]
    async fn a_transposed_variable_is_read_on_its_own_axis_order() {
        // `a` is last, so it drives the grid (lat, lon). `v` has the axes in the other order.
        let ds = dataset(vec![
            ramp("v", &[("lon", 3), ("lat", 2)]),
            ramp("a", &[("lat", 2), ("lon", 3)]),
        ]);
        let reader = reader(&ds, 2);
        assert_eq!(reader.grid().dimensions().names(), vec!["lat", "lon"]);
        // The chunk at lat 0 and lon 2.
        let chunk = reader.grid().chunk(1);
        assert_eq!(chunk, Region::new(vec![0, 2], vec![1, 1]));
        let batch = reader.read(&chunk).await.unwrap();
        // `v` at lon 2 and lat 0 is its element 4. `a` at lat 0 and lon 2 is its element 2.
        assert_eq!(values(&batch, 0), vec![4.0]);
        assert_eq!(values(&batch, 1), vec![2.0]);
    }

    #[tokio::test]
    async fn a_chunk_of_no_variable_has_rows_and_no_read() {
        let ds = sst();
        let grid = ChunkGrid::try_new(&ds, 8192).unwrap();
        let view = select_columns(Arc::new(ds.clone()), &[]);
        let reader = ChunkReader::try_new(view.as_ref(), grid).unwrap();
        let chunk = reader.grid().chunk(0);
        let batch = reader.read(&chunk).await.unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), chunk.len());
        assert_eq!(ds.reads("analysed_sst") + ds.reads("lat"), 0);
    }

    #[tokio::test]
    async fn a_rank_0_variable_goes_in_each_chunk() {
        let ds = dataset(vec![ramp("v", &[("n", 6)]), ramp("title", &[])]);
        let reader = reader(&ds, 4);
        let batch = reader.read(&reader.grid().chunk(0)).await.unwrap();
        assert_eq!(batch.column(1).len(), 1);
    }
}
