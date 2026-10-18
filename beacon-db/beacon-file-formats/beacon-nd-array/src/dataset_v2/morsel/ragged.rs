//! The morsel of one range of casts of a ragged dataset.

use std::{fmt, ops::Range, sync::Arc};

use arrow::{
    array::{ArrayRef, RecordBatch, new_null_array},
    compute::{concat, concat_batches},
    datatypes::{DataType, Field, Schema, SchemaRef},
};
use beacon_datafusion_ext::{
    morsel_planner::Morsel,
    nd::{Dimension, Dimensions, NdArrowArray, NdRecordBatch, encode_nd_record_batch},
};
use datafusion::error::Result;
use futures::future::BoxFuture;
use indexmap::IndexMap;

use super::common::{
    arrow_error, block_size, column_error, file_field, null_column, read_subset, row_count_batch,
};
use crate::{NdArrayD, array::subset::ArraySubset, dataset_v2::ragged::RaggedDataset};

/// The grid axis that holds the observations of one cast, in a ragged morsel.
pub const OBSERVATION_DIMENSION: &str = "obs";

/// One range of casts of a ragged dataset.
pub struct RaggedCastsMorsel {
    dataset: Arc<RaggedDataset>,
    casts: Range<usize>,
    offsets: Arc<IndexMap<String, Vec<usize>>>, // The observation offsets of each cast, per row-size array
    table_schema: SchemaRef,                    // The projected table schema
}

impl RaggedCastsMorsel {
    /// Create the morsel that reads the casts `casts` of `dataset`.
    ///
    /// # Arguments
    ///
    /// * `dataset` - The ragged dataset of one file
    /// * `casts` - The cast indices to read
    /// * `offsets` - The observation offsets of each cast, from [`RaggedDataset::row_offsets`]
    /// * `schema` - The logical file schema that the scan projects, without partition columns
    pub fn new(
        dataset: Arc<RaggedDataset>,
        casts: Range<usize>,
        offsets: Arc<IndexMap<String, Vec<usize>>>,
        table_schema: SchemaRef,
    ) -> Self {
        Self {
            dataset,
            casts,
            offsets,
            table_schema,
        }
    }

    /// The cast indices that this morsel reads.
    pub fn casts(&self) -> &Range<usize> {
        &self.casts
    }

    /// Read the casts into an nd-encoded batch with one row per cast, in the column order of the scan schema.
    ///
    /// The grid of a cast is (instance dimension of size 1, [`OBSERVATION_DIMENSION`]).
    /// The observation axis has the length of the longest variable of the cast.
    /// A ragged array fills the cast from the start, and nulls pad the rest.
    /// An instance array keeps its own axes, and an attribute stays rank 0.
    /// A column that the file does not hold is a rank-0 null.
    /// A scan without columns (`COUNT(*)`) gets a batch with only a row count.
    async fn read(&self) -> Result<RecordBatch> {
        let lengths = self.cast_lengths();
        if self.table_schema.fields().is_empty() {
            return row_count_batch(lengths.iter().sum());
        }

        let (schema, sources) = self.read_sources().await?;
        let rows = self
            .casts
            .clone()
            .zip(lengths)
            .map(|(cast, length)| self.cast_batch(&schema, &sources, cast, length))
            .collect::<Result<Vec<_>>>()?;
        concat_batches(&rows[0].schema(), &rows).map_err(arrow_error)
    }

    /// The length of the observation axis of each cast: the most observations of one variable.
    fn cast_lengths(&self) -> Vec<usize> {
        self.casts
            .clone()
            .map(|cast| {
                self.offsets
                    .values()
                    .map(|offsets| offsets[cast + 1] - offsets[cast])
                    .max()
                    .unwrap_or(0)
            })
            .collect()
    }

    /// Read each column of the scan schema one time for all casts of the range.
    ///
    /// # Returns
    ///
    /// The batch schema, with the file type of each column, and the columns
    async fn read_sources(&self) -> Result<(SchemaRef, Vec<RaggedColumn<'_>>)> {
        let mut fields = Vec::with_capacity(self.table_schema.fields().len());
        let mut sources = Vec::with_capacity(self.table_schema.fields().len());
        for field in self.table_schema.fields() {
            let source = self
                .read_source(field)
                .await
                .map_err(|e| column_error(field.name(), e))?;
            fields.push(file_field(field, source.data_type().clone()));
            sources.push(source);
        }
        Ok((Arc::new(Schema::new(fields)), sources))
    }

    /// Read the column of `field` for all casts of the range.
    async fn read_source(&self, field: &Field) -> Result<RaggedColumn<'_>> {
        let name = field.name();
        let array = || self.dataset.inner.arrays[name].as_ref();
        if let Some(row_size_name) = self.dataset.ragged_arrays.get(name) {
            self.read_ragged(array(), &self.offsets[row_size_name])
                .await
        } else if self.dataset.instance_arrays.contains(name) {
            self.read_instance(array()).await
        } else {
            // A column that the file does not hold, or a row-size array.
            null_column(field.data_type()).map(RaggedColumn::Whole)
        }
    }

    /// Read the observations of all casts of a ragged array.
    async fn read_ragged<'a>(
        &self,
        array: &dyn NdArrayD,
        offsets: &'a [usize],
    ) -> Result<RaggedColumn<'a>> {
        let first = offsets[self.casts.start];
        let subset = first_axis_subset(array, first..offsets[self.casts.end]);
        let (values, mut dimensions) = read_subset(array, subset).await?;
        dimensions.remove(0);
        Ok(RaggedColumn::Ragged {
            values,
            offsets,
            first,
            trailing: dimensions,
        })
    }

    /// Read an array that is not ragged.
    ///
    /// An array on the instance dimension is cut to the casts of the range.
    /// All other arrays, attributes too, are read whole.
    async fn read_instance(&self, array: &dyn NdArrayD) -> Result<RaggedColumn<'static>> {
        if array.dimensions().first() != Some(&self.dataset.instance_dimension) {
            let subset = ArraySubset::new(vec![0; array.shape().len()], array.shape());
            let (values, dimensions) = read_subset(array, subset).await?;
            return NdArrowArray::try_new(values, Dimensions::try_new(dimensions)?)
                .map(RaggedColumn::Whole);
        }
        let subset = first_axis_subset(array, self.casts.clone());
        let (values, mut dimensions) = read_subset(array, subset).await?;
        dimensions.remove(0);
        Ok(RaggedColumn::Instance {
            values,
            first_cast: self.casts.start,
            trailing: dimensions,
        })
    }

    /// Build the nd-encoded row of one cast.
    fn cast_batch(
        &self,
        schema: &SchemaRef,
        sources: &[RaggedColumn],
        cast: usize,
        length: usize,
    ) -> Result<RecordBatch> {
        let instance = self.dataset.instance_dimension.as_str();
        let columns = schema
            .fields()
            .iter()
            .zip(sources)
            .map(|(field, source)| {
                source
                    .for_cast(cast, length, instance)
                    .map_err(|e| column_error(field.name(), e))
            })
            .collect::<Result<Vec<_>>>()?;
        let target = cast_target(instance, length, &columns)?;
        let batch = NdRecordBatch::try_new(Arc::clone(schema), columns, target)?;
        encode_nd_record_batch(&batch)
    }
}

impl fmt::Debug for RaggedCastsMorsel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RaggedCastsMorsel")
            .field("dataset", &self.dataset.inner.name)
            .field("casts", &self.casts)
            .field("schema", &self.table_schema)
            .finish()
    }
}

impl Morsel for RaggedCastsMorsel {
    fn produce(self: Box<Self>) -> BoxFuture<'static, Result<RecordBatch>> {
        Box::pin(async move { self.read().await })
    }
}

/// A column of a ragged morsel, read one time for all casts of the range.
enum RaggedColumn<'a> {
    /// The observations of all casts, which `offsets` splits.
    Ragged {
        values: ArrayRef,
        offsets: &'a [usize],
        first: usize,             // The offset of the first observation in `values`
        trailing: Vec<Dimension>, // The axes after the observation axis
    },
    /// The values of all casts, one block of the trailing axes per cast.
    Instance {
        values: ArrayRef,
        first_cast: usize,        // The cast of the first block in `values`
        trailing: Vec<Dimension>, // The axes after the instance axis
    },
    /// An array that is the same for every cast, such as an attribute.
    Whole(NdArrowArray),
}

impl RaggedColumn<'_> {
    /// The file type of the values.
    fn data_type(&self) -> &DataType {
        match self {
            Self::Ragged { values, .. } | Self::Instance { values, .. } => values.data_type(),
            Self::Whole(column) => column.data_type(),
        }
    }

    /// The column of one cast.
    ///
    /// # Arguments
    ///
    /// * `cast` - The cast index in the file
    /// * `length` - The length of the observation axis of the cast
    /// * `instance` - The name of the instance dimension
    fn for_cast(&self, cast: usize, length: usize, instance: &str) -> Result<NdArrowArray> {
        match self {
            Self::Ragged {
                values,
                offsets,
                first,
                trailing,
            } => ragged_cast(values, offsets, *first, trailing, cast, length),
            Self::Instance {
                values,
                first_cast,
                trailing,
            } => instance_cast(values, cast - first_cast, trailing, instance),
            Self::Whole(column) => Ok(column.clone()),
        }
    }
}

/// The observations of one cast, with nulls at the end up to `length`.
///
/// The result has the axes ([`OBSERVATION_DIMENSION`], trailing axes of the array).
fn ragged_cast(
    values: &ArrayRef,
    offsets: &[usize],
    first: usize,
    trailing: &[Dimension],
    cast: usize,
    length: usize,
) -> Result<NdArrowArray> {
    let block = block_size(trailing);
    let observations = offsets[cast + 1] - offsets[cast];
    let mut values = values.slice((offsets[cast] - first) * block, observations * block);
    if observations < length {
        let padding = new_null_array(values.data_type(), (length - observations) * block);
        values = concat(&[values.as_ref(), padding.as_ref()]).map_err(arrow_error)?;
    }
    let mut dimensions = vec![Dimension::new(OBSERVATION_DIMENSION, length)];
    dimensions.extend(trailing.iter().cloned());
    NdArrowArray::try_new(values, Dimensions::try_new(dimensions)?)
}

/// The values of one cast of an instance array, on an instance axis of size 1.
fn instance_cast(
    values: &ArrayRef,
    local: usize,
    trailing: &[Dimension],
    instance: &str,
) -> Result<NdArrowArray> {
    let block = block_size(trailing);
    let mut dimensions = vec![Dimension::new(instance, 1)];
    dimensions.extend(trailing.iter().cloned());
    NdArrowArray::try_new(
        values.slice(local * block, block),
        Dimensions::try_new(dimensions)?,
    )
}

/// The grid of one cast: (instance of size 1, [`OBSERVATION_DIMENSION`]), then the
/// other axes of the columns in the order they first occur.
fn cast_target(instance: &str, length: usize, columns: &[NdArrowArray]) -> Result<Dimensions> {
    let mut dimensions = vec![
        Dimension::new(instance, 1),
        Dimension::new(OBSERVATION_DIMENSION, length),
    ];
    for column in columns {
        for dimension in column.dims().iter() {
            if !dimensions.iter().any(|d| d.name() == dimension.name()) {
                dimensions.push(dimension.clone());
            }
        }
    }
    Dimensions::try_new(dimensions)
}

/// The subset of `array` that cuts its first axis to `range` and keeps the other axes whole.
fn first_axis_subset(array: &dyn NdArrayD, range: Range<usize>) -> ArraySubset {
    let mut start = vec![0; array.shape().len()];
    let mut shape = array.shape();
    start[0] = range.start;
    shape[0] = range.end - range.start;
    ArraySubset::new(start, shape)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use arrow::array::Array;
    use beacon_datafusion_ext::nd::{
        decode_nd_record_batch,
        encoding::{decode_nd_record_batch_row, nd_batch_count},
    };

    use super::*;
    use crate::dataset_v2::{
        Dataset,
        morsel::test_util::{array, decode_all, i32_values, nullable_i32_values, schema, text},
    };

    /// Three casts with row sizes [2, 1, 3] on `z_obs` and [1, 1, 1] on `t_obs`.
    async fn casts() -> (Arc<RaggedDataset>, Arc<IndexMap<String, Vec<usize>>>) {
        let arrays = IndexMap::from([
            (
                "station".to_string(),
                array(vec![1, 2, 3], &[3], &["casts"]),
            ),
            (
                "z_row_size".to_string(),
                array(vec![2, 1, 3], &[3], &["casts"]),
            ),
            ("z_row_size.sample_dimension".to_string(), text("z_obs")),
            (
                "t_row_size".to_string(),
                array(vec![1, 1, 1], &[3], &["casts"]),
            ),
            ("t_row_size.sample_dimension".to_string(), text("t_obs")),
            (
                "depth".to_string(),
                array((0..6).collect(), &[6], &["z_obs"]),
            ),
            ("depth.scale".to_string(), array(vec![7], &[], &[])),
            (
                "temp".to_string(),
                array(vec![100, 101, 102], &[3], &["t_obs"]),
            ),
        ]);
        let dataset = Dataset::from_arrays("test".to_string(), arrays, HashSet::new()).unwrap();
        let ragged = RaggedDataset::try_new(dataset, None).await.unwrap();
        let offsets = Arc::new(ragged.row_offsets().await.unwrap());
        (Arc::new(ragged), offsets)
    }

    async fn produce(schema: SchemaRef, range: Range<usize>) -> RecordBatch {
        let (dataset, offsets) = casts().await;
        Box::new(RaggedCastsMorsel::new(dataset, range, offsets, schema))
            .produce()
            .await
            .unwrap()
    }

    /// Produce the morsel of the casts `range`, then decode and broadcast each cast to flat rows.
    async fn read_casts(schema: SchemaRef, range: Range<usize>) -> RecordBatch {
        decode_all(&produce(schema, range).await)
    }

    #[tokio::test]
    async fn a_short_variable_is_padded_with_nulls_at_the_end_of_its_cast() {
        let schema = schema(&[("depth", DataType::Int32), ("temp", DataType::Int32)]);

        let batch = read_casts(schema, 0..2).await;

        assert_eq!(
            nullable_i32_values(&batch, "depth"),
            vec![Some(0), Some(1), Some(2)]
        );
        assert_eq!(
            nullable_i32_values(&batch, "temp"),
            vec![Some(100), None, Some(101)]
        );
    }

    #[tokio::test]
    async fn each_cast_is_one_row_of_the_encoded_batch() {
        let encoded = produce(schema(&[("depth", DataType::Int32)]), 0..3).await;

        assert_eq!(nd_batch_count(&encoded), 3);
        let rows: Vec<usize> = (0..3)
            .map(|row| {
                decode_nd_record_batch_row(&encoded, row)
                    .unwrap()
                    .num_rows()
            })
            .collect();
        assert_eq!(rows, vec![2, 1, 3]);
    }

    #[tokio::test]
    async fn instance_arrays_and_attributes_broadcast_onto_the_casts() {
        let schema = schema(&[
            ("station", DataType::Int32),
            ("depth.scale", DataType::Int32),
            ("depth", DataType::Int32),
        ]);

        let batch = read_casts(schema, 1..3).await;

        assert_eq!(i32_values(&batch, "station"), vec![2, 3, 3, 3]);
        assert_eq!(i32_values(&batch, "depth.scale"), vec![7; 4]);
        assert_eq!(i32_values(&batch, "depth"), vec![2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn the_instance_arrays_stay_one_dimensional() {
        let schema = schema(&[
            ("station", DataType::Int32),
            ("depth.scale", DataType::Int32),
        ]);

        let encoded = produce(schema, 0..3).await;

        let batch = decode_nd_record_batch(&encoded).unwrap();
        assert_eq!(batch.column(0).dims().rank(), 1);
        assert_eq!(batch.column(1).dims().rank(), 0);
    }

    #[tokio::test]
    async fn a_missing_ragged_column_is_null_on_every_row() {
        let schema = schema(&[("depth", DataType::Int32), ("salt", DataType::Float64)]);

        let batch = read_casts(schema, 0..1).await;

        let salt = batch.column_by_name("salt").unwrap();
        assert_eq!(salt.len(), 2);
        assert_eq!(salt.null_count(), 2);
    }

    #[tokio::test]
    async fn a_projected_row_size_array_is_null() {
        let schema = schema(&[("depth", DataType::Int32), ("z_row_size", DataType::Int32)]);

        let batch = read_casts(schema, 0..1).await;

        let row_sizes = batch.column_by_name("z_row_size").unwrap();
        assert_eq!(row_sizes.len(), 2);
        assert_eq!(row_sizes.null_count(), 2);
    }

    #[tokio::test]
    async fn an_empty_schema_counts_the_rows_of_all_casts() {
        let batch = produce(schema(&[]), 0..3).await;

        assert_eq!(batch.num_rows(), 6);
    }
}
