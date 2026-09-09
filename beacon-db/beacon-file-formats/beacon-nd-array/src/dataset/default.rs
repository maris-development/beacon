use std::sync::Arc;

use beacon_datafusion_ext::nd::NdRecordBatch;
use indexmap::IndexMap;

use crate::{
    NdArrayD,
    array::subset::ArraySubset,
    arrow::{
        batch::{build_dataset_schema, generate_chunk_subsets},
        nd_provider::read_nd_chunk,
    },
    dataset::source::DatasetSource,
};

#[derive(Debug, Clone)]
pub struct DefaultDataset {
    pub name: String,
    pub dimensions: Vec<String>,
    pub shape: Vec<usize>,
    pub chunk_shape: Vec<usize>,
    pub arrays: IndexMap<String, Arc<dyn NdArrayD>>,
}

impl DefaultDataset {
    pub fn new(name: String, arrays: IndexMap<String, Arc<dyn NdArrayD>>) -> anyhow::Result<Self> {
        let mut dimensions = Vec::new();
        let mut shape = Vec::new();
        let mut chunk_shape = Vec::new();
        for (_, array) in &arrays {
            let array_dims = array.dimensions();
            let array_shape = array.shape();
            let array_chunk_shape = array.chunk_shape();

            if array_dims.len() > dimensions.len() {
                // Array dims should contain all the dimensions of the dataset, in order.
                for (i, dim) in dimensions.iter().enumerate() {
                    let array_dim = array_dims.get(i);
                    if array_dim != Some(dim) {
                        return Err(anyhow::anyhow!(
                            "Array dimensions {:?} has incompatible dimension {:?} at index {}",
                            array_dims,
                            dim,
                            i
                        ));
                    }
                }
                dimensions = array_dims.clone();
                shape = array_shape.clone();
                chunk_shape = array_chunk_shape.clone();
            } else {
                // Array dims should be a prefix of the dataset dimensions.
                for (i, array_dim) in array_dims.iter().enumerate() {
                    let dim = dimensions.get(i);
                    if dim != Some(array_dim) {
                        return Err(anyhow::anyhow!(
                            "Array dimensions {:?} has incompatible dimension {:?} at index {}",
                            array_dims,
                            array_dim,
                            i
                        ));
                    }
                }
            }
        }

        Ok(Self {
            name,
            dimensions,
            shape,
            chunk_shape,
            arrays,
        })
    }
}

#[async_trait::async_trait]
impl DatasetSource for DefaultDataset {
    /// Every chunk of the dataset grid, in C order, as an [`ArraySubset`].
    ///
    /// The cut comes from [`generate_chunk_subsets`], so a boundary chunk
    /// shrinks to fit. A scalar dataset has one empty chunk. A dataset with an
    /// empty axis has none.
    fn chunks(&self) -> Vec<Arc<dyn std::any::Any + Send + Sync>> {
        generate_chunk_subsets(&self.shape, &self.chunk_shape)
            .into_iter()
            .map(|subset| Arc::new(subset) as Arc<dyn std::any::Any + Send + Sync>)
            .collect()
    }

    /// The cells of the chunk's grid. A scalar dataset has one.
    fn chunk_rows(&self, chunk: &Arc<dyn std::any::Any + Send + Sync>) -> Option<usize> {
        chunk
            .downcast_ref::<ArraySubset>()
            .map(|subset| subset.shape.iter().product())
    }

    /// Read one chunk into an un-broadcast [`NdRecordBatch`].
    ///
    /// `chunk` is one of [`DatasetSource::chunks`]. Each array is sliced on
    /// its own axes, so an array of lower rank reads only the axes it has. The
    /// broadcast onto the chunk grid happens above the scan.
    async fn poll_next(
        &self,
        chunk: Arc<dyn std::any::Any + Send + Sync>,
    ) -> anyhow::Result<Option<NdRecordBatch>> {
        let subset = chunk
            .downcast_ref::<ArraySubset>()
            .ok_or_else(|| anyhow::anyhow!("chunk is not an ArraySubset"))?;

        let schema = build_dataset_schema(&self.arrays);
        let batch = read_nd_chunk(&self.arrays, &self.dimensions, schema, subset.clone()).await?;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        NdArray,
        array::{backend::ArrayBackend, backend::mem::InMemoryArrayBackend, subset::ArraySubset},
    };
    use ndarray::ArrayD;

    /// An in-memory array that reports the chunk shape a test hands it.
    #[derive(Debug)]
    struct Chunked {
        inner: InMemoryArrayBackend<f64>,
        chunk_shape: Vec<usize>,
    }

    #[async_trait::async_trait]
    impl ArrayBackend<f64> for Chunked {
        fn len(&self) -> usize {
            self.inner.len()
        }
        fn shape(&self) -> Vec<usize> {
            self.inner.shape()
        }
        fn chunk_shape(&self) -> Vec<usize> {
            self.chunk_shape.clone()
        }
        fn dimensions(&self) -> Vec<String> {
            self.inner.dimensions()
        }
        async fn read_subset(&self, subset: ArraySubset) -> anyhow::Result<ArrayD<f64>> {
            self.inner.read_subset(subset).await
        }
    }

    fn arr(dims: &[&str], shape: &[usize], chunk: &[usize]) -> Arc<dyn NdArrayD> {
        let len = shape.iter().product();
        let values = ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; len]).unwrap();
        let inner = InMemoryArrayBackend::new(
            values,
            shape.to_vec(),
            dims.iter().map(|d| d.to_string()).collect(),
            None,
        );
        let backend = Chunked {
            inner,
            chunk_shape: chunk.to_vec(),
        };
        Arc::new(NdArray::new_with_backend(backend).unwrap())
    }

    fn dataset(arrays: Vec<(&str, Arc<dyn NdArrayD>)>) -> anyhow::Result<DefaultDataset> {
        DefaultDataset::new(
            "test".to_string(),
            arrays
                .into_iter()
                .map(|(name, array)| (name.to_string(), array))
                .collect(),
        )
    }

    fn names(dims: &[&str]) -> Vec<String> {
        dims.iter().map(|d| d.to_string()).collect()
    }

    #[test]
    fn the_array_with_the_most_dimensions_decides() {
        let ds = dataset(vec![
            ("time", arr(&["time"], &[4], &[4])),
            ("data", arr(&["time", "lat", "lon"], &[4, 6, 8], &[1, 3, 4])),
            ("time_lat", arr(&["time", "lat"], &[4, 6], &[4, 6])),
        ])
        .unwrap();
        assert_eq!(ds.dimensions, names(&["time", "lat", "lon"]));
        assert_eq!(ds.shape, vec![4, 6, 8]);
        assert_eq!(ds.chunk_shape, vec![1, 3, 4]);
    }

    #[test]
    fn the_first_array_wins_a_rank_tie() {
        let ds = dataset(vec![
            ("first", arr(&["x", "y"], &[4, 6], &[2, 3])),
            ("second", arr(&["x", "y"], &[4, 6], &[4, 6])),
        ])
        .unwrap();
        assert_eq!(ds.chunk_shape, vec![2, 3]);
    }

    #[test]
    fn prefix_arrays_and_scalars_pass() {
        let ds = dataset(vec![
            ("data", arr(&["x", "y"], &[4, 6], &[2, 3])),
            ("scalar", arr(&[], &[], &[])),
            ("x_only", arr(&["x"], &[4], &[4])),
        ])
        .unwrap();
        assert_eq!(ds.dimensions, names(&["x", "y"]));
        assert_eq!(ds.arrays.len(), 3);
    }

    #[test]
    fn a_dimension_outside_the_prefix_is_an_error() {
        let err = dataset(vec![
            ("data", arr(&["x", "y"], &[4, 6], &[2, 3])),
            ("bounds", arr(&["y", "nv"], &[6, 2], &[6, 2])),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("\"nv\""), "{err}");
    }

    #[test]
    fn an_anchor_that_does_not_extend_the_prefix_is_an_error() {
        let err = dataset(vec![
            ("x_only", arr(&["x"], &[4], &[4])),
            ("data", arr(&["y", "x"], &[6, 4], &[6, 4])),
        ])
        .unwrap_err();
        assert!(err.to_string().contains("\"x\""), "{err}");
    }

    #[test]
    fn chunks_cover_the_grid_in_c_order() {
        let ds = dataset(vec![("data", arr(&["x", "y"], &[5, 4], &[2, 4]))]).unwrap();
        let chunks: Vec<ArraySubset> = ds
            .chunks()
            .into_iter()
            .map(|chunk| chunk.downcast_ref::<ArraySubset>().unwrap().clone())
            .collect();
        let starts: Vec<Vec<usize>> = chunks.iter().map(|c| c.start.clone()).collect();
        let shapes: Vec<Vec<usize>> = chunks.iter().map(|c| c.shape.clone()).collect();
        assert_eq!(starts, vec![vec![0, 0], vec![2, 0], vec![4, 0]]);
        assert_eq!(shapes, vec![vec![2, 4], vec![2, 4], vec![1, 4]]);
    }

    #[test]
    fn a_scalar_dataset_has_one_empty_chunk() {
        let ds = dataset(vec![("scalar", arr(&[], &[], &[]))]).unwrap();
        let chunks = ds.chunks();
        assert_eq!(chunks.len(), 1);
        let subset = chunks[0].downcast_ref::<ArraySubset>().unwrap();
        assert!(subset.start.is_empty());
        assert!(subset.shape.is_empty());
    }

    #[test]
    fn an_empty_axis_has_no_chunks() {
        let ds = dataset(vec![("data", arr(&["x", "y"], &[0, 4], &[2, 4]))]).unwrap();
        assert!(ds.chunks().is_empty());
    }

    /// A dataset with a coordinate per axis and one 2-D variable, cut on the
    /// variable's chunk shape.
    fn gridded(chunk: &[usize]) -> DefaultDataset {
        let time = NdArray::<i64>::try_new_from_vec_in_mem(
            (0..4).map(|v| v * 100).collect(),
            vec![4],
            names(&["time"]),
            None,
        )
        .unwrap();
        let lat = NdArray::<f64>::try_new_from_vec_in_mem(
            vec![-30.0, 0.0, 30.0],
            vec![3],
            names(&["lat"]),
            None,
        )
        .unwrap();
        let sst = {
            let values =
                ArrayD::from_shape_vec(vec![4, 3], (0..12).map(|v| v as f64).collect()).unwrap();
            let inner =
                InMemoryArrayBackend::new(values, vec![4, 3], names(&["time", "lat"]), None);
            NdArray::new_with_backend(Chunked {
                inner,
                chunk_shape: chunk.to_vec(),
            })
            .unwrap()
        };
        // Built directly: `new` applies the prefix rule, and `lat(lat)` is not
        // a prefix of `sst(time, lat)`. The read itself maps every array onto
        // the grid by dimension name.
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        arrays.insert("time".to_string(), Arc::new(time));
        arrays.insert("lat".to_string(), Arc::new(lat));
        arrays.insert("sst".to_string(), Arc::new(sst));
        DefaultDataset {
            name: "gridded".to_string(),
            dimensions: names(&["time", "lat"]),
            shape: vec![4, 3],
            chunk_shape: chunk.to_vec(),
            arrays,
        }
    }

    /// A chunk states its own row count, so a count reads nothing.
    #[test]
    fn a_chunk_states_its_rows() {
        let ds = gridded(&[2, 2]);
        let rows: Vec<usize> = ds
            .chunks()
            .iter()
            .map(|chunk| ds.chunk_rows(chunk).unwrap())
            .collect();
        assert_eq!(rows, vec![4, 2, 4, 2], "a [4, 3] grid chunked [2, 2]");
    }

    /// Poll every chunk, broadcast each, and stitch the rows back together.
    async fn read_all(ds: &DefaultDataset) -> arrow::record_batch::RecordBatch {
        let mut batches = Vec::new();
        for chunk in ds.chunks() {
            let nd = ds.poll_next(chunk).await.unwrap().unwrap();
            batches.push(nd.materialize().unwrap());
        }
        let schema = build_dataset_schema(&ds.arrays);
        arrow::compute::concat_batches(&schema, &batches).unwrap()
    }

    /// The chunked read equals the whole-array read, row for row.
    #[tokio::test]
    async fn polling_every_chunk_reads_the_whole_grid() {
        use arrow::array::{Float64Array, Int64Array};

        let whole = read_all(&gridded(&[4, 3])).await;
        assert_eq!(whole.num_rows(), 12);

        let sst = whole.column_by_name("sst").unwrap();
        let sst = sst.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(
            sst.values(),
            &(0..12).map(|v| v as f64).collect::<Vec<_>>()[..]
        );

        let time = whole.column_by_name("time").unwrap();
        let time = time.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(
            time.values(),
            &[0, 0, 0, 100, 100, 100, 200, 200, 200, 300, 300, 300]
        );

        // A cut on the outer axis alone keeps the row order.
        assert_eq!(read_all(&gridded(&[2, 3])).await, whole);

        // A cut on an inner axis reorders the rows, and loses none.
        for chunk in [[3, 2], [1, 1], [2, 2]] {
            let ds = gridded(&chunk);
            assert!(ds.chunks().len() > 1, "chunk {chunk:?} must cut the grid");
            let mut chunked = rows(&read_all(&ds).await);
            chunked.sort();
            let mut expected = rows(&whole);
            expected.sort();
            assert_eq!(chunked, expected, "chunk {chunk:?}");
        }
    }

    /// The `(time, lat, sst)` of every row, as integers so they sort.
    fn rows(batch: &arrow::record_batch::RecordBatch) -> Vec<(i64, i64, i64)> {
        use arrow::array::{Float64Array, Int64Array};
        let time = batch.column_by_name("time").unwrap();
        let time = time.as_any().downcast_ref::<Int64Array>().unwrap();
        let lat = batch.column_by_name("lat").unwrap();
        let lat = lat.as_any().downcast_ref::<Float64Array>().unwrap();
        let sst = batch.column_by_name("sst").unwrap();
        let sst = sst.as_any().downcast_ref::<Float64Array>().unwrap();
        (0..batch.num_rows())
            .map(|i| (time.value(i), lat.value(i) as i64, sst.value(i) as i64))
            .collect()
    }

    /// A chunk leaves un-broadcast: a coordinate keeps its own rank.
    #[tokio::test]
    async fn a_polled_chunk_is_not_broadcast() {
        let ds = gridded(&[2, 3]);
        let chunk = ds.chunks().into_iter().next().unwrap();
        let nd = ds.poll_next(chunk).await.unwrap().unwrap();
        assert_eq!(nd.target().rank(), 2);
        assert_eq!(nd.num_rows(), 6);
        assert_eq!(nd.column(0).dims().rank(), 1, "time stays 1-D");
        assert_eq!(nd.column(2).dims().rank(), 2, "sst is 2-D");
    }

    #[tokio::test]
    async fn a_chunk_of_another_type_is_an_error() {
        let ds = gridded(&[4, 3]);
        let err = ds.poll_next(Arc::new(42usize)).await.unwrap_err();
        assert!(err.to_string().contains("not an ArraySubset"), "{err}");
    }

    #[test]
    fn no_arrays_gives_no_dimensions() {
        let ds = dataset(vec![]).unwrap();
        assert!(ds.dimensions.is_empty());
        assert!(ds.shape.is_empty());
        assert!(ds.chunk_shape.is_empty());
    }
}
