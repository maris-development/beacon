use std::sync::Arc;

use arrow::array::ArrayRef;
use futures::{StreamExt, stream::BoxStream};
use ndarray::IxDyn;
use object_store::ObjectStore;

use crate::{
    array::{
        data_type::{DataType, I8Type, PrimitiveArrayDataType, VLenByteArrayDataType},
        nd::{AsNdArray, NdArray},
    },
    column::ColumnReader,
};

pub struct DatasetView {
    pub id: u32,
    pub name: String,
    pub column_readers: Vec<Arc<ColumnReader<Arc<dyn ObjectStore>>>>,
}

impl DatasetView {
    pub fn new(
        id: u32,
        name: String,
        column_readers: Vec<Arc<ColumnReader<Arc<dyn ObjectStore>>>>,
    ) -> Self {
        Self {
            id,
            name,
            column_readers,
        }
    }

    pub fn as_arrow_batch_stream(
        &self,
    ) -> Option<anyhow::Result<BoxStream<'static, anyhow::Result<arrow::record_batch::RecordBatch>>>>
    {
        let arrays = self
            .column_readers
            .iter()
            .map(|reader| reader.read_column_array(self.id))
            .collect::<Vec<_>>();

        // Check if all arrays are none, which means the dataset is empty or all columns are missing
        if arrays.iter().all(|array| array.is_none()) {
            return None;
        }

        // let mut max_dims = None;
        // let mut max_dims_sizes = None;

        for array in &arrays {
            if let Some(arr) = array {
                // match max_dims.as_mut() {}
            }
        }

        todo!()
    }

    fn broadcast_array_to_shape(
        array: Arc<dyn NdArray>,
        target_shape: &[usize],
        chunk_size: usize,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<ArrayRef>>> {
        // array.data_type()
        let nd_array = array.as_primitive_nd::<I8Type>().unwrap();
        let view = nd_array.broadcast(target_shape);

        if let Some(view) = view {
            match array.data_type() {
                DataType::I8 => {
                    let stream = Self::array_view_primitive_stream::<I8Type>(view, chunk_size);
                    return Ok(stream);
                }
                // Handle other data types similarly...
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported data type {:?} for broadcasting",
                        array.data_type()
                    ));
                }
            }
        } else {
            return Err(anyhow::anyhow!(
                "Failed to broadcast array of shape {:?} to target shape {:?}",
                array.shape(),
                target_shape
            ));
        }
    }

    fn array_view_primitive_stream<'a, T: PrimitiveArrayDataType>(
        view: ndarray::ArrayViewD<'a, T::Native>,
        chunk_size: usize,
    ) -> BoxStream<'a, anyhow::Result<ArrayRef>>
    where
        T::Native: Copy + 'a,
        arrow::array::PrimitiveArray<T::ArrowNativeType>: From<Vec<T::Native>>,
    {
        assert!(chunk_size > 0);

        let len = view.len();
        let index = 0;

        futures::stream::unfold((view, index), move |(view, mut idx)| async move {
            if idx >= len {
                return None;
            }

            let mut chunk = Vec::with_capacity(chunk_size);

            for _ in 0..chunk_size {
                if idx >= len {
                    break;
                }
                chunk.push(view[idx]);
                idx += 1;
            }

            let array = arrow::array::PrimitiveArray::<T::ArrowNativeType>::from(chunk);
            Some((Ok(Arc::new(array) as ArrayRef), (view, idx)))
        })
        .boxed()
    }

    fn array_view_vlen_stream<'a, T: VLenByteArrayDataType>(
        view: ndarray::ArrayViewD<'a, T::View<'a>>,
        chunk_size: usize,
    ) -> BoxStream<'a, anyhow::Result<ArrayRef>>
    where
        T::View<'a>: Clone + 'a,
        arrow::array::GenericByteArray<T::ArrowNativeType>: From<Vec<Option<T::View<'a>>>>,
    {
        assert!(chunk_size > 0);

        let len = view.len();
        let index = 0;

        futures::stream::unfold((view, index), move |(view, mut idx)| async move {
            if idx >= len {
                return None;
            }

            let mut chunk = Vec::with_capacity(chunk_size);

            for _ in 0..chunk_size {
                if idx >= len {
                    break;
                }
                chunk.push(Some(view[idx].clone()));
                idx += 1;
            }

            let array = arrow::array::GenericByteArray::<T::ArrowNativeType>::from(chunk);
            Some((Ok(Arc::new(array) as ArrayRef), (view, idx)))
        })
        .boxed()
    }
}
