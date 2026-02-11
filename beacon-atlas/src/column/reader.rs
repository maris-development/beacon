use std::sync::Arc;

use object_store::ObjectStore;

// use crate::{
//     array::{io_cache::IoCache, reader::ArrayReader},
//     column::Column,
// };

// #[async_trait::async_trait]
// pub trait ColumnReader {
//     async fn read_column(&self, dataset_index: u32) -> anyhow::Result<Option<Column>>;
// }

// pub struct ColumnArrayReader<S: ObjectStore + Clone> {
//     object_store: S,
//     column_directory: object_store::path::Path,

//     // Array Reader
//     reader: Arc<ArrayReader<S>>,
// }

// impl<S: ObjectStore + Clone> ColumnArrayReader<S> {
//     pub async fn new(
//         object_store: S,
//         column_directory: object_store::path::Path,
//         io_cache: Arc<IoCache>,
//     ) -> anyhow::Result<Self> {
//         let array_path = column_directory.child("array.arrow");
//         let layout_path = column_directory.child("layout.arrow");

//         let reader = Arc::new(
//             ArrayReader::new_with_cache(object_store.clone(), layout_path, array_path, io_cache)
//                 .await?,
//         );

//         Ok(Self {
//             object_store,
//             column_directory,
//             reader,
//         })
//     }
// }

// #[async_trait::async_trait]
// impl<S: ObjectStore + Clone> ColumnReader for ColumnArrayReader<S> {
//     async fn read_column(&self, dataset_index: u32) -> anyhow::Result<Option<Column>> {
//         let array_opt = self.reader.read_dataset_array(dataset_index);
//         Ok(array_opt.map(Column::Array))
//     }
// }
