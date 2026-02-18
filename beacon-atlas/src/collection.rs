use std::sync::Arc;

use datafusion::parquet::column;
use futures::Stream;

use crate::{
    array::{Array, store::ChunkStore},
    column::{Column, ColumnWriter},
};

pub struct CollectionReader<S: object_store::ObjectStore + Clone> {
    object_store: S,
    collection_directory: object_store::path::Path,
}

pub struct CollectionWriter<S: object_store::ObjectStore + Clone> {
    object_store: S,
    collection_directory: object_store::path::Path,

    column_writers: indexmap::IndexMap<String, ColumnWriter<S>>,
}

impl<S: object_store::ObjectStore + Clone> CollectionWriter<S> {
    pub fn new(object_store: S, collection_directory: object_store::path::Path) -> Self {
        let mut column_writers = indexmap::IndexMap::new();

        // Add by default the "__entry_key" column for storing the entry keys of the collection.
        column_writers.insert(
            "__entry_key".to_string(),
            ColumnWriter::new(
                object_store.clone(),
                collection_directory.child("columns").child("__entry_key"),
                arrow::datatypes::DataType::Utf8,
                16 * 1024,
            ),
        );

        Self {
            object_store,
            collection_directory,
            column_writers,
        }
    }

    pub async fn write_dataset<C: Stream<Item = Column>>(
        &self,
        name: &str,
        columns: C,
    ) -> anyhow::Result<()> {
        todo!()
    }
}
