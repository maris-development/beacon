use std::{collections::HashMap, sync::Arc};

use beacon_nd_arrow::{NdArrayD, dataset::Dataset};
use object_store::ObjectStore;

use crate::{
    cache::Cache,
    column::ColumnReader,
    partition::{Partition, column_name_to_path},
};

pub struct PartitionReader<S: ObjectStore + Clone> {
    object_store: S,
    partition: Partition<S>,
    cached_column_readers: Option<Arc<HashMap<String, Arc<ColumnReader<S>>>>>,
}

impl<S: ObjectStore + Clone> PartitionReader<S> {
    pub fn new(object_store: S, partition: Partition<S>) -> Self {
        Self {
            object_store,
            partition,
            cached_column_readers: None,
        }
    }

    pub async fn dataset(&self, dataset_index: usize) -> anyhow::Result<Dataset> {
        unimplemented!()
    }
}

pub(crate) fn read_dataset_columns_by_index<S: ObjectStore + Clone>(
    readers: &[Arc<ColumnReader<S>>],
    dataset_index: u32,
) -> anyhow::Result<Vec<Option<Arc<dyn NdArrayD>>>> {
    readers
        .iter()
        .map(|reader| reader.read_column_array(dataset_index).transpose())
        .collect::<Result<Vec<_>, _>>()
}
