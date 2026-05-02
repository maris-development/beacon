use std::collections::HashSet;

use datafusion::{
    arrow::{
        array::{ArrayRef, BooleanArray},
        datatypes::SchemaRef,
    },
    common::pruning::PruningStatistics,
    object_store::ObjectMeta,
    prelude::Column,
    scalar::ScalarValue,
};

pub struct StatisticsTable {
    pub objects: Vec<ObjectMeta>,
    pub columns: indexmap::IndexMap<String, ArrayRef>,
    pub schema: SchemaRef,
}

impl PruningStatistics for StatisticsTable {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        todo!()
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        todo!()
    }

    fn num_containers(&self) -> usize {
        todo!()
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        todo!()
    }

    fn row_counts(&self, column: &Column) -> Option<ArrayRef> {
        todo!()
    }

    fn contained(&self, column: &Column, values: &HashSet<ScalarValue>) -> Option<BooleanArray> {
        None
    }
}
