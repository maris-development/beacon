use arrow::datatypes::{FieldRef, SchemaRef};
use atlas::Atlas;
use beacon_nd_array::{
    NdArrayD,
    dataset::{default::DefaultDataset, source::DatasetSource},
};
use datafusion::physical_plan::PhysicalExpr;
use indexmap::IndexMap;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;

use crate::{
    compat,
    datafusion::{
        metrics::AtlasScanMetrics,
        opener::{AtlasColumnView, column_views},
        pruning::prune_datasets,
    },
    store::{AtlasReaderCache, get_or_open_atlas},
};

#[derive(Clone)]
pub struct AtlasView {
    atlas: Arc<Atlas>,
    table_schema: SchemaRef,
    column_views: Arc<IndexMap<FieldRef, Option<AtlasColumnView>>>,
}

impl AtlasView {
    /// Open the collection at `object_meta`, through `cache` when given, and
    /// resolve every column of `table_schema` against it.
    pub async fn new(
        cache: Option<&AtlasReaderCache>,
        store: Arc<dyn ObjectStore>,
        object_meta: ObjectMeta,
        table_schema: SchemaRef,
    ) -> anyhow::Result<Self> {
        let atlas = get_or_open_atlas(cache, store, &object_meta).await?;
        let views = column_views(&atlas, &table_schema).await?;

        Ok(Self {
            atlas,
            table_schema,
            column_views: Arc::new(views),
        })
    }

    /// The table schema the view resolves columns for, in field order.
    pub fn table_schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    pub async fn list_datasets(
        &self,
        pruning_predicate: Option<Arc<dyn PhysicalExpr>>,
        scan_metrics: AtlasScanMetrics,
    ) -> anyhow::Result<Vec<String>> {
        let mut datasets = self.atlas.list_datasets();

        if let Some(predicate) = &pruning_predicate {
            let prune_timer = scan_metrics.prune_time.timer();
            let listed = datasets.len();
            datasets =
                prune_datasets(&self.column_views, datasets, predicate, &self.table_schema).await;
            scan_metrics.datasets_pruned.add(listed - datasets.len());
            drop(prune_timer);
        }

        Ok(datasets)
    }

    pub async fn dataset(
        &self,
        dataset_name: &str,
    ) -> anyhow::Result<Option<Arc<dyn DatasetSource>>> {
        let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();
        for (field, view) in &*self.column_views {
            let array = match view {
                None => None,
                Some(AtlasColumnView::Array { segment }) => match segment.array(dataset_name) {
                    Some(info) => Some(compat::array_to_nd_array(
                        Arc::clone(segment),
                        dataset_name,
                        &info.dtype,
                    )?),
                    None => None,
                },
                Some(AtlasColumnView::GlobalAttribute { map })
                | Some(AtlasColumnView::VariableAttribute { map, .. }) => {
                    // A list has no rank-0 form, and the schema holds no list
                    // column. A dataset that stores a list under a scalar
                    // column's key reads as null.
                    map.get(dataset_name)
                        .and_then(|attr| compat::attribute_to_nd_array(attr).ok())
                }
            };
            if let Some(array) = array {
                arrays.insert(field.name().clone(), array);
            }
        }
        let dataset = DefaultDataset::new(dataset_name.to_string(), arrays)?;
        Ok(Some(Arc::new(dataset)))
    }
}
