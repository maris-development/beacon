//! One partition's opener: a collection in, nd batches out.
//!
//! The opener reads through the [`AtlasReaderPool`]. The first partition to
//! reach a collection opens it and queues its datasets, and every partition
//! then streams the datasets it pops. How a column resolves, and how a chunk
//! comes out under the scan's fields, lives in [`view`](super::view).

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::{
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::Result,
    physical_plan::PhysicalExpr,
};
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store::ObjectStore;

use crate::{
    datafusion::{
        error::external,
        metrics::AtlasScanMetrics,
        pool::{AtlasReaderPool, PoolOpen},
    },
    store::AtlasReaderCache,
};

/// One partition's opener: a collection in, its batches out.
///
/// Every field is a handle or a clone, so the opener itself is cloned into the
/// stream it returns and outlives the call that made it.
#[derive(Clone)]
pub struct AtlasOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub cache: AtlasReaderCache,
    /// The scan's output schema, nd-encoded. Its field *names* are the columns
    /// to keep, and the encoding leaves names alone.
    pub projected_schema: SchemaRef,
    /// The same schema with the encoding unwrapped, which is what a predicate
    /// and the pruning engine are written against.
    pub logical_schema: SchemaRef,
    pub read_dimensions: Option<Vec<String>>,
    pub predicate: Option<Arc<dyn PhysicalExpr>>,
    pub scan_metrics: AtlasScanMetrics,
    /// The scan's pools, one per collection, shared by every partition.
    pub reader_pool: Arc<AtlasReaderPool>,
}

impl FileOpener for AtlasOpener {
    /// One collection in, one encoded batch per stored chunk of every dataset
    /// worth reading out.
    ///
    /// The collection is opened through the reader pool. The first partition
    /// to reach it opens it, prunes its datasets in one pass over the footer's
    /// statistics, and queues the survivors. A dataset the deletion mask hides
    /// is not queued, and neither is one the predicate rules out. Every
    /// partition then streams the datasets it pops off that queue, so the
    /// partitions that share a collection share its work.
    fn open(&self, file: PartitionedFile) -> Result<FileOpenFuture> {
        let store = self.object_store.clone();
        let cache = self.cache.clone();
        let projected_schema = self.projected_schema.clone();
        let logical_schema = self.logical_schema.clone();
        let predicate = self.predicate.clone();
        let scan_metrics = self.scan_metrics.clone();
        let pool = Arc::clone(&self.reader_pool);

        let fut = async move {
            let open = PoolOpen {
                cache: Some(&cache),
                logical_schema,
                projected_schema,
                predicate,
                scan_metrics,
            };
            let stream = pool
                .open(store, file.object_meta, open)
                .await
                .map_err(external)?;
            Ok(stream.map_err(external).boxed())
        };

        Ok(fut.boxed())
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, ArrayRef, RecordBatch};
    use beacon_datafusion_ext::nd::{decode_nd_record_batch, encoded_schema};
    use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column as ColumnExpr, Literal};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;
    use std::path::Path;

    use super::*;
    use crate::{compat, test_support};

    fn column<'a>(batch: &'a RecordBatch, name: &str) -> &'a ArrayRef {
        batch
            .column_by_name(name)
            .unwrap_or_else(|| panic!("no column {name}"))
    }

    // ── the opener ──────────────────────────────────────────────────────

    /// An opener over a fixture, built the way `AtlasSource` builds one.
    async fn opener(dir: &Path) -> (AtlasOpener, PartitionedFile) {
        let atlas = test_support::open(dir).await;
        let logical_schema = Arc::new(
            compat::collection_arrow_schema(
                &atlas.footer().collection_schema(),
                &ArrowTypeWidening::default_extension(),
            )
            .unwrap(),
        );
        let projected_schema = Arc::new(encoded_schema(&logical_schema));
        let (store, marker) = test_support::store_and_marker(dir);
        let metrics = ExecutionPlanMetricsSet::new();
        let opener = AtlasOpener {
            object_store: store,
            cache: AtlasReaderCache::new(4),
            projected_schema,
            logical_schema,
            read_dimensions: None,
            predicate: None,
            scan_metrics: AtlasScanMetrics::new(&metrics, 0),
            reader_pool: Arc::new(AtlasReaderPool::new()),
        };
        (opener, PartitionedFile::from(marker))
    }

    async fn stream(opener: &AtlasOpener, file: PartitionedFile) -> Vec<RecordBatch> {
        opener
            .open(file)
            .unwrap()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    }

    /// One encoded batch per dataset, in write order, each on the scan's own
    /// schema.
    #[tokio::test]
    async fn the_opener_streams_one_encoded_batch_per_dataset() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        let (opener, file) = opener(tmp.path()).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 2);
        for batch in &batches {
            assert_eq!(
                batch.schema(),
                opener.projected_schema,
                "the scan's schema, marks and all"
            );
        }
        let rows: Vec<usize> = batches
            .iter()
            .map(|batch| decode_nd_record_batch(batch).unwrap().num_rows())
            .collect();
        assert_eq!(rows, vec![4, 3], "winter, then summer");
        assert_eq!(opener.scan_metrics.datasets_scanned.value(), 2);
    }

    /// The deletion mask hides a dataset from the scan, though not from the
    /// schema.
    #[tokio::test]
    async fn a_deleted_dataset_is_not_streamed() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::two_datasets(tmp.path()).await;
        test_support::open(tmp.path())
            .await
            .delete_dataset("winter")
            .await
            .unwrap();
        let (opener, file) = opener(tmp.path()).await;

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 1);
        let summer = decode_nd_record_batch(&batches[0])
            .unwrap()
            .materialize()
            .unwrap();
        assert_eq!(summer.num_rows(), 3);
        assert_eq!(
            column(&summer, "cycle").null_count(),
            3,
            "winter's column, summer's nulls"
        );
    }

    /// A predicate the statistics can judge skips the datasets it rules out
    /// before any of them is read.
    #[tokio::test]
    async fn a_predicate_prunes_datasets_before_the_read() {
        let tmp = tempfile::tempdir().unwrap();
        test_support::ranged(tmp.path(), 10).await;
        let (mut opener, file) = opener(tmp.path()).await;
        opener.predicate = Some(Arc::new(BinaryExpr::new(
            Arc::new(ColumnExpr::new("temperature", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Float32(Some(45.0)))),
        )));

        let batches = stream(&opener, file).await;

        assert_eq!(batches.len(), 5, "d5 to d9 reach past 45");
        assert_eq!(opener.scan_metrics.datasets_pruned.value(), 5);
        assert_eq!(opener.scan_metrics.datasets_scanned.value(), 5);
    }
}
