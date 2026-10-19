//! A [`FileOpener`] wrapper that casts each batch of a file to one target schema.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::Result,
    physical_expr_adapter::{BatchAdapter, BatchAdapterFactory, PhysicalExprAdapterFactory},
};
use futures::{FutureExt, StreamExt};

/// A [`FileOpener`] that adapts each batch of `opener` to `target_schema`.
///
/// A batch that has the target schema passes through unchanged. A batch with a different
/// schema goes through a [`BatchAdapter`] from the physical expression adapter factory.
pub struct BatchSchemaAdapter {
    opener: Arc<dyn FileOpener>,
    // `Arc` lets each open future share the factory, because the factory is not `Clone`.
    schema_adapter_factory: Arc<BatchAdapterFactory>,
    target_schema: SchemaRef,
}

impl BatchSchemaAdapter {
    /// Wrap `opener`, so that each batch it gives has `target_schema`.
    ///
    /// # Arguments
    ///
    /// * `opener` - The opener whose batches to adapt
    /// * `physical_expr_adapter_factory` - Makes the expressions that cast or fill columns
    /// * `target_schema` - The schema of each output batch
    pub fn new(
        opener: Arc<dyn FileOpener>,
        physical_expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
        target_schema: SchemaRef,
    ) -> Self {
        let schema_adapter_factory = BatchAdapterFactory::new(Arc::clone(&target_schema))
            .with_adapter_factory(physical_expr_adapter_factory);

        Self {
            opener,
            schema_adapter_factory: Arc::new(schema_adapter_factory),
            target_schema,
        }
    }
}

impl FileOpener for BatchSchemaAdapter {
    /// Open `partitioned_file` with the inner opener and adapt each batch to the target schema.
    ///
    /// # Errors
    ///
    /// Returns the errors of the inner opener, and an error for a batch that does not adapt.
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let inner = self.opener.open(partitioned_file)?;
        let factory = Arc::clone(&self.schema_adapter_factory);
        let target_schema = Arc::clone(&self.target_schema);
        Ok(async move {
            // The batches of one file almost always share a schema, so keep the last adapter.
            let mut cached: Option<(SchemaRef, BatchAdapter)> = None;
            let stream = inner.await?.map(move |batch| {
                let batch = batch?;
                let batch_schema = batch.schema();
                if batch_schema == target_schema {
                    return Ok(batch);
                }
                let adapter = match cached.take() {
                    Some((schema, adapter)) if schema == batch_schema => adapter,
                    _ => factory.make_adapter(&batch_schema)?,
                };
                let adapted = adapter.adapt_batch(&batch);
                cached = Some((batch_schema, adapter));
                adapted
            });
            Ok(stream.boxed())
        }
        .boxed())
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, Int32Array, Int64Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory;
    use futures::TryStreamExt;

    use super::*;

    /// Gives each batch of `batches` as one file.
    struct MockOpener {
        batches: Vec<RecordBatch>,
    }

    impl FileOpener for MockOpener {
        fn open(&self, _partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let batches = self.batches.clone();
            Ok(
                async move { Ok(futures::stream::iter(batches.into_iter().map(Ok)).boxed()) }
                    .boxed(),
            )
        }
    }

    fn int32_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))])
            .expect("a valid batch")
    }

    fn target_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("extra", DataType::Utf8, true),
        ]))
    }

    /// The batches that a `BatchSchemaAdapter` to `target` around `batches` gives.
    async fn adapt(batches: Vec<RecordBatch>, target: SchemaRef) -> Vec<RecordBatch> {
        let opener = BatchSchemaAdapter::new(
            Arc::new(MockOpener { batches }),
            Arc::new(DefaultPhysicalExprAdapterFactory),
            target,
        );
        opener
            .open(PartitionedFile::new("file.nc", 10))
            .expect("the mock opens")
            .await
            .expect("the mock future resolves")
            .try_collect()
            .await
            .expect("each batch adapts")
    }

    #[tokio::test]
    async fn a_batch_with_the_target_schema_passes_through() {
        let batch = int32_batch(vec![1, 2]);

        let adapted = adapt(vec![batch.clone()], batch.schema()).await;

        assert_eq!(adapted, vec![batch]);
    }

    #[tokio::test]
    async fn each_batch_gets_cast_and_missing_columns_get_nulls() {
        let target = target_schema();

        let adapted = adapt(
            vec![int32_batch(vec![1, 2]), int32_batch(vec![3])],
            Arc::clone(&target),
        )
        .await;

        assert_eq!(adapted.len(), 2);
        assert!(adapted.iter().all(|batch| batch.schema() == target));
        let values = adapted[1]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an Int64 column");
        assert_eq!(values.values(), &[3]);
        assert_eq!(adapted[0].column(1).null_count(), 2);
    }
}
