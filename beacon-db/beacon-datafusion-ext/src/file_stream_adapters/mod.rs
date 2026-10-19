//! [`FileOpener`] wrappers that change the batch stream of a file scan.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::{
        listing::PartitionedFile,
        physical_plan::{FileOpenFuture, FileOpener},
    },
    error::Result,
    physical_expr_adapter::PhysicalExprAdapterFactory,
};

use batch_schema_adapter::BatchSchemaAdapter;
use skip_error_adapter::open_skip_errors;

pub mod batch_schema_adapter;
pub mod skip_error_adapter;

/// What a [`FileStreamAdapter`] does with an error of a file or a batch.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OnError {
    /// Give the error to the caller, which fails the scan.
    #[default]
    Fail,
    /// Log the error and drop the file or the batch, so the scan goes on.
    Skip,
}

/// A [`FileOpener`] that combines the adapters of this module around one opener.
///
/// The schema adapter wraps the opener first. The error mode applies last, so `OnError::Skip`
/// also drops a batch that fails to adapt. The call order of the builder methods has no effect.
pub struct FileStreamAdapter {
    file_opener: Arc<dyn FileOpener>,
    on_error: OnError,
}

impl FileStreamAdapter {
    /// Wrap `file_opener` with no adapters and `OnError::Fail`.
    ///
    /// # Arguments
    ///
    /// * `file_opener` - The opener that reads the files
    pub fn new(file_opener: Arc<dyn FileOpener>) -> Self {
        Self {
            file_opener,
            on_error: OnError::default(),
        }
    }

    /// Set what to do with an error of a file or a batch.
    ///
    /// # Arguments
    ///
    /// * `on_error` - The error mode
    pub fn with_on_error(mut self, on_error: OnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Adapt each batch to `target_schema` with a [`BatchSchemaAdapter`].
    ///
    /// # Arguments
    ///
    /// * `expr_adapter_factory` - Makes the expressions that cast or fill columns
    /// * `target_schema` - The schema of each output batch
    pub fn with_schema_adapter(
        mut self,
        expr_adapter_factory: Arc<dyn PhysicalExprAdapterFactory>,
        target_schema: SchemaRef,
    ) -> Self {
        // Wrap now, so each `open` call reuses one adapter factory.
        self.file_opener = Arc::new(BatchSchemaAdapter::new(
            self.file_opener,
            expr_adapter_factory,
            target_schema,
        ));
        self
    }
}

impl FileOpener for FileStreamAdapter {
    /// Open `partitioned_file` with the wrapped opener and the error mode.
    ///
    /// # Errors
    ///
    /// With `OnError::Fail`, returns each error of the wrapped opener and of the schema adapter.
    /// With `OnError::Skip`, returns no error.
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        match self.on_error {
            OnError::Fail => self.file_opener.open(partitioned_file),
            OnError::Skip => open_skip_errors(self.file_opener.as_ref(), partitioned_file),
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Int32Array, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::{
        error::DataFusionError, physical_expr_adapter::DefaultPhysicalExprAdapterFactory,
    };
    use futures::{FutureExt, StreamExt, TryStreamExt};

    use super::*;

    /// Gives an `Int32` batch `[1]`, a `Utf8` batch that cannot cast to `Int64`, and an
    /// error batch.
    struct MockOpener;

    impl FileOpener for MockOpener {
        fn open(&self, _partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
            let int_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
            let text_schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
            let batches = vec![
                RecordBatch::try_new(int_schema, vec![Arc::new(Int32Array::from(vec![1]))])
                    .map_err(DataFusionError::from),
                RecordBatch::try_new(text_schema, vec![Arc::new(StringArray::from(vec!["x"]))])
                    .map_err(DataFusionError::from),
                Err(DataFusionError::Execution("mock failure".to_string())),
            ];
            Ok(async move { Ok(futures::stream::iter(batches).boxed()) }.boxed())
        }
    }

    fn adapter(on_error: OnError) -> FileStreamAdapter {
        let target = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        // `on_error` comes first, to show that the call order has no effect.
        FileStreamAdapter::new(Arc::new(MockOpener))
            .with_on_error(on_error)
            .with_schema_adapter(Arc::new(DefaultPhysicalExprAdapterFactory), target)
    }

    async fn collect(on_error: OnError) -> Result<Vec<RecordBatch>> {
        adapter(on_error)
            .open(PartitionedFile::new("file.nc", 10))?
            .await?
            .try_collect()
            .await
    }

    #[tokio::test]
    async fn skip_keeps_only_the_batches_that_adapt() {
        let batches = collect(OnError::Skip).await.expect("skip mode never fails");

        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("an Int64 column");
        assert_eq!(values.values(), &[1]);
    }

    #[tokio::test]
    async fn fail_gives_the_first_error() {
        assert!(collect(OnError::Fail).await.is_err());
    }
}
