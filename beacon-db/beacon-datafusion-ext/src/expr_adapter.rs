//! Maps the batches of one file onto the schema a scan reports.
//!
//! A collection holds files that do not agree. One file types a column `Int32`
//! and another `Float32`. One file holds a column that another lacks. The merge
//! rule of the session settles one schema for the collection, and the table
//! reports it. See [`type_widening`](crate::type_widening) for that rule.
//!
//! Every format adapts a file with the `PhysicalExprAdapterFactory` of its
//! `FileScanConfig`, which [`scan_adapter_factory`] reads. The external table
//! sets the factory of the table strategy there. A config without a factory
//! takes the factory of the strategy the format captured from the session.
//!
//! A format that reads the batches of a file as they are wraps its opener in
//! [`ExprAdapterOpener`]. A format that builds its batches itself builds one
//! [`BatchAdapter`] per file.
//!
//! A scan that skips this step reports a schema it cannot produce. The reader
//! then fails on the first batch, and `LIMIT 0` still succeeds, because it reads
//! no batch.

use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileOpenFuture, FileOpener, FileScanConfig};
use datafusion::error::Result;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
use futures::StreamExt;

use crate::type_widening::ArrowTypeWideningStrategy;

/// The adapter factory of `config`, or the factory of `fallback` when the
/// config holds none.
///
/// `fallback` is the strategy the format captured from the session. A plain
/// DataFusion `ListingTable` sets no factory, and still reads with that rule.
pub fn scan_adapter_factory(
    config: &FileScanConfig,
    fallback: &dyn ArrowTypeWideningStrategy,
) -> Arc<dyn PhysicalExprAdapterFactory> {
    config
        .expr_adapter_factory
        .clone()
        .unwrap_or_else(|| fallback.physical_expr_adapter_factory())
}

/// Maps the batches of one file onto one target schema with the expression
/// adapter of a scan.
///
/// Each target column is a `Column` expression that the adapter rewrites for
/// the file schema. DataFusion's `BatchAdapterFactory` does the same, but it
/// then simplifies the rewritten expressions against the target schema. Those
/// expressions address the file schema, so a file with more columns than the
/// target fails a debug assertion there. This adapter does not simplify.
#[derive(Debug)]
pub struct BatchAdapter {
    target: SchemaRef,
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl BatchAdapter {
    /// The map from `source` onto `target` with `factory`.
    pub fn try_new(
        target: SchemaRef,
        source: &SchemaRef,
        factory: &dyn PhysicalExprAdapterFactory,
    ) -> Result<Self> {
        let adapter = factory.create(Arc::clone(&target), Arc::clone(source))?;
        let exprs = target
            .fields()
            .iter()
            .enumerate()
            .map(|(at, field)| adapter.rewrite(Arc::new(Column::new(field.name(), at))))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { target, exprs })
    }

    /// `batch`, as one batch of the target schema.
    pub fn adapt_batch(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        let columns = self
            .exprs
            .iter()
            .map(|expr| expr.evaluate(batch)?.into_array(rows))
            .collect::<Result<Vec<_>>>()?;
        // A batch of no column states its row count nowhere else.
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.target),
            columns,
            &options,
        )?)
    }
}

/// A [`FileOpener`] that maps the batches of `inner` onto one target schema.
///
/// The target is the file schema of the table, narrowed to the columns the scan
/// reads. A `FileSource` that hands this opener to `ProjectionOpener` derives
/// that schema the same way, so the two always agree.
pub struct ExprAdapterOpener {
    inner: Arc<dyn FileOpener>,
    target: SchemaRef,
    factory: Arc<dyn PhysicalExprAdapterFactory>,
}

impl ExprAdapterOpener {
    /// Wrap `inner` so that every batch it produces carries `target`. The
    /// expression adapter of `factory` maps each file.
    pub fn wrap(
        inner: Arc<dyn FileOpener>,
        target: SchemaRef,
        factory: Arc<dyn PhysicalExprAdapterFactory>,
    ) -> Arc<dyn FileOpener> {
        Arc::new(Self {
            inner,
            target,
            factory,
        })
    }
}

impl FileOpener for ExprAdapterOpener {
    fn open(&self, partitioned_file: PartitionedFile) -> Result<FileOpenFuture> {
        let target = Arc::clone(&self.target);
        let factory = Arc::clone(&self.factory);
        let inner = self.inner.open(partitioned_file)?;

        Ok(Box::pin(async move {
            let stream = inner.await?;
            // A reader keeps one schema for a file, so the first batch settles
            // the adapter. A reader that changes schema settles a second one.
            let mut adapter: Option<(SchemaRef, BatchAdapter)> = None;
            let stream = stream.map(move |batch| {
                let batch = batch?;
                let source = batch.schema();
                let settled = matches!(&adapter, Some((held, _)) if held == &source);
                if !settled {
                    let built =
                        BatchAdapter::try_new(Arc::clone(&target), &source, factory.as_ref())?;
                    adapter = Some((source, built));
                }
                let (_, adapter) = adapter.as_ref().expect("just settled");
                adapter.adapt_batch(&batch)
            });
            Ok(stream.boxed())
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::type_widening::DefaultArrowTypeWidening;
    use arrow::array::{Array, Float64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::physical_plan::FileScanConfigBuilder;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use futures::TryStreamExt;

    /// An opener that gives back one batch of `v: Utf8`.
    struct Text;

    impl FileOpener for Text {
        fn open(&self, _file: PartitionedFile) -> Result<FileOpenFuture> {
            Ok(Box::pin(async {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)])),
                    vec![Arc::new(StringArray::from(vec!["1.5", "abc"]))],
                )?;
                Ok(futures::stream::iter([Ok(batch)]).boxed())
            }))
        }
    }

    fn target() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("v", DataType::Float64, true)]))
    }

    async fn read(factory: Arc<dyn PhysicalExprAdapterFactory>) -> Result<Vec<RecordBatch>> {
        let opener = ExprAdapterOpener::wrap(Arc::new(Text), target(), factory);
        opener
            .open(PartitionedFile::new("f", 1))?
            .await?
            .try_collect()
            .await
    }

    /// The factory decides the cast. The lenient rule reads "abc" as null, and
    /// the strict rule reports it.
    #[tokio::test]
    async fn the_opener_maps_batches_with_the_factory() {
        let lenient =
            DefaultArrowTypeWidening::keeping_first_type().physical_expr_adapter_factory();
        let batches = read(lenient).await.expect("a settled column may not fail");
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(values.value(0), 1.5);
        assert!(values.is_null(1));

        let strict = DefaultArrowTypeWidening::new().physical_expr_adapter_factory();
        assert!(read(strict).await.is_err(), "\"abc\" must be told");
    }

    /// A file with more columns than the target maps onto the target. The
    /// rewritten column addresses the file schema, not the target schema.
    #[test]
    fn a_file_wider_than_the_target_maps_onto_it() {
        let source = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Utf8, true),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(StringArray::from(vec!["2.5"])),
            ],
        )
        .unwrap();
        let factory =
            DefaultArrowTypeWidening::keeping_first_type().physical_expr_adapter_factory();
        let adapted = BatchAdapter::try_new(target(), &source.schema(), factory.as_ref())
            .and_then(|adapter| adapter.adapt_batch(&source))
            .expect("the file holds `v`");
        assert_eq!(adapted.schema(), target());
        let values = adapted
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("Float64");
        assert_eq!(values.value(0), 2.5);
    }

    /// The config factory wins over the fallback, and the fallback serves a
    /// config without one.
    #[test]
    fn the_config_factory_wins_over_the_fallback() {
        let source = Arc::new(datafusion::datasource::physical_plan::CsvSource::new(
            target(),
        ));
        let plain = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source).build();
        let fallback = DefaultArrowTypeWidening::new();
        let chosen = scan_adapter_factory(&plain, &fallback);
        assert!(format!("{chosen:?}").contains("LenientCastAdapterFactory"));

        let set = FileScanConfigBuilder::from(plain)
            .with_expr_adapter(Some(Arc::new(
                datafusion::physical_expr_adapter::DefaultPhysicalExprAdapterFactory,
            )))
            .build();
        let chosen = scan_adapter_factory(&set, &fallback);
        assert!(format!("{chosen:?}").contains("DefaultPhysicalExprAdapterFactory"));
    }
}
