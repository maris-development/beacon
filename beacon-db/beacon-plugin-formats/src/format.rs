//! The DataFusion file format of one configured plugin format.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use beacon_plugin_sdk::file_format::{FormatFactory, FormatReader, OutputKind, ScanRequest};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::{Statistics, exec_err, not_impl_err, plan_err};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::physical_plan::{
    FileOutputMode, FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource,
};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::physical_expr::expressions::col;
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::ExecutionPlan;
use futures::{StreamExt, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore};

use crate::nd::encoded_partition_cols;
use crate::sink::PluginFileSink;
use crate::source::PluginFileSource;
use beacon_datafusion_ext::nd::encoding::encoded_schema;
use beacon_datafusion_ext::nd::exec::{NdBroadcastExec, NdSourceExec};
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};

/// One configured plugin format: the plugin reader for the options of one table.
#[derive(Clone)]
pub struct PluginFileFormat {
    pub(crate) factory: Arc<dyn FormatFactory>,
    pub(crate) reader: Arc<dyn FormatReader>,
    pub(crate) ext: String,
    /// The options the plugin got.
    pub(crate) options: HashMap<String, String>,
    pub(crate) single_file_output: Option<bool>,
    /// Only a format for the file analyzer computes statistics.
    pub(crate) statistics: bool,
    /// Shared with the factory: `false` after the plugin is uninstalled.
    pub(crate) installed: Arc<AtomicBool>,
}

impl std::fmt::Debug for PluginFileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PluginFileFormat")
            .field("name", &self.name())
            .field("options", &self.options)
            .finish()
    }
}

impl PluginFileFormat {
    /// The format name.
    pub fn name(&self) -> &str {
        self.factory.name()
    }

    fn context(&self, error: DataFusionError) -> DataFusionError {
        error.context(format!("file format {}", self.name()))
    }

    /// The input sort order that the plugin writer asks for, or `None`.
    fn write_requirement(&self, schema: &SchemaRef) -> Result<Option<LexRequirement>> {
        let keys = self
            .reader
            .write_ordering(Arc::clone(schema))
            .map_err(|e| self.context(e))?;
        let mut requirements = Vec::with_capacity(keys.len());
        for key in keys {
            let Ok(expr) = col(&key.column, schema) else {
                return plan_err!(
                    "file format {}: the write ordering names column `{}`, which is not in the input",
                    self.name(),
                    key.column
                );
            };
            let options = SortOptions {
                descending: key.descending,
                nulls_first: key.nulls_first,
            };
            requirements.push(PhysicalSortRequirement::new(expr, Some(options)));
        }
        Ok(LexRequirement::new(requirements))
    }

    fn ensure_installed(&self) -> Result<()> {
        if self.installed.load(Ordering::SeqCst) {
            Ok(())
        } else {
            plan_err!("{}", not_installed(self.name()))
        }
    }
}

/// A reader that fails each call, for a `default()` format whose `create` failed.
pub(crate) struct ErrorReader(pub(crate) String);

#[async_trait]
impl FormatReader for ErrorReader {
    async fn infer_schema(&self, _: Arc<dyn ObjectStore>, _: &ObjectMeta) -> Result<SchemaRef> {
        exec_err!("{}", self.0)
    }

    async fn open(
        &self,
        _: Arc<dyn ObjectStore>,
        _: &ObjectMeta,
        _: ScanRequest,
    ) -> Result<SendableRecordBatchStream> {
        exec_err!("{}", self.0)
    }
}

#[async_trait]
impl FileFormat for PluginFileFormat {
    fn get_ext(&self) -> String {
        self.ext.clone()
    }

    fn get_ext_with_compression(&self, _: &FileCompressionType) -> Result<String> {
        Ok(self.ext.clone())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> Result<SchemaRef> {
        self.ensure_installed()?;
        // Bounded like the built-in formats: each call may hold an open file.
        let width = state
            .config_options()
            .execution
            .meta_fetch_concurrency
            .max(1);
        let calls: Vec<_> = objects
            .iter()
            .cloned()
            .map(|object| {
                let reader = Arc::clone(&self.reader);
                let store = Arc::clone(store);
                async move {
                    reader.infer_schema(store, &object).await.map_err(|e| {
                        e.context(format!("cannot read the schema of {}", object.location))
                    })
                }
            })
            .collect();
        let schemas: Vec<SchemaRef> = futures::stream::iter(calls)
            .buffered(width)
            .try_collect()
            .await
            .map_err(|e| self.context(e))?;
        if schemas.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| self.context(DataFusionError::ArrowError(Box::new(e), None)))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> Result<Statistics> {
        let unknown = Statistics::new_unknown(&table_schema);
        if !self.statistics {
            return Ok(unknown);
        }
        match self
            .reader
            .infer_stats(Arc::clone(store), object, Arc::clone(&table_schema))
            .await
        {
            Ok(Some(statistics))
                if statistics.column_statistics.len() == table_schema.fields().len() =>
            {
                Ok(statistics)
            }
            Ok(Some(_)) => {
                tracing::warn!(
                    "file format {}: statistics of {} do not match the table schema",
                    self.name(),
                    object.location
                );
                Ok(unknown)
            }
            Ok(None) => Ok(unknown),
            Err(error) => {
                tracing::warn!(
                    "file format {}: cannot compute statistics of {}: {error}",
                    self.name(),
                    object.location
                );
                Ok(unknown)
            }
        }
    }

    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.ensure_installed()?;
        let kind = self.factory.output_kind();
        let table_schema = match kind {
            OutputKind::Flat => conf.file_source().table_schema().clone(),
            // Every scan column is a `beacon.nd` struct, as for the built-in nd formats.
            OutputKind::Nd => TableSchema::new(
                Arc::new(encoded_schema(conf.file_schema())),
                encoded_partition_cols(conf.table_partition_cols()),
            ),
        };
        let projection = conf.file_source().projection().cloned();
        let source = PluginFileSource::new(
            self.name().to_string(),
            Arc::clone(&self.reader),
            kind,
            table_schema,
            Arc::clone(&session_widening(state).strategy),
        )
        .with_projection(projection.as_ref());
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        let scan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(conf);
        match kind {
            OutputKind::Flat => Ok(scan),
            OutputKind::Nd => {
                let nd_source = Arc::new(NdSourceExec::try_new(scan)?);
                Ok(Arc::new(NdBroadcastExec::try_new(nd_source)?))
            }
        }
    }

    async fn create_writer_physical_plan(
        &self,
        input: Arc<dyn ExecutionPlan>,
        _state: &dyn Session,
        mut conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.ensure_installed()?;
        let name = self.name();
        if !self.factory.can_write() {
            return not_impl_err!("file format {name} cannot write");
        }
        if conf.insert_op != InsertOp::Append {
            return not_impl_err!("file format {name} supports only COPY and append writes");
        }
        if let Some(single) = self.single_file_output {
            conf.file_output_mode = if single {
                FileOutputMode::SingleFile
            } else {
                FileOutputMode::Directory
            };
        }
        let requirement = self
            .write_requirement(&input.schema())?
            .or(order_requirements);
        let sink = PluginFileSink::new(
            conf,
            name.to_string(),
            Arc::clone(&self.reader),
            self.factory.split_output(),
        );
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(sink),
            requirement,
        )))
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        Arc::new(PluginFileSource::new(
            self.name().to_string(),
            Arc::clone(&self.reader),
            // A placeholder: `create_physical_plan` builds the source for the kind.
            OutputKind::Flat,
            table_schema,
            Arc::new(beacon_datafusion_ext::type_widening::DefaultArrowTypeWidening::new()),
        ))
    }
}

/// The error text for a format whose plugin is uninstalled.
pub(crate) fn not_installed(name: &str) -> String {
    format!("file format `{name}` is not installed")
}
