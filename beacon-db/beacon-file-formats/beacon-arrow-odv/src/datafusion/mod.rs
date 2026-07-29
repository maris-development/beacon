use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_common::super_typing;
use datafusion::{
    catalog::{memory::DataSourceExec, Session},
    common::{Column, DFSchema, GetExt, Statistics},
    datasource::{
        file_format::{file_compression_type::FileCompressionType, FileFormat, FileFormatFactory},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSinkConfig, FileSource},
        sink::DataSinkExec,
    },
    physical_expr::{create_physical_sort_exprs, LexOrdering, LexRequirement},
    physical_plan::{sorts::sort::SortExec, ExecutionPlan},
    prelude::Expr,
};
use futures::{future::try_join_all, TryStreamExt};
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::datafusion::{sink::OdvSink, source::OdvSource};
use crate::reader::AsyncOdvDecoder;
use crate::writer::OdvOptions;

pub mod sink;
pub mod source;

#[derive(Debug)]
pub struct OdvFileFormatFactory {
    options: Option<OdvOptions>,
}

impl OdvFileFormatFactory {
    pub fn new(options: Option<OdvOptions>) -> Self {
        OdvFileFormatFactory { options }
    }

    pub fn options(&self) -> &Option<OdvOptions> {
        &self.options
    }

    pub fn set_options(&mut self, options: OdvOptions) {
        self.options = Some(options);
    }

    pub fn clear_options(&mut self) {
        self.options = None;
    }
}

impl GetExt for OdvFileFormatFactory {
    fn get_ext(&self) -> String {
        "txt".to_string()
    }
}

impl FileFormatFactory for OdvFileFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &std::collections::HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        match self.options {
            Some(ref options) => {
                let format = OdvFormat::new_with_options(options.clone());
                Ok(Arc::new(format) as Arc<dyn FileFormat>)
            }
            None => Ok(Arc::new(OdvFormat::new()) as Arc<dyn FileFormat>),
        }
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        Arc::new(OdvFormat::new())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Debug)]
pub struct OdvFormat {
    options: Option<OdvOptions>,
    file_compression_type: FileCompressionType,
}

impl OdvFormat {
    pub fn new() -> Self {
        OdvFormat {
            options: None,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
    pub fn new_with_options(options: OdvOptions) -> Self {
        OdvFormat {
            options: Some(options),
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }

    pub fn with_file_compression_type(mut self, compression: FileCompressionType) -> Self {
        self.file_compression_type = compression;
        self
    }

    fn infer_compression(object_meta: &ObjectMeta) -> FileCompressionType {
        if let Some(ext) = object_meta.location.extension() {
            match ext {
                "gz" => FileCompressionType::GZIP,
                "bz2" => FileCompressionType::BZIP2,
                "xz" => FileCompressionType::XZ,
                "zstd" => FileCompressionType::ZSTD,
                "zst" => FileCompressionType::ZSTD,
                _ => FileCompressionType::UNCOMPRESSED,
            }
        } else {
            FileCompressionType::UNCOMPRESSED
        }
    }
}

#[async_trait::async_trait]
impl FileFormat for OdvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        let ext = self.get_ext();
        Ok(format!("{}{}", ext, file_compression_type.get_ext()))
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    fn get_ext(&self) -> String {
        "txt".to_string()
    }

    async fn infer_schema(
        &self,
        _state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schema_futures = objects.iter().map(|object| {
            let store = Arc::clone(store);
            let object = object.clone();

            let compression = Self::infer_compression(&object);
            async move {
                let stream = store
                    .get(&object.location)
                    .await?
                    .into_stream()
                    .map_err(Into::into);

                let uncompressed_stream = compression.convert_stream(Box::pin(stream))?;

                let schema_mapper =
                    AsyncOdvDecoder::decode_schema_mapper(uncompressed_stream.map_err(Into::into))
                        .await
                        .map_err(|e| {
                            datafusion::error::DataFusionError::Execution(format!(
                                "Failed to decode schema: {}",
                                e
                            ))
                        })?;

                Ok::<_, datafusion::error::DataFusionError>(schema_mapper.output_schema())
            }
        });

        let schemas = try_join_all(schema_futures).await?;

        let super_schema = super_typing::super_type_schema(&schemas).map_err(|e| {
            datafusion::error::DataFusionError::Execution(format!("Failed to infer schema: {}", e))
        })?;

        Ok(Arc::new(super_schema))
    }

    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source — rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = OdvSource::new(table_schema).with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    async fn create_writer_physical_plan(
        &self,
        mut input: Arc<dyn ExecutionPlan>,
        state: &dyn Session,
        conf: FileSinkConfig,
        order_requirements: Option<LexRequirement>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let odv_options = self.options.clone().unwrap_or_default();
        let entry_sort_col = odv_options.key_column.clone();
        let time_sort_col = odv_options.time_column.column_name.clone();
        let depth_sort_col = odv_options.depth_column.column_name.clone();
        // Create sort exec by the sort key
        let key_sort_expr = Expr::Column(Column::from_name(entry_sort_col)).sort(true, false);
        let time_sort_expr = Expr::Column(Column::from_name(time_sort_col)).sort(true, false);
        let depth_sort_expr = Expr::Column(Column::from_name(depth_sort_col)).sort(true, false);

        let physical_sort_exprs = create_physical_sort_exprs(
            &[key_sort_expr, time_sort_expr, depth_sort_expr],
            &DFSchema::try_from(input.schema()).unwrap(),
            state.execution_props(),
        )?;

        let lex_ordering = LexOrdering::new(physical_sort_exprs);
        if let Some(lex_ordering) = lex_ordering {
            input = Arc::new(SortExec::new(lex_ordering, input));
        } else {
            tracing::warn!(
                "No valid sort expressions could be created for OdvSink. Data will be written in the order it is received."
            );
        }

        let object_store = state.runtime_env().object_store(&conf.object_store_url)?;

        let odv_sink = Arc::new(OdvSink::new(conf, self.options.clone(), object_store));
        Ok(Arc::new(DataSinkExec::new(
            input,
            odv_sink,
            order_requirements,
        )))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(OdvSource::new(table_schema))
    }
}

pub mod table_function;
pub use table_function::ReadOdvAsciiFunc;
