use std::{any::Any, sync::Arc};

use arrow::datatypes::{Schema, SchemaRef};
use datafusion::{
    common::Statistics,
    datasource::{
        file_format::{
            file_compression_type::FileCompressionType, FileFormat, FilePushdownSupport,
        },
        physical_plan::{FileScanConfig, FileSinkConfig},
    },
    execution::SessionState,
    physical_expr::LexRequirement,
    physical_plan::{ExecutionPlan, PhysicalExpr},
    prelude::Expr,
};
use object_store::{ObjectMeta, ObjectStore};
#[derive(Debug)]
pub struct OdvFormat {}

#[async_trait::async_trait]
impl FileFormat for OdvFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "txt".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok("txt".to_string())
    }

    async fn infer_schema(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        todo!()
    }

    async fn infer_stats(
        &self,
        state: &SessionState,
        store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&table_schema))
    }

    async fn create_physical_plan(
        &self,
        state: &SessionState,
        conf: FileScanConfig,
        filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}
