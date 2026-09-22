use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_binary_format::{
    object_store::ArrowBBFObjectReader, reader::async_reader::AsyncBBFReader,
};
use beacon_common::file_descriptors::file_open_parallelism;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::nd::{encoded_schema, exec::nd_scan_plan};
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
use datafusion::{
    catalog::Session,
    common::{GetExt, Statistics},
    datasource::{
        file_format::{FileFormat, FileFormatFactory, file_compression_type::FileCompressionType},
        physical_plan::{FileScanConfig, FileScanConfigBuilder, FileSource},
    },
    physical_plan::ExecutionPlan,
};
use futures::{StreamExt, TryStreamExt, stream};
use object_store::{ObjectMeta, ObjectStore};

use crate::datafusion::source::BBFSource;

pub mod metrics;
pub mod opener;
pub mod source;
pub mod stream_share;

pub const BBF_FORMAT_NAME: &str = "bbf";

/// Builds the BBF format. The format has no options: a file carries its own
/// schema, and the nd pipeline decides the shape of a batch.
#[derive(Clone, Debug, Default)]
pub struct BBFFormatFactory;

impl GetExt for BBFFormatFactory {
    fn get_ext(&self) -> String {
        BBF_FORMAT_NAME.to_string()
    }
}

impl FileFormatFactory for BBFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(Arc::new(BBFFormat))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        std::sync::Arc::new(BBFFormat)
    }
}

impl FileFormatFactoryExt for BBFFormatFactory {
    /// BBF opts into the schema cache on its name alone. A file carries its own
    /// schema, and the format has no options.
    fn schema_options_fingerprint(&self, format: &dyn FileFormat) -> Option<u64> {
        format.as_any().downcast_ref::<BBFFormat>()?;
        Some(SchemaOptions::new("bbf").finish())
    }

    fn discover_datasets(
        &self,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let datasets = objects
            .iter()
            .filter(|obj| {
                obj.location
                    .extension()
                    .map(|ext| ext == BBF_FORMAT_NAME)
                    .unwrap_or(false)
            })
            .map(|obj| DatasetMetadata::new(obj.location.to_string(), self.get_ext()))
            .collect();
        Ok(datasets)
    }

    fn file_format_name(&self) -> String {
        self.get_ext()
    }
}

#[derive(Clone, Debug, Default)]
pub struct BBFFormat;

#[async_trait::async_trait]
impl FileFormat for BBFFormat {
    /// Returns the table provider as [`Any`](std::any::Any) so that it can be
    /// downcast to a specific implementation.
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Returns the extension for this FileFormat, e.g. "file.csv" -> csv
    fn get_ext(&self) -> String {
        BBF_FORMAT_NAME.to_string()
    }

    /// Returns the extension for this FileFormat when compressed, e.g. "file.csv.gz" -> csv
    fn get_ext_with_compression(
        &self,
        _file_compression_type: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(self.get_ext())
    }

    /// Returns whether this instance uses compression if applicable
    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let schemas = stream::iter(objects.iter().cloned())
            .map(|object| {
                let store = Arc::clone(store);
                async move {
                    let async_reader = ArrowBBFObjectReader::new(object.location, store);

                    let reader = AsyncBBFReader::new(Arc::new(async_reader), 128)
                        .await
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

                    Ok::<_, datafusion::error::DataFusionError>(Arc::new(reader.arrow_schema()))
                }
            })
            // Keep the listing order.
            .buffered(file_open_parallelism())
            .try_collect::<Vec<_>>()
            .await?;

        // Merge & widen types across all files, using the session's type widening
        // rules. Each schema names its file, so a refused column names both files.
        session_widening(state)
            .merge_schemas(&label_by_object(objects, &schemas))
            .map_err(|e| {
                datafusion::error::DataFusionError::Execution(format!(
                    "Failed to infer schema: {}",
                    e
                ))
            })
    }

    /// BBF Maintains its own internal statistics, so we don't need to compute them here. Return unknown stats to avoid unnecessary work.
    async fn infer_stats(
        &self,
        _state: &dyn Session,
        _store: &Arc<dyn ObjectStore>,
        table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> datafusion::error::Result<Statistics> {
        return Ok(Statistics::new_unknown(&table_schema));
    }

    /// Take a list of files and convert it to the appropriate executor
    /// according to this file format.
    async fn create_physical_plan(
        &self,
        state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        beacon_nd_array::arrow::morsel::reject_partition_columns("BBF", &conf)?;

        // The scan carries each entry as one `beacon.nd`-encoded row. The nd
        // spine above decodes and broadcasts it onto the logical schema.
        let encoded = Arc::new(encoded_schema(conf.file_schema()));
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            encoded,
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source. Rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = BBFSource::new(table_schema)
            .with_projection(projection)
            .with_type_widening(Arc::clone(&session_widening(state).strategy));
        // Keep the token a caller set on the incoming source.
        if let Some(incoming) = conf.file_source().as_any().downcast_ref::<BBFSource>() {
            source.set_cancellation_token(incoming.cancellation_token());
        }
        // Fail at plan time, so the user sees the error before the scan runs.
        source.require_projection()?;
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        nd_scan_plan(conf)
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(BBFSource::new(table_schema))
    }
}

pub mod table_function;
pub use table_function::ReadBBFFunc;

#[cfg(test)]
pub(crate) mod test_util {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use beacon_binary_format::array::dimensions::Dimensions;
    use beacon_binary_format::entry::{ArrayCollection, Column, Entry};
    use beacon_binary_format::writer::BBFWriter;
    use object_store::ObjectMeta;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    /// Writes a small two-entry BBF file into `dir` and returns a local-filesystem
    /// object store rooted there plus the file's metadata. Used by the tests that
    /// need a genuine BBF file rather than a synthetic schema.
    pub(crate) async fn write_bbf_fixture(
        dir: &std::path::Path,
        file_name: &str,
    ) -> (Arc<LocalFileSystem>, ObjectMeta) {
        let file_path = dir.join(file_name);
        {
            let mut writer =
                BBFWriter::new(&file_path, 1024 * 1024, None, true).expect("bbf writer");

            let ints: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
            let collection = ArrayCollection::new(
                "first",
                Box::new(std::iter::once(Column::new(
                    "ints",
                    ints,
                    Dimensions::Multi(vec![("dim1", 3).into()]),
                ))),
            );
            writer.append(Entry::new(collection), "entry_a");

            let names: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
            let more_ints: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
            let collection = ArrayCollection::new(
                "second",
                Box::new(
                    vec![
                        Column::new("names", names, Dimensions::Multi(vec![("dim1", 2).into()])),
                        Column::new(
                            "ints",
                            more_ints,
                            Dimensions::Multi(vec![("dim1", 2).into()]),
                        ),
                    ]
                    .into_iter(),
                ),
            );
            writer.append(Entry::new(collection), "entry_b");

            writer.finish().expect("finish bbf file");
        }

        let store = Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"));
        let location = Path::from(file_name);
        let meta = {
            use object_store::ObjectStoreExt;
            store.head(&location).await.expect("stat bbf fixture")
        };
        (store, meta)
    }

    /// Writes a one-entry BBF file whose `ints` column is `Int64`, beside a
    /// fixture whose `ints` is `Int32`, so a scan must widen the column.
    pub(crate) fn write_bbf_int64_file(dir: &std::path::Path, file_name: &str) {
        use arrow::array::Int64Array;

        let mut writer =
            BBFWriter::new(dir.join(file_name), 1024 * 1024, None, true).expect("bbf writer");
        let ints: ArrayRef = Arc::new(Int64Array::from(vec![100, 200]));
        let collection = ArrayCollection::new(
            "wide",
            Box::new(std::iter::once(Column::new(
                "ints",
                ints,
                Dimensions::Multi(vec![("dim1", 2).into()]),
            ))),
        );
        writer.append(Entry::new(collection), "entry_c");
        writer.finish().expect("finish bbf file");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    /// The format has no options. A key a user writes is ignored, so an old
    /// `CREATE EXTERNAL TABLE` that names one keeps working.
    #[test]
    fn create_ignores_options() {
        let ctx = SessionContext::new();
        let opts = HashMap::from([("split_streams_slice".to_string(), "nope".to_string())]);
        let format = BBFFormatFactory
            .create(&ctx.state(), &opts)
            .expect("an unknown option must not fail");
        assert!(format.as_any().downcast_ref::<BBFFormat>().is_some());
    }

    /// Only `.bbf` objects are BBF datasets; anything else in the listing must be
    /// left to the other format factories.
    #[tokio::test]
    async fn discover_datasets_selects_only_bbf_objects() {
        let store = Arc::new(InMemory::new());
        let mut objects = Vec::new();
        for loc in ["x/a.bbf", "x/b.parquet", "x/plain", "x/c.bbf2"] {
            let path = Path::from(loc);
            store
                .put(&path, PutPayload::from(Vec::new()))
                .await
                .expect("put");
            objects.push(store.head(&path).await.expect("head"));
        }
        let datasets = BBFFormatFactory
            .discover_datasets(&objects)
            .unwrap();
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, vec!["x/a.bbf"]);
        assert_eq!(datasets[0].format, "bbf");
        assert_eq!(
            BBFFormatFactory.file_extensions(),
            vec!["bbf"]
        );
    }

    /// BBF carries its own internal compression, so the container extension is
    /// always `bbf` no matter what compression the caller asks about.
    #[test]
    fn extension_is_always_bbf() {
        let format = BBFFormat;
        assert_eq!(format.get_ext(), "bbf");
        assert_eq!(format.compression_type(), None);
        assert_eq!(
            format
                .get_ext_with_compression(&FileCompressionType::GZIP)
                .unwrap(),
            "bbf"
        );
    }

    /// End-to-end schema inference over a real BBF file: the union of all entries'
    /// columns must be visible, plus the synthetic entry-key column, and a column
    /// that only exists in one entry must still appear.
    #[tokio::test]
    async fn infer_schema_reads_real_bbf_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (store, meta) = super::test_util::write_bbf_fixture(dir.path(), "fixture.bbf").await;
        let object_store: Arc<dyn ObjectStore> = store;

        let ctx = SessionContext::new();
        let schema = BBFFormat
            .infer_schema(&ctx.state(), &object_store, &[meta])
            .await
            .expect("real BBF file should infer");

        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"ints"), "fields were {names:?}");
        assert!(names.contains(&"names"), "fields were {names:?}");
        assert!(
            names.contains(&beacon_binary_format::entry::Entry::FIELD_NAME),
            "fields were {names:?}"
        );
    }

    /// A file that is not BBF must produce an error rather than an empty schema,
    /// so a mis-registered extension surfaces immediately.
    #[tokio::test]
    async fn infer_schema_errors_on_non_bbf_bytes() {
        let store = Arc::new(InMemory::new());
        let path = Path::from("junk.bbf");
        store
            .put(&path, PutPayload::from(b"definitely not bbf".to_vec()))
            .await
            .expect("put");
        let meta = store.head(&path).await.expect("head");
        let object_store: Arc<dyn ObjectStore> = store;

        let ctx = SessionContext::new();
        assert!(
            BBFFormat
                .infer_schema(&ctx.state(), &object_store, &[meta])
                .await
                .is_err()
        );
    }

    async fn plan_with_projection(
        indices: Vec<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int32, true),
        ]));
        let format = BBFFormat;
        let source = format.file_source(TableSchema::from_file_schema(schema));
        let conf = FileScanConfigBuilder::new(ObjectStoreUrl::parse("file://")?, source)
            .with_projection_indices(Some(indices))?
            .build();
        format
            .create_physical_plan(&SessionContext::new().state(), conf)
            .await
    }

    /// `SELECT *` must fail when the query is planned, not when it runs.
    #[tokio::test]
    async fn create_physical_plan_refuses_a_projection_of_every_column() {
        let err = plan_with_projection(vec![0, 1])
            .await
            .expect_err("plan must fail");
        assert!(
            matches!(err, datafusion::error::DataFusionError::Plan(_)),
            "{err}"
        );
        assert!(err.to_string().contains("SELECT *"), "{err}");
    }

    /// A column list that leaves out a column plans.
    #[tokio::test]
    async fn create_physical_plan_accepts_a_projection_of_some_columns() {
        plan_with_projection(vec![1])
            .await
            .expect("subset projection plans");
    }

    /// The format rebuilds the source when it plans. The token set on the
    /// incoming source must reach the source in the plan.
    #[tokio::test]
    async fn create_physical_plan_keeps_the_cancellation_token() {
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;
        use tokio_util::sync::CancellationToken;

        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int32, true),
        ]));
        let format = BBFFormat;
        let source = format.file_source(TableSchema::from_file_schema(schema));
        let token = CancellationToken::new();
        source
            .as_any()
            .downcast_ref::<BBFSource>()
            .expect("bbf source")
            .set_cancellation_token(token.clone());
        let conf = FileScanConfigBuilder::new(ObjectStoreUrl::parse("file://").unwrap(), source)
            .with_projection_indices(Some(vec![1]))
            .unwrap()
            .build();
        let plan = format
            .create_physical_plan(&SessionContext::new().state(), conf)
            .await
            .expect("plan");

        // NdBroadcastExec over NdSourceExec over the scan.
        let scan = plan.children()[0].children()[0];
        let planned = scan
            .as_any()
            .downcast_ref::<datafusion::datasource::source::DataSourceExec>()
            .expect("data source exec")
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
            .expect("file scan config")
            .file_source()
            .as_any()
            .downcast_ref::<BBFSource>()
            .expect("bbf source")
            .cancellation_token();
        token.cancel();
        assert!(
            planned.is_cancelled(),
            "the plan must share the caller's token"
        );
    }

    /// Registers the fixture as table `t` on a fresh session, so a test can run
    /// SQL through the planner the way a user does.
    async fn session_with_fixture(dir: &std::path::Path) -> SessionContext {
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        super::test_util::write_bbf_fixture(dir, "sql.bbf").await;
        let ctx = SessionContext::new();
        // The whole directory, so a test can add a second file beside the fixture.
        let url = ListingTableUrl::parse(dir.to_str().expect("utf8 path")).expect("listing url");
        let options =
            ListingOptions::new(Arc::new(BBFFormat)).with_file_extension("bbf");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .infer_schema(&ctx.state())
            .await
            .expect("infer schema");
        let table = ListingTable::try_new(config).expect("listing table");
        ctx.register_table("t", Arc::new(table)).expect("register");
        ctx
    }

    /// The planner pushes every column as the projection of `SELECT *`, and the
    /// scan refuses it with the message that tells the user what to do.
    #[tokio::test]
    async fn sql_select_star_fails_at_plan_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = session_with_fixture(dir.path()).await;

        let err = ctx
            .sql("SELECT * FROM t")
            .await
            .expect("parse")
            .create_physical_plan()
            .await
            .expect_err("SELECT * must not plan");
        assert!(err.to_string().contains("column list"), "{err}");
    }

    /// A column list reads the rows of every entry.
    #[tokio::test]
    async fn sql_column_list_reads_the_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = session_with_fixture(dir.path()).await;

        let batches = ctx
            .sql("SELECT ints FROM t")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 5, "3 rows from entry_a + 2 rows from entry_b");
    }

    /// A count over a named column selects that column, so it passes the rule
    /// and counts the rows of every entry.
    #[tokio::test]
    async fn sql_count_of_a_column_counts_the_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = session_with_fixture(dir.path()).await;

        let batches = ctx
            .sql("SELECT count(ints) FROM t")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let count = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("count is Int64")
            .value(0);
        assert_eq!(count, 5, "3 rows from entry_a + 2 rows from entry_b");
    }

    /// The scan sits under the nd spine, so the broadcast happens in the plan
    /// and not in the opener.
    #[tokio::test]
    async fn create_physical_plan_wraps_the_scan_in_the_nd_spine() {
        let plan = plan_with_projection(vec![1]).await.expect("plan");
        let shown = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(false)
            .to_string();
        assert!(shown.contains("NdBroadcastExec"), "{shown}");
        assert!(shown.contains("NdSourceExec"), "{shown}");
        assert!(shown.contains("DataSourceExec"), "{shown}");
    }

    /// The nd spine cannot carry a partition column, so a partitioned BBF table
    /// is refused at plan time with the format named.
    #[tokio::test]
    async fn create_physical_plan_rejects_partition_columns() {
        use datafusion::datasource::table_schema::TableSchema;
        use datafusion::execution::object_store::ObjectStoreUrl;

        let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int32, true),
            arrow::datatypes::Field::new("b", arrow::datatypes::DataType::Int32, true),
        ]));
        let partition = Arc::new(arrow::datatypes::Field::new(
            "p",
            arrow::datatypes::DataType::Utf8,
            false,
        ));
        let format = BBFFormat;
        let source = format.file_source(TableSchema::new(schema, vec![partition]));
        let conf = FileScanConfigBuilder::new(ObjectStoreUrl::parse("file://").unwrap(), source)
            .with_projection_indices(Some(vec![1]))
            .unwrap()
            .build();
        let err = format
            .create_physical_plan(&SessionContext::new().state(), conf)
            .await
            .expect_err("a partitioned table must be refused");
        assert!(err.to_string().contains("BBF"), "{err}");
        assert!(err.to_string().contains("PARTITIONED BY"), "{err}");
    }

    /// An expression over a column stays above the broadcast and computes.
    #[tokio::test]
    async fn sql_expression_over_a_column_computes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = session_with_fixture(dir.path()).await;

        let batches = ctx
            .sql("SELECT ints + 1 AS x FROM t ORDER BY x")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        // The literal `1` is Int64, so DataFusion widens `ints` to Int64.
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("x is Int64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![2, 3, 4, 11, 21]);
    }

    /// Two files type `ints` differently. The scan widens the column of the
    /// narrow file onto the merged type, and every row arrives.
    #[tokio::test]
    async fn sql_widens_a_column_across_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        super::test_util::write_bbf_int64_file(dir.path(), "wide.bbf");
        let ctx = session_with_fixture(dir.path()).await;

        let batches = ctx
            .sql("SELECT ints FROM t ORDER BY ints")
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect");
        let values: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("ints widens to Int64")
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![1, 2, 3, 10, 20, 100, 200]);
    }

    /// `count(*)` selects no column. The scan refuses it like `SELECT *`.
    #[tokio::test]
    async fn sql_count_star_fails_at_plan_time() {
        let dir = tempfile::tempdir().expect("tempdir");
        let ctx = session_with_fixture(dir.path()).await;

        let err = ctx
            .sql("SELECT count(*) FROM t")
            .await
            .expect("parse")
            .create_physical_plan()
            .await
            .expect_err("count(*) must not plan");
        assert!(err.to_string().contains("count(*)"), "{err}");
    }
}
