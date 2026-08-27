use std::{any::Any, collections::HashMap, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_binary_format::{
    object_store::ArrowBBFObjectReader, reader::async_reader::AsyncBBFReader,
};
use beacon_common::file_descriptors::file_open_parallelism;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt, SchemaOptions};
use beacon_datafusion_ext::format_options::format_option;
use beacon_datafusion_ext::type_widening::{label_by_object, session_widening};
use datafusion::{
    catalog::{Session, memory::DataSourceExec},
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

/// Runtime configuration for the BBF format.
#[derive(Clone, Debug, Default)]
pub struct BbfConfig {
    /// Whether to split each record batch into `batch_size`-row slices to bound
    /// peak memory for wide tables. Defaults to `false` for backward compatibility.
    pub split_streams_slice: bool,
}

/// Parse a boolean value supplied through a `CREATE EXTERNAL TABLE` option.
fn parse_bool_option(key: &str, value: &str) -> datafusion::error::Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        other => Err(datafusion::error::DataFusionError::Execution(format!(
            "invalid boolean for BBF option '{key}': '{other}'"
        ))),
    }
}

#[derive(Clone, Debug, Default)]
pub struct BBFFormatFactory {
    pub config: BbfConfig,
}

impl BBFFormatFactory {
    pub fn new(config: BbfConfig) -> Self {
        Self { config }
    }
}

impl GetExt for BBFFormatFactory {
    fn get_ext(&self) -> String {
        BBF_FORMAT_NAME.to_string()
    }
}

impl FileFormatFactory for BBFFormatFactory {
    fn create(
        &self,
        _state: &dyn Session,
        format_options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        // Per-table override from `CREATE EXTERNAL TABLE ... OPTIONS (...)`,
        // defaulting to the runtime config.
        let mut split_streams_slice = self.config.split_streams_slice;
        if let Some(value) = format_option(format_options, "split_streams_slice") {
            split_streams_slice = parse_bool_option("split_streams_slice", value)?;
        }
        Ok(Arc::new(BBFFormat {
            split_streams_slice,
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn default(&self) -> std::sync::Arc<dyn FileFormat> {
        std::sync::Arc::new(BBFFormat {
            split_streams_slice: self.config.split_streams_slice,
        })
    }
}

impl FileFormatFactoryExt for BBFFormatFactory {
    /// BBF opts into the schema cache on its name alone. A file carries its own
    /// schema, and `split_streams_slice` only decides how many rows a batch
    /// holds.
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
pub struct BBFFormat {
    /// Whether to split each record batch into `batch_size`-row slices.
    pub split_streams_slice: bool,
}

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
        _state: &dyn Session,
        conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let table_schema = datafusion::datasource::table_schema::TableSchema::new(
            conf.file_schema().clone(),
            conf.table_partition_cols().clone(),
        );
        // Preserve a projection that the scan pushed down into the incoming
        // source. Rebuilding the source below would otherwise drop it.
        let projection = conf.file_source().projection().cloned();
        let source = BBFSource::new(table_schema)
            .with_split_streams_slice(self.split_streams_slice)
            .with_projection(projection);
        let conf = FileScanConfigBuilder::from(conf)
            .with_source(Arc::new(source))
            .build();
        Ok(DataSourceExec::from_data_source(conf))
    }

    fn file_source(
        &self,
        table_schema: datafusion::datasource::table_schema::TableSchema,
    ) -> Arc<dyn FileSource> {
        Arc::new(BBFSource::new(table_schema).with_split_streams_slice(self.split_streams_slice))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::prelude::SessionContext;
    use object_store::memory::InMemory;
    use object_store::path::Path;
    use object_store::{ObjectStoreExt, PutPayload};

    /// Every spelling accepted by `CREATE EXTERNAL TABLE ... OPTIONS` must map to
    /// the same boolean, including odd casing and surrounding whitespace, because
    /// SQL option values arrive verbatim from the parser.
    #[test]
    fn parse_bool_option_accepts_all_documented_spellings() {
        for truthy in ["true", "TRUE", " True ", "1", "yes", "YES", "on"] {
            assert!(
                parse_bool_option("split_streams_slice", truthy).unwrap(),
                "{truthy:?} should parse as true"
            );
        }
        for falsy in ["false", "FALSE", " off ", "0", "no", "OFF"] {
            assert!(
                !parse_bool_option("split_streams_slice", falsy).unwrap(),
                "{falsy:?} should parse as false"
            );
        }
    }

    /// A typo in an option must fail loudly (naming the offending option) instead of
    /// silently falling back to the default.
    #[test]
    fn parse_bool_option_rejects_unknown_values() {
        let err = parse_bool_option("split_streams_slice", "maybe").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("split_streams_slice"), "message was: {msg}");
        assert!(msg.contains("maybe"), "message was: {msg}");
        assert!(parse_bool_option("split_streams_slice", "").is_err());
    }

    /// `create()` must layer the per-table option on top of the runtime default:
    /// absent option keeps the runtime value, present option overrides it.
    #[test]
    fn create_layers_table_option_over_runtime_config() {
        let ctx = SessionContext::new();
        let state = ctx.state();

        let on_by_default = BBFFormatFactory::new(BbfConfig {
            split_streams_slice: true,
        });
        let format = on_by_default.create(&state, &HashMap::new()).unwrap();
        assert!(downcast(&format).split_streams_slice);

        let opts = HashMap::from([("split_streams_slice".to_string(), "false".to_string())]);
        let format = on_by_default.create(&state, &opts).unwrap();
        assert!(!downcast(&format).split_streams_slice);

        let off_by_default = <BBFFormatFactory as Default>::default();
        let opts = HashMap::from([("split_streams_slice".to_string(), "on".to_string())]);
        let format = off_by_default.create(&state, &opts).unwrap();
        assert!(downcast(&format).split_streams_slice);
    }

    /// An invalid option must abort table creation rather than be ignored.
    #[test]
    fn create_propagates_invalid_option_error() {
        let ctx = SessionContext::new();
        let opts = HashMap::from([("split_streams_slice".to_string(), "nope".to_string())]);
        assert!(
            <BBFFormatFactory as Default>::default()
                .create(&ctx.state(), &opts)
                .is_err()
        );
    }

    /// `default()` has no options to consult, so it must still carry the runtime
    /// configuration through to the format.
    #[test]
    fn default_format_carries_runtime_config() {
        let factory = BBFFormatFactory::new(BbfConfig {
            split_streams_slice: true,
        });
        assert!(downcast(&FileFormatFactory::default(&factory)).split_streams_slice);
        assert!(
            !downcast(&FileFormatFactory::default(
                &<BBFFormatFactory as Default>::default()
            ))
            .split_streams_slice
        );
    }

    fn downcast(format: &Arc<dyn FileFormat>) -> &BBFFormat {
        format
            .as_any()
            .downcast_ref::<BBFFormat>()
            .expect("factory should produce a BBFFormat")
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
        let datasets = <BBFFormatFactory as Default>::default()
            .discover_datasets(&objects)
            .unwrap();
        let paths: Vec<&str> = datasets.iter().map(|d| d.file_path.as_str()).collect();
        assert_eq!(paths, vec!["x/a.bbf"]);
        assert_eq!(datasets[0].format, "bbf");
        assert_eq!(
            <BBFFormatFactory as Default>::default().file_extensions(),
            vec!["bbf"]
        );
    }

    /// BBF carries its own internal compression, so the container extension is
    /// always `bbf` no matter what compression the caller asks about.
    #[test]
    fn extension_is_always_bbf() {
        let format = BBFFormat::default();
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
        let schema = BBFFormat::default()
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
            BBFFormat::default()
                .infer_schema(&ctx.state(), &object_store, &[meta])
                .await
                .is_err()
        );
    }
}

#[cfg(test)]
mod scan_tests {
    //! End-to-end checks on the columns the BBF scan returns.
    //!
    //! `BBFSource` accepts a pushed-down projection, so it has to apply the
    //! whole of it. The projections here rename a column after the first one —
    //! the shape [#382](https://github.com/maris-development/beacon/issues/382)
    //! reported: the scan looked for a file column under the alias, found none,
    //! and read nothing.

    use std::sync::Arc;

    use arrow::array::{Array, AsArray};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    use datafusion::prelude::{SessionConfig, SessionContext};

    use super::BBFFormat;

    /// A session over the two-entry fixture. The temp dir is returned so it
    /// outlives the queries.
    async fn table() -> (SessionContext, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        super::test_util::write_bbf_fixture(dir.path(), "scan.bbf").await;

        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
        let options =
            ListingOptions::new(Arc::new(BBFFormat::default())).with_file_extension(".bbf");
        let url = ListingTableUrl::parse(dir.path().to_str().expect("utf-8 path")).expect("url");
        let schema = options
            .infer_schema(&ctx.state(), &url)
            .await
            .expect("schema");
        let config = ListingTableConfig::new(url)
            .with_listing_options(options)
            .with_schema(schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(config).expect("table")))
            .expect("register");
        (ctx, dir)
    }

    async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("planning {sql}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("running {sql}: {e}"))
    }

    fn one_batch(batches: Vec<RecordBatch>) -> RecordBatch {
        let schema = batches.first().expect("at least one batch").schema();
        arrow::compute::concat_batches(&schema, &batches).expect("concat")
    }

    /// Every value of `ints`, in the fixture's own order.
    fn ints(batch: &RecordBatch, column: usize) -> Vec<i32> {
        batch
            .column(column)
            .as_primitive::<arrow::datatypes::Int32Type>()
            .values()
            .to_vec()
    }

    /// An aliased projection is pushed into the scan whole. The scan has to
    /// rename the column it read, not look for a file column under the alias.
    ///
    /// `ints` sits after the entry-key column, so a scan that kept the file's
    /// own column order would also answer with the wrong values here.
    #[tokio::test]
    async fn applies_an_aliased_projection() {
        let (ctx, _dir) = table().await;
        let batch = one_batch(query(&ctx, "SELECT ints AS measurement FROM t").await);

        assert_eq!(batch.schema().field(0).name(), "measurement");
        assert_eq!(batch.column(0).null_count(), 0);
        assert_eq!(
            ints(&batch, 0),
            vec![1, 2, 3, 10, 20],
            "3 rows from entry_a, then 2 from entry_b"
        );
    }

    /// A projection that reorders columns and renames one of them returns both,
    /// in the order asked for and with their own values.
    #[tokio::test]
    async fn applies_a_reordered_projection() {
        let (ctx, _dir) = table().await;
        let batch = one_batch(
            query(
                &ctx,
                "SELECT ints AS measurement, __entry_key AS entry FROM t",
            )
            .await,
        );

        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
            vec!["measurement", "entry"]
        );
        assert_eq!(ints(&batch, 0), vec![1, 2, 3, 10, 20]);

        let entries = batch.column(1).as_string::<i32>();
        assert_eq!(
            (0..5).map(|i| entries.value(i)).collect::<Vec<_>>(),
            vec!["entry_a", "entry_a", "entry_a", "entry_b", "entry_b"]
        );
    }

    /// A computed column is a projection too, and it is pushed down whole.
    #[tokio::test]
    async fn applies_a_computed_projection() {
        let (ctx, _dir) = table().await;
        let batch = one_batch(query(&ctx, "SELECT ints + 1 AS bumped FROM t").await);

        assert_eq!(batch.schema().field(0).name(), "bumped");
        assert_eq!(
            batch
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .values()
                .to_vec(),
            vec![2, 3, 4, 11, 21]
        );
    }

    /// An alias must change nothing but the column's name. A scan that resolved
    /// file columns by the *output* name would silently return a different set
    /// of rows here, which is what #382 reported.
    #[tokio::test]
    async fn an_alias_changes_only_the_name() {
        let (ctx, _dir) = table().await;
        let plain = one_batch(query(&ctx, "SELECT names FROM t").await);
        let aliased = one_batch(query(&ctx, "SELECT names AS label FROM t").await);

        assert_eq!(aliased.schema().field(0).name(), "label");
        assert_eq!(aliased.num_rows(), plain.num_rows());
        assert_eq!(aliased.column(0), plain.column(0));
    }
}
