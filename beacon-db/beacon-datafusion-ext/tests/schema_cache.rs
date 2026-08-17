//! What the schema cache does to a table's inference, end to end.
//!
//! The format here is a fake, and deliberately so. The question these tests ask
//! is "how many files did inference open", which a real format answers slowly
//! and a counting one answers exactly. Whether netCDF reads a variable correctly
//! is the format layer's problem, and it has its own tests.
//!
//! Each object's bytes *are* its column name, so a schema is a fact about a
//! file's content and a rewrite is visible in the answer. That is what lets
//! these assert the cache never serves a schema for content that is gone.

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::format_ext::{
    DatasetMetadata, FileFormatFactoryExt, FileFormatRegistry, SchemaOptions,
    new_file_format_registry_handle,
};
use beacon_datafusion_ext::type_widening::ArrowTypeWidening;
use beacon_file_stats::{FileKey, FileStatsStore, Registry, SchemaCache, stamp_object};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{GetExt, Statistics, not_impl_err};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::{FileFormat, FileFormatFactory};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, FileSource};
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path};

const STORE_URL: &str = "test://schemas/";

// ── the fake format ─────────────────────────────────────────────────────────

/// A format whose schema is the file's own bytes, and which counts its opens.
///
/// `variant` stands in for a per-table option, so two tables that read the same
/// file differently can be told apart. netCDF's `read_dimensions` is the real
/// thing this models.
#[derive(Debug)]
struct CountingFormat {
    opens: Arc<AtomicUsize>,
    variant: &'static str,
    /// Everything a scan needs, which these tests never build. Delegated rather
    /// than reimplemented.
    inner: datafusion::datasource::file_format::arrow::ArrowFormat,
}

#[async_trait::async_trait]
impl FileFormat for CountingFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_ext(&self) -> String {
        "counted".to_string()
    }

    fn get_ext_with_compression(
        &self,
        _compression: &FileCompressionType,
    ) -> datafusion::error::Result<String> {
        Ok(self.get_ext())
    }

    fn compression_type(&self) -> Option<FileCompressionType> {
        None
    }

    /// One column per object. The bytes of the object give the column name. The
    /// merge uses the widening rule of the session, as every Beacon format does.
    async fn infer_schema(
        &self,
        state: &dyn Session,
        store: &Arc<dyn ObjectStore>,
        objects: &[ObjectMeta],
    ) -> datafusion::error::Result<SchemaRef> {
        let mut schemas = Vec::with_capacity(objects.len());
        for object in objects {
            self.opens.fetch_add(1, Ordering::SeqCst);
            let bytes = store.get(&object.location).await?.bytes().await?;
            let column = String::from_utf8_lossy(&bytes).into_owned();
            schemas.push(Arc::new(Schema::new(vec![Field::new(
                format!("{column}{}", self.variant),
                DataType::Float64,
                true,
            )])));
        }
        if schemas.is_empty() {
            return Ok(Arc::new(Schema::empty()));
        }
        beacon_datafusion_ext::type_widening::session_widening(state)
            .merge_schemas(&schemas)
            .map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))
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
        _conf: FileScanConfig,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("these tests never scan")
    }

    fn file_source(&self, table_schema: TableSchema) -> Arc<dyn FileSource> {
        self.inner.file_source(table_schema)
    }
}

/// Builds [`CountingFormat`]s, and decides whether they may be cached.
#[derive(Debug)]
struct CountingFactory {
    opens: Arc<AtomicUsize>,
    variant: &'static str,
    /// False makes `schema_options_fingerprint` answer `None`, which is how a
    /// format stays out of the cache.
    cacheable: bool,
}

impl CountingFactory {
    fn format(&self) -> Arc<dyn FileFormat> {
        Arc::new(CountingFormat {
            opens: Arc::clone(&self.opens),
            variant: self.variant,
            inner: datafusion::datasource::file_format::arrow::ArrowFormat,
        })
    }

    /// The key the collector would have written under.
    fn fingerprint(&self) -> u64 {
        SchemaOptions::new("counted").str(self.variant).finish()
    }
}

impl GetExt for CountingFactory {
    fn get_ext(&self) -> String {
        "counted".to_string()
    }
}

impl FileFormatFactory for CountingFactory {
    fn create(
        &self,
        _state: &dyn Session,
        _options: &HashMap<String, String>,
    ) -> datafusion::error::Result<Arc<dyn FileFormat>> {
        Ok(self.format())
    }

    fn default(&self) -> Arc<dyn FileFormat> {
        self.format()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FileFormatFactoryExt for CountingFactory {
    fn discover_datasets(
        &self,
        _objects: &[ObjectMeta],
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        Ok(Vec::new())
    }

    fn file_format_name(&self) -> String {
        "counted".to_string()
    }

    fn schema_options_fingerprint(&self, _format: &dyn FileFormat) -> Option<u64> {
        self.cacheable.then(|| self.fingerprint())
    }
}

// ── the fixture ─────────────────────────────────────────────────────────────

struct Fixture {
    ctx: SessionContext,
    stats: Arc<FileStatsStore>,
    objects: Arc<InMemory>,
    opens: Arc<AtomicUsize>,
    factory: Arc<CountingFactory>,
    _dir: tempfile::TempDir,
}

impl Fixture {
    async fn new() -> Self {
        Self::with(true, "").await
    }

    async fn with(cacheable: bool, variant: &'static str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(Registry::open(dir.path().join("registry.redb")).unwrap());
        let segments: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let stats = Arc::new(
            FileStatsStore::open(registry, segments, Path::from("segments"))
                .await
                .unwrap(),
        );

        let opens = Arc::new(AtomicUsize::new(0));
        let factory = Arc::new(CountingFactory {
            opens: Arc::clone(&opens),
            variant,
            cacheable,
        });

        let stats_handle = beacon_file_stats::new_file_stats_handle();
        stats_handle.set(Arc::clone(&stats)).ok();
        let formats = new_file_format_registry_handle();
        formats
            .set(FileFormatRegistry::new(&[
                Arc::clone(&factory) as Arc<dyn FileFormatFactoryExt>
            ]))
            .ok();

        let config = SessionConfig::new()
            .with_extension(stats_handle)
            .with_extension(formats)
            .with_extension(ArrowTypeWidening::default_extension());
        let ctx = SessionContext::new_with_config(config);

        let objects = Arc::new(InMemory::new());
        ctx.runtime_env().register_object_store(
            ObjectStoreUrl::parse(STORE_URL).unwrap().as_ref(),
            Arc::clone(&objects) as Arc<dyn ObjectStore>,
        );

        Self {
            ctx,
            stats,
            objects,
            opens,
            factory,
            _dir: dir,
        }
    }

    /// Write an object whose bytes name its column.
    async fn put(&self, path: &str, column: &str) -> ObjectMeta {
        self.objects
            .put(&Path::from(path), column.as_bytes().to_vec().into())
            .await
            .unwrap();
        self.objects.head(&Path::from(path)).await.unwrap()
    }

    /// Record what a collector pass would have recorded for `paths`.
    async fn analyze(&self, paths: &[&str]) {
        let mut entries = Vec::new();
        for path in paths {
            let object = self.objects.head(&Path::from(*path)).await.unwrap();
            let bytes = self
                .objects
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let column = String::from_utf8_lossy(&bytes).into_owned();
            entries.push((
                FileKey::new(STORE_URL, path, self.factory.fingerprint()),
                stamp_object(
                    object.size,
                    object.last_modified.timestamp_millis(),
                    object.e_tag.as_deref(),
                ),
                Arc::new(Schema::new(vec![Field::new(
                    format!("{column}{}", self.factory.variant),
                    DataType::Float64,
                    true,
                )])) as SchemaRef,
            ));
        }
        self.cache().put_file_schemas(&entries).unwrap();
    }

    fn cache(&self) -> &Arc<SchemaCache> {
        self.stats.schema_cache()
    }

    async fn table(&self, urls: &[&str]) -> FastObjectTable {
        let urls = urls
            .iter()
            .map(|url| ListingTableUrl::parse(url).unwrap())
            .collect();
        FastObjectTable::try_new(&self.ctx.state(), self.factory.format(), urls)
            .await
            .unwrap()
    }

    fn opens(&self) -> usize {
        self.opens.load(Ordering::SeqCst)
    }

    fn take_opens(&self) -> usize {
        self.opens.swap(0, Ordering::SeqCst)
    }
}

fn names(schema: &Schema) -> Vec<String> {
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

// ── the tests ───────────────────────────────────────────────────────────────

/// The headline. A collection the collector has been over costs no opens at
/// all, and reports the same schema it did when it was inferred.
#[tokio::test]
async fn an_analysed_collection_opens_no_files() {
    let fixture = Fixture::new().await;
    fixture.put("argo/a", "TEMP").await;
    fixture.put("argo/b", "PSAL").await;

    let cold = fixture.table(&["test://schemas/argo/"]).await;
    assert_eq!(fixture.take_opens(), 2, "nothing is cached yet");
    assert_eq!(names(&cold.schema()), vec!["TEMP", "PSAL"]);

    fixture.analyze(&["argo/a", "argo/b"]).await;

    let warm = fixture.table(&["test://schemas/argo/"]).await;
    assert_eq!(fixture.opens(), 0, "every schema came from the cache");
    assert_eq!(warm.schema(), cold.schema());
    assert_eq!(fixture.cache().counters().hits, 2);
}

/// The point of interning per file rather than per table: a collection that
/// gained one file pays for one file.
#[tokio::test]
async fn a_new_file_costs_one_inference() {
    let fixture = Fixture::new().await;
    for i in 0..5 {
        fixture.put(&format!("argo/{i}"), "TEMP").await;
    }
    fixture
        .analyze(&["argo/0", "argo/1", "argo/2", "argo/3", "argo/4"])
        .await;

    fixture.put("argo/5", "PSAL").await;
    let table = fixture.table(&["test://schemas/argo/"]).await;

    assert_eq!(fixture.opens(), 1, "only the file nobody has analysed");
    assert_eq!(names(&table.schema()), vec!["TEMP", "PSAL"]);
}

/// A rewritten file must not answer with the schema of bytes that are gone.
/// This is the one failure that would make the cache wrong rather than slow.
#[tokio::test]
async fn a_rewritten_file_is_inferred_again() {
    let fixture = Fixture::new().await;
    fixture.put("argo/a", "TEMP").await;
    fixture.put("argo/b", "PSAL").await;
    fixture.analyze(&["argo/a", "argo/b"]).await;

    // The file now holds a different column, under the same path.
    fixture.put("argo/a", "SALINITY_LONGER").await;
    fixture.take_opens();

    let table = fixture.table(&["test://schemas/argo/"]).await;
    assert_eq!(fixture.opens(), 1, "the changed file, and only it");
    assert_eq!(names(&table.schema()), vec!["SALINITY_LONGER", "PSAL"]);
}

/// The same bytes read under different options are different schemas. Sharing
/// a key between them would answer one table with the other's columns.
#[tokio::test]
async fn two_option_sets_do_not_share_an_entry() {
    let plain = Fixture::with(true, "").await;
    plain.put("argo/a", "TEMP").await;
    plain.analyze(&["argo/a"]).await;
    let table = plain.table(&["test://schemas/argo/"]).await;
    assert_eq!(plain.opens(), 0);
    assert_eq!(names(&table.schema()), vec!["TEMP"]);

    // A second table over an identically named file, read a different way. Its
    // fingerprint differs, so the entry above cannot answer for it.
    let widened = Fixture::with(true, "_ADJUSTED").await;
    widened.put("argo/a", "TEMP").await;
    plain.analyze(&["argo/a"]).await;
    let table = widened.table(&["test://schemas/argo/"]).await;
    assert_eq!(widened.opens(), 1, "a different reading is a different key");
    assert_eq!(names(&table.schema()), vec!["TEMP_ADJUSTED"]);
}

/// A format that has not opted in behaves exactly as it did before the cache
/// existed: it infers, every time, and nothing is recorded against it.
#[tokio::test]
async fn a_format_that_has_not_opted_in_is_untouched() {
    let fixture = Fixture::with(false, "").await;
    fixture.put("argo/a", "TEMP").await;

    let first = fixture.table(&["test://schemas/argo/"]).await;
    assert_eq!(fixture.take_opens(), 1);
    let second = fixture.table(&["test://schemas/argo/"]).await;
    assert_eq!(fixture.opens(), 1, "inference happens every time");

    assert_eq!(first.schema(), second.schema());
    assert_eq!(fixture.cache().counters().hits, 0);
    assert_eq!(
        fixture.cache().counters().misses,
        0,
        "nothing was even asked"
    );
}

/// Field order is user-visible through `SELECT *`, so a cached table reports the
/// same columns in the same order as an uncached one — across several URLs, and
/// with the listing repeating itself.
#[tokio::test]
async fn field_order_survives_the_cache() {
    let fixture = Fixture::new().await;
    fixture.put("b/1", "BETA").await;
    fixture.put("a/1", "ALPHA").await;
    fixture.put("a/2", "ALPHA").await;
    fixture.put("a/3", "GAMMA").await;

    let urls = ["test://schemas/b/", "test://schemas/a/"];
    let cold = fixture.table(&urls).await;
    assert_eq!(names(&cold.schema()), vec!["BETA", "ALPHA", "GAMMA"]);

    fixture.analyze(&["b/1", "a/1", "a/2", "a/3"]).await;
    let warm = fixture.table(&urls).await;
    assert_eq!(fixture.opens(), 4, "the cold pass, and nothing since");
    assert_eq!(names(&warm.schema()), names(&cold.schema()));
}

/// A session with no file-statistics store has no cache to consult, and must
/// still plan. This is the shape every deployment has by default.
#[tokio::test]
async fn a_session_without_a_store_still_infers() {
    let opens = Arc::new(AtomicUsize::new(0));
    let factory = Arc::new(CountingFactory {
        opens: Arc::clone(&opens),
        variant: "",
        cacheable: true,
    });
    let formats = new_file_format_registry_handle();
    formats
        .set(FileFormatRegistry::new(&[
            Arc::clone(&factory) as Arc<dyn FileFormatFactoryExt>
        ]))
        .ok();
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_extension(formats)
            .with_extension(ArrowTypeWidening::default_extension()),
    );
    let objects = Arc::new(InMemory::new());
    objects
        .put(&Path::from("argo/a"), b"TEMP".to_vec().into())
        .await
        .unwrap();
    ctx.runtime_env().register_object_store(
        ObjectStoreUrl::parse(STORE_URL).unwrap().as_ref(),
        Arc::clone(&objects) as Arc<dyn ObjectStore>,
    );

    let table = FastObjectTable::try_new(
        &ctx.state(),
        factory.format(),
        vec![ListingTableUrl::parse("test://schemas/argo/").unwrap()],
    )
    .await
    .unwrap();
    assert_eq!(names(&table.schema()), vec!["TEMP"]);
    assert_eq!(opens.load(Ordering::SeqCst), 1);
}

/// An empty listing is the format's own business. The cache has nothing to key
/// on, so it hands the question straight over.
#[tokio::test]
async fn an_empty_listing_is_left_to_the_format() {
    let fixture = Fixture::new().await;
    let table = fixture.table(&["test://schemas/empty/"]).await;
    assert!(table.schema().fields().is_empty());
    assert_eq!(fixture.opens(), 0, "there was nothing to open");
}
