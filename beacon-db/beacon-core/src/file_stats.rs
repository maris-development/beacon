//! The [`FileAnalyzer`] that connects the file-statistics store to Beacon's
//! formats.
//!
//! [`beacon_file_stats`] deliberately knows nothing about file formats: reading
//! a netCDF file's ranges needs the format layer, which needs DataFusion, and
//! the storage engine stays free of both. This module is the other side of that
//! seam.
//!
//! # What it costs
//!
//! Two opens per file. `infer_schema` names the columns, then `infer_stats`
//! fills them, and for netCDF both read the file. That is the dominant cost of a
//! backfill, and it is also the cost this whole subsystem removes from the
//! *query* path: `FileCollection` scans with `collect_stat(true)`, so today
//! every cold query over a netCDF collection generates these same statistics
//! inline, cached only in a 10 000-entry map that dies on restart.
//!
//! # Formats that yield nothing
//!
//! ODV, TIFF and CSV return `Statistics::new_unknown`, so every column comes
//! back `Absent`. Those files analyze successfully and contribute **zero
//! columns**. That is deliberate: a row with a null range costs bytes and prunes
//! nothing. [`FileAnalysis::columns`] being empty is the signal, and the
//! collector records it so a format that yields nothing is visible rather than
//! silently inert.
//!
//! Zarr is a partial case. A store reports ranges for its rank-0 and rank-1
//! arrays — the coordinates — and unknown for its data grids, so its column
//! count is above zero but well below its column *names*. A store is also a
//! directory, so the listing reports every object in it and only the top-level
//! `zarr.json` has a group behind it; the rest fail, which is why a zarr
//! collection shows a large `failed` count on an otherwise healthy pass.
//!
//! **netCDF joins that list unless the Rust reader is on.** Every netcdf-c call
//! serialises on a process-global mutex and the read is synchronous, so
//! computing ranges under it is serial *and* parks a tokio worker. The format
//! therefore reports unknown unless `use_rust_reader` is set, which is off by
//! default. A netCDF node that prunes nothing is usually this, and
//! `column_count = 0` on its records is how it shows.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::{DataType, SchemaRef};
use beacon_datafusion_ext::format_ext::try_file_format_factory_ext;
use beacon_datafusion_ext::listing_factory::try_listing_factory_from_session;
use beacon_common::FileStatsConfig;
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{
    CollectorConfig, FileAnalysis, FileAnalyzer, FileRecord, FileStatsError, FileStatsStore,
    ObservedFile, StatsCollector,
};
use chrono::TimeZone;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore, path::Path};

use crate::statement_plan::{SessionCell, upgrade_session};

/// Reads one file's statistics through Beacon's format registry.
pub struct FormatFileAnalyzer {
    /// Weak, like every other holder: the session owns the runtime that owns
    /// the collector that owns this.
    session: SessionCell,
    /// The store bare dataset paths resolve against.
    datasets_url: ObjectStoreUrl,
}

impl FormatFileAnalyzer {
    pub fn new(session: SessionCell, datasets_url: ObjectStoreUrl) -> Self {
        Self {
            session,
            datasets_url,
        }
    }

    fn session(&self) -> Result<Arc<SessionContext>, FileStatsError> {
        upgrade_session(&self.session, "file statistics analyzer")
            .map_err(|e| FileStatsError::Format(e.to_string()))
    }
}

#[async_trait::async_trait]
impl FileAnalyzer for FormatFileAnalyzer {
    async fn analyze(&self, record: &FileRecord) -> beacon_file_stats::Result<FileAnalysis> {
        let session = self.session()?;
        let state = session.state();

        let object = object_meta(record)?;
        let store = state
            .runtime_env()
            .object_store(&self.datasets_url)
            .map_err(|e| FileStatsError::Format(format!("datasets store unavailable: {e}")))?;

        let (format_name, format) = resolve_format(&session, &self.datasets_url, &object)?;

        // The file's *own* schema, not the table's. `column_statistics` is
        // positional against whatever schema is passed, so handing over a merged
        // table schema would make every file report every column in the
        // collection. At 160K column names that is the dense matrix this crate
        // exists to avoid.
        let schema = format
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .map_err(|e| {
                FileStatsError::Format(format!("schema for {}: {e}", record.path))
            })?;

        let statistics = format
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .map_err(|e| {
                FileStatsError::Format(format!("statistics for {}: {e}", record.path))
            })?;

        Ok(to_analysis(&format_name, &schema, &statistics))
    }
}

/// Build the object metadata from the registry record.
///
/// No `head` call: the record already carries size, last-modified and etag,
/// because a listing supplied them and the registry kept them to decide whether
/// the file changed. A backfill over a million files does not need a million
/// extra round trips to learn what it already knows.
fn object_meta(record: &FileRecord) -> Result<ObjectMeta, FileStatsError> {
    let location = Path::parse(&record.path)
        .map_err(|e| FileStatsError::Format(format!("bad path {}: {e}", record.path)))?;
    let last_modified = chrono::Utc
        .timestamp_millis_opt(record.last_modified_millis)
        .single()
        .unwrap_or_else(chrono::Utc::now);
    Ok(ObjectMeta {
        location,
        last_modified,
        size: record.size,
        e_tag: record.e_tag.clone(),
        version: None,
    })
}

/// Resolve the format for one object, honouring native readers.
///
/// Not `factory.default()`. A netCDF format read through netcdf-c carries a
/// resolver that turns an object into a local path, and only
/// `create_with_native_root` can build it, because `create` has no location to
/// build it from. Taking the default would hand back a format that cannot open
/// the file it was asked about.
fn resolve_format(
    session: &Arc<SessionContext>,
    datasets_url: &ObjectStoreUrl,
    object: &ObjectMeta,
) -> Result<(String, Arc<dyn FileFormat>), FileStatsError> {
    let state = session.state();
    let key = format_key(object).ok_or_else(|| {
        FileStatsError::Format(format!("no file extension on {}", object.location))
    })?;

    let factory = try_file_format_factory_ext(&state, &key).ok_or_else(|| {
        FileStatsError::Format(format!("no format registered for '{key}'"))
    })?;
    let name = factory.file_format_name();

    let listing = try_listing_factory_from_session(&state).ok_or_else(|| {
        FileStatsError::Format("the session has no listing factory".to_string())
    })?;
    let url = ListingTableUrl::parse(format!("{}{}", datasets_url.as_str(), object.location))
        .map_err(|e| FileStatsError::Format(format!("bad listing url: {e}")))?;

    let format = factory
        .create_with_native_root(&state, &HashMap::new(), &url, &listing)
        .map_err(|e| FileStatsError::Format(format!("cannot open {}: {e}", object.location)))?;
    Ok((name, format))
}

/// The registry key for an object: its extension, with Zarr's metadata file
/// special-cased the way the rest of Beacon special-cases it.
fn format_key(object: &ObjectMeta) -> Option<String> {
    let extension = object.location.extension()?;
    if extension == "json"
        && object
            .location
            .filename()
            .is_some_and(|name| name.starts_with("zarr"))
    {
        return Some("zarr".to_string());
    }
    Some(extension.to_string())
}

/// Turn DataFusion's positional statistics into named per-column ranges.
fn to_analysis(format: &str, schema: &SchemaRef, statistics: &Statistics) -> FileAnalysis {
    let num_rows = statistics.num_rows.get_value().map(|n| *n as u64);
    let total_byte_size = statistics.total_byte_size.get_value().map(|n| *n as u64);

    let columns = schema
        .fields()
        .iter()
        .zip(&statistics.column_statistics)
        .filter_map(|(field, column)| {
            to_column_stat(column, field.data_type(), num_rows)
                .map(|stat| (field.name().clone(), stat))
        })
        .collect();

    FileAnalysis {
        format: format.to_string(),
        num_rows,
        total_byte_size,
        columns,
    }
}

/// One column's range, or `None` when it carries nothing worth storing.
///
/// # On unknown counts
///
/// `null_count` and `row_count` pass through as `Option`, and an absent one is
/// stored as null rather than zero. DataFusion prunes `IS NOT NULL` on
/// `null_count != row_count`; a pair of zeroes means "every value is null" and
/// would drop the file.
///
/// # On `Precision`
///
/// `get_value` accepts `Exact` and `Inexact` alike, which is what DataFusion's
/// own pruning consumers do. That is only sound while `Inexact` means a *widened*
/// estimate: a min above the true minimum, or a max below the true maximum,
/// would silently drop rows. Every format Beacon reads ranges from today
/// (netCDF, Parquet) derives them from real data or real file metadata, so they
/// are bounds. A format that starts narrowing its estimates would need this
/// tightened to `Exact` only.
fn to_column_stat(
    column: &ColumnStatistics,
    data_type: &DataType,
    num_rows: Option<u64>,
) -> Option<ColumnStat> {
    let min = column.min_value.get_value();
    let max = column.max_value.get_value();

    // A column with no range prunes nothing, and a row storing two nulls costs
    // 40 bytes to say so. Formats returning `new_unknown` land here for every
    // column, and contribute no rows at all.
    if min.is_none() && max.is_none() {
        return None;
    }

    let min = min.and_then(|value| value.to_array().ok());
    let max = max.and_then(|value| value.to_array().ok());

    Some(ColumnStat::from_arrays(
        min.as_ref(),
        max.as_ref(),
        // Absent stays absent. Writing an unknown count as zero makes
        // `null_count != row_count` false, and DataFusion prunes `IS NOT NULL`
        // on exactly that, so it would drop files full of values.
        column.null_count.get_value().map(|n| *n as u64),
        num_rows,
        data_type,
    ))
}


// ── the background service ──────────────────────────────────────────────────

/// Shared, late-filled handle to the service, registered as a session extension
/// so `ANALYZE FILES` can reach it. The same pattern the crawler manager uses,
/// and for the same reason: the session owns the runtime that owns this.
pub type FileStatsServiceHandle = Arc<std::sync::OnceLock<Arc<FileStatsService>>>;

/// Create an empty handle to register as a session extension.
pub fn new_file_stats_service_handle() -> FileStatsServiceHandle {
    Arc::new(std::sync::OnceLock::new())
}

/// Owns the store and the collector, and drives them on a timer.
///
/// One pass discovers, then analyzes:
///
/// 1. **Discover.** List the datasets store and register what it reports. New
///    paths get an id and join the queue; changed ones go stale and rejoin it.
///    Listing is streamed in chunks so a store of a million files never has to be
///    held whole.
/// 2. **Analyze.** Drain the queue into segments, bounded by `batch_files` per
///    pass so one tick cannot run away with the machine.
///
/// Discovery here only ever adds or updates. A listing reports what is there,
/// never what is gone, so tombstoning a deleted file needs
/// [`Registry::reconcile_prefix`], which must see a complete listing for its
/// prefix and so does not belong on a chunked hot path. It is called explicitly
/// instead.
///
/// The background task holds a [`Weak`] back to the service, so dropping the
/// runtime stops it. [`Drop`] aborts it outright.
pub struct FileStatsService {
    store: Arc<FileStatsStore>,
    collector: StatsCollector,
    session: SessionCell,
    datasets_url: ObjectStoreUrl,
    config: FileStatsConfig,
    task: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl FileStatsService {
    pub fn new(
        store: Arc<FileStatsStore>,
        analyzer: Arc<dyn FileAnalyzer>,
        session: SessionCell,
        datasets_url: ObjectStoreUrl,
        config: FileStatsConfig,
    ) -> Arc<Self> {
        let collector = StatsCollector::new(
            store.clone(),
            analyzer,
            CollectorConfig {
                batch_files: config.batch_files,
                concurrency: config.concurrency,
                target_group_files: config.target_group_files,
                min_group_files: config.min_group_files,
                prefix_depth: config.prefix_depth,
            },
        );
        Arc::new(Self {
            store,
            collector,
            session,
            datasets_url,
            config,
            task: parking_lot::Mutex::new(None),
        })
    }

    pub fn store(&self) -> &Arc<FileStatsStore> {
        &self.store
    }

    /// Start the timer. The first pass runs one interval from now, so startup is
    /// not competing with a backfill.
    pub fn start(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                let Some(service) = weak.upgrade() else {
                    break; // the runtime went away
                };
                if let Err(error) = service.run_once().await {
                    tracing::warn!(%error, "a file statistics pass failed");
                }
            }
        });
        *self.task.lock() = Some(handle);
    }

    /// Run to completion now, rather than waiting for the timer.
    ///
    /// Backing `ANALYZE FILES`. The timer takes `batch_files` every
    /// `interval_secs`, so a fresh store is hours from being useful; this drains
    /// it, optionally over one prefix.
    ///
    /// `force` re-queues files that are already analyzed. Nothing else does, and
    /// without it a reader that has only just become able to produce ranges
    /// (netCDF's, after `use_rust_reader`) leaves every file recorded as analyzed
    /// with nothing in it.
    pub async fn analyze_now(
        &self,
        prefix: Option<&str>,
        force: bool,
    ) -> anyhow::Result<AnalyzePass> {
        let requeued = if force {
            self.store.registry().requeue(prefix)?
        } else {
            0
        };

        let discovered = self.discover_under(prefix).await?;
        let report = self
            .collector
            .run_until_idle(MAX_ON_DEMAND_PASSES)
            .await
            .map_err(|e| anyhow::anyhow!("file statistics collection failed: {e}"))?;

        Ok(AnalyzePass {
            discovered,
            requeued,
            analyzed: report.analyzed,
            failed: report.failed,
            segments: report.segments,
            pending: self.store.registry().num_pending().unwrap_or(0),
        })
    }

    /// Discover, then analyze. Exposed so a caller can force a pass.
    pub async fn run_once(&self) -> anyhow::Result<FileStatsPass> {
        let discovered = self.discover().await?;
        let report = self
            .collector
            .run_once()
            .await
            .map_err(|e| anyhow::anyhow!("file statistics collection failed: {e}"))?;

        let pass = FileStatsPass {
            discovered,
            analyzed: report.analyzed,
            failed: report.failed,
            segments: report.segments,
            pending: self.store.registry().num_pending().unwrap_or(0),
        };
        if !report.is_idle() || discovered > 0 {
            tracing::info!(
                discovered = pass.discovered,
                analyzed = pass.analyzed,
                failed = pass.failed,
                segments = pass.segments,
                pending = pass.pending,
                "file statistics pass"
            );
        }
        Ok(pass)
    }

    /// Register everything the datasets store currently lists, in chunks.
    async fn discover(&self) -> anyhow::Result<usize> {
        self.discover_under(None).await
    }

    /// The same, restricted to a prefix. `None` uses the configured scan prefix.
    async fn discover_under(&self, prefix: Option<&str>) -> anyhow::Result<usize> {
        let session = self.session()?;
        let store = session
            .state()
            .runtime_env()
            .object_store(&self.datasets_url)
            .map_err(|e| anyhow::anyhow!("datasets store unavailable: {e}"))?;

        let scan_prefix = prefix.unwrap_or(self.config.scan_prefix.as_str());
        let prefix = (!scan_prefix.is_empty()).then(|| Path::from(scan_prefix));
        let mut listing = store.list(prefix.as_ref());

        let mut batch: Vec<ObservedFile> = Vec::with_capacity(self.config.discovery_chunk);
        let mut total = 0usize;
        while let Some(entry) = listing.next().await {
            let meta = match entry {
                Ok(meta) => meta,
                Err(error) => {
                    tracing::warn!(%error, "listing the datasets store failed part-way");
                    break;
                }
            };
            batch.push(
                ObservedFile::new(
                    meta.location.as_ref(),
                    meta.size,
                    meta.last_modified.timestamp_millis(),
                )
                .with_e_tag(meta.e_tag.clone()),
            );
            if batch.len() >= self.config.discovery_chunk {
                total += batch.len();
                self.store.registry().intern_files(&batch)?;
                batch.clear();
            }
        }
        if !batch.is_empty() {
            total += batch.len();
            self.store.registry().intern_files(&batch)?;
        }
        Ok(total)
    }

    fn session(&self) -> anyhow::Result<Arc<SessionContext>> {
        upgrade_session(&self.session, "file statistics service")
    }
}

impl Drop for FileStatsService {
    fn drop(&mut self) {
        if let Some(task) = self.task.lock().take() {
            task.abort();
        }
    }
}

/// How many passes `analyze_now` will run before giving up.
///
/// A bound rather than a loop: a file that fails, re-queues and fails again
/// would otherwise trap the statement forever.
const MAX_ON_DEMAND_PASSES: usize = 10_000;

/// What `ANALYZE FILES` did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnalyzePass {
    pub discovered: usize,
    /// Files put back on the queue by `FORCE`.
    pub requeued: usize,
    pub analyzed: usize,
    pub failed: usize,
    pub segments: usize,
    pub pending: u64,
}

/// What one pass of the service did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FileStatsPass {
    /// Files the listing reported, new or already known.
    pub discovered: usize,
    pub analyzed: usize,
    pub failed: usize,
    pub segments: usize,
    /// Files still awaiting analysis when the pass ended.
    pub pending: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, Schema};
    use datafusion::common::stats::Precision;
    use datafusion::scalar::ScalarValue;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, true),
            Field::new("PSAL", DataType::Float64, true),
        ]))
    }

    fn known(min: f64, max: f64, nulls: usize) -> ColumnStatistics {
        ColumnStatistics {
            null_count: Precision::Exact(nulls),
            max_value: Precision::Exact(ScalarValue::Float64(Some(max))),
            min_value: Precision::Exact(ScalarValue::Float64(Some(min))),
            ..Default::default()
        }
    }

    #[test]
    fn named_ranges_come_out_positionally_matched_to_the_schema() {
        let statistics = Statistics {
            num_rows: Precision::Exact(1_000),
            total_byte_size: Precision::Exact(4_096),
            column_statistics: vec![known(0.0, 10.0, 3), known(34.0, 35.0, 0)],
        };

        let analysis = to_analysis("netcdf", &schema(), &statistics);
        assert_eq!(analysis.format, "netcdf");
        assert_eq!(analysis.num_rows, Some(1_000));
        assert_eq!(analysis.total_byte_size, Some(4_096));

        let names: Vec<&str> = analysis.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["TEMP", "PSAL"]);
        assert_eq!(analysis.columns[0].1.null_count, Some(3));
        assert_eq!(analysis.columns[0].1.row_count, Some(1_000));
    }

    /// A format returning `new_unknown` must analyze cleanly and contribute
    /// nothing. This is ODV, Zarr, TIFF and CSV today.
    #[test]
    fn a_format_with_no_statistics_yields_no_columns() {
        let statistics = Statistics::new_unknown(&schema());
        let analysis = to_analysis("odv", &statistics_schema(), &statistics);
        assert!(
            analysis.columns.is_empty(),
            "absent ranges must not become rows"
        );
        assert_eq!(analysis.num_rows, None);
    }

    fn statistics_schema() -> SchemaRef {
        schema()
    }

    /// One known column beside one absent one keeps the known and drops the
    /// other, rather than padding the file out to the schema's width.
    #[test]
    fn absent_columns_are_dropped_individually() {
        let statistics = Statistics {
            num_rows: Precision::Exact(10),
            total_byte_size: Precision::Absent,
            column_statistics: vec![known(1.0, 2.0, 0), ColumnStatistics::default()],
        };

        let analysis = to_analysis("parquet", &schema(), &statistics);
        assert_eq!(analysis.columns.len(), 1);
        assert_eq!(analysis.columns[0].0, "TEMP");
        assert_eq!(analysis.total_byte_size, None);
    }

    /// Inexact bounds are accepted, matching DataFusion's own consumers.
    #[test]
    fn inexact_bounds_are_still_bounds() {
        let statistics = Statistics {
            num_rows: Precision::Inexact(50),
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Absent,
                    max_value: Precision::Inexact(ScalarValue::Float64(Some(9.0))),
                    min_value: Precision::Inexact(ScalarValue::Float64(Some(1.0))),
                    ..Default::default()
                },
                ColumnStatistics::default(),
            ],
        };

        let analysis = to_analysis("parquet", &schema(), &statistics);
        assert_eq!(analysis.columns.len(), 1);
        assert_eq!(analysis.num_rows, Some(50));
        // Absent stays absent. Zero would mean "no nulls", and paired with an
        // absent row count that reads as "all values null" to the pruning engine.
        assert_eq!(analysis.columns[0].1.null_count, None);
        assert_eq!(analysis.columns[0].1.row_count, Some(50));
    }

    // ── against a real file ────────────────────────────────────────────

    /// Parquet keeps min/max in its footer, so a range costs a metadata read
    /// rather than a scan. This drives the real format end to end and asserts
    /// the ranges survive the mapping into `FileAnalysis`.
    #[tokio::test]
    async fn parquet_yields_real_ranges_through_the_analyzer() {
        use arrow::array::{Float64Array, Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{Field, Schema};
        use datafusion::datasource::file_format::FileFormat;
        use datafusion::execution::session_state::SessionStateBuilder;
        use datafusion::parquet::arrow::ArrowWriter;
        use datafusion::prelude::SessionContext;

        let dir = tempfile::tempdir().unwrap();
        let file_path = dir.path().join("obs.parquet");

        let file_schema = Arc::new(Schema::new(vec![
            Field::new("TEMP", DataType::Float64, false),
            Field::new("DEPTH", DataType::Int64, false),
            Field::new("PLATFORM", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            file_schema.clone(),
            vec![
                Arc::new(Float64Array::from(vec![3.5, 18.25, 7.0])),
                Arc::new(Int64Array::from(vec![10, 4000, 250])),
                Arc::new(StringArray::from(vec!["argo", "ctd", "buoy"])),
            ],
        )
        .unwrap();
        {
            let file = std::fs::File::create(&file_path).unwrap();
            let mut writer = ArrowWriter::try_new(file, file_schema.clone(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }

        let store: Arc<dyn object_store::ObjectStore> =
            Arc::new(object_store::local::LocalFileSystem::new());
        let location = Path::from_absolute_path(&file_path).unwrap();
        let file_meta = std::fs::metadata(&file_path).unwrap();
        let object = ObjectMeta {
            location,
            last_modified: file_meta.modified().map(Into::into).unwrap_or_default(),
            size: file_meta.len(),
            e_tag: None,
            version: None,
        };

        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new().with_default_features().build(),
        );
        let state = ctx.state();
        let format = beacon_arrow_parquet::datafusion::ParquetFormat::new();

        let schema = format
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .expect("parquet infers a schema");
        let statistics = format
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .expect("parquet reports statistics from its footer");

        let analysis = to_analysis("parquet", &schema, &statistics);

        assert_eq!(analysis.num_rows, Some(3));
        let names: Vec<&str> = analysis.columns.iter().map(|(n, _)| n.as_str()).collect();
        assert!(
            names.contains(&"TEMP") && names.contains(&"DEPTH"),
            "parquet must yield ranges for its numeric columns, got {names:?}"
        );
        // Parquet keeps byte-array bounds too, and the segment format stores
        // them, so a predicate on a platform or station name prunes as well.
        assert!(
            names.contains(&"PLATFORM"),
            "parquet string bounds should survive the mapping, got {names:?}"
        );

        // And the ranges are the real ones, not placeholders.
        let temp = &analysis
            .columns
            .iter()
            .find(|(name, _)| name == "TEMP")
            .unwrap()
            .1;
        assert_eq!(temp.min, beacon_file_stats::StatScalar::F64(3.5));
        assert_eq!(temp.max, beacon_file_stats::StatScalar::F64(18.25));
        assert_eq!(temp.row_count, Some(3));
    }

    #[test]
    fn zarr_metadata_files_resolve_to_the_zarr_format() {
        let meta = |path: &str| ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        };
        assert_eq!(format_key(&meta("a/zarr.json")).as_deref(), Some("zarr"));
        assert_eq!(format_key(&meta("a/other.json")).as_deref(), Some("json"));
        assert_eq!(format_key(&meta("a/b.nc")).as_deref(), Some("nc"));
        assert_eq!(format_key(&meta("a/noext")), None);
    }

    /// The record already holds everything an `ObjectMeta` needs, so a backfill
    /// never re-stats a million files to learn what the listing told it.
    #[test]
    fn object_metadata_comes_from_the_record() {
        let mut record = FileRecord::pending("argo/2024/0.nc", 4096, 1_700_000_000_000);
        record.e_tag = Some("abc".into());

        let meta = object_meta(&record).unwrap();
        assert_eq!(meta.location.as_ref(), "argo/2024/0.nc");
        assert_eq!(meta.size, 4096);
        assert_eq!(meta.e_tag.as_deref(), Some("abc"));
        assert_eq!(meta.last_modified.timestamp_millis(), 1_700_000_000_000);
    }
}
