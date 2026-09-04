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
//! Two opens for each file. `infer_schema` names the columns. `infer_stats`
//! fills them. For netCDF both calls read the file. This is the largest cost of
//! a backfill.
//!
//! A query gets two returns for that cost. It prunes its file list against the
//! ranges. It reads the schema cache instead of each file.
//!
//! A scan does not compute the statistics itself. `FastObjectTable` and
//! `ListingTableFactoryExt` both set `with_collect_stat(false)`. A scan that
//! computed them for each file made a plan over 16k files take minutes. A
//! server with this subsystem off therefore prunes no file.
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
//! **netCDF and HDF5 join that list unless the Rust reader is on.** Every
//! netcdf-c call serialises on a process-global mutex and the read is
//! synchronous, so computing ranges under it is serial *and* parks a tokio
//! worker. The format therefore reports unknown when `use_rust_reader` is off.
//! The flag is on by default. A netCDF node that prunes no file usually has the
//! flag off. `column_count = 0` on its records shows this.
//!
//! `.h5` and `.hdf5` follow the same rule through their own variable. HDF5 owns
//! the identity and picks the reader; on the default `BEACON_HDF5_USE_RUST_READER=true`
//! a file is read by `beacon-arrow-hdf5`, which computes ranges for every rank-0
//! and rank-1 array, in plain HDF5 and NetCDF-4 alike, whatever the extension
//! says. With it off the read goes to netcdf-c and the ranges are unknown.
//! Each pass says so once, through
//! [`FormatFileAnalyzer::report_netcdf_c_once`].

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use arrow::datatypes::{DataType, SchemaRef};
use beacon_arrow_netcdf::datafusion::{NetcdfFormat, ReaderBackend};
use beacon_common::FileStatsConfig;
use beacon_datafusion_ext::format_ext::{try_file_format_factory_ext, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::try_listing_factory_from_session;
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{
    CollectorConfig, FileAnalysis, FileAnalyzer, FileRecord, FileStatsError, FileStatsStore,
    InternedSchema, ObservedFile, StatsCollector,
};
use chrono::TimeZone;
use datafusion::common::{ColumnStatistics, Statistics};
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::{path::Path, ObjectMeta};

use crate::statement_plan::{upgrade_session, SessionCell};

/// A latch that fires once in a pass, however many files ask it.
///
/// Files are analyzed concurrently, so this is atomic rather than a `bool`.
#[derive(Debug, Default)]
struct OncePerPass(std::sync::atomic::AtomicBool);

impl OncePerPass {
    /// True for exactly one caller in a pass, false for the rest.
    fn take(&self) -> bool {
        !self.0.swap(true, std::sync::atomic::Ordering::Relaxed)
    }

    /// Arm it again for the next pass.
    fn reset(&self) {
        self.0.store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

/// Reads one file's statistics through Beacon's format registry.
pub struct FormatFileAnalyzer {
    /// Weak, like every other holder: the session owns the runtime that owns
    /// the collector that owns this.
    session: SessionCell,
    /// The store bare dataset paths resolve against.
    datasets_url: ObjectStoreUrl,
    /// Whether this pass has yet to say that netcdf-c yields no ranges.
    ///
    /// The condition holds for every `.nc`, `.h5` and `.hdf5` file the pass
    /// touches, so it is reported once per pass rather than once per file: a
    /// backfill of a million files would otherwise write a million identical
    /// lines. Re-armed by [`FileAnalyzer::begin_pass`].
    netcdf_c_reason: OncePerPass,
}

impl FormatFileAnalyzer {
    pub fn new(session: SessionCell, datasets_url: ObjectStoreUrl) -> Self {
        Self {
            session,
            datasets_url,
            netcdf_c_reason: OncePerPass::default(),
        }
    }

    fn session(&self) -> Result<Arc<SessionContext>, FileStatsError> {
        upgrade_session(&self.session, "file statistics analyzer")
            .map_err(|e| FileStatsError::Format(e.to_string()))
    }

    /// Say once per pass that this reader records no ranges, and which switch
    /// changes that.
    ///
    /// A `.nc`, `.h5` or `.hdf5` file read through netcdf-c analyzes cleanly and
    /// contributes nothing, so the record looks the same as a file the reader
    /// simply found no ranges in. Naming the reason is the difference between a
    /// node that prunes nothing and a node whose operator knows why.
    fn report_netcdf_c_once(&self, format_name: &str, format: &dyn FileFormat) {
        let Some(netcdf) = format.as_any().downcast_ref::<NetcdfFormat>() else {
            return; // the Rust reader, or a format with no netcdf-c in it
        };
        if netcdf.reader_backend() != ReaderBackend::NetcdfC {
            return;
        }
        if !self.netcdf_c_reason.take() {
            return;
        }
        let switch = rust_reader_switch(format_name);
        tracing::info!(
            format = format_name,
            switch,
            "this pass records no column ranges for {format_name} files: netcdf-c \
             serialises every call on a process-global lock, so statistics need \
             the pure-Rust reader. Set {switch}=true and run ANALYZE FILES FORCE."
        );
    }
}

#[async_trait::async_trait]
impl FileAnalyzer for FormatFileAnalyzer {
    fn begin_pass(&self) {
        tracing::debug!("beginning a file statistics pass");
        self.netcdf_c_reason.reset();
    }

    async fn analyze(&self, record: &FileRecord) -> beacon_file_stats::Result<FileAnalysis> {
        // The entry line is TRACE and the outcome below is DEBUG. A file that
        // hangs shows an entry with no outcome, which is the only thing the entry
        // line tells you that the outcome line does not.
        tracing::trace!(path = record.path.as_str(), "analyzing file");
        let started = std::time::Instant::now();
        let session = self.session()?;
        let state = session.state();

        let object = object_meta(record)?;
        let store = state
            .runtime_env()
            .object_store(&self.datasets_url)
            .map_err(|e| FileStatsError::Format(format!("datasets store unavailable: {e}")))?;

        let (format_name, factory, format) = resolve_format(&session, &self.datasets_url, &object)?;
        self.report_netcdf_c_once(&format_name, format.as_ref());

        // The file's *own* schema, not the table's. `column_statistics` is
        // positional against whatever schema is passed, so handing over a merged
        // table schema would make every file report every column in the
        // collection. At 160K column names that is the dense matrix this crate
        // exists to avoid.
        let schema = format
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .map_err(|e| FileStatsError::Format(format!("schema for {}: {e}", record.path)))?;

        let statistics = format
            .infer_stats(&state, &store, schema.clone(), &object)
            .await
            .map_err(|e| FileStatsError::Format(format!("statistics for {}: {e}", record.path)))?;

        // Keep the schema this analysis just derived. The read is already paid
        // for, and without this every query over the file derives it again — 83%
        // of a netCDF query over a hundred thousand files.
        let mut analysis = to_analysis(&format_name, &schema, &statistics);
        analysis.schema = interned_schema(
            factory.as_ref(),
            format.as_ref(),
            &self.datasets_url,
            record,
            &schema,
        );
        tracing::debug!(
            path = record.path.as_str(),
            format = format_name.as_str(),
            rows = analysis.num_rows,
            // `columns` counts the columns that got a range; `fields` counts the
            // columns the file has. `columns = 0` against a non-zero `fields` is
            // the signature of a reader that records no ranges, so the file
            // analyzes cleanly and prunes nothing.
            columns = analysis.columns.len(),
            fields = schema.fields().len(),
            elapsed_ms = started.elapsed().as_millis() as u64,
            "analyzed a file"
        );
        Ok(analysis)
    }
}

/// The environment switch that turns on the Rust reader for `format`.
///
/// The HDF5 identity delegates its reads to the netCDF format, so an `.h5` file
/// on netcdf-c *is* that format. Name the variable that owns the file at hand
/// anyway: the HDF5 one covers every HDF5 layout, including the plain ones the
/// netCDF data model cannot express.
fn rust_reader_switch(format: &str) -> &'static str {
    // Both spellings the factory answers to: it registers under `hdf5`, and
    // under `h5` as well for `STORED AS H5`.
    if beacon_arrow_hdf5::HDF5_EXTENSIONS.contains(&format) {
        "BEACON_HDF5_USE_RUST_READER"
    } else {
        "BEACON_NETCDF_USE_RUST_READER"
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
) -> Result<(String, Arc<dyn FileFormatFactoryExt>, Arc<dyn FileFormat>), FileStatsError> {
    let state = session.state();
    let key = format_key(object).ok_or_else(|| {
        FileStatsError::Format(format!("no file extension on {}", object.location))
    })?;

    let factory = try_file_format_factory_ext(&state, &key)
        .ok_or_else(|| FileStatsError::Format(format!("no format registered for '{key}'")))?;
    let name = factory.file_format_name();

    let listing = try_listing_factory_from_session(&state)
        .ok_or_else(|| FileStatsError::Format("the session has no listing factory".to_string()))?;
    let url = ListingTableUrl::parse(format!("{}{}", datasets_url.as_str(), object.location))
        .map_err(|e| FileStatsError::Format(format!("bad listing url: {e}")))?;

    // The analysis form: a format built any other way reports unknown
    // statistics, so that a query never pays to compute them. This is the one
    // caller that wants them, and what it finds goes to the store the scan
    // prunes from.
    let format = factory
        .create_for_analysis(&state, &HashMap::new(), &url, &listing)
        .map_err(|e| FileStatsError::Format(format!("cannot open {}: {e}", object.location)))?;
    Ok((name, factory, format))
}

/// The schema-cache entry for a file the analyzer just read, or `None` when the
/// format keeps out of the cache.
///
/// The key must be the one a *query* would build, or the entry is written and
/// never found. Both sides use the store URL and the store-relative path, and
/// both take the fingerprint from the same factory hook.
///
/// The stamp comes from the record, which is what the listing reported. That is
/// also what a query's listing will report while the file is unchanged, so an
/// entry stays valid exactly as long as its file does.
fn interned_schema(
    factory: &dyn FileFormatFactoryExt,
    format: &dyn FileFormat,
    datasets_url: &ObjectStoreUrl,
    record: &FileRecord,
    schema: &SchemaRef,
) -> Option<InternedSchema> {
    let fingerprint = factory.schema_options_fingerprint(format)?;
    Some(InternedSchema {
        key: beacon_file_stats::FileKey::new(datasets_url.as_str(), &record.path, fingerprint),
        stamp: beacon_file_stats::stamp_object(
            record.size,
            record.last_modified_millis,
            record.e_tag.as_deref(),
        ),
        schema: schema.clone(),
    })
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
        // Filled by the caller, which knows the format's cache identity.
        schema: None,
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
/// 2. **Analyze.** Drain the queue into segments, one `batch_files` batch at a
///    time, until nothing is pending. The batch bounds memory, not the pass.
///
/// A pass runs alone. [`Self::pass_lock`] is held for the length of it, so a
/// tick that lands on a running `ANALYZE FILES` is skipped rather than taking
/// the same files off the queue a second time.
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
    config: FileStatsConfig,
    /// The timer, and the startup collection when one was asked for. Both are
    /// aborted on drop.
    tasks: parking_lot::Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Held for the length of a pass, so only one pass runs at a time.
    ///
    /// Three things start a pass: the timer, the startup collection, and
    /// `ANALYZE FILES`. Nothing claims a file when it is taken off the queue, so
    /// two passes at once read the same files and write a segment each for them.
    /// Pruning survives that, because the newest row for a file wins, but every
    /// one of those reads is paid twice.
    ///
    /// An `Arc` of its own, never reached through the service. A pass upgrades
    /// the [`Weak`] once per batch and drops it again, so that the service, and
    /// with it the database file, is not held for the length of a backfill.
    /// Holding a guard that borrowed the service would undo exactly that.
    pass_lock: Arc<tokio::sync::Mutex<()>>,
}

impl FileStatsService {
    /// Takes no datasets store URL. Discovery resolves the scan prefix through
    /// the listing factory on the session, so the store it reads is the store a
    /// query would read.
    pub fn new(
        store: Arc<FileStatsStore>,
        analyzer: Arc<dyn FileAnalyzer>,
        session: SessionCell,
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
                write_schemas: config.schema_cache,
            },
        );
        Arc::new(Self {
            store,
            collector,
            session,
            config,
            tasks: parking_lot::Mutex::new(Vec::new()),
            pass_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub fn store(&self) -> &Arc<FileStatsStore> {
        &self.store
    }

    /// Start the timer, and the startup collection when `on_startup` asks for
    /// one.
    ///
    /// The timer's first pass runs one interval from now, so startup is not
    /// competing with a backfill by default. `on_startup` is the opt-out of that
    /// trade: it collects immediately instead, which is what a short-lived or
    /// frequently restarted server needs, since the interval starts again on
    /// every boot.
    ///
    /// A tick drains the queue. It does not stop after one batch, so a fresh
    /// store is covered by the first pass that reaches it rather than by as many
    /// ticks as it has batches. A tick that finds a pass already running is
    /// skipped.
    pub fn start(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let pass_lock = self.pass_lock.clone();
        let interval = Duration::from_secs(self.config.interval_secs.max(1));
        tracing::debug!(
            interval_secs = interval.as_secs(),
            "file statistics timer started; the first pass runs one interval from now"
        );
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // A pass over a large archive outruns the interval. The default
            // `Burst` then fires every tick it missed back to back, and each one
            // re-lists the whole store for a queue the pass just emptied.
            // `Delay` starts the interval again when the pass ends.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // consume the immediate first tick
            loop {
                ticker.tick().await;
                if weak.strong_count() == 0 {
                    break; // the runtime went away
                }
                // Skipped, not queued: the work is whatever is pending, and the
                // running pass is already draining exactly that. Waiting here
                // would only run a second pass over an empty queue.
                let Ok(guard) = pass_lock.clone().try_lock_owned() else {
                    tracing::debug!(
                        "a file statistics pass is already running; skipping this tick"
                    );
                    continue;
                };
                let counts = run_pass(&weak).await;
                drop(guard);
                match counts {
                    Some(counts) => counts.report(),
                    None => break, // the runtime went away mid-pass
                }
            }
        });
        self.tasks.lock().push(handle);

        if self.config.on_startup {
            self.collect_on_startup();
        }
    }

    /// Collect now, in the background, until the queue is empty.
    ///
    /// What `BEACON_FILE_STATS_ON_STARTUP` turns on. It drains rather than taking
    /// one batch: a restart is exactly when the store is behind, and a single
    /// `batch_files` pass would leave a large archive short until the timer had
    /// ticked its way through the rest.
    ///
    /// Spawned, never awaited, so boot is not held up by it. Queries run against
    /// whatever statistics exist meanwhile; a file with none is read, as it was
    /// before the subsystem existed.
    fn collect_on_startup(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        let pass_lock = self.pass_lock.clone();
        let handle = tokio::spawn(async move {
            tracing::info!("collecting file statistics at startup");
            // Waited for, not skipped: this runs once, and a timer tick that got
            // in first is draining the same queue. The wait ends when it does.
            let guard = pass_lock.lock_owned().await;
            let counts = run_pass(&weak).await;
            drop(guard);
            let Some(counts) = counts else {
                return; // the runtime went away mid-pass
            };
            tracing::info!(
                discovered = counts.discovered,
                analyzed = counts.analyzed,
                failed = counts.failed,
                segments = counts.segments,
                "startup file statistics collection finished"
            );
        });
        self.tasks.lock().push(handle);
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
    ///
    /// Fails when a pass is already running, rather than waiting for it. Only
    /// one pass runs at a time, and waiting for one over a large archive would
    /// hold the statement for minutes with nothing to report.
    pub async fn analyze_now(
        &self,
        prefix: Option<&str>,
        force: bool,
    ) -> anyhow::Result<AnalyzePass> {
        tracing::debug!(prefix = ?prefix, force, "ANALYZE FILES started");
        // Refused, not queued. A pass over a large archive runs for minutes, and
        // a statement that waited for one would look hung for all of it with
        // nothing to show. The running pass covers the same queue, so the answer
        // is to say so and let the caller decide.
        let _guard = self.pass_lock.try_lock().map_err(|_| {
            anyhow::anyhow!(
                "a file statistics pass is already running, over the same files. \
                 Wait for it to finish and run this again. \
                 `SELECT state, count(*) FROM beacon.system.file_stats GROUP BY state` \
                 shows its progress."
            )
        })?;
        let requeued = if force {
            self.store.registry().requeue(prefix)?
        } else {
            0
        };

        let discovered = self.discover_under(prefix).await?;
        let report = self
            .collector
            .run_until_idle(MAX_BATCHES_PER_PASS)
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

    /// Discover, then analyze **one batch**. Exposed so a caller can step the
    /// subsystem by hand, which is what the tests do.
    ///
    /// Neither the timer nor `ANALYZE FILES` goes through here any more: both
    /// drain. This also takes no pass guard, so a caller that runs it beside
    /// either of those reads the same files twice.
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
        PassCounts {
            discovered: pass.discovered,
            analyzed: pass.analyzed,
            failed: pass.failed,
            segments: pass.segments,
            pending: pass.pending,
        }
        .report();
        Ok(pass)
    }

    /// Register everything the datasets store currently lists, in chunks.
    async fn discover(&self) -> anyhow::Result<usize> {
        self.discover_under(None).await
    }

    /// The same, restricted to a prefix. `None` uses the configured scan prefix.
    async fn discover_under(&self, prefix: Option<&str>) -> anyhow::Result<usize> {
        use beacon_datafusion_ext::listing_factory::ListingFactory;

        let session = self.session()?;
        let state = session.state();
        let scan_prefix = prefix.unwrap_or(self.config.scan_prefix.as_str());

        // Through the listing factory rather than the store directly, so the
        // scan prefix resolves by the same rules a query would use: the
        // configured datasets store, and a glob if one is given.
        let factory = state
            .config()
            .get_extension::<ListingFactory>()
            .ok_or_else(|| anyhow::anyhow!("the listing factory is not registered"))?;
        let mut listing = factory
            .listing(&state, scan_prefix)
            .map_err(|e| anyhow::anyhow!("cannot resolve the scan prefix `{scan_prefix}`: {e}"))?
            .stream();

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
        tracing::debug!(
            listed = total,
            scan_prefix,
            pending = self.store.registry().num_pending().unwrap_or(0),
            "listed the datasets store"
        );
        Ok(total)
    }

    fn session(&self) -> anyhow::Result<Arc<SessionContext>> {
        upgrade_session(&self.session, "file statistics service")
    }
}

impl Drop for FileStatsService {
    fn drop(&mut self) {
        for task in self.tasks.lock().drain(..) {
            task.abort();
        }
    }
}

/// How many batches one pass will take before giving up.
///
/// A bound rather than a loop: a file that fails, re-queues and fails again
/// would otherwise trap the pass forever.
const MAX_BATCHES_PER_PASS: usize = 10_000;

/// What one pass did. `None` from [`run_pass`] means the runtime went away.
#[derive(Debug, Default, Clone, Copy)]
struct PassCounts {
    discovered: usize,
    analyzed: usize,
    failed: usize,
    segments: usize,
    pending: u64,
}

impl PassCounts {
    /// `discovered` counts every file the listing reported, not just the new
    /// ones, so it is above zero on every pass over a store that holds anything.
    /// Only work is worth an INFO line; an idle pass is a DEBUG heartbeat, which
    /// is what tells you the timer is still running.
    fn report(&self) {
        if self.analyzed == 0 && self.failed == 0 {
            tracing::debug!(
                discovered = self.discovered,
                pending = self.pending,
                "file statistics pass found nothing to do"
            );
        } else {
            tracing::info!(
                discovered = self.discovered,
                analyzed = self.analyzed,
                failed = self.failed,
                segments = self.segments,
                pending = self.pending,
                "file statistics pass"
            );
        }
    }
}

/// Discover once, then analyze until the queue is empty.
///
/// The caller holds [`FileStatsService::pass_lock`], so this is the only pass
/// running.
///
/// Discovery is once per pass, not once per batch: a listing of a large store is
/// the expensive half, and nothing new appears mid-backfill that the next pass
/// will not pick up.
///
/// The [`Weak`] is upgraded per batch and dropped again. A backfill runs for
/// minutes, and holding the service across all of it would hold the database
/// file with it, so a dropped runtime could never tear this down. Returns `None`
/// when that drop happens, which ends the pass at the next batch boundary.
///
/// [`MAX_BATCHES_PER_PASS`] is a backstop, not the stop condition. A file whose
/// analysis *panics* is neither marked failed nor analyzed, so it stays at the
/// head of the queue and comes back in the next batch. A batch of nothing but
/// such files reports idle, which ends the loop.
async fn run_pass(weak: &Weak<FileStatsService>) -> Option<PassCounts> {
    let mut counts = PassCounts::default();

    counts.discovered = match weak.upgrade()?.discover().await {
        Ok(discovered) => discovered,
        Err(error) => {
            tracing::warn!(%error, "file statistics discovery failed");
            return Some(counts);
        }
    };

    for _ in 0..MAX_BATCHES_PER_PASS {
        let service = weak.upgrade()?;
        match service.collector.run_once().await {
            Ok(report) if report.is_idle() => break,
            Ok(report) => {
                counts.analyzed += report.analyzed;
                counts.failed += report.failed;
                counts.segments += report.segments;
            }
            Err(error) => {
                tracing::warn!(%error, "a file statistics pass failed");
                break;
            }
        }
    }

    counts.pending = weak.upgrade()?.store.registry().num_pending().unwrap_or(0);
    Some(counts)
}

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

    /// The latch behind the once-per-pass reason. Every file of a pass asks it;
    /// exactly one gets to speak, and the next pass re-arms it.
    #[test]
    fn a_pass_reports_a_reason_once_and_the_next_pass_may_report_it_again() {
        let latch = OncePerPass::default();
        assert!(latch.take(), "the first file of a pass reports");
        assert!(!latch.take(), "the second does not");
        assert!(!latch.take());

        latch.reset();
        assert!(latch.take(), "the next pass reports again");
        assert!(!latch.take());
    }

    /// The reason has to name the switch of the format that owns the file. An
    /// `.h5` file on netcdf-c is read by the netCDF format, but the switch to
    /// reach for is the HDF5 one: it covers every HDF5 layout, not only the
    /// NetCDF-4 ones.
    #[test]
    fn the_reason_names_the_switch_of_the_format_that_owns_the_file() {
        assert_eq!(rust_reader_switch("hdf5"), "BEACON_HDF5_USE_RUST_READER");
        assert_eq!(rust_reader_switch("h5"), "BEACON_HDF5_USE_RUST_READER");
        assert_eq!(
            rust_reader_switch("netcdf"),
            "BEACON_NETCDF_USE_RUST_READER"
        );
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
