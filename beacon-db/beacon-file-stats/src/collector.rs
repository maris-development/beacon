//! The background pass that fills the store.
//!
//! # Batching by prefix, not by arrival
//!
//! A batch becomes one segment, and a segment's usefulness depends entirely on
//! how few distinct columns it holds. Files under one path prefix share columns,
//! so a prefix-local segment is skipped outright by any predicate on a column it
//! does not hold. A segment batched by arrival order holds a broad slice of the
//! column space instead, matches nearly every query, and the manifest's skip
//! stops working. `tests/scale.rs` measures both shapes.
//!
//! So [`StatsCollector::run_once`] groups its batch by prefix and emits one
//! segment per group, rather than one segment per batch.
//!
//! # Where the format knowledge lives
//!
//! Not here. Reading a netCDF or Parquet file's statistics needs the format
//! layer, which needs DataFusion, which this crate does not depend on. The
//! collector takes a [`FileAnalyzer`] instead, and Beacon supplies one over its
//! format registry. That also makes the whole pass testable against a fake.
//!
//! # Crash behaviour
//!
//! A segment is committed before its files are marked analyzed. A crash between
//! the two leaves those files pending, so the next pass analyzes them again into
//! a second segment. The reader resolves the duplicate by preferring the newest
//! segment, so the outcome is wasted work, never a wrong answer. The reverse
//! order would lose statistics outright.

use std::collections::BTreeMap;
use std::sync::Arc;

use futures::stream::{self, StreamExt};

use crate::error::Result;
use crate::registry::AnalyzedFile;
use crate::schema_cache::{FileKey, Stamp};
use crate::segment::{ColumnStat, SegmentBuilder};
use crate::store::FileStatsStore;
use crate::types::{FileId, FileRecord, FileState};

/// What one file's analysis produced.
pub struct FileAnalysis {
    /// The format that read it, for diagnostics.
    pub format: String,
    pub num_rows: Option<u64>,
    pub total_byte_size: Option<u64>,
    /// Per-column statistics, by column name. A column the reader found no
    /// range for may be omitted entirely: absent reads as unknown.
    pub columns: Vec<(String, ColumnStat)>,
    /// The file's own schema, to intern.
    ///
    /// The analyzer derives this to position the statistics — `column_statistics`
    /// is positional, so it needs the file's schema, not the table's — and used
    /// to drop it afterwards. Handing it back instead costs one encode and one
    /// write per batch, and no read that was not happening anyway. It is what
    /// stops a query from deriving the same schema from the same file forever.
    ///
    /// `None` when the format has not opted into the cache. See
    /// `FileFormatFactoryExt::schema_options_fingerprint`.
    pub schema: Option<InternedSchema>,
}

/// One file's schema, and what it describes.
///
/// The key says which file and under which format options; the stamp says which
/// version of it, so a query over changed content reads a miss rather than the
/// schema of bytes that are gone.
pub struct InternedSchema {
    pub key: FileKey,
    pub stamp: Stamp,
    pub schema: arrow::datatypes::SchemaRef,
}

/// Reads one file's statistics.
///
/// The seam that keeps this crate free of DataFusion. An implementation lives
/// wherever the format registry does.
#[async_trait::async_trait]
pub trait FileAnalyzer: Send + Sync {
    async fn analyze(&self, record: &FileRecord) -> Result<FileAnalysis>;

    /// Called once per pass, before the first [`Self::analyze`] of that pass.
    ///
    /// The hook an implementation reports a whole-pass condition through. A
    /// reader that cannot produce ranges is true of every file it opens, so
    /// saying so per file would be a million identical lines on a backfill; the
    /// analyzer clears a flag here and logs the reason once.
    fn begin_pass(&self) {}
}

/// How hard the collector works.
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Files taken off the queue per pass. Bounds the builder's memory: a batch
    /// costs roughly `batch_files x columns-per-file` cells.
    pub batch_files: usize,
    /// Files analyzed at once. Analysis is IO bound, and this is the knob that
    /// keeps a background pass from starving queries.
    pub concurrency: usize,
    /// Files a segment should aim to cover.
    ///
    /// The grouping descends the path tree while a coherent group is larger
    /// than this, so segments land near this size wherever the layout allows.
    /// Smaller means narrower segments and sharper skipping for rare columns,
    /// but more segments to read for a column present in all of them.
    pub target_group_files: usize,
    /// Never split a group smaller than this, even across directories.
    ///
    /// A block costs ~112 bytes of framing however few rows it holds, so
    /// splitting a handful of files across several segments pays that many
    /// times over for no skip worth having.
    pub min_group_files: usize,
    /// Fix the grouping at this directory depth instead of deriving it.
    ///
    /// `None` derives it per batch, which is what suits a store whose roots
    /// have different shapes: `argo/f.nc` beside `cmems/2024/01/15/f.nc`. Set
    /// it only when a layout is known and the derivation gets it wrong.
    pub prefix_depth: Option<usize>,
    /// Keep the schema each analysis derives, so a query does not derive it
    /// again.
    ///
    /// On by default. The schema is already computed and already dropped, so
    /// keeping it costs one encode and one write per batch. Turn it off only to
    /// take the cache out of a query's path while leaving statistics on.
    pub write_schemas: bool,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            batch_files: 10_000,
            concurrency: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            target_group_files: 10_000,
            min_group_files: 500,
            prefix_depth: None,
            write_schemas: true,
        }
    }
}

/// What one pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CollectReport {
    pub analyzed: usize,
    pub failed: usize,
    /// Segments committed, which is the number of non-empty prefix groups.
    pub segments: usize,
    pub groups: usize,
    /// Schemas interned. Below `analyzed` when a format has not opted into the
    /// cache, and zero when `write_schemas` is off.
    pub schemas: usize,
}

impl CollectReport {
    /// Whether this pass found anything to do.
    pub fn is_idle(&self) -> bool {
        self.analyzed == 0 && self.failed == 0
    }
}

/// Turns pending files into segments.
pub struct StatsCollector {
    store: Arc<FileStatsStore>,
    analyzer: Arc<dyn FileAnalyzer>,
    config: CollectorConfig,
}

impl StatsCollector {
    pub fn new(
        store: Arc<FileStatsStore>,
        analyzer: Arc<dyn FileAnalyzer>,
        config: CollectorConfig,
    ) -> Self {
        Self {
            store,
            analyzer,
            config,
        }
    }

    pub fn config(&self) -> &CollectorConfig {
        &self.config
    }

    /// Analyze one batch of pending files and commit the segments it produces.
    ///
    /// Returns an idle report when the queue is empty, so a scheduler can back
    /// off. A file whose analysis fails is marked [`FileState::Failed`] and does
    /// not stop the rest of the batch.
    pub async fn run_once(&self) -> Result<CollectReport> {
        let pending = self
            .store
            .registry()
            .next_pending(self.config.batch_files)?;
        if pending.is_empty() {
            tracing::trace!("no files are pending analysis");
            return Ok(CollectReport::default());
        }
        tracing::debug!(
            files = pending.len(),
            batch_files = self.config.batch_files,
            concurrency = self.config.concurrency,
            "taking a batch of pending files"
        );
        // A pass with work in it. An idle one is not a pass, so a server that
        // ticks all night does not repeat a condition nothing acted on.
        self.analyzer.begin_pass();

        // `next_pending` hands out ascending ids, and every grouping below keeps
        // input order within a group, so each group stays ascending -- which is
        // what `SegmentBuilder::push_file` requires.
        let groups = match self.config.prefix_depth {
            Some(depth) => group_at_depth(pending, depth),
            None => group_by_locality(
                pending,
                self.config.target_group_files.max(1),
                self.config.min_group_files,
            ),
        };

        tracing::debug!(
            groups = groups.len(),
            target_group_files = self.config.target_group_files,
            "grouped the batch by prefix; one group becomes one segment"
        );

        let mut report = CollectReport::default();
        for (prefix, files) in groups {
            report.groups += 1;
            self.run_group(&prefix, files, &mut report).await?;
        }
        Ok(report)
    }

    /// Keep running passes until the queue is empty.
    ///
    /// `max_passes` bounds the work so a caller cannot be trapped by a file that
    /// fails, re-queues, and fails again.
    pub async fn run_until_idle(&self, max_passes: usize) -> Result<CollectReport> {
        let mut total = CollectReport::default();
        for _ in 0..max_passes {
            let pass = self.run_once().await?;
            if pass.is_idle() {
                break;
            }
            total.analyzed += pass.analyzed;
            total.failed += pass.failed;
            total.segments += pass.segments;
            total.groups += pass.groups;
            total.schemas += pass.schemas;
        }
        Ok(total)
    }

    async fn run_group(
        &self,
        prefix: &str,
        files: Vec<(FileId, FileRecord)>,
        report: &mut CollectReport,
    ) -> Result<()> {
        // Each analysis is *spawned*, and `buffer_unordered` bounds how many are
        // in flight. Both halves matter, and the first was a real mistake to get
        // wrong: `buffer_unordered` on its own gives concurrency, not
        // parallelism. Every future it holds is polled from one task, so work
        // that is CPU bound between await points runs single-threaded however
        // high the limit is set.
        //
        // Reading a netCDF file's ranges is exactly that shape: fetch, then
        // parse and scan. Measured on eight cores, `buffer_unordered(8)` alone
        // managed 296 files/s against 287 serial. Spawning the same work reached
        // 1193 -- see `statistics_backfill_cost` in beacon-arrow-netcdf.
        tracing::debug!(prefix, files = files.len(), "analyzing a group");

        // The crate's `Result` alias fixes the error type, so the join result is spelled out.
        // The path travels with the outcome: a failure names the file, not an id
        // the operator has no way to look up.
        type Joined =
            std::result::Result<(FileId, String, Result<FileAnalysis>), tokio::task::JoinError>;
        let outcomes: Vec<Joined> = stream::iter(files)
            .map(|(id, record)| {
                let analyzer = self.analyzer.clone();
                tokio::spawn(async move {
                    let outcome = analyzer.analyze(&record).await;
                    (id, record.path, outcome)
                })
            })
            .buffer_unordered(self.config.concurrency)
            .collect()
            .await;

        let mut analyzed: Vec<(FileId, FileAnalysis)> = Vec::with_capacity(outcomes.len());
        let mut failed: Vec<FileId> = Vec::new();
        for outcome in outcomes {
            match outcome {
                Ok((id, _, Ok(analysis))) => analyzed.push((id, analysis)),
                Ok((id, path, Err(error))) => {
                    // A bad file must not stop the batch, and it must leave the
                    // queue: a failure that stays pending is retried forever.
                    tracing::warn!(file_id = id, path, %error, "file statistics analysis failed");
                    failed.push(id);
                }
                Err(error) => {
                    // A panic inside an analysis. The file stays pending rather
                    // than being marked failed, because nothing was learned
                    // about it -- including which file it was.
                    tracing::error!(%error, "a file statistics analysis task panicked");
                }
            }
        }
        report.failed += failed.len();
        self.store
            .registry()
            .set_state_batch(&failed, FileState::Failed)?;

        if analyzed.is_empty() {
            // Every file of the group failed, so there is no segment to write.
            tracing::debug!(
                prefix,
                failed = failed.len(),
                "the group produced no statistics"
            );
            return Ok(());
        }

        // Concurrency returns results out of order; blocks must be sorted.
        analyzed.sort_by_key(|(id, _)| *id);

        let names: Vec<&str> = {
            let mut names: Vec<&str> = analyzed
                .iter()
                .flat_map(|(_, analysis)| analysis.columns.iter().map(|(name, _)| name.as_str()))
                .collect();
            names.sort_unstable();
            names.dedup();
            names
        };
        let ids = self.store.registry().intern_columns(&names)?;
        let column_ids: std::collections::HashMap<&str, u32> =
            names.into_iter().zip(ids).collect();

        let mut builder = SegmentBuilder::new();
        for (file_id, analysis) in &analyzed {
            let stats: Vec<(u32, ColumnStat)> = analysis
                .columns
                .iter()
                .filter_map(|(name, stat)| {
                    column_ids.get(name.as_str()).map(|id| (*id, stat.clone()))
                })
                .collect();
            builder.push_file(*file_id, stats);
        }

        if self.store.commit_segment(builder).await?.is_some() {
            report.segments += 1;
        }

        // The schemas these analyses already derived, in one transaction. A
        // failure here is logged and dropped: the statistics are the pass's
        // real output, and a query that finds no schema derives one, exactly as
        // it did before this cache existed.
        report.schemas += self.intern_schemas(&analyzed);

        // Only after the segment is durable, and in one transaction: a redb
        // commit is an fsync, so per-file marking caps the whole collector at a
        // few hundred files a second however fast the analysis is.
        let marks: Vec<AnalyzedFile<'_>> = analyzed
            .iter()
            .map(|(id, analysis)| AnalyzedFile {
                id: *id,
                format: &analysis.format,
                num_rows: analysis.num_rows,
                total_byte_size: analysis.total_byte_size,
                column_count: analysis.columns.len() as u32,
            })
            .collect();
        self.store.registry().mark_analyzed_batch(&marks)?;
        report.analyzed += marks.len();

        tracing::debug!(
            prefix,
            analyzed = analyzed.len(),
            failed = failed.len(),
            // Columns with a recorded range. Zero here, against a file that has
            // columns, means the reader produced no ranges to prune on.
            columns = column_ids.len(),
            "committed a file statistics segment"
        );
        Ok(())
    }

    /// Keep the schemas this group's analyses derived. Returns how many landed.
    ///
    /// One transaction for the batch, beside the one that marks the files
    /// analyzed. A redb commit is an fsync, so a write per file would cap the
    /// whole collector however fast the analysis is.
    fn intern_schemas(&self, analyzed: &[(FileId, FileAnalysis)]) -> usize {
        if !self.config.write_schemas {
            return 0;
        }
        let entries: Vec<_> = analyzed
            .iter()
            .filter_map(|(_, analysis)| analysis.schema.as_ref())
            .map(|interned| (interned.key, interned.stamp, interned.schema.clone()))
            .collect();
        if entries.is_empty() {
            return 0;
        }
        match self.store.schema_cache().put_file_schemas(&entries) {
            Ok(()) => entries.len(),
            Err(error) => {
                tracing::warn!(%error, "could not intern this batch's schemas; queries will infer them");
                0
            }
        }
    }

}

/// One batch item.
type Item = (FileId, FileRecord);

/// The directory components of a path, without the file name.
fn directories(path: &str) -> Vec<&str> {
    let mut segments: Vec<&str> = path.split('/').collect();
    segments.pop();
    segments
}

/// Group at a fixed directory depth. The explicit-override path.
fn group_at_depth(files: Vec<Item>, depth: usize) -> Vec<(String, Vec<Item>)> {
    let mut groups: BTreeMap<String, Vec<Item>> = BTreeMap::new();
    for item in files {
        let mut key = directories(&item.1.path);
        key.truncate(depth);
        groups.entry(key.join("/")).or_default().push(item);
    }
    groups.into_iter().collect()
}

/// Derive the grouping from the paths themselves.
///
/// Descends the path tree, splitting wherever a directory level actually
/// separates the batch, and stopping once a coherent group is small enough to be
/// a segment. That handles a store whose roots have different shapes without
/// anyone configuring a depth per root, which a single global depth cannot.
///
/// Input order is preserved inside every group, so ascending file ids stay
/// ascending.
fn group_by_locality(files: Vec<Item>, target: usize, min_group: usize) -> Vec<(String, Vec<Item>)> {
    let mut out = Vec::new();
    descend(files, 0, String::new(), target, min_group, &mut out);
    out
}

fn descend(
    files: Vec<Item>,
    depth: usize,
    prefix: String,
    target: usize,
    min_group: usize,
    out: &mut Vec<(String, Vec<Item>)>,
) {
    if files.is_empty() {
        return;
    }

    // Partition by this level's directory component. `None` means the file sits
    // at this level and cannot be descended any further.
    let mut buckets: BTreeMap<Option<String>, Vec<Item>> = BTreeMap::new();
    for item in files {
        let component = directories(&item.1.path)
            .get(depth)
            .map(|segment| (*segment).to_string());
        buckets.entry(component).or_default().push(item);
    }

    let total: usize = buckets.values().map(|bucket| bucket.len()).sum();

    // Splitting a small group buys a skip that is not worth the block framing.
    if total <= min_group {
        out.push((prefix, buckets.into_values().flatten().collect()));
        return;
    }

    if buckets.len() == 1 {
        let (component, only) = buckets.into_iter().next().expect("one bucket");
        let Some(component) = component else {
            // Every file is at this level, so there is nothing left to split on.
            // A directory holding more than a segment's worth gets cut by size.
            emit_chunked(only, prefix, target, out);
            return;
        };
        // One shared component: descending cannot separate anything yet, so keep
        // going only while the group is bigger than a segment should be.
        if only.len() <= target {
            out.push((join(&prefix, &component), only));
        } else {
            descend(only, depth + 1, join(&prefix, &component), target, min_group, out);
        }
        return;
    }

    // This level separates the batch, so take it.
    for (component, bucket) in buckets {
        match component {
            Some(component) => descend(
                bucket,
                depth + 1,
                join(&prefix, &component),
                target,
                min_group,
                out,
            ),
            // Files sitting at this level, beside subdirectories.
            None => emit_chunked(bucket, prefix.clone(), target, out),
        }
    }
}

/// Cut a group that cannot be split structurally into segment-sized pieces.
fn emit_chunked(files: Vec<Item>, prefix: String, target: usize, out: &mut Vec<(String, Vec<Item>)>) {
    if files.len() <= target {
        out.push((prefix, files));
        return;
    }
    for chunk in files.chunks(target) {
        out.push((prefix.clone(), chunk.to_vec()));
    }
}

fn join(prefix: &str, component: &str) -> String {
    if prefix.is_empty() {
        component.to_string()
    } else {
        format!("{prefix}/{component}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn items(paths: &[&str]) -> Vec<Item> {
        paths
            .iter()
            .enumerate()
            .map(|(index, path)| (index as FileId, FileRecord::pending(*path, 1, 1)))
            .collect()
    }

    fn keys(groups: &[(String, Vec<Item>)]) -> Vec<(String, usize)> {
        groups
            .iter()
            .map(|(prefix, files)| (prefix.clone(), files.len()))
            .collect()
    }

    #[test]
    fn a_fixed_depth_still_works_when_asked_for() {
        let groups = group_at_depth(items(&["argo/2024/01/a.nc", "argo/2024/02/b.nc"]), 2);
        assert_eq!(keys(&groups), vec![("argo/2024".to_string(), 2)]);

        let groups = group_at_depth(items(&["argo/a.nc", "ctd/b.nc"]), 1);
        assert_eq!(
            keys(&groups),
            vec![("argo".to_string(), 1), ("ctd".to_string(), 1)]
        );
    }

    /// Roots of different shapes are exactly what a single global depth cannot
    /// serve. The derivation splits each at the level that separates it.
    #[test]
    fn roots_with_different_shapes_each_get_their_own_depth() {
        let mut paths: Vec<String> = Vec::new();
        for i in 0..600 {
            paths.push(format!("argo/{i:04}.nc"));
        }
        for i in 0..600 {
            paths.push(format!("cmems/2024/01/{i:04}.nc"));
        }
        for i in 0..600 {
            paths.push(format!("cmems/2024/02/{i:04}.nc"));
        }
        let refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();

        let groups = group_by_locality(items(&refs), 500, 100);

        // Each root resolved to its own depth: `argo` at one level, `cmems` at
        // three. A single global depth cannot do both.
        let mut names: Vec<&str> = groups.iter().map(|(p, _)| p.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names, vec!["argo", "cmems/2024/01", "cmems/2024/02"]);

        // Each of those holds 600 files against a target of 500, and has nothing
        // left to split on, so the size rule cuts each into two segments.
        assert_eq!(groups.len(), 6);
        assert_eq!(groups.iter().map(|(_, f)| f.len()).sum::<usize>(), 1_800);
    }

    /// A group small enough to be one segment is left whole, however many
    /// directories it spans. Block framing costs more than the skip would save.
    #[test]
    fn a_small_batch_is_not_split_across_directories() {
        let groups = group_by_locality(
            items(&["argo/a.nc", "ctd/b.nc", "wod/c.nc"]),
            10_000,
            500,
        );
        assert_eq!(groups.len(), 1, "three files must not become three segments");
        assert_eq!(groups[0].1.len(), 3);
    }

    /// One directory holding more than a segment's worth, with nothing to split
    /// on, is cut by size rather than left as one huge segment.
    #[test]
    fn a_flat_directory_larger_than_the_target_is_chunked() {
        let paths: Vec<String> = (0..25).map(|i| format!("argo/{i:03}.nc")).collect();
        let refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();

        let groups = group_by_locality(items(&refs), 10, 5);
        assert_eq!(groups.len(), 3, "25 files at a target of 10");
        assert_eq!(groups.iter().map(|(_, f)| f.len()).sum::<usize>(), 25);
    }

    /// The rule `SegmentBuilder::push_file` depends on: whatever the grouping
    /// does, ids inside a group stay ascending.
    #[test]
    fn every_group_keeps_its_ids_ascending() {
        let mut paths: Vec<String> = Vec::new();
        for family in 0..4 {
            for i in 0..300 {
                paths.push(format!("fam{family}/2024/{i:04}.nc"));
            }
        }
        let refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();

        for (prefix, files) in group_by_locality(items(&refs), 200, 50) {
            let ids: Vec<FileId> = files.iter().map(|(id, _)| *id).collect();
            assert!(
                ids.windows(2).all(|w| w[0] < w[1]),
                "group {prefix} came out unsorted"
            );
        }
    }

    /// Files sitting beside subdirectories are their own group rather than being
    /// silently dropped or merged into a sibling.
    #[test]
    fn files_beside_subdirectories_are_kept() {
        let mut paths: Vec<String> = vec!["root.nc".to_string()];
        for i in 0..600 {
            paths.push(format!("argo/{i:04}.nc"));
        }
        let refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();

        let groups = group_by_locality(items(&refs), 500, 100);
        let total: usize = groups.iter().map(|(_, f)| f.len()).sum();
        assert_eq!(total, 601, "no file may be lost by the grouping");
        assert!(groups.iter().any(|(prefix, _)| prefix.is_empty()));
    }

    /// Nothing is lost and nothing is duplicated, whatever the shape.
    #[test]
    fn grouping_is_a_partition() {
        let mut paths: Vec<String> = Vec::new();
        for i in 0..50 {
            paths.push(format!("a/b/{i}.nc"));
        }
        for i in 0..50 {
            paths.push(format!("a/c/d/{i}.nc"));
        }
        paths.push("top.nc".to_string());
        let refs: Vec<&str> = paths.iter().map(|p| p.as_str()).collect();

        let groups = group_by_locality(items(&refs), 20, 5);
        let mut seen: Vec<FileId> = groups
            .iter()
            .flat_map(|(_, files)| files.iter().map(|(id, _)| *id))
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, (0..101).collect::<Vec<FileId>>());
    }

    #[test]
    fn an_idle_report_is_recognised() {
        assert!(CollectReport::default().is_idle());
        assert!(
            !CollectReport {
                failed: 1,
                ..Default::default()
            }
            .is_idle()
        );
    }
}
