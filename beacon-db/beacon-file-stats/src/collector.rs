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
}

/// Reads one file's statistics.
///
/// The seam that keeps this crate free of DataFusion. An implementation lives
/// wherever the format registry does.
#[async_trait::async_trait]
pub trait FileAnalyzer: Send + Sync {
    async fn analyze(&self, record: &FileRecord) -> Result<FileAnalysis>;
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
    /// Path segments that define a batch group. `argo/2024/01/f.nc` at depth 2
    /// groups under `argo/2024`.
    pub prefix_depth: usize,
}

impl Default for CollectorConfig {
    fn default() -> Self {
        Self {
            batch_files: 10_000,
            concurrency: std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4),
            prefix_depth: 2,
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
            return Ok(CollectReport::default());
        }

        // `next_pending` hands out ascending ids, so each group's subsequence is
        // ascending too, which is what `SegmentBuilder::push_file` requires.
        let mut groups: BTreeMap<String, Vec<(FileId, FileRecord)>> = BTreeMap::new();
        for (id, record) in pending {
            let key = prefix_of(&record.path, self.config.prefix_depth);
            groups.entry(key).or_default().push((id, record));
        }

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
        }
        Ok(total)
    }

    async fn run_group(
        &self,
        prefix: &str,
        files: Vec<(FileId, FileRecord)>,
        report: &mut CollectReport,
    ) -> Result<()> {
        // `buffer_unordered` rather than spawned tasks: it bounds concurrency
        // without forcing every future to be 'static, so the analyzer can borrow
        // from self.
        let outcomes: Vec<(FileId, Result<FileAnalysis>)> = stream::iter(files)
            .map(|(id, record)| async move {
                let outcome = self.analyzer.analyze(&record).await;
                (id, outcome)
            })
            .buffer_unordered(self.config.concurrency)
            .collect()
            .await;

        let mut analyzed: Vec<(FileId, FileAnalysis)> = Vec::with_capacity(outcomes.len());
        for (id, outcome) in outcomes {
            match outcome {
                Ok(analysis) => analyzed.push((id, analysis)),
                Err(error) => {
                    // A bad file must not stop the batch, and it must leave the
                    // queue: a failure that stays pending is retried forever.
                    tracing::warn!(file_id = id, %error, "file statistics analysis failed");
                    self.store.registry().set_state(id, FileState::Failed)?;
                    report.failed += 1;
                }
            }
        }

        if analyzed.is_empty() {
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

        // Only after the segment is durable.
        for (file_id, analysis) in &analyzed {
            self.store.registry().mark_analyzed(
                *file_id,
                &analysis.format,
                analysis.num_rows,
                analysis.total_byte_size,
            )?;
            report.analyzed += 1;
        }

        tracing::debug!(
            prefix,
            analyzed = analyzed.len(),
            "committed a file statistics segment"
        );
        Ok(())
    }

}

/// The batch group a path belongs to: its leading directory segments, without
/// the file name.
///
/// A path with no directory groups under the empty string, which keeps
/// everything at the store root in one group rather than one group each.
fn prefix_of(path: &str, depth: usize) -> String {
    let mut segments: Vec<&str> = path.split('/').collect();
    segments.pop(); // the file name is not part of the group
    segments.truncate(depth);
    segments.join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_is_the_leading_directories_without_the_file_name() {
        assert_eq!(prefix_of("argo/2024/01/f.nc", 2), "argo/2024");
        assert_eq!(prefix_of("argo/2024/01/f.nc", 1), "argo");
        assert_eq!(prefix_of("argo/f.nc", 2), "argo");
        assert_eq!(prefix_of("f.nc", 2), "");
        assert_eq!(prefix_of("argo/2024/01/f.nc", 0), "");
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
