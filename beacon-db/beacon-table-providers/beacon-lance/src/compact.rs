//! Compaction and version cleanup for managed Lance tables (`COMPACT TABLE`).
//!
//! Every write commits a new dataset version and nothing shrinks on its own:
//! each `INSERT` adds its own fragments, `DELETE` writes deletion files that
//! keep the deleted rows on disk, and `UPDATE` rewrites the fragments it
//! touches. A table written by a long series of small inserts ends up with far
//! more fragments than rows justify, and [`LanceTable::scan`] hands each
//! fragment group to DataFusion as a partition — so the fragment count is a
//! scan-planning cost, not just a storage one.
//!
//! Compaction merges those fragments back into target-sized ones and
//! materializes the deletions. It commits one new version; the superseded
//! versions still reference the old files, so the cleanup pass is what actually
//! frees the disk space.
//!
//! Lance remaps the table's scalar indexes onto the rewritten fragments as part
//! of the compaction commit, so indexes survive a compaction (unlike an
//! overwrite, which drops them).
//!
//! [`LanceTable::scan`]: crate::LanceTable

use std::time::Duration;

use lance::dataset::builder::DatasetBuilder;
use lance::dataset::optimize::{compact_files, CompactionOptions};

use crate::warehouse::LanceWarehouse;

/// Default age below which a superseded version is kept.
///
/// Cleanup deletes the data files that only the removed versions referenced, and
/// beacon's readers open a dataset version at plan time and read it while the
/// scan runs. A retention window keeps those readers safe: a query would have to
/// run for longer than the window to have its files deleted underneath it. It is
/// also the threshold Lance itself uses to decide whether an unreferenced file
/// might belong to a write that is still in flight.
pub const DEFAULT_CLEANUP_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

/// The keys accepted in `COMPACT TABLE ... WITH (...)`.
const OPTION_KEYS: &[&str] = &["target_rows_per_fragment", "cleanup_older_than"];

/// How a single `COMPACT TABLE` run should behave.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactOptions {
    /// Rows to aim for per fragment. `None` uses Lance's default (1Mi rows).
    /// Fragments already at or above this size are left alone, so this doubles
    /// as the "which fragments need compacting" threshold.
    pub target_rows_per_fragment: Option<usize>,
    /// Remove superseded versions older than this. `None` skips the cleanup pass
    /// entirely, leaving every old version (and its files) in place.
    pub cleanup_older_than: Option<Duration>,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            target_rows_per_fragment: None,
            cleanup_older_than: Some(DEFAULT_CLEANUP_AGE),
        }
    }
}

impl CompactOptions {
    /// Build options from the statement's `WITH (key 'value', ...)` pairs.
    ///
    /// An unknown key is an error rather than a silent no-op: a mistyped option
    /// would otherwise look like it took effect.
    pub fn from_pairs<K, V>(pairs: &[(K, V)]) -> Result<Self, String>
    where
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut options = Self::default();
        for (key, value) in pairs {
            let value = value.as_ref();
            match key.as_ref().trim().to_ascii_lowercase().as_str() {
                "target_rows_per_fragment" => {
                    let rows = value.trim().parse::<usize>().map_err(|_| {
                        format!("invalid target_rows_per_fragment value '{value}'")
                    })?;
                    if rows == 0 {
                        return Err("target_rows_per_fragment must be greater than 0".to_string());
                    }
                    options.target_rows_per_fragment = Some(rows);
                }
                "cleanup_older_than" => {
                    options.cleanup_older_than = parse_cleanup_age(value)?;
                }
                other => {
                    return Err(format!(
                        "unknown COMPACT option '{other}', expected one of {}",
                        OPTION_KEYS.join(", ")
                    ));
                }
            }
        }
        Ok(options)
    }
}

/// Parse a `cleanup_older_than` value: a duration (`30s`, `15m`, `2h`, `7d`, or a
/// bare integer of seconds — the spelling `CREATE CRAWLER`'s `schedule` uses), or
/// `never` to skip the cleanup pass.
fn parse_cleanup_age(value: &str) -> Result<Option<Duration>, String> {
    let v = value.trim();
    if v.is_empty() {
        return Err("empty cleanup_older_than".to_string());
    }
    if matches!(v.to_ascii_lowercase().as_str(), "never" | "none" | "off") {
        return Ok(None);
    }

    let (number, unit_secs) = match v.chars().last().expect("checked non-empty") {
        's' | 'S' => (&v[..v.len() - 1], 1),
        'm' | 'M' => (&v[..v.len() - 1], 60),
        'h' | 'H' => (&v[..v.len() - 1], 3600),
        'd' | 'D' => (&v[..v.len() - 1], 86_400),
        c if c.is_ascii_digit() => (v, 1),
        other => return Err(format!("invalid cleanup_older_than unit '{other}' in '{value}'")),
    };
    let seconds = number
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("invalid cleanup_older_than number in '{value}'"))?
        .checked_mul(unit_secs)
        .ok_or_else(|| format!("cleanup_older_than '{value}' is too large"))?;
    Ok(Some(Duration::from_secs(seconds)))
}

/// What one `COMPACT TABLE` run did.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactionReport {
    /// Fragments merged away.
    pub fragments_removed: u64,
    /// Fragments written in their place.
    pub fragments_added: u64,
    /// Files (data and deletion) the compaction superseded.
    pub files_removed: u64,
    /// Files the compaction wrote.
    pub files_added: u64,
    /// Superseded dataset versions the cleanup pass removed.
    pub versions_removed: u64,
    /// Bytes the cleanup pass freed. Zero when no version was old enough, even
    /// after a compaction that rewrote everything.
    pub bytes_removed: u64,
}

/// Compact the Lance table at `uri`, then remove the versions the options allow.
///
/// Serialized against concurrent writers through the warehouse's per-dataset
/// lock, like every other mutating path. A table with nothing to merge is not an
/// error: Lance makes no commit and the report is all zeros.
pub async fn compact_table(
    warehouse: &LanceWarehouse,
    uri: &str,
    options: &CompactOptions,
) -> anyhow::Result<CompactionReport> {
    tracing::info!(uri = %uri, ?options, "compacting Lance table");

    let lock = warehouse.lock(uri);
    let _guard = lock.lock().await;

    let mut dataset = DatasetBuilder::from_uri(uri)
        .with_session(warehouse.session())
        .load()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to open Lance dataset '{uri}': {e}"))?;

    let mut compaction = CompactionOptions::default();
    if let Some(target) = options.target_rows_per_fragment {
        compaction.target_rows_per_fragment = target;
    }

    let metrics = compact_files(&mut dataset, compaction, None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to compact Lance dataset '{uri}': {e}"))?;

    let mut report = CompactionReport {
        fragments_removed: metrics.fragments_removed as u64,
        fragments_added: metrics.fragments_added as u64,
        files_removed: metrics.files_removed as u64,
        files_added: metrics.files_added as u64,
        ..Default::default()
    };

    if let Some(older_than) = options.cleanup_older_than {
        let older_than = chrono::Duration::from_std(older_than)
            .map_err(|e| anyhow::anyhow!("Invalid cleanup_older_than duration: {e}"))?;
        // `delete_unverified` stays at its default (false): a file Lance cannot
        // tie to a removed version may belong to a write still in flight, and
        // dropping it would corrupt that write.
        let stats = dataset
            .cleanup_old_versions(older_than, None, None)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to clean up old versions of '{uri}': {e}"))?;
        report.versions_removed = stats.old_versions;
        report.bytes_removed = stats.bytes_removed;
    }

    tracing::info!(uri = %uri, ?report, "compacted Lance table");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_compact_and_clean_up_after_the_retention_window() {
        let options = CompactOptions::default();
        assert_eq!(options.target_rows_per_fragment, None);
        assert_eq!(options.cleanup_older_than, Some(DEFAULT_CLEANUP_AGE));
    }

    #[test]
    fn parses_both_options() {
        let options = CompactOptions::from_pairs(&[
            ("target_rows_per_fragment", "500000"),
            ("cleanup_older_than", "2h"),
        ])
        .unwrap();
        assert_eq!(options.target_rows_per_fragment, Some(500_000));
        assert_eq!(options.cleanup_older_than, Some(Duration::from_secs(7200)));
    }

    /// The duration spelling matches `CREATE CRAWLER`'s `schedule` option, and a
    /// bare number is seconds.
    #[test]
    fn parses_every_duration_unit() {
        for (value, seconds) in [("30s", 30), ("15m", 900), ("2h", 7200), ("7d", 604_800), ("45", 45)] {
            let options = CompactOptions::from_pairs(&[("cleanup_older_than", value)]).unwrap();
            assert_eq!(
                options.cleanup_older_than,
                Some(Duration::from_secs(seconds)),
                "{value}"
            );
        }
    }

    /// `0s` is the "reclaim now" setting: every superseded version qualifies.
    #[test]
    fn zero_cleanup_age_is_kept_distinct_from_never() {
        let now = CompactOptions::from_pairs(&[("cleanup_older_than", "0s")]).unwrap();
        assert_eq!(now.cleanup_older_than, Some(Duration::ZERO));

        for value in ["never", "NONE", "off"] {
            let skipped = CompactOptions::from_pairs(&[("cleanup_older_than", value)]).unwrap();
            assert_eq!(skipped.cleanup_older_than, None, "{value}");
        }
    }

    #[test]
    fn rejects_bad_values_and_unknown_keys() {
        let err = CompactOptions::from_pairs(&[("cleanup_older_than", "7x")]).unwrap_err();
        assert!(err.contains("unit"), "{err}");

        let err = CompactOptions::from_pairs(&[("target_rows_per_fragment", "0")]).unwrap_err();
        assert!(err.contains("greater than 0"), "{err}");

        let err = CompactOptions::from_pairs(&[("target_rows_per_file", "10")]).unwrap_err();
        assert!(err.contains("target_rows_per_fragment"), "{err}");
    }
}
