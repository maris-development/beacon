//! Turning the files a scan listed into the file ids whose statistics a prune may trust.

use std::sync::Arc;

use crate::store::FileStatsStore;
use crate::types::{FileId, ObservedFile};

/// Candidates below which pruning stays one call.
///
/// A chunk pays for a task, its own id lookups and its own segment reads. Below
/// this the whole list is decided in one call, which is what a collection of
/// ordinary size wants.
pub const PRUNE_CHUNK: usize = 65_536;

/// How many prune tasks to run at once for `candidates` files.
pub fn prune_tasks(candidates: usize) -> usize {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    (candidates / PRUNE_CHUNK).clamp(1, cores)
}

/// Whether a listing filled this entry's metadata in at all.
///
/// A format that plans its own entries may state only their paths. Zarr expands
/// a store into the group keys its reader opens, and Icechunk into the keys of a
/// snapshot, and both build an entry with `PartitionedFile::new`, which stamps
/// the Unix epoch as the modification time and carries no etag. No listing
/// reports that pair, so it is the signature of an entry that says nothing about
/// the object behind it — and an entry that says nothing cannot contradict the
/// record. Such a file is trusted as it always was, and the collector's pass
/// stays its only check. Atlas is the other way round: it carries the marker's
/// own metadata into every entry it plans, so the comparison applies in full.
pub fn states_metadata(observed: &ObservedFile) -> bool {
    observed.e_tag.is_some() || observed.last_modified_millis != 0
}

/// Every file's id, in the caller's order, `None` where nothing may rule the file out.
///
/// Three things read as unknown. A path the registry has never seen. A path
/// whose record describes a different object — a different etag, or a different
/// size and modification time — which is why the metadata is carried here at
/// all: a file rewritten after its analysis keeps the state `Analyzed` until
/// the next collector pass, and its segment rows still describe the old
/// content, so pruning on them drops rows the query asked for. That pass runs
/// every `BEACON_FILE_STATS_INTERVAL_SECS`, and over a store Beacon never lists
/// it never runs at all, so the query makes the comparison itself. Third, a
/// failed lookup, which drops the whole batch.
///
/// Looking a file up is a redb read with no await in it, so this runs on
/// blocking threads, in chunks. `None` for the whole batch means the lookup
/// failed and the caller keeps every file.
///
/// Each chunk opens its own read transaction, so a file interned between two of
/// them is resolved by the later one. That fails open: a freshly interned file
/// has no rows in any segment yet, so pruning reads its statistics as unknown
/// and keeps it.
pub async fn resolve_trusted_ids(
    store: &Arc<FileStatsStore>,
    files: Vec<ObservedFile>,
) -> Option<Vec<Option<FileId>>> {
    if files.is_empty() {
        return Some(Vec::new());
    }
    let size = files.len().div_ceil(prune_tasks(files.len()));

    let lookups: Vec<_> = files
        .chunks(size)
        .map(|chunk| {
            let chunk: Vec<ObservedFile> = chunk.to_vec();
            let store = Arc::clone(store);
            tokio::task::spawn_blocking(move || {
                let resolved = store.registry().resolve_observed(&chunk).ok()?;
                Some(
                    resolved
                        .into_iter()
                        .zip(&chunk)
                        .map(|(found, file)| {
                            let found = found?;
                            // The record is trusted unless this entry carries
                            // metadata of its own that disagrees with it.
                            (found.unchanged || !states_metadata(file)).then_some(found.id)
                        })
                        .collect::<Vec<Option<FileId>>>(),
                )
            })
        })
        .collect();

    let mut ids = Vec::with_capacity(files.len());
    for lookup in lookups {
        match lookup.await {
            Ok(Some(chunk)) => ids.extend(chunk),
            // A failed lookup or a panicked task keeps every file.
            _ => return None,
        }
    }
    Some(ids)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Chunking is decided by candidate count, and only above the threshold.
    #[test]
    fn pruning_stays_one_call_until_it_is_worth_splitting() {
        assert_eq!(prune_tasks(0), 1);
        assert_eq!(prune_tasks(1_000), 1, "an ordinary collection is one call");
        assert_eq!(prune_tasks(PRUNE_CHUNK - 1), 1);

        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        assert_eq!(prune_tasks(PRUNE_CHUNK * 4), 4.min(cores));
        assert_eq!(
            prune_tasks(PRUNE_CHUNK * 1_000),
            cores,
            "never more tasks than the machine has"
        );
    }

    /// Only an entry a listing filled in may contradict the record.
    #[test]
    fn only_a_listed_entry_states_metadata() {
        assert!(!states_metadata(&ObservedFile::new(
            "store.zarr/zarr.json",
            1024,
            0
        )));
        assert!(states_metadata(&ObservedFile::new(
            "a.parquet",
            1024,
            1_700_000_000_000
        )));
        // An etag alone is enough, whatever the modification time says.
        let tagged = ObservedFile::new("a.parquet", 1024, 0).with_e_tag(Some("v1".into()));
        assert!(states_metadata(&tagged));
    }
}
