//! Dropping the files a predicate rules out, before the scan is built.
//!
//! See the [module docs](super) for where this sits in `scan`.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use beacon_file_stats::{FileId, FileStatsStore};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;

/// Everything needed to drop files a predicate rules out.
///
/// Built in `scan`, which costs no I/O: the predicate is compiled and the store
/// handle cloned. Every read it implies happens in [`prune_file_groups`].
#[derive(Clone)]
pub struct Pruning {
    pub store: Arc<FileStatsStore>,
    pub predicate: Arc<dyn PhysicalExpr>,
    /// The table schema the predicate is written against — not the projected
    /// one, because a column a predicate prunes on need not be selected.
    pub table_schema: SchemaRef,
}

impl fmt::Debug for Pruning {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pruning")
            .field("predicate", &self.predicate)
            .finish()
    }
}

/// What one prune did, for the counters `EXPLAIN ANALYZE` shows.
pub struct Pruned {
    pub groups: Vec<FileGroup>,
    pub considered: usize,
    pub dropped: usize,
}

/// Candidates below which pruning stays one call.
///
/// A chunk pays for a task, its own id lookups and its own segment reads. Below
/// this the whole list is decided in one call, which is what a collection of
/// ordinary size wants.
const PRUNE_CHUNK: usize = 65_536;

/// How many prune tasks to run at once.
fn prune_tasks(candidates: usize) -> usize {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    (candidates / PRUNE_CHUNK).clamp(1, cores)
}

/// Drop the files a predicate rules out from a planned scan.
///
/// The format decides its own file list — Zarr and Atlas expand a store
/// directory into the groups their reader opens, and reduce it to the marker at
/// its root — so this prunes what the format planned rather than the listing it
/// was given. Pruning the listing first would drop a store's analysed root
/// marker and keep its unanalysed children, and the format would then read one
/// of those as a store.
///
/// Returns the plan unchanged when it is not a file scan, or when pruning drops
/// nothing.
pub async fn prune_plan(
    plan: Arc<dyn ExecutionPlan>,
    pruning: &Pruning,
) -> (Arc<dyn ExecutionPlan>, Option<(usize, usize)>) {
    // An nd format stacks decode and broadcast nodes over its scan, so descend
    // to it and rebuild the stack afterwards. Each node re-derives itself from
    // its new child, which differs only in which files it lists.
    let mut wrappers: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    let mut node = Arc::clone(&plan);
    let config = loop {
        if let Some(exec) = node.as_any().downcast_ref::<DataSourceExec>() {
            match exec.data_source().as_any().downcast_ref::<FileScanConfig>() {
                Some(config) => break config.clone(),
                None => return (plan, None),
            }
        }
        let children = node.children();
        let [child] = children[..] else {
            return (plan, None);
        };
        let child = Arc::clone(child);
        wrappers.push(node);
        node = child;
    };

    let pruned = prune_file_groups(pruning, config.file_groups.clone()).await;
    let counts = Some((pruned.considered, pruned.dropped));
    if pruned.dropped == 0 {
        return (plan, counts);
    }

    // Only the file list changes. The configuration is otherwise the one the
    // format built — its projection, its predicate, its source — and rebuilding
    // it through the builder would re-derive a projection the scan has already
    // pushed down. Its statistics still describe every file the format planned,
    // which is an overestimate now, and the same one the scan-rewriting path
    // has always reported.
    let mut config = config;
    config.file_groups = pruned.groups;
    let mut rebuilt: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(config);
    for wrapper in wrappers.into_iter().rev() {
        match wrapper.with_new_children(vec![rebuilt]) {
            Ok(node) => rebuilt = node,
            // A node that will not rebuild leaves the plan as it was.
            Err(_) => return (plan, counts),
        }
    }
    (rebuilt, counts)
}

/// Drop the files whose recorded ranges say they cannot match.
///
/// A path the registry has never seen has no statistics and is kept: a
/// partially analyzed store must not lose files. Every failure keeps the whole
/// list for the same reason, which is what makes this infallible.
///
/// Groups keep their shape: a file is removed from the group it was in, and a
/// group emptied by pruning is dropped. The listing table decided that grouping
/// by size, and pruning has no better idea.
pub async fn prune_file_groups(pruning: &Pruning, groups: Vec<FileGroup>) -> Pruned {
    let files: Vec<String> = groups
        .iter()
        .flat_map(|group| group.iter().map(|file| file.object_meta.location.to_string()))
        .collect();
    let considered = files.len();

    let Some(ids) = resolve_ids(pruning, files).await else {
        return Pruned {
            groups,
            considered,
            dropped: 0,
        };
    };

    let mut candidates: Vec<FileId> = ids.iter().filter_map(|id| *id).collect();
    if candidates.is_empty() {
        // Nothing here is analyzed, so nothing is prunable.
        return Pruned {
            groups,
            considered,
            dropped: 0,
        };
    }
    // `prune_files` wants them ascending, and answers ascending.
    candidates.sort_unstable();
    candidates.dedup();

    let kept = prune_candidates(pruning, candidates).await;

    // Walk the groups in the order the ids were collected in.
    let mut position = 0usize;
    let mut dropped = 0usize;
    let mut pruned_groups = Vec::with_capacity(groups.len());
    for group in groups {
        let mut files = Vec::with_capacity(group.len());
        for file in group.iter() {
            let keep = match ids[position] {
                Some(id) => kept.binary_search(&id).is_ok(),
                // Never seen by the registry, so it has no statistics to be
                // ruled out by.
                None => true,
            };
            position += 1;
            if keep {
                files.push(file.clone());
            } else {
                dropped += 1;
            }
        }
        if !files.is_empty() {
            pruned_groups.push(FileGroup::new(files));
        }
    }

    Pruned {
        groups: pruned_groups,
        considered,
        dropped,
    }
}

/// Every candidate's file id, in the caller's order, `None` where the path is
/// unknown.
///
/// Looking up a path is a redb read with no await in it, so this runs on
/// blocking threads, in chunks. `None` for the whole batch means the lookup
/// failed and the caller keeps every file.
///
/// Each chunk opens its own read transaction, so a file interned between two of
/// them is resolved by the later one. That fails open: a freshly interned file
/// has no rows in any segment yet, so pruning reads its statistics as unknown
/// and keeps it.
async fn resolve_ids(pruning: &Pruning, files: Vec<String>) -> Option<Vec<Option<FileId>>> {
    let tasks = prune_tasks(files.len());
    let size = files.len().div_ceil(tasks.max(1));

    let mut lookups = Vec::with_capacity(tasks);
    for chunk in files.chunks(size) {
        let paths: Vec<String> = chunk.to_vec();
        let store = Arc::clone(&pruning.store);
        lookups.push(tokio::task::spawn_blocking(move || {
            let borrowed: Vec<&str> = paths.iter().map(String::as_str).collect();
            store.registry().file_ids(&borrowed).ok()
        }));
    }

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

/// The candidates that survive the predicate, ascending.
///
/// Chunked over the *sorted* ids, never over the file list. That is the whole
/// reason this can be parallel at all: the store skips a segment whose file-id
/// range a chunk does not touch, so contiguous id chunks read nearly disjoint
/// segments and only one spanning a boundary is read twice. Chunking the file
/// list instead would scatter ids across the whole space, and every chunk would
/// read every segment.
///
/// Each chunk is spawned, so the packing and the predicate evaluation — real
/// CPU over a row per candidate — run on different threads. Within a chunk the
/// store already fetches a predicate's columns together and each column's
/// segments in parallel.
async fn prune_candidates(pruning: &Pruning, candidates: Vec<FileId>) -> Vec<FileId> {
    let tasks = prune_tasks(candidates.len());
    if tasks <= 1 {
        return beacon_file_stats::prune_files(
            &pruning.store,
            &pruning.predicate,
            &pruning.table_schema,
            &candidates,
        )
        .await;
    }

    let size = candidates.len().div_ceil(tasks);
    let mut prunes = Vec::with_capacity(tasks);
    for chunk in candidates.chunks(size) {
        let chunk = chunk.to_vec();
        let pruning = pruning.clone();
        prunes.push(tokio::spawn(async move {
            beacon_file_stats::prune_files(
                &pruning.store,
                &pruning.predicate,
                &pruning.table_schema,
                &chunk,
            )
            .await
        }));
    }

    // Chunks are ascending and each answers ascending, so the concatenation is
    // ascending and the caller may binary-search it.
    let mut kept = Vec::with_capacity(candidates.len());
    for (prune, chunk) in prunes.into_iter().zip(candidates.chunks(size)) {
        match prune.await {
            Ok(survivors) => kept.extend(survivors),
            // A panicked task keeps its chunk, the same way every other failure
            // here does.
            Err(error) => {
                tracing::debug!(%error, "a prune task panicked; keeping its files");
                kept.extend_from_slice(chunk);
            }
        }
    }
    kept
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
}
