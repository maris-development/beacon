//! The store: registry, manifest, and segments behind one handle.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::datatypes::DataType;
use futures::stream::{self, StreamExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::manifest::{Manifest, SegmentEntry};
use crate::registry::Registry;
use crate::schema_cache::SchemaCache;
use crate::segment::{ColumnStats, SegmentBuilder, SegmentReader};
use crate::types::{ColumnId, FileId};

/// Segment reads in flight for one column.
///
/// Bounded rather than unbounded: a wide column can live in every segment of a
/// store, and opening all of them at once would trade one queue for a thundering
/// herd on the object store.
const SEGMENT_READ_CONCURRENCY: usize = 16;

/// The crate's `Result` alias fixes the error type, so the join result is
/// spelled out.
type Joined = std::result::Result<
    std::result::Result<(usize, Option<ColumnStats>), crate::FileStatsError>,
    tokio::task::JoinError,
>;

/// One column's recorded statistics for one file.
///
/// What [`FileStatsStore::file_column_stats`] returns: a single row of the
/// segment, kept in the column's own Arrow type rather than rendered, so the
/// caller decides how to present it. `min` and `max` are one-element arrays.
#[derive(Debug, Clone)]
pub struct FileColumnStat {
    pub column_id: ColumnId,
    /// The segment this row came from.
    pub segment: String,
    pub data_type: DataType,
    pub min: ArrayRef,
    pub max: ArrayRef,
    /// `None` where the format reported no count.
    pub null_count: Option<u64>,
    /// `None` for a format that reports no row count, such as netCDF.
    pub row_count: Option<u64>,
}

/// Reads one count out of a nullable `UInt64` column.
fn count_at(counts: &ArrayRef, row: usize) -> Option<u64> {
    let counts = counts.as_any().downcast_ref::<UInt64Array>()?;
    (!counts.is_null(row)).then(|| counts.value(row))
}

/// Statistics for every known file, split across immutable segments.
pub struct FileStatsStore {
    registry: Arc<Registry>,
    schema_cache: Arc<SchemaCache>,
    store: Arc<dyn ObjectStore>,
    prefix: Path,
    manifest: RwLock<Manifest>,
}

impl FileStatsStore {
    /// Open a store over `prefix`, loading the manifest if one is there.
    pub async fn open(
        registry: Arc<Registry>,
        store: Arc<dyn ObjectStore>,
        prefix: Path,
    ) -> Result<Self> {
        let manifest = Manifest::load(&store, &prefix).await?;
        // The cache opens its own tables in the registry's database. Both are
        // tenants of one `beacon.db`, and the store is what a session already
        // holds, so this is the shortest way for a scan to reach the cache.
        let schema_cache = Arc::new(SchemaCache::from_database(Arc::clone(registry.database()))?);
        Ok(Self {
            registry,
            schema_cache,
            store,
            prefix,
            manifest: RwLock::new(manifest),
        })
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// Each file's interned schema, so a plan derives one only for the files
    /// that changed.
    pub fn schema_cache(&self) -> &Arc<SchemaCache> {
        &self.schema_cache
    }

    /// Seal a batch: write the segment object, then record it in the manifest.
    ///
    /// That order matters. A crash between the two leaves an orphan object,
    /// which a later vacuum reclaims. The reverse order would leave the manifest
    /// pointing at bytes that are not there.
    pub async fn commit_segment(&self, builder: SegmentBuilder) -> Result<Option<SegmentEntry>> {
        if builder.is_empty() {
            return Ok(None);
        }
        let finished = builder.finish()?;

        let mut manifest = self.manifest.write().await;
        // From the monotonic counter, not from the list length: a compaction
        // shrinks the list, and a length-derived name would then be handed to
        // the next write while a live segment still held it.
        let seq = manifest.claim_seq();
        let name = format!("segment-{seq:08}.bfs");
        self.store
            .put(&self.prefix.clone().join(name.as_str()), finished.bytes.into())
            .await?;

        let entry = SegmentEntry {
            seq,
            name,
            min_file_id: finished.min_file_id,
            max_file_id: finished.max_file_id,
            num_files: finished.num_files,
            column_ids: finished.column_ids,
        };
        manifest.add_segment(entry.clone());
        manifest.save(&self.store, &self.prefix).await?;
        Ok(Some(entry))
    }

    /// Every segment's statistics for one column, within a file id range.
    ///
    /// Segments that cannot hold the column, or cannot cover the range, are
    /// skipped on the manifest alone. Nothing else in this crate reads a segment
    /// the query did not need.
    pub async fn column_stats(
        &self,
        column_id: ColumnId,
        file_id_range: (FileId, FileId),
    ) -> Result<Vec<ColumnStats>> {
        let names: Vec<String> = {
            let manifest = self.manifest.read().await;
            manifest
                .candidates(column_id, file_id_range)
                .into_iter()
                .map(|entry| entry.name.clone())
                .collect()
        };

        // Segments are read in parallel, then put back in manifest order. Order
        // is not cosmetic: the reader folds oldest first so the newest row for a
        // file wins, and losing that lets a stale range beat a fresh one.
        //
        // Spawned, not merely buffered. A segment read decodes buffers and
        // rebuilds Arrow arrays, so it is CPU bound between awaits, and
        // `buffer_unordered` alone would poll every one of them from a single
        // task. For a column present in every segment that is the difference
        // between reading 100 segments in parallel and reading them in a queue.
        let indexed: Vec<Joined> = stream::iter(names.into_iter().enumerate())
            .map(|(position, name)| {
                let store = self.store.clone();
                let path = self.prefix.clone().join(name.as_str());
                tokio::spawn(async move {
                    let reader = SegmentReader::open(store, path).await?;
                    Ok::<_, crate::FileStatsError>((position, reader.column(column_id).await?))
                })
            })
            .buffer_unordered(SEGMENT_READ_CONCURRENCY)
            .collect()
            .await;

        let mut found: Vec<(usize, ColumnStats)> = Vec::with_capacity(indexed.len());
        for outcome in indexed {
            match outcome {
                Ok(Ok((position, Some(stats)))) => found.push((position, stats)),
                // The manifest said the segment held the column and it did not.
                // Not an error: a reader treats a missing statistic as unknown.
                Ok(Ok((_, None))) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => {
                    return Err(crate::FileStatsError::Format(format!(
                        "a segment read task panicked: {error}"
                    )));
                }
            }
        }
        found.sort_by_key(|(position, _)| *position);
        Ok(found.into_iter().map(|(_, stats)| stats).collect())
    }

    /// Statistics for a column named rather than numbered.
    ///
    /// An unknown name is not an error: no file ever declared it, so there is
    /// nothing to prune on.
    pub async fn column_stats_by_name(
        &self,
        name: &str,
        file_id_range: (FileId, FileId),
    ) -> Result<Vec<ColumnStats>> {
        match self.registry.column_id(name)? {
            Some(column_id) => self.column_stats(column_id, file_id_range).await,
            None => Ok(Vec::new()),
        }
    }

    /// Every column statistic recorded for one file.
    ///
    /// The inverse of [`Self::column_stats`], which reads one column across many
    /// files. Pruning never needs this direction; an operator asking "what does
    /// Beacon actually hold for this file" does, and answering it from the
    /// segments is the only honest way to answer it.
    ///
    /// Segments are folded in manifest order, so a file that appears in more than
    /// one is reported from the newest — the same resolution the pruning reader
    /// applies. A column with no recorded range is absent rather than null: the
    /// segment holds nothing for it.
    pub async fn file_column_stats(&self, file_id: FileId) -> Result<Vec<FileColumnStat>> {
        let covering: Vec<SegmentEntry> = {
            let manifest = self.manifest.read().await;
            manifest
                .segments
                .iter()
                .filter(|entry| entry.min_file_id <= file_id && file_id <= entry.max_file_id)
                .cloned()
                .collect()
        };

        // Keyed by column so a newer segment replaces an older one's row.
        let mut newest: std::collections::BTreeMap<ColumnId, FileColumnStat> =
            std::collections::BTreeMap::new();

        for entry in covering {
            let path = self.prefix.clone().join(entry.name.as_str());
            let reader = SegmentReader::open(self.store.clone(), path).await?;

            // The segment's columns are read concurrently but not spawned: the
            // reader is borrowed, and one file's columns are a bounded set.
            let rows: Vec<(ColumnId, Option<ColumnStats>)> =
                stream::iter(entry.column_ids.iter().copied())
                    .map(|column_id| {
                        let reader = &reader;
                        async move {
                            Ok::<_, crate::FileStatsError>((
                                column_id,
                                reader.column(column_id).await?,
                            ))
                        }
                    })
                    .buffer_unordered(SEGMENT_READ_CONCURRENCY)
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .collect::<Result<Vec<_>>>()?;

            for (column_id, stats) in rows {
                let Some(stats) = stats else { continue };
                // A segment holds many files; find this one's row, if it has one.
                let Some(row) = stats.file_ids.iter().position(|id| *id == file_id) else {
                    continue;
                };
                newest.insert(
                    column_id,
                    FileColumnStat {
                        column_id,
                        segment: entry.name.clone(),
                        data_type: stats.data_type.clone(),
                        min: stats.min.slice(row, 1),
                        max: stats.max.slice(row, 1),
                        null_count: count_at(&stats.null_count, row),
                        row_count: count_at(&stats.row_count, row),
                    },
                );
            }
        }

        Ok(newest.into_values().collect())
    }

    /// A snapshot of the manifest, for diagnostics.
    pub async fn segments(&self) -> Vec<SegmentEntry> {
        self.manifest.read().await.segments.clone()
    }

    pub async fn num_segments(&self) -> usize {
        self.manifest.read().await.segments.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scalar::StatScalar;
    use crate::segment::ColumnStat;
    use crate::types::ObservedFile;
    use arrow::array::AsArray;
    use arrow::datatypes::{DataType, Float64Type};
    use object_store::memory::InMemory;

    fn stat(min: f64, max: f64) -> ColumnStat {
        ColumnStat {
            min: StatScalar::F64(min),
            max: StatScalar::F64(max),
            null_count: Some(1),
            row_count: Some(100),
            data_type: DataType::Float64,
        }
    }

    async fn store() -> (FileStatsStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let registry = Arc::new(Registry::open(dir.path().join("registry.redb")).unwrap());
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = FileStatsStore::open(registry, object_store, Path::from("stats"))
            .await
            .unwrap();
        (store, dir)
    }

    #[tokio::test]
    async fn a_batch_round_trips_through_a_segment() {
        let (store, _dir) = store().await;

        let files: Vec<ObservedFile> = (0..3)
            .map(|i| ObservedFile::new(format!("argo/{i}.nc"), 10, 1))
            .collect();
        let file_ids = store.registry().intern_files(&files).unwrap();
        let columns = store.registry().intern_columns(&["TEMP", "PSAL"]).unwrap();

        let mut builder = SegmentBuilder::new();
        builder.push_file(file_ids[0], [(columns[0], stat(0.0, 10.0))]);
        builder.push_file(
            file_ids[1],
            [(columns[0], stat(20.0, 30.0)), (columns[1], stat(34.0, 35.0))],
        );
        builder.push_file(file_ids[2], [(columns[1], stat(30.0, 31.0))]);
        store.commit_segment(builder).await.unwrap().unwrap();

        let temp = store.column_stats_by_name("TEMP", (0, 2)).await.unwrap();
        assert_eq!(temp.len(), 1);
        let temp = &temp[0];
        assert_eq!(temp.file_ids, vec![0, 1], "only the files that declare it");
        assert_eq!(temp.data_type, DataType::Float64);
        assert_eq!(temp.min.as_primitive::<Float64Type>().values(), &[0.0, 20.0]);
        assert_eq!(temp.max.as_primitive::<Float64Type>().values(), &[10.0, 30.0]);
        assert_eq!(
            temp.null_count.as_ref(),
            &arrow::array::UInt64Array::from(vec![Some(1), Some(1)]) as &dyn arrow::array::Array
        );
        assert_eq!(
            temp.row_count.as_ref(),
            &arrow::array::UInt64Array::from(vec![Some(100), Some(100)])
                as &dyn arrow::array::Array
        );

        let psal = store.column_stats_by_name("PSAL", (0, 2)).await.unwrap();
        assert_eq!(psal[0].file_ids, vec![1, 2]);
    }

    /// The per-file direction: every column of one file, which is what an
    /// operator asking "what does Beacon hold for this file" needs. A column the
    /// file never declared is absent rather than null.
    #[tokio::test]
    async fn one_file_reports_only_the_columns_it_declares() {
        let (store, _dir) = store().await;

        let files: Vec<ObservedFile> = (0..3)
            .map(|i| ObservedFile::new(format!("argo/{i}.nc"), 10, 1))
            .collect();
        let file_ids = store.registry().intern_files(&files).unwrap();
        let columns = store.registry().intern_columns(&["TEMP", "PSAL"]).unwrap();

        let mut builder = SegmentBuilder::new();
        builder.push_file(file_ids[0], [(columns[0], stat(0.0, 10.0))]);
        builder.push_file(
            file_ids[1],
            [
                (columns[0], stat(20.0, 30.0)),
                (columns[1], stat(34.0, 35.0)),
            ],
        );
        store.commit_segment(builder).await.unwrap().unwrap();

        // The file that declared both.
        let both = store.file_column_stats(file_ids[1]).await.unwrap();
        assert_eq!(both.len(), 2);
        assert_eq!(both[0].min.as_primitive::<Float64Type>().value(0), 20.0);
        assert_eq!(both[0].max.as_primitive::<Float64Type>().value(0), 30.0);
        assert_eq!(both[0].null_count, Some(1));
        assert_eq!(both[0].row_count, Some(100));
        assert_eq!(both[1].max.as_primitive::<Float64Type>().value(0), 35.0);

        // The file that declared one; PSAL is missing, not null.
        let one = store.file_column_stats(file_ids[0]).await.unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].column_id, columns[0]);

        // A file the segment covers by id range but holds no row for.
        assert!(store.file_column_stats(file_ids[2]).await.unwrap().is_empty());
    }

    /// A file analyzed twice appears in two segments. The newest wins, exactly as
    /// it does for the pruning reader.
    #[tokio::test]
    async fn a_re_analyzed_file_reports_its_newest_range() {
        let (store, _dir) = store().await;
        let ids = store
            .registry()
            .intern_files(&[ObservedFile::new("a.nc", 1, 1)])
            .unwrap();
        let columns = store.registry().intern_columns(&["TEMP"]).unwrap();

        let mut first = SegmentBuilder::new();
        first.push_file(ids[0], [(columns[0], stat(0.0, 1.0))]);
        store.commit_segment(first).await.unwrap().unwrap();

        let mut second = SegmentBuilder::new();
        second.push_file(ids[0], [(columns[0], stat(50.0, 60.0))]);
        store.commit_segment(second).await.unwrap().unwrap();

        let stats = store.file_column_stats(ids[0]).await.unwrap();
        assert_eq!(stats.len(), 1, "one row per column, not one per segment");
        assert_eq!(stats[0].min.as_primitive::<Float64Type>().value(0), 50.0);
        assert_eq!(stats[0].segment, "segment-00000001.bfs");
    }

    #[tokio::test]
    async fn an_unknown_column_yields_nothing_rather_than_an_error() {
        let (store, _dir) = store().await;
        assert!(
            store
                .column_stats_by_name("NOT_A_COLUMN", (0, 100))
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn a_disjoint_file_range_reads_no_segment() {
        let (store, _dir) = store().await;
        let ids = store
            .registry()
            .intern_files(&[ObservedFile::new("a.nc", 1, 1)])
            .unwrap();
        let columns = store.registry().intern_columns(&["TEMP"]).unwrap();

        let mut builder = SegmentBuilder::new();
        builder.push_file(ids[0], [(columns[0], stat(0.0, 1.0))]);
        store.commit_segment(builder).await.unwrap();

        assert!(store.column_stats(columns[0], (500, 600)).await.unwrap().is_empty());
        assert_eq!(store.column_stats(columns[0], (0, 0)).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn an_empty_batch_commits_nothing() {
        let (store, _dir) = store().await;
        assert!(store.commit_segment(SegmentBuilder::new()).await.unwrap().is_none());
        assert_eq!(store.num_segments().await, 0);
    }

    #[tokio::test]
    async fn several_segments_each_contribute_their_own_rows() {
        let (store, _dir) = store().await;
        let ids = store
            .registry()
            .intern_files(&[
                ObservedFile::new("a.nc", 1, 1),
                ObservedFile::new("b.nc", 1, 1),
            ])
            .unwrap();
        let columns = store.registry().intern_columns(&["TEMP"]).unwrap();

        for (index, id) in ids.iter().enumerate() {
            let mut builder = SegmentBuilder::new();
            builder.push_file(*id, [(columns[0], stat(index as f64, index as f64 + 1.0))]);
            store.commit_segment(builder).await.unwrap();
        }

        assert_eq!(store.num_segments().await, 2);
        let stats = store.column_stats(columns[0], (0, 1)).await.unwrap();
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].file_ids, vec![0]);
        assert_eq!(stats[1].file_ids, vec![1]);
    }
}
