//! The store: registry, manifest, and segments behind one handle.

use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::manifest::{Manifest, SegmentEntry};
use crate::registry::Registry;
use crate::segment::{ColumnStats, SegmentBuilder, SegmentReader};
use crate::types::{ColumnId, FileId};

/// Statistics for every known file, split across immutable segments.
pub struct FileStatsStore {
    registry: Arc<Registry>,
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
        Ok(Self {
            registry,
            store,
            prefix,
            manifest: RwLock::new(manifest),
        })
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
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
        let name = format!("segment-{:08}.bfs", manifest.segments.len());
        self.store
            .put(&self.prefix.clone().join(name.as_str()), finished.bytes.into())
            .await?;

        let entry = SegmentEntry {
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

        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let reader =
                SegmentReader::open(self.store.clone(), self.prefix.clone().join(name.as_str()))
                    .await?;
            if let Some(stats) = reader.column(column_id).await? {
                out.push(stats);
            }
        }
        Ok(out)
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
            null_count: 1,
            row_count: 100,
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
        assert_eq!(temp.null_count, vec![1, 1]);
        assert_eq!(temp.row_count, vec![100, 100]);

        let psal = store.column_stats_by_name("PSAL", (0, 2)).await.unwrap();
        assert_eq!(psal[0].file_ids, vec![1, 2]);
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
