//! The scale properties the format exists to provide.
//!
//! These are the claims that decide whether the design survives 1M files and
//! 160K columns. Unit tests confirm the pieces behave; these confirm the pieces
//! behave *at shape*: cost tracks the cells that exist, and a lookup does not
//! get more expensive as a segment gets wider.
//!
//! The counts here are small enough to run in a normal test suite. The ratios
//! they assert are what carry to the real numbers.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::DataType;
use beacon_file_stats::segment::{ColumnStat, SegmentBuilder, SegmentReader};
use beacon_file_stats::{ColumnId, FileId, StatScalar};
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as OsResult,
};

/// An object store that counts the `get_opts` calls reaching it.
///
/// A column lookup is meant to cost a fixed number of ranged reads whatever the
/// segment's width. Nothing but a request count proves that.
#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    gets: AtomicUsize,
}

impl CountingStore {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(InMemory::new()),
            gets: AtomicUsize::new(0),
        })
    }

    fn take_gets(&self) -> usize {
        self.gets.swap(0, Ordering::SeqCst)
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> OsResult<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> OsResult<GetResult> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> OsResult<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn stat(min: f64, max: f64) -> ColumnStat {
    ColumnStat {
        min: StatScalar::F64(min),
        max: StatScalar::F64(max),
        null_count: 0,
        row_count: 1_000,
        data_type: DataType::Float64,
    }
}

/// Build a segment over `files` files drawn from `distinct_columns` names, each
/// file declaring `per_file` of them. Column `k` goes to the files whose id is
/// congruent to it, so every column lands in a predictable, sparse set of files.
fn build(files: u64, distinct_columns: u32, per_file: u32) -> SegmentBuilder {
    let mut builder = SegmentBuilder::new();
    for file_id in 0..files {
        let stats: Vec<(ColumnId, ColumnStat)> = (0..per_file)
            .map(|slot| {
                let column = ((file_id as u32).wrapping_mul(per_file) + slot) % distinct_columns;
                (column, stat(file_id as f64, file_id as f64 + 1.0))
            })
            .collect();
        builder.push_file(file_id as FileId, stats);
    }
    builder
}

/// The claim the whole design rests on: cost follows the (file, column) pairs
/// that exist, not the product of files and columns.
///
/// At the real numbers the dense matrix is 1.6e11 cells and cannot be built,
/// while the sparse one is ~50M cells and fits in about a gigabyte. This test
/// holds the same ratio at a size that runs in a second.
#[test]
fn segment_size_tracks_the_cells_that_exist() {
    let (files, distinct_columns, per_file) = (5_000u64, 4_000u32, 8u32);
    let finished = build(files, distinct_columns, per_file).finish().unwrap();

    let cells = files * per_file as u64;
    let dense_cells = files * distinct_columns as u64;
    let bytes = finished.bytes.len() as u64;
    let per_cell = bytes as f64 / cells as f64;

    println!(
        "files={files} distinct_columns={distinct_columns} per_file={per_file}\n\
         cells={cells} (dense would be {dense_cells}, {}x more)\n\
         segment={bytes} bytes, {per_cell:.1} bytes/cell, {} columns present",
        dense_cells / cells,
        finished.column_ids.len()
    );

    // The dense matrix is 100x larger here. Even at a pessimistic byte-per-cell
    // the sparse segment must stay far under a dense one at the same rate.
    assert!(
        bytes < dense_cells * 8,
        "segment is not tracking the sparse cell count"
    );

    // Per-cell cost with headroom for the rare-column tail: 4000 blocks over
    // 40000 cells means only ten rows per block, so block framing is paid ten
    // times more often here than in a realistic prefix-local batch.
    assert!(
        per_cell < 120.0,
        "per-cell cost regressed to {per_cell:.1} bytes"
    );
}

/// What a block's framing costs, so a regression in it is visible.
///
/// A cell is 40 bytes today: file id, min, max, null count, and row count, all
/// 8 bytes. A prefix-local batch reaches that floor because each block spreads
/// its framing over many rows. A scattered batch of the same cell count pays the
/// framing per handful of rows instead.
///
/// The saving is real but modest, and it is *not* the reason to batch by prefix.
/// That reason is manifest skipping, which
/// [`prefix_local_batches_let_the_manifest_skip`] covers.
#[test]
fn block_framing_is_bounded_and_amortizes_over_rows() {
    let (files, per_file) = (5_000u64, 8u32);
    let cells = files * per_file as u64;

    let local = build(files, 64, per_file).finish().unwrap();
    let scattered = build(files, 4_000, per_file).finish().unwrap();

    let local_per_cell = local.bytes.len() as f64 / cells as f64;
    let scattered_per_cell = scattered.bytes.len() as f64 / cells as f64;
    let extra_blocks = (scattered.column_ids.len() - local.column_ids.len()) as f64;
    let framing_per_block =
        (scattered.bytes.len() - local.bytes.len()) as f64 / extra_blocks;

    println!(
        "prefix-local : {} columns, {} bytes, {local_per_cell:.1} bytes/cell\n\
         scattered    : {} columns, {} bytes, {scattered_per_cell:.1} bytes/cell\n\
         framing      : ~{framing_per_block:.0} bytes per extra block",
        local.column_ids.len(),
        local.bytes.len(),
        scattered.column_ids.len(),
        scattered.bytes.len(),
    );

    // 40 bytes of payload per cell, and framing amortized to almost nothing.
    assert!(
        (40.0..41.0).contains(&local_per_cell),
        "a prefix-local batch should sit at the 40-byte payload floor, not {local_per_cell:.1}"
    );
    assert!(
        local_per_cell < scattered_per_cell,
        "spreading the same cells over more blocks must not get cheaper"
    );
    assert!(
        framing_per_block < 200.0,
        "block framing grew to {framing_per_block:.0} bytes"
    );
}

/// The actual reason to batch a segment by path prefix.
///
/// A prefix-local segment holds few distinct columns, so a predicate on a column
/// it does not hold skips it without a single read. A segment batched by arrival
/// order holds a broad slice of the column space, so nearly every query matches
/// nearly every segment and the skip stops working.
#[test]
fn prefix_local_batches_let_the_manifest_skip() {
    use beacon_file_stats::{Manifest, SegmentEntry};

    fn entry(name: &str, finished: &beacon_file_stats::segment::FinishedSegment) -> SegmentEntry {
        SegmentEntry {
            seq: 0,
            name: name.to_string(),
            min_file_id: finished.min_file_id,
            max_file_id: finished.max_file_id,
            num_files: finished.num_files,
            column_ids: finished.column_ids.clone(),
        }
    }

    // Four prefix-local batches, each drawing from its own 64-column family.
    let mut local = Manifest::new();
    for family in 0..4u32 {
        let mut builder = SegmentBuilder::new();
        for file in 0..200u64 {
            let file_id = family as u64 * 200 + file;
            let columns: Vec<(ColumnId, ColumnStat)> = (0..8)
                .map(|slot| (family * 64 + slot, stat(0.0, 1.0)))
                .collect();
            builder.push_file(file_id, columns);
        }
        local.add_segment(entry(&format!("local-{family}"), &builder.finish().unwrap()));
    }

    // Four scattered batches over the same 256 columns and the same files.
    let mut scattered = Manifest::new();
    for batch in 0..4u32 {
        let mut builder = SegmentBuilder::new();
        for file in 0..200u64 {
            let file_id = batch as u64 * 200 + file;
            let columns: Vec<(ColumnId, ColumnStat)> = (0..8)
                .map(|slot| (((file_id as u32) * 8 + slot) % 256, stat(0.0, 1.0)))
                .collect();
            builder.push_file(file_id, columns);
        }
        scattered.add_segment(entry(&format!("scattered-{batch}"), &builder.finish().unwrap()));
    }

    let range = (0, 799);
    let local_hits = local.candidates(70, range).len();
    let scattered_hits = scattered.candidates(70, range).len();

    println!(
        "column 70 over 4 segments: prefix-local reads {local_hits}, scattered reads {scattered_hits}"
    );

    assert_eq!(local_hits, 1, "only the family that owns the column is read");
    assert_eq!(
        scattered_hits, 4,
        "a scattered batch leaves every segment a candidate"
    );
}

/// Every column present must be readable, and a column must report exactly the
/// files that declared it. A sparse layout that loses rows is worse than none.
#[tokio::test]
async fn every_column_reads_back_exactly_the_files_that_declared_it() {
    let (files, distinct_columns, per_file) = (500u64, 200u32, 4u32);

    // Rebuild the expected mapping independently of the writer.
    let mut expected: std::collections::HashMap<ColumnId, Vec<u64>> = Default::default();
    for file_id in 0..files {
        for slot in 0..per_file {
            let column = ((file_id as u32).wrapping_mul(per_file) + slot) % distinct_columns;
            expected.entry(column).or_default().push(file_id);
        }
    }
    for ids in expected.values_mut() {
        ids.dedup();
    }

    let finished = build(files, distinct_columns, per_file).finish().unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let path = Path::from("segment.bfs");
    store.put(&path, finished.bytes.into()).await.unwrap();

    let reader = SegmentReader::open(store, path).await.unwrap();
    for (column, want) in &expected {
        let stats = reader
            .column(*column)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("column {column} is missing from the segment"));
        assert_eq!(&stats.file_ids, want, "column {column} lost or gained files");
        assert!(
            stats.file_ids.windows(2).all(|w| w[0] < w[1]),
            "column {column} is not sorted by file id"
        );
    }
}

/// A lookup costs a fixed number of ranged reads however wide the segment gets.
///
/// This is what the two-level index buys. If it ever regresses, a query against
/// a 160K-column store starts paying for columns it never asked about.
#[tokio::test]
async fn a_column_lookup_costs_the_same_however_wide_the_segment() {
    async fn reads_for_one_lookup(distinct_columns: u32) -> usize {
        let counting = CountingStore::new();
        let store: Arc<dyn ObjectStore> = counting.clone();
        let path = Path::from("segment.bfs");

        let finished = build(2_000, distinct_columns, 8).finish().unwrap();
        store.put(&path, finished.bytes.into()).await.unwrap();

        counting.take_gets();
        let reader = SegmentReader::open(store.clone(), path).await.unwrap();
        let open_cost = counting.take_gets();

        reader.column(distinct_columns / 2).await.unwrap().unwrap();
        let lookup_cost = counting.take_gets();

        println!("{distinct_columns} columns: open={open_cost} reads, lookup={lookup_cost} reads");
        lookup_cost
    }

    let narrow = reads_for_one_lookup(100).await;
    let wide = reads_for_one_lookup(8_000).await;

    assert_eq!(narrow, wide, "lookup cost grew with the segment's width");
    assert!(
        wide <= 2,
        "a lookup should cost one index read and one block read, not {wide}"
    );
}
