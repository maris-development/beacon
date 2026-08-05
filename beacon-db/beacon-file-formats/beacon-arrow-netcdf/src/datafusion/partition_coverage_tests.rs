//! A NetCDF scan must read every file whatever `target_partitions` is.
//!
//! Regression test for silent row loss: when the scan's file-group count does
//! not equal `target_partitions`, DataFusion inserts a
//! `RepartitionExec(RoundRobinBatch)`, which *coalesces* small batches. Each
//! nd-encoded batch is one row carrying one file's nd array, so coalescing
//! produced multi-row encoded batches — and the decoder only read row 0, so
//! every file after the first in a coalesced batch vanished from the results.
//!
//! `count(*)` did not catch it: it is answered without decoding nd arrays, so
//! it stayed correct while the scan returned fewer rows.

use std::sync::Arc;

use beacon_datafusion_ext::file_collection::FileCollection;
use beacon_datafusion_ext::listing_factory::{ListingFactory, RootStore};
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::datafusion::{options::NetcdfOptions, NetcdfFormat};

/// Copy the ragged fixture `copies` times into a fresh temp dir.
fn stage_files(copies: usize) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("test_files")
        .join("wod_ctd_1964.nc");
    for i in 0..copies {
        std::fs::copy(&src, dir.path().join(format!("part_{i:04}.nc"))).expect("copy fixture");
    }
    dir
}

async fn ctx_for(dir: &tempfile::TempDir, target_partitions: usize) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(target_partitions);
    let ctx = SessionContext::new_with_config(config);
    let state = ctx.state();

    let factory = Arc::new(ListingFactory::dynamic());
    // Same wiring `create_with_native_root` does for a `file://` listing url:
    // object paths are absolute w.r.t. the filesystem root.
    let resolver = crate::datafusion::object_meta_resolver::create_object_resolver(
        &RootStore::FileSystem(std::path::PathBuf::from("/")),
    );
    let format = Arc::new(
        NetcdfFormat::new(factory, NetcdfOptions::default()).with_object_path_resolver(resolver),
    );

    let url = datafusion::datasource::listing::ListingTableUrl::parse(format!(
        "file://{}/",
        dir.path().to_string_lossy().replace('\\', "/")
    ))
    .expect("listing url");

    let table = FileCollection::new(&state, format, vec![url])
        .await
        .expect("file collection");
    ctx.register_table("nc", Arc::new(table)).unwrap();
    ctx
}

/// Rows the pipeline actually produces, by draining the record batches.
async fn scanned_rows(ctx: &SessionContext) -> usize {
    let batches = ctx
        .sql("SELECT * FROM nc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn count_star(ctx: &SessionContext) -> usize {
    let batches = ctx
        .sql("SELECT count(*) FROM nc")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("int64 count")
        .value(0) as usize
}

#[tokio::test(flavor = "multi_thread")]
async fn scan_reads_every_file_regardless_of_target_partitions() {
    const FILES: usize = 32;

    // One file first, to learn the per-file row count.
    let one = stage_files(1);
    let per_file = scanned_rows(&ctx_for(&one, 4).await).await;
    assert!(per_file > 0, "fixture produced no rows");

    let dir = stage_files(FILES);
    let expected = per_file * FILES;

    // 8/16/32 give one file group per partition (no repartition). 9, 24 and 31
    // do not, so a coalescing RoundRobinBatch repartition is inserted — those
    // were the shapes that silently lost files. 33 exceeds the file count, so
    // coalescing has nothing to merge.
    let mut failures = vec![];
    for tp in [1usize, 8, 9, 16, 24, 31, 32, 33] {
        let ctx = ctx_for(&dir, tp).await;
        let scanned = scanned_rows(&ctx).await;
        let counted = count_star(&ctx).await;

        if scanned != expected || counted != expected {
            failures.push(format!(
                "target_partitions={tp}: scanned {scanned} ({} files), count(*) {counted}, \
                 expected {expected}",
                scanned / per_file
            ));
        }
    }
    assert!(failures.is_empty(), "row loss:\n  {}", failures.join("\n  "));
}
