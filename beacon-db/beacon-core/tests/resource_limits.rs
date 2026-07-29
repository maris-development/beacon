//! Builder-configured execution limits: the VM memory pool and the batch size.
//!
//! The memory test doubles as the unit-contract guard for
//! `with_vm_memory_limit`: the builder takes **bytes**. A caller passing a
//! megabyte count through unconverted produces a kilobyte-scale pool, and the
//! first real sort fails — exactly the symptom this asserts.

mod common;

use common::{runtime, runtime_with, scalar_i64, total_rows, write_file, TestRuntime};

/// Writes a CSV big enough that sorting it cannot fit in a tiny memory pool.
fn write_big_csv(rt: &TestRuntime, rows: usize) {
    let mut contents = String::from("v\n");
    for i in 0..rows {
        contents.push_str(&format!("{i}\n"));
    }
    write_file(&rt.datasets_dir().join("big.csv"), &contents);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tiny_memory_pool_fails_a_large_sort() {
    // 16 KiB pool — what an accidental "16 (MB)" would produce.
    let rt = runtime_with("mem-tiny", |b| b.with_vm_memory_limit(16 * 1024)).await;
    write_big_csv(&rt, 200_000);

    let err = rt
        .try_sql("SELECT v FROM read_csv('big.csv') ORDER BY v")
        .await
        .err()
        .expect("a full sort of 200k rows cannot fit in a 16 KiB pool");
    let msg = err.to_string();
    assert!(
        msg.contains("memory") || msg.contains("Resources"),
        "the failure should be a memory-pool error, got: {msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_default_pool_runs_the_same_sort() {
    let rt = runtime("mem-default").await;
    write_big_csv(&rt, 200_000);

    let batches = rt
        .sql("SELECT v FROM read_csv('big.csv') ORDER BY v")
        .await;
    assert_eq!(total_rows(&batches), 200_000);
}

/// With beacon's result coalescer off, a file scan's `batch_size` is observable
/// directly on the output: no result batch exceeds it. (The default coalescer
/// would merge the scan's small batches back up to 64K rows, hiding the knob
/// entirely — see `coalescing_merges_scan_batches` below.)
#[tokio::test(flavor = "multi_thread")]
async fn batch_size_bounds_scan_batches_when_coalescing_is_off() {
    let rt = runtime_with("batch-size", |b| {
        b.with_batch_size(100).with_sql_stream_coalesce(
            beacon_core::settings::SqlStreamCoalesceSettings {
                enabled: false,
                ..Default::default()
            },
        )
    })
    .await;
    write_big_csv(&rt, 1_000);

    // Scan the CSV directly: `batch_size` governs the file reader. (A managed
    // table returns whatever batch shape its own scan produces, so it would not
    // reflect the knob.)
    let batches = rt.sql("SELECT v FROM read_csv('big.csv')").await;
    assert_eq!(total_rows(&batches), 1_000, "all rows must still arrive");
    let max_batch = batches.iter().map(|b| b.num_rows()).max().unwrap_or(0);
    assert!(
        max_batch <= 100,
        "with coalescing off, no result batch may exceed the batch size, got {max_batch}"
    );
    assert!(
        batches.len() >= 10,
        "1000 rows at a batch size of 100 should arrive in ≥10 batches, got {}",
        batches.len()
    );

    // Correctness is unaffected by the batch size.
    assert_eq!(
        scalar_i64(&rt.sql("SELECT sum(v) FROM read_csv('big.csv')").await),
        999 * 1_000 / 2
    );
}

/// The default coalescer merges the scan's many small batches into few large
/// ones (its target is far above these row counts), so a small `batch_size`
/// yields a single output batch — the behaviour that masks `batch_size` above.
#[tokio::test(flavor = "multi_thread")]
async fn coalescing_merges_scan_batches() {
    let rt = runtime_with("coalesce-on", |b| b.with_batch_size(100)).await;
    write_big_csv(&rt, 1_000);

    let batches = rt.sql("SELECT v FROM read_csv('big.csv')").await;
    assert_eq!(total_rows(&batches), 1_000);
    assert_eq!(
        batches.len(),
        1,
        "the default coalescer should merge 1000 rows into one batch"
    );
}
