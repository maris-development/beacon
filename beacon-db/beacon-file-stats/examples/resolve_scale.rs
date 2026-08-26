//! How long does a scan wait to turn its file list into prune candidates?
//!
//! ```text
//! cargo run --release -p beacon-file-stats --example resolve_scale
//! FILES=1000000 cargo run --release -p beacon-file-stats --example resolve_scale
//! ```
//!
//! Builds a registry of `FILES` analyzed files, then times the two lookups over
//! the whole list. `file_ids` resolves the path alone. `resolve_observed` also
//! reads each record and compares it against what the caller observed, which is
//! what keeps a file that changed since its analysis out of the prune. Each is
//! timed as one call, and as the chunked parallel form the prune path plans.
//!
//! Two rounds run, because the first pays for a cold page cache and a server
//! answering queries is warm.

use std::sync::Arc;
use std::time::Instant;

use beacon_file_stats::{AnalyzedFile, ObservedFile, Registry, Result};

fn files() -> u64 {
    std::env::var("FILES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000_000)
}

const BATCH: usize = 50_000;
/// The threshold `prune_tasks` uses.
const PRUNE_CHUNK: usize = 65_536;

fn observed_of(i: u64) -> ObservedFile {
    let family = i / 10_000;
    let index = i % 10_000;
    ObservedFile::new(
        format!("family{family}/2024/{index:05}.nc"),
        4096 + i,
        1_700_000_000_000 + i as i64,
    )
    .with_e_tag(Some(format!("\"{:016x}-{:x}\"", i, 4096 + i)))
}

fn prune_tasks(candidates: usize) -> usize {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    (candidates / PRUNE_CHUNK).clamp(1, cores)
}

fn main() -> Result<()> {
    let n = files();
    let dir = tempfile::tempdir().unwrap();
    let registry = Arc::new(Registry::open(dir.path().join("registry.redb"))?);

    // ── build ────────────────────────────────────────────────────────────
    let start = Instant::now();
    for base in (0..n).step_by(BATCH) {
        let batch: Vec<ObservedFile> = (base..(base + BATCH as u64).min(n)).map(observed_of).collect();
        let ids = registry.intern_files(&batch)?;
        let analyzed: Vec<AnalyzedFile> = ids
            .iter()
            .map(|id| AnalyzedFile {
                id: *id,
                format: "netcdf",
                num_rows: Some(2048),
                total_byte_size: Some(4096),
                column_count: 20,
            })
            .collect();
        registry.mark_analyzed_batch(&analyzed)?;
        if base % 500_000 == 0 {
            println!("  interned {base}...");
        }
    }
    let db_bytes = std::fs::metadata(dir.path().join("registry.redb")).map(|m| m.len()).unwrap_or(0);
    println!(
        "build: {n} files in {:.1}s, registry file {:.0} MiB",
        start.elapsed().as_secs_f64(),
        db_bytes as f64 / (1024.0 * 1024.0)
    );

    // The list a scan hands over: every file, in listing order.
    let observed: Vec<ObservedFile> = (0..n).map(observed_of).collect();
    let paths: Vec<String> = observed.iter().map(|o| o.path.clone()).collect();
    let refs: Vec<&str> = paths.iter().map(|s| s.as_str()).collect();

    let report = |what: &str, elapsed: std::time::Duration| {
        println!(
            "{what:<34} {:>8.0} ms   ({:.2} us/file)",
            elapsed.as_secs_f64() * 1e3,
            elapsed.as_secs_f64() * 1e6 / n as f64
        );
    };

    for round in 1..=2 {
        println!("\n── round {round} ──");

        let t = Instant::now();
        let ids = registry.file_ids(&refs)?;
        report("file_ids, one call", t.elapsed());
        assert_eq!(ids.iter().filter(|i| i.is_some()).count(), n as usize);

        let t = Instant::now();
        let resolved = registry.resolve_observed(&observed)?;
        report("resolve_observed, one call", t.elapsed());
        assert_eq!(
            resolved.iter().filter(|r| r.map(|r| r.unchanged).unwrap_or(false)).count(),
            n as usize
        );

        let tasks = prune_tasks(n as usize);
        let size = (n as usize).div_ceil(tasks);

        let t = Instant::now();
        std::thread::scope(|scope| {
            let handles: Vec<_> = refs
                .chunks(size)
                .map(|chunk| {
                    let registry = Arc::clone(&registry);
                    scope.spawn(move || registry.file_ids(chunk).unwrap())
                })
                .collect();
            for h in handles {
                std::hint::black_box(h.join().unwrap());
            }
        });
        report(&format!("file_ids, {tasks} threads"), t.elapsed());

        let t = Instant::now();
        std::thread::scope(|scope| {
            let handles: Vec<_> = observed
                .chunks(size)
                .map(|chunk| {
                    let registry = Arc::clone(&registry);
                    scope.spawn(move || registry.resolve_observed(chunk).unwrap())
                })
                .collect();
            for h in handles {
                std::hint::black_box(h.join().unwrap());
            }
        });
        report(&format!("resolve_observed, {tasks} threads"), t.elapsed());
    }

    Ok(())
}
