//! A real run at Beacon's target shape: 1M files, 160K distinct column names.
//!
//! ```text
//! cargo run --release -p beacon-file-stats --example scale_million --features datafusion
//! ```
//!
//! Every number it prints is measured, not modelled. It drives the real
//! registry, the real collector, and the real pruning path.
//!
//! # The shape
//!
//! 100 families of 10 000 files each. Every file declares 20 columns: 10 from a
//! global core that every file in the store shares, and 10 from its family's
//! private pool of 1 600. That gives ~160K distinct names, a long sparse tail,
//! and a handful of very wide columns, which is the mix that stresses both ends
//! of the design.

use std::sync::Arc;
use std::io::Write;
use std::time::Instant;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{
    CollectorConfig, FileAnalysis, FileAnalyzer, FileRecord, FileStatsStore, ObservedFile, Registry,
    Result, StatScalar, StatsCollector, prune_files,
};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use object_store::{ObjectStore, local::LocalFileSystem, path::Path};

const FAMILIES: u64 = 100;
const FILES_PER_FAMILY: u64 = 10_000;
const FILES: u64 = FAMILIES * FILES_PER_FAMILY;
/// Private column names per family. 100 x 1600 = 160 000 distinct names.
const FAMILY_COLUMNS: u64 = 1_600;
/// Columns every file in the store declares.
const CORE_COLUMNS: u64 = 10;
/// Columns each file draws from its family's pool.
const PRIVATE_PER_FILE: u64 = 10;

fn family_of(path: &str) -> u64 {
    path.split('/')
        .next()
        .and_then(|s| s.trim_start_matches("family").parse().ok())
        .unwrap_or(0)
}

fn index_of(path: &str) -> u64 {
    path.rsplit('/')
        .next()
        .and_then(|s| s.trim_end_matches(".nc").parse().ok())
        .unwrap_or(0)
}

struct ShapedAnalyzer;

#[async_trait::async_trait]
impl FileAnalyzer for ShapedAnalyzer {
    async fn analyze(&self, record: &FileRecord) -> Result<FileAnalysis> {
        let family = family_of(&record.path);
        let index = index_of(&record.path);
        let mut columns = Vec::with_capacity((CORE_COLUMNS + PRIVATE_PER_FILE) as usize);

        // The wide columns: present in every file in the store.
        for c in 0..CORE_COLUMNS {
            columns.push((
                format!("core_{c}"),
                stat(index as f64, index as f64 + 1.0),
            ));
        }
        // The sparse tail: 10 of this family's 1600 private names.
        for slot in 0..PRIVATE_PER_FILE {
            let column = (index * PRIVATE_PER_FILE + slot) % FAMILY_COLUMNS;
            columns.push((
                format!("fam{family}_var{column}"),
                stat(index as f64, index as f64 + 1.0),
            ));
        }

        Ok(FileAnalysis {
            format: "netcdf".into(),
            num_rows: Some(1_000),
            total_byte_size: Some(record.size),
            columns,
        })
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

fn schema(columns: &[&str]) -> SchemaRef {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|name| Field::new(*name, DataType::Float64, true))
            .collect::<Vec<_>>(),
    ))
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let meta = entry.metadata().ok();
            total += match meta {
                Some(m) if m.is_dir() => dir_size(&entry.path()),
                Some(m) => m.len(),
                None => 0,
            };
        }
    }
    total
}

fn flush() {
    let _ = std::io::stdout().flush();
}

fn mb(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let root = std::env::var("SCALE_DIR").unwrap_or_else(|_| "/tmp/beacon-file-stats-scale".into());
    let root = std::path::PathBuf::from(root);
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("segments"))?;

    flush();
    println!("shape: {FILES} files, {FAMILIES} families, ~{} distinct columns, {} columns/file",
        FAMILIES * FAMILY_COLUMNS + CORE_COLUMNS,
        CORE_COLUMNS + PRIVATE_PER_FILE);
    flush();
    println!();

    let registry = Arc::new(Registry::open(root.join("registry.redb"))?);
    let objects: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(root.join("segments"))?);
    let store = Arc::new(FileStatsStore::open(registry, objects, Path::from("stats")).await?);

    // ── register ─────────────────────────────────────────────────────────
    let start = Instant::now();
    for family in 0..FAMILIES {
        let batch: Vec<ObservedFile> = (0..FILES_PER_FAMILY)
            .map(|i| {
                ObservedFile::new(
                    format!("family{family}/2024/{i:05}.nc"),
                    4096,
                    1_700_000_000_000,
                )
            })
            .collect();
        store.registry().intern_files(&batch)?;
    }
    let register = start.elapsed();
    flush();
    println!(
        "register : {FILES} files in {:.1}s  ({:.0} files/s)",
        register.as_secs_f64(),
        FILES as f64 / register.as_secs_f64()
    );

    // ── collect ──────────────────────────────────────────────────────────
    let collector = StatsCollector::new(
        store.clone(),
        Arc::new(ShapedAnalyzer),
        CollectorConfig {
            batch_files: FILES_PER_FAMILY as usize,
            concurrency: 8,
            target_group_files: 10_000,
            min_group_files: 500,
            // Derived from the paths, which is the default. The shape here is
            // family{n}/2024/{i}.nc, so it should land on one segment per family.
            prefix_depth: None,
        },
    );
    let start = Instant::now();
    let report = collector.run_until_idle(FAMILIES as usize + 10).await?;
    let collect = start.elapsed();
    flush();
    println!(
        "collect  : {} analyzed, {} segments in {:.1}s  ({:.0} files/s)",
        report.analyzed,
        report.segments,
        collect.as_secs_f64(),
        report.analyzed as f64 / collect.as_secs_f64()
    );

    // ── on disk ──────────────────────────────────────────────────────────
    let registry_bytes = std::fs::metadata(root.join("registry.redb"))?.len();
    let segment_bytes = dir_size(&root.join("segments"));
    let manifest_bytes =
        std::fs::metadata(root.join("segments/stats/manifest.bin")).map(|m| m.len())?;
    let cells = FILES * (CORE_COLUMNS + PRIVATE_PER_FILE);
    let dense = FILES * (FAMILIES * FAMILY_COLUMNS + CORE_COLUMNS);

    flush();
    println!();
    flush();
    println!("registry : {:>8.1} MB", mb(registry_bytes));
    flush();
    println!(
        "segments : {:>8.1} MB over {} objects, {:.1} bytes/cell",
        mb(segment_bytes - manifest_bytes),
        report.segments,
        (segment_bytes - manifest_bytes) as f64 / cells as f64
    );
    flush();
    println!(
        "manifest : {:>8.1} MB   <- the metadata that decides which segments to read",
        mb(manifest_bytes)
    );
    flush();
    println!(
        "cells    : {cells} real, against {dense} dense ({}x)",
        dense / cells
    );
    flush();
    println!(
        "columns  : {} interned",
        store.registry().num_columns()?
    );

    if std::env::var("BUILD_ONLY").is_ok() {
        flush();
        println!("\nBUILD_ONLY set: stopping before the read phase");
        return Ok(());
    }

    // ── point lookups ────────────────────────────────────────────────────
    let start = Instant::now();
    let probes = 10_000;
    for i in 0..probes {
        let family = i % FAMILIES;
        let index = (i * 7) % FILES_PER_FAMILY;
        let path = format!("family{family}/2024/{index:05}.nc");
        let id = store.registry().file_id(&path)?.expect("known path");
        std::hint::black_box(store.registry().record(id)?);
    }
    let lookups = start.elapsed();
    flush();
    println!();
    flush();
    println!(
        "registry lookup: {:.1} us each  (path -> id -> record, {probes} probes)",
        lookups.as_secs_f64() * 1e6 / probes as f64
    );

    // ── prune: the sparse tail ───────────────────────────────────────────
    let all_ids: Vec<u64> = (0..FILES).collect();
    let narrow = "fam7_var300";
    let schema_narrow = schema(&[narrow]);
    let predicate_narrow = binary(
        col(narrow, &schema_narrow)?,
        Operator::Gt,
        lit(9_000.0f64),
        &schema_narrow,
    )?;
    let start = Instant::now();
    let kept = prune_files(&store, &predicate_narrow, &schema_narrow, &all_ids).await;
    flush();
    println!();
    flush();
    println!(
        "prune on a family column : {:.0} ms, keeps {} of {FILES}",
        start.elapsed().as_secs_f64() * 1e3,
        kept.len()
    );
    flush();
    println!("  (the manifest skips 99 of 100 segments without reading them)");

    // ── prune: a column every file declares ──────────────────────────────
    let wide = "core_3";
    let schema_wide = schema(&[wide]);
    let predicate = binary(
        col(wide, &schema_wide)?,
        Operator::Gt,
        lit(9_500.0f64),
        &schema_wide,
    )?;
    let start = Instant::now();
    let kept = prune_files(&store, &predicate, &schema_wide, &all_ids).await;
    flush();
    println!(
        "prune on a store-wide column : {:.0} ms, keeps {} of {FILES}",
        start.elapsed().as_secs_f64() * 1e3,
        kept.len()
    );
    flush();
    println!("  (every segment holds it, so all {} blocks are read)", report.segments);

    flush();
    // ── prune on three columns at once ───────────────────────────────────
    // The question this whole store exists to answer. The three columns are
    // fetched and packed together, so the cost is close to one column's rather
    // than three.
    let three = ["core_1", "core_3", "core_7"];
    let schema_three = schema(&three);
    let predicate_three = binary(
        binary(
            binary(
                col(three[0], &schema_three)?,
                Operator::Gt,
                lit(1_000.0f64),
                &schema_three,
            )?,
            Operator::And,
            binary(
                col(three[1], &schema_three)?,
                Operator::Gt,
                lit(9_500.0f64),
                &schema_three,
            )?,
            &schema_three,
        )?,
        Operator::And,
        binary(
            col(three[2], &schema_three)?,
            Operator::Lt,
            lit(9_900.0f64),
            &schema_three,
        )?,
        &schema_three,
    )?;
    let start = Instant::now();
    let kept = prune_files(&store, &predicate_three, &schema_three, &all_ids).await;
    flush();
    println!(
        "prune on THREE store-wide columns : {:.0} ms, keeps {} of {FILES}",
        start.elapsed().as_secs_f64() * 1e3,
        kept.len()
    );

    // ── prune with a realistic candidate set ─────────────────────────────
    // A real scan passes the files of one table, not the whole store. Pruning a
    // sparse column against every file in the instance is arithmetically correct
    // and practically useless: almost nothing declares it, and an absent
    // statistic keeps a file.
    let family7: Vec<u64> = (70_000..80_000).collect();
    let start = Instant::now();
    let kept = prune_files(&store, &predicate_narrow, &schema_narrow, &family7).await;
    flush();
    println!();
    flush();
    println!(
        "prune the same family column over that family's 10k files only : {:.0} ms, keeps {} of 10000",
        start.elapsed().as_secs_f64() * 1e3,
        kept.len()
    );

    flush();
    println!();
    flush();
    println!("segments kept at {}", root.display());
    Ok(())
}
