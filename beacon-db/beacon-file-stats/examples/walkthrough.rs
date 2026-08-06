//! One full pass through the system, printed as it goes.
//!
//! Run it with:
//!
//! ```text
//! cargo run -p beacon-file-stats --example walkthrough --features datafusion
//! ```
//!
//! Everything here is real: a real redb database, real segment objects, and a
//! real DataFusion `PruningPredicate`. Only the analyzer is a stand-in, because
//! reading a netCDF file's statistics belongs to the format layer.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use beacon_file_stats::segment::ColumnStat;
use beacon_file_stats::{
    CollectorConfig, FileAnalysis, FileAnalyzer, FileRecord, FileStatsStore, ObservedFile, Registry,
    Result, StatScalar, StatsCollector, prune_files,
};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use object_store::{ObjectStore, memory::InMemory, path::Path};

/// Stands in for the format layer. A real one opens the file and asks the
/// reader for its statistics; this one derives them from the path so the
/// example stays self-contained.
struct DemoAnalyzer;

#[async_trait::async_trait]
impl FileAnalyzer for DemoAnalyzer {
    async fn analyze(&self, record: &FileRecord) -> Result<FileAnalysis> {
        // Pretend each float's temperature band follows its index, and that the
        // Atlantic files also carry salinity.
        let index: f64 = record
            .path
            .rsplit('/')
            .next()
            .and_then(|name| name.trim_end_matches(".nc").parse().ok())
            .unwrap_or(0.0);

        let temp = ColumnStat {
            min: StatScalar::F64(index),
            max: StatScalar::F64(index + 2.0),
            null_count: Some(3),
            row_count: Some(1_000),
            data_type: DataType::Float64,
        };

        let mut columns = vec![("TEMP".to_string(), temp)];
        if record.path.starts_with("atlantic/") {
            columns.push((
                "PSAL".to_string(),
                ColumnStat {
                    min: StatScalar::F64(34.0),
                    max: StatScalar::F64(35.5),
                    null_count: Some(0),
                    row_count: Some(1_000),
                    data_type: DataType::Float64,
                },
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

fn table_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("TEMP", DataType::Float64, true),
        Field::new("PSAL", DataType::Float64, true),
    ]))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;

    // ── 1. Open the store ────────────────────────────────────────────────
    // The registry is a B-tree, because "what is this path's id" is a point
    // lookup. The segments are objects, because they are read by byte range.
    // In Beacon both sit in the one beacon.db; here the object half is memory.
    let registry = Arc::new(Registry::open(dir.path().join("registry.redb"))?);
    let objects: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(FileStatsStore::open(registry, objects, Path::from("__file_stats__")).await?);

    println!("1. store opened");

    // ── 2. Register what a listing found ─────────────────────────────────
    // Nothing is read yet. This only assigns ids and queues the files.
    let mut discovered = Vec::new();
    for i in 0..6 {
        discovered.push(ObservedFile::new(format!("atlantic/2024/{i}.nc"), 4096, 1_700_000_000_000));
    }
    for i in 0..4 {
        discovered.push(ObservedFile::new(format!("pacific/2024/{i}.nc"), 4096, 1_700_000_000_000));
    }
    let ids = store.registry().intern_files(&discovered)?;

    println!(
        "2. registered {} files as ids {}..={}, all pending",
        ids.len(),
        ids[0],
        ids[ids.len() - 1]
    );

    // ── 3. Let the collector fill the store ──────────────────────────────
    // It groups the batch by path prefix, so "atlantic/2024" and "pacific/2024"
    // become separate segments. That is what lets the manifest skip later.
    let collector = StatsCollector::new(
        store.clone(),
        Arc::new(DemoAnalyzer),
        CollectorConfig {
            batch_files: 1_000,
            concurrency: 4,
            target_group_files: 10_000,
            min_group_files: 500,
            prefix_depth: Some(2),
        },
    );
    let report = collector.run_once().await?;

    println!(
        "3. collector: {} analyzed, {} failed, {} groups -> {} segments",
        report.analyzed, report.failed, report.groups, report.segments
    );

    // ── 4. Ask a question ────────────────────────────────────────────────
    // The registry answers per-file questions without touching a segment.
    let id = store.registry().file_id("atlantic/2024/3.nc")?.unwrap();
    let record = store.registry().record(id)?.unwrap();
    println!(
        "4. registry: {} is id {id}, {} rows, read by {}",
        record.path,
        record.num_rows.unwrap(),
        record.format
    );

    // ── 5. Prune ─────────────────────────────────────────────────────────
    // WHERE TEMP > 6.5. Only the TEMP blocks are read. PSAL is never touched,
    // and neither is any segment that holds no TEMP.
    let schema = table_schema();
    let predicate = binary(col("TEMP", &schema)?, Operator::Gt, lit(6.5f64), &schema)?;

    let kept = prune_files(&store, &predicate, &schema, &ids).await;
    println!("5. WHERE TEMP > 6.5 keeps {} of {} files: {kept:?}", kept.len(), ids.len());

    for id in &ids {
        let path = store.registry().record(*id)?.unwrap().path;
        let index: f64 = path.rsplit('/').next().unwrap().trim_end_matches(".nc").parse()?;
        let mark = if kept.contains(id) { "keep" } else { "SKIP" };
        println!("     {mark}  id={id:<2} {path:<22} TEMP in [{index}, {}]", index + 2.0);
    }

    // ── 6. A predicate on a column only one family declares ──────────────
    let predicate = binary(col("PSAL", &schema)?, Operator::Gt, lit(40.0f64), &schema)?;
    let kept = prune_files(&store, &predicate, &schema, &ids).await;
    println!(
        "6. WHERE PSAL > 40 keeps {} of {} files: {kept:?}",
        kept.len(),
        ids.len()
    );
    println!("     the pacific files never declared PSAL, so they are not prunable on it");

    Ok(())
}
