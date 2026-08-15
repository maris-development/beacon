//! Run one nd scan over a netCDF collection, for a profiler to watch.
//!
//! The server is not involved: this is the scan and the nd pipeline above it,
//! and nothing else. What it measures is what `time_elapsed_scanning_total`
//! measures, without a transport, a catalog or an HTTP client in the way.
//!
//! ```text
//! cargo build --release --example scan_profile
//! cargo flamegraph --example scan_profile -- data/datasets/cora/2023 24
//! ```
//!
//! Arguments: the directory to scan, and the partition count (default: the
//! machine's).

use std::sync::Arc;
use std::time::Instant;

use beacon_arrow_netcdf::datafusion::{FileAccess, NetcdfFormat};
use beacon_datafusion_ext::fast_object::FastObjectTable;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use beacon_datafusion_ext::nd::optimizer::{NdFilterPushdown, NdProjectionPushdown};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;

/// The query the profile is about by default: two data-variable predicates over
/// five projected columns. It reads every chunk of every file and the filter
/// then drops most of the cells, which is the shape a real CORA query has.
///
/// Override it with `BEACON_PROFILE_QUERY` to attribute the cost by stage —
/// fewer columns isolates the per-column read, dropping the predicate isolates
/// the filter, and `count(*)` takes the flat path that never builds an nd array.
const QUERY: &str = r#"SELECT "TEMP", "TIME", "LATITUDE", "LONGITUDE", "DEPH" FROM scan
                       WHERE "DEPH" < 5 AND "TEMP" IS NOT NULL"#;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .unwrap_or_else(|| "data/datasets/cora/2023".to_string());
    let partitions: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(8, |n| n.get()));

    let config = SessionConfig::new()
        .with_target_partitions(partitions)
        .with_extension(beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension());

    // The two rules `RuntimeBuilder` adds for the nd pipeline. Without them the
    // filter runs above the broadcast and the profile is dominated by
    // materialising cells the query throws away — which is a real cost, but not
    // the one this is for.
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(NdFilterPushdown::new()))
        .with_physical_optimizer_rule(Arc::new(NdProjectionPushdown::new()))
        .build();
    let ctx = SessionContext::new_with_state(state);

    // The Rust reader, which is what a server reads with by default. The
    // netcdf-c backend serialises on a process-global mutex, so profiling it
    // would measure the mutex.
    let format = NetcdfFormat::new(Arc::new(ListingFactory::dynamic()), Default::default())
        .with_access(FileAccess::Oxcdf);
    let url = ListingTableUrl::parse(&dir)?;

    let listed = Instant::now();
    let table = FastObjectTable::try_new(&ctx.state(), Arc::new(format), vec![url]).await?;
    ctx.register_table("scan", Arc::new(table))?;
    eprintln!("listed and inferred in {:.1?}", listed.elapsed());

    let query = std::env::var("BEACON_PROFILE_QUERY").unwrap_or_else(|_| QUERY.to_string());
    let planned = Instant::now();
    let plan = ctx.sql(&query).await?.create_physical_plan().await?;
    eprintln!("planned in {:.1?}", planned.elapsed());
    eprintln!(
        "{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
    );

    // Drain every partition concurrently, as a query does, and count rather than
    // collect: holding the result would profile the allocator instead.
    let ran = Instant::now();
    let mut tasks = Vec::new();
    let partition_count = ExecutionPlan::properties(plan.as_ref())
        .output_partitioning()
        .partition_count();
    for partition in 0..partition_count {
        let stream = ExecutionPlan::execute(plan.as_ref(), partition, ctx.task_ctx())?;
        tasks.push(tokio::spawn(async move {
            let mut rows = 0usize;
            let mut batches = stream;
            while let Some(batch) = batches.try_next().await? {
                rows += batch.num_rows();
            }
            Ok::<usize, datafusion::error::DataFusionError>(rows)
        }));
    }

    let mut rows = 0usize;
    for task in tasks {
        rows += task.await??;
    }
    let elapsed = ran.elapsed();

    eprintln!("{rows} rows on {partitions} partitions in {elapsed:.1?}");
    Ok(())
}
