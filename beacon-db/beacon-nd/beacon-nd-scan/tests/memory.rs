//! The memory use of a scan of a file that is much larger than the memory budget.
//!
//! The file has no stored data: each read makes its values. So the peak of the
//! allocator shows only what the scan holds at one time.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Array, AsArray, Float64Array};
use arrow::datatypes::{DataType, Float64Type};
use async_trait::async_trait;
use beacon_nd_dataset::{
    Attributes, Dimension, Dimensions, NdArrowArray, NdDataset, NdVariable, Region,
};
use beacon_nd_execution::exec::nd_scan_plan;
use beacon_nd_scan::{NdFileOpener, NdScanOptions, NdSource, encoded_table_schema, file_schema};
use datafusion::config::ConfigOptions;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{binary, col, lit};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::{ObjectMeta, ObjectStore};

/// An allocator that counts the live bytes and their peak.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: each method gives its arguments to the system allocator and only counts bytes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size());
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_pointer = unsafe { System.realloc(pointer, layout, new_size) };
        if !new_pointer.is_null() {
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
            grow(new_size);
        }
        new_pointer
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const MIB: usize = 1024 * 1024;
/// The size of the `x` dimension: one time step of `temp` is 256 KiB.
const X: usize = 32 * 1024;
/// The rows of one chunk: 1 MiB of `temp`.
const BATCH_SIZE: usize = 128 * 1024;
const PARTITIONS: usize = 8;
const STORE: &str = "memory://";

/// `temp(time, x)` or `time(time)`. Each value is the index of its time step.
#[derive(Debug)]
struct LazyVariable {
    name: &'static str,
    dimensions: Dimensions,
    attributes: Attributes,
}

#[async_trait]
impl NdVariable for LazyVariable {
    fn name(&self) -> &str {
        self.name
    }

    fn data_type(&self) -> &DataType {
        &DataType::Float64
    }

    fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    fn chunk_shape(&self) -> Option<&[usize]> {
        None
    }

    fn attributes(&self) -> &Attributes {
        &self.attributes
    }

    async fn read(&self, region: &Region) -> beacon_nd_dataset::Result<NdArrowArray> {
        region.check(self.name, &self.dimensions)?;
        let (first, steps) = (region.start()[0], region.shape()[0]);
        let width = region.shape().get(1).copied().unwrap_or(1);
        let mut values = Vec::with_capacity(steps * width);
        for step in first..first + steps {
            values.resize(values.len() + width, step as f64);
        }
        let dimensions = Dimensions::try_new(
            self.dimensions
                .iter()
                .zip(region.shape())
                .map(|(dimension, size)| Dimension::new(dimension.name(), *size)),
        )?;
        Ok(NdArrowArray::try_new(
            Arc::new(Float64Array::from(values)),
            dimensions,
        )?)
    }
}

/// A file with `temp(time, x)` and `time(time)` and no stored data.
#[derive(Debug)]
struct LazyDataset {
    dimensions: Dimensions,
    attributes: Attributes,
}

impl LazyDataset {
    fn new(time: usize) -> Self {
        let dimensions =
            Dimensions::try_new([Dimension::new("time", time), Dimension::new("x", X)]).unwrap();
        Self {
            dimensions,
            attributes: Attributes::new(),
        }
    }
}

impl NdDataset for LazyDataset {
    fn name(&self) -> &str {
        "big.nd"
    }

    fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    fn attributes(&self) -> &Attributes {
        &self.attributes
    }

    fn variable_names(&self) -> Vec<Arc<str>> {
        vec!["temp".into(), "time".into()]
    }

    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        let (name, rank) = match name {
            "temp" => ("temp", 2),
            "time" => ("time", 1),
            _ => return None,
        };
        let dimensions = Dimensions::try_new(self.dimensions.iter().take(rank).cloned()).unwrap();
        Some(Arc::new(LazyVariable {
            name,
            dimensions,
            attributes: Attributes::new(),
        }))
    }
}

/// Opens the lazy file with `time` time steps and counts its opens.
#[derive(Debug)]
struct LazyOpener {
    time: usize,
    opens: AtomicUsize,
}

#[async_trait]
impl NdFileOpener for LazyOpener {
    fn format_name(&self) -> &'static str {
        "lazy"
    }

    async fn open(
        &self,
        _store: Arc<dyn ObjectStore>,
        _file: &ObjectMeta,
    ) -> beacon_nd_dataset::Result<Arc<dyn NdDataset>> {
        self.opens.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(LazyDataset::new(self.time)))
    }
}

fn session() -> SessionContext {
    let config = SessionConfig::new()
        .with_batch_size(BATCH_SIZE)
        .with_target_partitions(PARTITIONS);
    let ctx = SessionContext::new_with_config(config);
    let url = ObjectStoreUrl::parse(STORE).unwrap();
    ctx.register_object_store(url.as_ref(), Arc::new(InMemory::new()));
    ctx
}

/// The scan of the lazy file, repartitioned as a session with default settings does it.
fn scan(opener: Arc<LazyOpener>) -> (Arc<dyn ExecutionPlan>, FileScanConfig) {
    let time = opener.time;
    let schema = file_schema(&LazyDataset::new(time));
    let source = NdSource::new(
        opener,
        NdScanOptions::default(),
        encoded_table_schema(&schema),
    );
    let bytes = (time * X * 8) as u64;
    let config =
        FileScanConfigBuilder::new(ObjectStoreUrl::parse(STORE).unwrap(), Arc::new(source))
            .with_file(PartitionedFile::new("big.nd", bytes))
            .build();
    let minimum = ConfigOptions::default().optimizer.repartition_file_min_size;
    let config = config
        .file_source()
        .repartitioned(PARTITIONS, minimum, None, &config)
        .unwrap()
        .expect("the scan repartitions");
    (nd_scan_plan(config.clone()).unwrap(), config)
}

/// What one run of a plan gave.
struct Run {
    rows: usize,
    /// The peak of the live bytes in the run, above the live bytes at its start.
    peak: usize,
}

/// Run each partition of `plan` on its own task. Check and drop each batch.
async fn run(plan: Arc<dyn ExecutionPlan>) -> Run {
    let task = session().task_ctx();
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);

    let partitions = plan.properties().partitioning.partition_count();
    let mut tasks = Vec::new();
    for partition in 0..partitions {
        let mut stream = plan.execute(partition, Arc::clone(&task)).unwrap();
        tasks.push(tokio::spawn(async move {
            let mut rows = 0;
            while let Some(batch) = stream.next().await {
                let batch = batch.unwrap();
                let temp = batch.column_by_name("temp").unwrap();
                let time = batch.column_by_name("time").unwrap();
                let (temp, time) = (
                    temp.as_primitive::<Float64Type>(),
                    time.as_primitive::<Float64Type>(),
                );
                assert_eq!(temp.null_count() + time.null_count(), 0);
                // Each value of `temp` is the index of its time step.
                let last = batch.num_rows() - 1;
                assert_eq!(temp.value(0), time.value(0));
                assert_eq!(temp.value(last), time.value(last));
                rows += batch.num_rows();
            }
            rows
        }));
    }
    let mut rows = 0;
    for task in tasks {
        rows += task.await.unwrap();
    }
    Run {
        rows,
        peak: PEAK.load(Ordering::Relaxed).saturating_sub(start),
    }
}

/// `plan` under the filter `time >= first`, with the filter offered to the scan.
fn from_time(plan: Arc<dyn ExecutionPlan>, first: usize) -> Arc<dyn ExecutionPlan> {
    let schema = plan.schema();
    let predicate = binary(
        col("time", &schema).unwrap(),
        Operator::GtEq,
        lit(first as f64),
        &schema,
    )
    .unwrap();
    let filter: Arc<dyn ExecutionPlan> = Arc::new(FilterExec::try_new(predicate, plan).unwrap());
    FilterPushdown::new()
        .optimize(filter, &ConfigOptions::default())
        .unwrap()
}

fn entries(config: &FileScanConfig) -> usize {
    config.file_groups.iter().map(|group| group.len()).sum()
}

/// One test, so that no other test changes the counts of the allocator.
#[test]
fn a_scan_holds_a_few_chunks_and_not_the_file() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(PARTITIONS)
        .build()
        .unwrap();
    // 8 partitions hold a few chunks of 1 MiB each, with their encoded and flat forms.
    let budget = 128 * MIB;

    runtime.block_on(async {
        // A file of 256 MiB and a file of 2 GiB: the peak does not grow with the file.
        let mut peaks = Vec::new();
        for time in [1024, 8192] {
            let opener = Arc::new(LazyOpener {
                time,
                opens: AtomicUsize::new(0),
            });
            let (plan, config) = scan(Arc::clone(&opener));
            assert_eq!(entries(&config), (time * X * 8 / (10 * MIB)).min(32));

            let full = run(plan).await;
            let megabytes = time * X * 8 / MIB;
            println!(
                "full scan of {megabytes} MiB: {} rows, peak {} MiB, {} opens",
                full.rows,
                full.peak / MIB,
                opener.opens.load(Ordering::Relaxed)
            );
            assert_eq!(full.rows, time * X);
            assert!(full.peak < budget, "peak of {} bytes", full.peak);
            peaks.push(full.peak);
        }
        assert!(
            peaks[1] < 2 * peaks[0] + 16 * MIB,
            "the peak grows with the file: {peaks:?}"
        );

        // A filter that keeps the last quarter of the 2 GiB file.
        let time = 8192;
        let opener = Arc::new(LazyOpener {
            time,
            opens: AtomicUsize::new(0),
        });
        let (plan, _) = scan(Arc::clone(&opener));
        let filtered = run(from_time(plan, 3 * time / 4)).await;
        println!(
            "filtered scan: {} rows, peak {} MiB, {} opens",
            filtered.rows,
            filtered.peak / MIB,
            opener.opens.load(Ordering::Relaxed)
        );
        assert_eq!(filtered.rows, time / 4 * X);
        assert!(filtered.peak < budget, "peak of {} bytes", filtered.peak);
    });
}
