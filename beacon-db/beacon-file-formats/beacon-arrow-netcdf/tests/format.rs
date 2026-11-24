//! Tests of the NetCDF file format: schema, statistics and the write plan.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_arrow_netcdf::datafusion::options::NetcdfOptions;
use beacon_arrow_netcdf::datafusion::NetcdfFormat;
use beacon_datafusion_ext::listing_factory::ListingFactory;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::FileSinkConfig;
use object_store::ObjectStore;

mod common;
use common::*;

/// A format with statistics on computes real ranges.
#[tokio::test]
async fn a_format_with_statistics_on_computes_ranges() {
    let ctx = one_partition_session();
    let state = ctx.state();
    let (store, object) = local_object(&test_file(GRIDDED_FILE)).await;

    let format = format().with_enable_statistics(true);
    let schema = format
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .expect("a schema");

    let statistics = format
        .infer_stats(&state, &store, schema, &object)
        .await
        .expect("statistics are never an error");
    assert!(
        statistics
            .column_statistics
            .iter()
            .any(|column| column.min_value.get_value().is_some()),
        "the coordinate variables get real ranges"
    );
}

/// A format with statistics off computes nothing.
#[tokio::test]
async fn a_format_with_statistics_off_computes_nothing() {
    let ctx = one_partition_session();
    let state = ctx.state();
    let (store, object) = local_object(&test_file(GRIDDED_FILE)).await;

    let oxcdf = format().with_enable_statistics(true);
    let schema = oxcdf
        .infer_schema(&state, &store, std::slice::from_ref(&object))
        .await
        .unwrap();

    let off = format().with_enable_statistics(false);
    let statistics = off
        .infer_stats(&state, &store, schema, &object)
        .await
        .unwrap();
    assert!(
        statistics
            .column_statistics
            .iter()
            .all(|column| column.min_value.get_value().is_none()),
        "enable_statistics=false must still mean no statistics"
    );
}

/// One bad file in a glob must be named, so the user can find it.
#[tokio::test]
async fn infer_schema_names_the_invalid_file() {
    // `put` and `head` live on this extension trait.
    use object_store::ObjectStoreExt;

    let ctx = one_partition_session();
    let store = Arc::new(object_store::memory::InMemory::new());
    let location = object_store::path::Path::from("argo/broken.nc");
    store
        .put(&location, b"not a netcdf file".to_vec().into())
        .await
        .expect("the in-memory store accepts the object");
    let object = store
        .head(&location)
        .await
        .expect("the object was just written");
    let object_store: Arc<dyn ObjectStore> = store;

    let error = format()
        .infer_schema(&ctx.state(), &object_store, &[object])
        .await
        .expect_err("random bytes are not a NetCDF file");

    let message = error.to_string();
    assert!(
        message.contains("'argo/broken.nc'"),
        "the error must name the file: {message}"
    );
    assert_eq!(
        message.matches("Execution error").count(),
        1,
        "the error must not repeat its prefix: {message}"
    );
}

/// What a statistics backfill actually costs per file.
///
/// Run it with:
///
/// ```text
/// cargo test --release -p beacon-arrow-netcdf --test format \
///     statistics_backfill_cost -- --ignored --nocapture
/// ```
///
/// Ignored because it is a measurement, not an assertion. It reads the same
/// file repeatedly, so the bytes are in the page cache: this is the parse
/// and range-scan cost, and a **lower bound**. A cold local disk adds seek
/// time, and object storage adds a round trip per file that will usually
/// dominate everything here.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "measurement, not an assertion"]
async fn statistics_backfill_cost() {
    use futures::StreamExt;
    use std::time::Instant;

    const ITERATIONS: usize = 200;
    const CONCURRENCY: usize = 8;

    for file in [GRIDDED_FILE] {
        let ctx = one_partition_session();
        let state = ctx.state();
        let (store, object) = local_object(&test_file(file)).await;
        let format = Arc::new(format().with_enable_statistics(true));

        let schema = format
            .infer_schema(&state, &store, std::slice::from_ref(&object))
            .await
            .unwrap();
        let bytes = object.size;

        // Serial: schema + stats, which is what the analyzer does per file.
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let schema = format
                .infer_schema(&state, &store, std::slice::from_ref(&object))
                .await
                .unwrap();
            let _ = format
                .infer_stats(&state, &store, schema, &object)
                .await
                .unwrap();
        }
        let serial = start.elapsed().as_secs_f64() / ITERATIONS as f64;

        // Concurrent: the shape a collector runs in.
        let start = Instant::now();
        futures::stream::iter(0..ITERATIONS)
            .map(|_| {
                let format = format.clone();
                let state = state.clone();
                let store = store.clone();
                let object = object.clone();
                async move {
                    let schema = format
                        .infer_schema(&state, &store, std::slice::from_ref(&object))
                        .await
                        .unwrap();
                    format
                        .infer_stats(&state, &store, schema, &object)
                        .await
                        .unwrap()
                }
            })
            .buffer_unordered(CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let concurrent_total = start.elapsed().as_secs_f64();
        let rate = ITERATIONS as f64 / concurrent_total;

        // The same work spawned onto the runtime rather than polled from one
        // task. `buffer_unordered` gives concurrency, not parallelism: if the
        // work is CPU bound between await points, every future runs on the
        // single task polling them.
        let start = Instant::now();
        let mut set = tokio::task::JoinSet::new();
        for _ in 0..ITERATIONS {
            let format = format.clone();
            let state = state.clone();
            let store = store.clone();
            let object = object.clone();
            set.spawn(async move {
                let schema = format
                    .infer_schema(&state, &store, std::slice::from_ref(&object))
                    .await
                    .unwrap();
                format
                    .infer_stats(&state, &store, schema, &object)
                    .await
                    .unwrap()
            });
        }
        while set.join_next().await.is_some() {}
        let spawned_rate = ITERATIONS as f64 / start.elapsed().as_secs_f64();

        let ranged = {
            let statistics = format
                .infer_stats(&state, &store, schema.clone(), &object)
                .await
                .unwrap();
            statistics
                .column_statistics
                .iter()
                .filter(|c| c.min_value.get_value().is_some())
                .count()
        };

        println!(
            "\n{file}  ({} KiB, {} columns, {ranged} with ranges)\n  \
             serial               : {:.1} ms/file ({:.0} files/s)\n  \
             buffer_unordered({CONCURRENCY})  : {:.0} files/s   -> {:.1} h for 1M\n  \
             spawned              : {:.0} files/s   -> {:.1} h for 1M",
            bytes / 1024,
            schema.fields().len(),
            serial * 1e3,
            1.0 / serial,
            rate,
            1_000_000.0 / rate / 3600.0,
            spawned_rate,
            1_000_000.0 / spawned_rate / 3600.0,
        );
    }
}

#[tokio::test]
async fn combine_attrs_with_write_dimensions_is_an_error() {
    use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode};
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::empty::EmptyExec;

    let format = NetcdfFormat::new(
        Arc::new(ListingFactory::dynamic()),
        NetcdfOptions {
            write_dimensions: Some(vec!["lat".to_string()]),
            writer: beacon_arrow_netcdf::datafusion::options::NetcdfWriterOptions {
                combine_attrs: Some(beacon_arrow_netcdf::attributes::CombineAttrs::Identical),
                ..Default::default()
            },
            ..NetcdfOptions::default()
        },
    );
    let ctx = one_partition_session();
    let schema: SchemaRef = Arc::new(arrow::datatypes::Schema::empty());
    let conf = FileSinkConfig {
        original_url: String::new(),
        object_store_url: ObjectStoreUrl::local_filesystem(),
        file_group: FileGroup::default(),
        table_paths: vec![],
        output_schema: schema.clone(),
        table_partition_cols: vec![],
        insert_op: InsertOp::Append,
        keep_partition_by_columns: false,
        file_extension: "nc".to_string(),
        file_output_mode: FileOutputMode::SingleFile,
    };
    let error = format
        .create_writer_physical_plan(Arc::new(EmptyExec::new(schema)), &ctx.state(), conf, None)
        .await
        .err()
        .expect("combine_attrs with write_dimensions is an error")
        .to_string();
    assert!(error.contains("combine_attrs"), "{error}");
}

/// A partitioned table is refused, and the error names the partition column.
#[tokio::test]
async fn a_partitioned_table_is_refused() {
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };

    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("year=2023");
    std::fs::create_dir_all(&part).unwrap();
    std::fs::copy(test_file(GRIDDED_FILE), part.join("obs.nc")).unwrap();

    let options = ListingOptions::new(Arc::new(format()))
        .with_file_extension("")
        .with_collect_stat(false)
        .with_table_partition_cols(vec![("year".to_string(), arrow::datatypes::DataType::Utf8)]);
    let ctx = one_partition_session();
    let url = ListingTableUrl::parse(dir.path().to_string_lossy()).unwrap();
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .unwrap();
    ctx.register_table("obs", Arc::new(ListingTable::try_new(config).unwrap()))
        .unwrap();

    let error = ctx
        .sql("SELECT lat FROM obs")
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("a partitioned table is refused");

    assert!(
        matches!(
            error.find_root(),
            datafusion::error::DataFusionError::NotImplemented(_)
        ),
        "{error}"
    );
    assert!(error.to_string().contains("PARTITIONED BY year"), "{error}");
}
