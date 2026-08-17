//! A CF time value that is not a time must not take the worker down with it.
//!
//! `hifitime` refuses a non-finite `Duration` by **panicking** rather than
//! returning an error. A Zarr store whose time array holds a NaN therefore
//! killed whichever worker read it — and because the fill value is decoded while
//! a *schema* is being read, a request that only wanted the schema died too.
//!
//! netCDF had the same fault and fixed it (`0293275`). This is the Zarr half.

use std::sync::Arc;

use beacon_arrow_zarr::datafusion::ZarrFormat;
use beacon_arrow_zarr::reader::schema_from_group_path;
use beacon_arrow_zarr::util::ZarrStorage;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;
use object_store::local::LocalFileSystem;
use zarrs::array::{data_type, ArrayBuilder};
use zarrs::group::GroupBuilder;
use zarrs_object_store::AsyncObjectStore;

const TIMES: usize = 6;
/// The index of the cell that is not a time.
const NAN_AT: usize = 3;

fn attributes(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
    pairs
        .iter()
        .map(|(key, value)| ((*key).to_string(), value.clone()))
        .collect()
}

/// A store with one CF time array holding a NaN.
async fn write_store(dir: &std::path::Path) -> anyhow::Result<()> {
    let store = Arc::new(AsyncObjectStore::new(LocalFileSystem::new_with_prefix(dir)?));

    GroupBuilder::new()
        .attributes(attributes(&[("Conventions", serde_json::json!("CF-1.8"))]))
        .build(store.clone(), "/")?
        .async_store_metadata()
        .await?;

    let mut time: Vec<f64> = (0..TIMES).map(|i| i as f64 * 3_600.0).collect();
    time[NAN_AT] = f64::NAN;

    let array = ArrayBuilder::new(
        vec![TIMES as u64],
        vec![TIMES as u64],
        data_type::float64(),
        f64::NAN,
    )
    .dimension_names(Some(["time"]))
    .attributes(attributes(&[(
        "units",
        serde_json::json!("seconds since 1970-01-01"),
    )]))
    .build(store.clone(), "/time")?;
    array.async_store_metadata().await?;
    array.async_store_chunk(&[0], time.as_slice()).await?;

    Ok(())
}

/// Reading the schema of such a store must not panic.
///
/// The fill value is decoded here, so this is the path that killed a request
/// that only ever asked what columns the store has.
#[tokio::test]
async fn a_nan_time_does_not_kill_a_schema_read() {
    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path()).await.unwrap();

    let store = Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
    let storage = ZarrStorage::from_object_store(store);

    let schema = schema_from_group_path(
        storage.inner(),
        "/",
        None,
        None,
        &beacon_datafusion_ext::type_widening::ArrowTypeWidening::default_extension(),
    )
    .await
    .expect("a store with a NaN time still has a schema");

    let names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
    assert!(names.contains(&&"time".to_string()), "time is in {names:?}");
}

/// Reading the values must not panic, and the NaN must arrive as null.
///
/// A cell that is not a time decodes to `NO_TIME` — `i64::MIN` nanoseconds, the
/// year -292277 — which the nd layer turns into a null because the backend
/// reports it as its fill value. Without that fallback the cell would reach a
/// query as that date, silently.
#[tokio::test]
async fn a_nan_time_arrives_as_null_and_the_rest_survive() {
    let dir = tempfile::tempdir().unwrap();
    write_store(dir.path()).await.unwrap();

    let ctx = SessionContext::new();
    let table_path =
        ListingTableUrl::parse(format!("file://{}/", dir.path().to_string_lossy())).unwrap();
    let format: Arc<dyn FileFormat> = Arc::new(ZarrFormat::default());
    let options = ListingOptions::new(format).with_file_extension("zarr.json");
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(options)
        .infer_schema(&ctx.state())
        .await
        .expect("the store's schema is readable");
    let table = ListingTable::try_new(config).unwrap();
    ctx.register_table("t", Arc::new(table)).unwrap();

    let batches = ctx
        .sql(r#"SELECT count(*) AS rows, count("time") AS times FROM t"#)
        .await
        .expect("the query plans")
        .collect()
        .await
        .expect("reading a NaN time does not panic");

    let summary = format!("{:?}", batches[0].columns());
    assert!(
        summary.contains(&format!("{TIMES}")),
        "every cell is a row: {summary}"
    );
    assert!(
        summary.contains(&format!("{}", TIMES - 1)),
        "and the cell that is not a time is null, so {} count: {summary}",
        TIMES - 1
    );
}
