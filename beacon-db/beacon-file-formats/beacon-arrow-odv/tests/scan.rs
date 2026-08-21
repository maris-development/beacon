//! End-to-end checks on the ODV scan: the columns it returns.
//!
//! `OdvSource` accepts a pushed-down projection, so it has to apply the whole
//! of it. Every projection here renames a column after the first one — the
//! shape [#382](https://github.com/maris-development/beacon/issues/382)
//! reported: the scan looked for a file column under the alias, found none, and
//! decoded nothing.

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;
use beacon_arrow_odv::datafusion::OdvFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::{SessionConfig, SessionContext};

/// A session over the crate's ODV fixture.
async fn table() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

    let options =
        ListingOptions::new(Arc::new(OdvFormat::new())).with_file_extension("test_file.txt");
    let url = ListingTableUrl::parse(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test-data")
            .to_str()
            .expect("utf-8 path"),
    )
    .expect("listing url");
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("schema");
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).expect("table")))
        .expect("register");
    ctx
}

async fn query(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning {sql}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("running {sql}: {e}"))
}

fn one_batch(batches: Vec<RecordBatch>) -> RecordBatch {
    let schema = batches.first().expect("at least one batch").schema();
    arrow::compute::concat_batches(&schema, &batches).expect("concat")
}

/// An aliased projection is pushed into the scan whole. The scan has to rename
/// the column it read, not look for a file column under the alias.
///
/// `Station` is the second column of the file, so a scan that kept the file's
/// own column order would also answer with the wrong values here.
#[tokio::test]
async fn applies_an_aliased_projection() {
    let ctx = table().await;
    let batch = one_batch(query(&ctx, r#"SELECT "Station" AS s FROM t LIMIT 3"#).await);

    assert_eq!(batch.schema().field(0).name(), "s");
    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.column(0).null_count(), 0);
    let stations = batch.column(0).as_string::<i32>();
    assert!(
        (0..stations.len()).all(|i| !stations.value(i).is_empty()),
        "every station name is a value from the file"
    );
}

/// A projection that reorders columns returns them in the order asked for.
#[tokio::test]
async fn applies_a_reordered_projection() {
    let ctx = table().await;
    let batch = one_batch(query(&ctx, r#"SELECT "Station", "Cruise" AS c FROM t LIMIT 3"#).await);

    assert_eq!(
        batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>(),
        vec!["Station", "c"]
    );
    assert_eq!(batch.column(0).null_count(), 0);
    assert_eq!(batch.column(1).null_count(), 0);
}

/// A computed column is a projection too, and it is pushed down whole.
#[tokio::test]
async fn applies_a_computed_projection() {
    let ctx = table().await;
    let batch = one_batch(query(&ctx, r#"SELECT "Depth" + 1.0 AS deeper FROM t LIMIT 3"#).await);

    assert_eq!(batch.schema().field(0).name(), "deeper");
    assert_eq!(batch.num_rows(), 3);
}

/// A partition column is part of the projection the scan accepts, so the scan
/// has to fill it from the file's path. Dropping it leaves the column null.
#[tokio::test]
async fn fills_a_partition_column() {
    let dir = tempfile::tempdir().expect("tempdir");
    let partition = dir.path().join("basin=atlantic");
    std::fs::create_dir(&partition).expect("partition dir");
    std::fs::copy(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test-data")
            .join("test_file.txt"),
        partition.join("test_file.txt"),
    )
    .expect("copy fixture");

    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let options = ListingOptions::new(Arc::new(OdvFormat::new()))
        .with_file_extension("test_file.txt")
        .with_table_partition_cols(vec![(
            "basin".to_string(),
            arrow::datatypes::DataType::Utf8,
        )]);
    let url = ListingTableUrl::parse(dir.path().to_str().expect("utf-8 path")).expect("url");
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("schema");
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).expect("table")))
        .expect("register");

    let batch = one_batch(query(&ctx, r#"SELECT basin, "Station" FROM t LIMIT 3"#).await);
    assert_eq!(batch.num_rows(), 3);
    let basin = batch.column(0).as_string::<i32>();
    assert_eq!(
        (0..3).map(|i| basin.value(i)).collect::<Vec<_>>(),
        vec!["atlantic"; 3]
    );
}

/// `count(*)` selects no column at all.
#[tokio::test]
async fn counts_rows_without_reading_a_column() {
    let ctx = table().await;
    let batch = one_batch(query(&ctx, "SELECT count(*) FROM t").await);
    let count = batch
        .column(0)
        .as_primitive::<arrow::datatypes::Int64Type>()
        .value(0);
    assert!(count > 0, "the fixture holds rows");
}
