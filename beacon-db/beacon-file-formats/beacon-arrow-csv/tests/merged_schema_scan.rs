//! A scan over CSV files must produce the merged schema it reports.
//!
//! The merge unions the columns of the files. A file that lacks a column then
//! has to read null for it, rather than fail the record parser.

use std::sync::Arc;

use arrow::datatypes::DataType;
use beacon_arrow_csv::datafusion::CsvFormat;
use beacon_datafusion_ext::type_widening::{ArrowTypeWidening, DefaultArrowTypeWidening};
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;

/// Registers a table over every CSV file in `dir` and returns the session.
async fn table_over(dir: &std::path::Path) -> SessionContext {
    let ctx = SessionContext::new();
    let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("listing url");
    let format = Arc::new(CsvFormat::new(b',', 1000));
    let options = ListingOptions::new(format).with_file_extension(".csv");
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("merged schema");
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config).expect("listing table");
    ctx.register_table("t", Arc::new(table)).expect("register");
    ctx
}

/// Two files that hold different columns union them, and each file reads null
/// for the column it lacks.
#[tokio::test]
async fn a_missing_column_reads_null() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "a,b\n1,2\n3,4\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "a,c\n5,6\n7,8\n9,10\n").expect("write b");

    let ctx = table_over(dir.path()).await;
    let table = ctx.table("t").await.expect("table");
    let names: Vec<String> = table
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["a", "b", "c"], "the merge unions the columns");

    let batches = ctx
        .sql("SELECT a, b, c FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a file without a column reads null for it");

    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 5, "both files contribute their rows");
    let b_nulls: usize = batches.iter().map(|b| b.column(1).null_count()).sum();
    assert_eq!(b_nulls, 3, "the file without `b` reads three nulls");
    let c_nulls: usize = batches.iter().map(|b| b.column(2).null_count()).sum();
    assert_eq!(c_nulls, 2, "the file without `c` reads two nulls");
}

/// A column that one file types as integral and another as decimal widens, and
/// the scan casts the integral file to the merged type.
#[tokio::test]
async fn a_widened_column_reads_as_the_merged_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "v\n1\n2\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "v\n4.5\n6.25\n").expect("write b");

    let ctx = table_over(dir.path()).await;
    let table = ctx.table("t").await.expect("table");
    let schema = table.schema();
    assert_eq!(
        schema.field_with_name(None, "v").unwrap().data_type(),
        &DataType::Float64
    );

    let batches = ctx
        .sql("SELECT v FROM t ORDER BY v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("the scan must produce the schema it reports");
    let values = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    let column = values
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64 column");
    assert_eq!(column.values(), &[1.0, 2.0, 4.5, 6.25]);
}

/// The header names the columns, so two files may hold them in either order.
#[tokio::test]
async fn the_column_order_may_differ_between_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "a,b\n1,2\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "b,a\n30,10\n").expect("write b");

    let ctx = table_over(dir.path()).await;
    let batches = ctx
        .sql("SELECT a, b FROM t ORDER BY a")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("the header decides which column is which");
    let values = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    let a = values
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Int64 column");
    let b = values
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("Int64 column");
    assert_eq!(a.values(), &[1, 10]);
    assert_eq!(b.values(), &[2, 30], "`b` follows `a`, not the file order");
}

/// A projection reads the columns it names, and no others.
#[tokio::test]
async fn a_projection_reads_only_the_columns_it_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "a,b\n1,2\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "a,c\n5,6\n").expect("write b");

    let ctx = table_over(dir.path()).await;
    let batches = ctx
        .sql("SELECT c FROM t ORDER BY c")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("one column of a union");
    let values = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    assert_eq!(values.num_columns(), 1);
    assert_eq!(values.num_rows(), 2);
    assert_eq!(values.column(0).null_count(), 1, "the file without `c`");
}

/// A collection whose files carry no header keeps the reading by position: no
/// name is stated, so no name can be matched.
#[tokio::test]
async fn a_headerless_collection_reads_by_position() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "1,2\n3,4\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "5,6\n").expect("write b");

    // The format leaves `has_header` to the session, so the session states it.
    let config =
        datafusion::prelude::SessionConfig::new().set_bool("datafusion.catalog.has_header", false);
    let ctx = SessionContext::new_with_config(config);
    let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
    let format = Arc::new(CsvFormat::new(b',', 1000));
    let options = ListingOptions::new(format).with_file_extension(".csv");
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("merged schema");
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).expect("table")))
        .expect("register");

    let batches = ctx
        .sql("SELECT * FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a headerless collection still reads");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
}

/// A custom delimiter decides where the header ends, as it does for the records.
#[tokio::test]
async fn a_custom_delimiter_applies_to_the_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "a\tb\n1\t2\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "a\tc\n5\t6\n").expect("write b");

    let ctx = SessionContext::new();
    let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
    let format = Arc::new(CsvFormat::new(b'\t', 1000));
    let options = ListingOptions::new(format).with_file_extension(".csv");
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("merged schema");
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).expect("table")))
        .expect("register");

    let batches = ctx
        .sql("SELECT a, b, c FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a tab-separated union reads");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 2);
    let b_nulls: usize = batches.iter().map(|b| b.column(1).null_count()).sum();
    assert_eq!(b_nulls, 1);
}

/// A file that a scan splits across partitions reads its header from the front
/// for every part of it, and each part reads its own rows once.
#[tokio::test]
async fn a_split_file_reads_its_header_for_every_part() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut wide = String::from("a,b\n");
    for row in 0..4000 {
        wide.push_str(&format!("{row},{}\n", row * 2));
    }
    std::fs::write(dir.path().join("a.csv"), wide).expect("write a");
    std::fs::write(dir.path().join("b.csv"), "a,c\n7,8\n").expect("write b");

    let config = datafusion::prelude::SessionConfig::new()
        .with_target_partitions(4)
        .with_repartition_file_min_size(1);
    let ctx = SessionContext::new_with_config(config);
    let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
    let format = Arc::new(CsvFormat::new(b',', 1000));
    let options = ListingOptions::new(format)
        .with_file_extension(".csv")
        .with_target_partitions(4);
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("merged schema");
    let table_config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table(
        "t",
        Arc::new(ListingTable::try_new(table_config).expect("table")),
    )
    .expect("register");

    let batches = ctx
        .sql("SELECT count(*) AS n, sum(a) AS total FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a split file reads");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count");
    assert_eq!(n.value(0), 4001, "every row once, from every part");
    let total = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("sum");
    assert_eq!(total.value(0), (0..4000i64).sum::<i64>() + 7);
}

/// The default merge refuses a column that one file types as a number and
/// another as a string. The table then answers no query at all.
#[tokio::test]
async fn a_column_of_two_families_is_refused_by_default() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("a.csv"), "v\n1.5\n2.5\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "v\nabc\ndef\n").expect("write b");

    let ctx = SessionContext::new();
    let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
    let format = Arc::new(CsvFormat::new(b',', 1000));
    let options = ListingOptions::new(format).with_file_extension(".csv");
    let error = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect_err("a number and a string share no type")
        .to_string();
    assert!(error.contains("Incompatible types for field 'v'"), "{error}");
}

/// `TypeConflict::KeepFirst` reports the type of the first file instead, and a
/// value that type cannot hold reads as null.
#[tokio::test]
async fn a_column_of_two_families_reads_null_under_the_setting() {
    let dir = tempfile::tempdir().expect("tempdir");
    // `a.csv` is listed first, so the column reads as `Float64`.
    std::fs::write(dir.path().join("a.csv"), "v\n1.5\n2.5\n").expect("write a");
    std::fs::write(dir.path().join("b.csv"), "v\nabc\ndef\n").expect("write b");

    let config = datafusion::prelude::SessionConfig::new().with_extension(Arc::new(
        ArrowTypeWidening::new(Arc::new(DefaultArrowTypeWidening::keeping_first_type())),
    ));
    let ctx = SessionContext::new_with_config(config);
    let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
    let format = Arc::new(CsvFormat::new(b',', 1000));
    let options = ListingOptions::new(format).with_file_extension(".csv");
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("the setting settles the column");
    assert_eq!(
        schema.field_with_name("v").unwrap().data_type(),
        &DataType::Float64,
        "the first file states the type"
    );

    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    ctx.register_table("t", Arc::new(ListingTable::try_new(config).expect("table")))
        .expect("register");

    let batches = ctx
        .sql("SELECT v FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a value the type cannot hold may not fail the scan");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 4, "both files contribute their rows");
    let nulls: usize = batches.iter().map(|b| b.column(0).null_count()).sum();
    assert_eq!(nulls, 2, "the two strings read as null");
}
