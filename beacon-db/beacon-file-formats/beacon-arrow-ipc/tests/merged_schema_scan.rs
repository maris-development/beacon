//! A scan over Arrow IPC files must produce the merged schema it reports.
//!
//! The merge widens a column that two files type differently, and it unions the
//! columns that the files hold. A scan then has to adapt each file to that
//! answer. See issue #377 for the merge itself.

use std::sync::Arc;

use arrow::array::{ArrayRef, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use beacon_arrow_ipc::datafusion::ArrowFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::prelude::SessionContext;

/// Writes one single-column IPC *stream* file into `dir`.
fn write_ipc(dir: &std::path::Path, name: &str, field: Field, column: ArrayRef) {
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).expect("valid batch");
    let file = std::fs::File::create(dir.join(name)).expect("create fixture");
    let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).expect("ipc writer");
    writer.write(&batch).expect("write batch");
    writer.finish().expect("finish ipc stream");
}

/// Registers a table over every file in `dir` and returns the session.
async fn table_over(dir: &std::path::Path) -> SessionContext {
    let ctx = SessionContext::new();
    let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("listing url");
    let format = Arc::new(ArrowFormat::default());
    let options = ListingOptions::new(format).with_file_extension(".arrow");
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

/// A column that one file types `Int32` and another `Float32` merges to
/// `Float64`. The scan must cast both files to it.
#[tokio::test]
async fn a_widened_column_reads_as_the_merged_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_ipc(
        dir.path(),
        "a.arrow",
        Field::new("v", DataType::Int32, true),
        Arc::new(Int32Array::from(vec![1, 2])),
    );
    write_ipc(
        dir.path(),
        "b.arrow",
        Field::new("v", DataType::Float32, true),
        Arc::new(Float32Array::from(vec![4.5, 6.0])),
    );

    let ctx = table_over(dir.path()).await;
    let table = ctx.table("t").await.expect("table");
    let schema = table.schema();
    assert_eq!(
        schema.field_with_name(None, "v").unwrap().data_type(),
        &DataType::Float64,
        "Int32 beside Float32 widens to Float64"
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
        .downcast_ref::<Float64Array>()
        .expect("Float64 column");
    assert_eq!(column.values(), &[1.0, 2.0, 4.5, 6.0]);
}

/// The same for a pair that widens within the integers.
#[tokio::test]
async fn an_integer_widening_reads_as_the_merged_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_ipc(
        dir.path(),
        "a.arrow",
        Field::new("v", DataType::Int32, true),
        Arc::new(Int32Array::from(vec![1, 2])),
    );
    write_ipc(
        dir.path(),
        "b.arrow",
        Field::new("v", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![3, 4])),
    );

    let ctx = table_over(dir.path()).await;
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
        .downcast_ref::<Int64Array>()
        .expect("Int64 column");
    assert_eq!(column.values(), &[1, 2, 3, 4]);
}

/// Files that hold different columns union them. A file that lacks a column
/// reads null for it.
#[tokio::test]
async fn a_missing_column_reads_null() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_ipc(
        dir.path(),
        "a.arrow",
        Field::new("a", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![1, 2])),
    );
    write_ipc(
        dir.path(),
        "b.arrow",
        Field::new("b", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![3])),
    );

    let ctx = table_over(dir.path()).await;
    let batches = ctx
        .sql("SELECT a, b FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a file without a column reads null for it");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "both files contribute their rows");
    let nulls: usize = batches.iter().map(|b| b.column(0).null_count()).sum();
    assert_eq!(nulls, 1, "the file without `a` reads one null");
}

/// Writes one two-column IPC file, in either container.
fn write_pair(
    dir: &std::path::Path,
    name: &str,
    fields: Vec<Field>,
    columns: Vec<ArrayRef>,
    as_file_container: bool,
) {
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), columns).expect("valid batch");
    let file = std::fs::File::create(dir.join(name)).expect("create fixture");
    if as_file_container {
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &schema).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish ipc file");
    } else {
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(file, &schema).expect("writer");
        writer.write(&batch).expect("write batch");
        writer.finish().expect("finish ipc stream");
    }
}

/// A file names its columns, so two files may hold them in either order.
#[tokio::test]
async fn the_column_order_may_differ_between_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_pair(
        dir.path(),
        "a.arrow",
        vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![2])),
        ],
        false,
    );
    write_pair(
        dir.path(),
        "b.arrow",
        vec![
            Field::new("b", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ],
        vec![
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(Int64Array::from(vec![10])),
        ],
        false,
    );

    let ctx = table_over(dir.path()).await;
    let batches = ctx
        .sql("SELECT a, b FROM t ORDER BY a")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("the file names which column is which");
    let values = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    let a = values
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column");
    let b = values
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64 column");
    assert_eq!(a.values(), &[1, 10]);
    assert_eq!(b.values(), &[2, 30], "`b` follows `a`, not the file order");
}

/// A projection reads the columns it names, over files that disagree on both
/// the column set and a column type.
#[tokio::test]
async fn a_projection_reads_only_the_columns_it_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_pair(
        dir.path(),
        "a.arrow",
        vec![
            Field::new("keep", DataType::Int32, true),
            Field::new("drop", DataType::Int64, true),
        ],
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![9, 9])),
        ],
        false,
    );
    write_pair(
        dir.path(),
        "b.arrow",
        vec![Field::new("keep", DataType::Float32, true)],
        vec![Arc::new(Float32Array::from(vec![4.5]))],
        false,
    );

    let ctx = table_over(dir.path()).await;
    let batches = ctx
        .sql("SELECT keep FROM t ORDER BY keep")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("one widened column of a union");
    let values = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    assert_eq!(values.num_columns(), 1);
    let column = values
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 column");
    assert_eq!(column.values(), &[1.0, 2.0, 4.5]);
}

/// The same, in the IPC *file* container. That container carries a footer, so a
/// reader addresses its record batches and a scan may split one file.
#[tokio::test]
async fn the_file_container_reads_the_merged_type() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_pair(
        dir.path(),
        "a.arrow",
        vec![Field::new("v", DataType::Int32, true)],
        vec![Arc::new(Int32Array::from(vec![1, 2]))],
        true,
    );
    write_pair(
        dir.path(),
        "b.arrow",
        vec![Field::new("v", DataType::Float32, true)],
        vec![Arc::new(Float32Array::from(vec![4.5]))],
        true,
    );

    let config = datafusion::prelude::SessionConfig::new()
        .with_target_partitions(4)
        .with_repartition_file_min_size(1);
    let ctx = SessionContext::new_with_config(config);
    let url = ListingTableUrl::parse(format!("file://{}/", dir.path().display())).expect("url");
    let format = Arc::new(ArrowFormat::default());
    let options = ListingOptions::new(format)
        .with_file_extension(".arrow")
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
        .sql("SELECT v FROM t ORDER BY v")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("the file container reads the merged type");
    let values = arrow::compute::concat_batches(&batches[0].schema(), &batches).expect("concat");
    let column = values
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .expect("Float64 column");
    assert_eq!(column.values(), &[1.0, 2.0, 4.5]);
}

/// `count(*)` reads no column, and still counts every row of every file.
#[tokio::test]
async fn a_count_reads_every_row() {
    let dir = tempfile::tempdir().expect("tempdir");
    write_ipc(
        dir.path(),
        "a.arrow",
        Field::new("a", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![1, 2])),
    );
    write_ipc(
        dir.path(),
        "b.arrow",
        Field::new("b", DataType::Int64, true),
        Arc::new(Int64Array::from(vec![3, 4, 5])),
    );

    let ctx = table_over(dir.path()).await;
    let batches = ctx
        .sql("SELECT count(*) AS n FROM t")
        .await
        .expect("plan")
        .collect()
        .await
        .expect("a count over a union");
    let n = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count");
    assert_eq!(n.value(0), 5);
}
