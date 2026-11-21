//! `INSERT INTO t BY NAME <query>`: columns match by name, a Lance table gets
//! the source columns that it does not have, and values cast to the table type.

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Float64Array, Int64Array, ListArray, StringArray};
use arrow::datatypes::{DataType, SchemaRef};
use beacon_core::plan_validator::{PlanFacts, PlanRead, PlanValidator, QueryLimits, TableFacts};
use common::{
    column_strings, restartable_runtime, runtime, scalar_i64, total_rows, unprivileged, write_file,
    TestRuntime,
};

/// The Arrow schema of `table`, as the catalog has it.
async fn schema(rt: &TestRuntime, table: &str) -> SchemaRef {
    rt.runtime
        .table_arrow_schema(table, rt.admin().as_ref())
        .await
        .expect("table schema")
}

/// The column names of `table`.
async fn columns(rt: &TestRuntime, table: &str) -> Vec<String> {
    schema(rt, table)
        .await
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

async fn count(rt: &TestRuntime, table: &str) -> i64 {
    scalar_i64(&rt.sql(&format!("SELECT count(*) FROM {table}")).await)
}

/// The `count` value of an INSERT result.
fn inserted(batches: &[arrow::record_batch::RecordBatch]) -> u64 {
    batches
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .expect("count column")
                .value(0)
        })
        .sum()
}

#[tokio::test(flavor = "multi_thread")]
async fn other_order_and_missing_columns() {
    let rt = runtime("by-name-order").await;
    rt.sql("CREATE TABLE t (a BIGINT, b VARCHAR, c DOUBLE)")
        .await;

    let result = rt
        .sql("INSERT INTO t BY NAME SELECT 'x' AS b, 1 AS a")
        .await;
    assert_eq!(inserted(&result), 1);

    let batches = rt.sql("SELECT a, b, c FROM t").await;
    assert_eq!(total_rows(&batches), 1);
    let batch = &batches[0];
    let a = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(a.value(0), 1);
    assert_eq!(column_strings(&batches, 1), vec!["x"]);
    assert!(batch.column(2).is_null(0));
}

#[tokio::test(flavor = "multi_thread")]
async fn new_columns_widen_lance() {
    let rt = restartable_runtime("by-name-widen", |b| b).await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;

    let result = rt
        .sql("INSERT INTO t BY NAME SELECT 3 AS a, 1.5 AS x")
        .await;
    assert_eq!(inserted(&result), 1);
    assert_eq!(columns(&rt, "t").await, vec!["a", "x"]);

    let batches = rt.sql("SELECT a, x FROM t ORDER BY a").await;
    assert_eq!(total_rows(&batches), 3);
    let x = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert!(x.is_null(0) && x.is_null(1));
    assert_eq!(x.value(2), 1.5);

    // The new column is in the dataset, not only in the session.
    let rt = rt.restart().await;
    assert_eq!(columns(&rt, "t").await, vec!["a", "x"]);
    assert_eq!(count(&rt, "t").await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn casts_to_lance_type() {
    let rt = runtime("by-name-cast").await;
    rt.sql("CREATE TABLE t (a DOUBLE)").await;

    rt.sql("INSERT INTO t BY NAME SELECT CAST(2 AS INT) AS a")
        .await;

    let batches = rt.sql("SELECT a FROM t").await;
    let a = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(a.value(0), 2.0);
    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Float64
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_cast_writes_nothing() {
    let rt = runtime("by-name-failed-cast").await;
    rt.sql("CREATE TABLE t (ts TIMESTAMP)").await;
    rt.sql("INSERT INTO t VALUES (TIMESTAMP '2020-01-01 00:00:00')")
        .await;

    // The rule keeps nanoseconds, and the year 3000 does not fit in them.
    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT to_timestamp_seconds(32503680000) AS ts, 2 AS z")
        .await
        .expect_err("a strict cast fails on a value out of range");
    assert!(
        error.to_string().to_lowercase().contains("overflow"),
        "{error}"
    );
    assert_eq!(count(&rt, "t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn conflict_writes_nothing() {
    let rt = runtime("by-name-conflict").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT 'abc' AS a")
        .await
        .expect_err("a string does not go into a BIGINT column");
    assert!(
        error
            .to_string()
            .contains("Incompatible types for field 'a'"),
        "{error}"
    );
    assert_eq!(count(&rt, "t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn uncastable_fails_before_widening() {
    let rt = restartable_runtime("by-name-uncastable", |b| b).await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT named_struct('f', 1) AS a, 2 AS y")
        .await
        .expect_err("a struct does not cast to BIGINT");
    assert!(
        error.to_string().contains("Column 'a': cannot cast"),
        "{error}"
    );

    let rt = rt.restart().await;
    assert_eq!(columns(&rt, "t").await, vec!["a"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_source_still_widens() {
    let rt = runtime("by-name-empty").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let result = rt
        .sql("INSERT INTO t BY NAME SELECT 1 AS a, 2.0 AS z WHERE false")
        .await;
    assert_eq!(inserted(&result), 0);
    assert_eq!(columns(&rt, "t").await, vec!["a", "z"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn null_into_not_null_column_fails() {
    let rt = runtime("by-name-not-null").await;
    rt.sql("CREATE TABLE t (a BIGINT NOT NULL, b BIGINT)").await;

    let result = rt
        .try_sql("INSERT INTO t BY NAME SELECT CAST(NULL AS BIGINT) AS a, 1 AS b")
        .await;
    let error = result.expect_err("a NULL into a NOT NULL column must fail");
    assert!(error.to_string().contains("non-nullable"), "{error}");
    assert_eq!(count(&rt, "t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_view_type_column_is_lance_compatible() {
    let rt = runtime("by-name-nested-view").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    rt.sql(
        "INSERT INTO t BY NAME \
         SELECT 1 AS a, make_array(arrow_cast('p', 'Utf8View'), arrow_cast('q', 'Utf8View')) AS tags",
    )
    .await;

    let tags = schema(&rt, "t")
        .await
        .field_with_name("tags")
        .unwrap()
        .clone();
    let DataType::List(item) = tags.data_type() else {
        panic!("tags is not a list: {tags:?}");
    };
    assert_eq!(item.data_type(), &DataType::Utf8);

    let batches = rt.sql("SELECT tags FROM t").await;
    let list = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<ListArray>()
        .unwrap();
    let values = list.value(0);
    let values = values.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(values.value(0), "p");
    assert_eq!(values.value(1), "q");
}

#[tokio::test(flavor = "multi_thread")]
async fn multi_partition_source_writes_all_rows() {
    let rt = runtime("by-name-partitions").await;
    for (index, rows) in [3usize, 5, 7].into_iter().enumerate() {
        let mut contents = String::from("v\n");
        for i in 0..rows {
            contents.push_str(&format!("{i}\n"));
        }
        write_file(
            &rt.datasets_dir().join(format!("parts/p{index}.csv")),
            &contents,
        );
    }
    rt.sql("CREATE TABLE t (v BIGINT)").await;

    let result = rt
        .sql("INSERT INTO t BY NAME SELECT * FROM read_csv('parts/*.csv')")
        .await;
    assert_eq!(inserted(&result), 15);
    assert_eq!(count(&rt, "t").await, 15);
}

/// Write a Delta table with one `id INT NOT NULL` column under the datasets dir.
async fn write_delta(datasets_dir: &Path, rel: &str) -> String {
    use deltalake::protocol::SaveMode;
    use deltalake::DeltaTableBuilder;

    let dir = datasets_dir.join(rel);
    std::fs::create_dir_all(&dir).unwrap();
    let abs = std::fs::canonicalize(&dir).unwrap();
    let url = url::Url::from_directory_path(&abs).unwrap();
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", DataType::Int32, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema,
        vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2]))],
    )
    .unwrap();
    let table = DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    let _ = table
        .write(vec![batch])
        .with_save_mode(SaveMode::Append)
        .await
        .unwrap();
    format!("datasets://{rel}")
}

#[tokio::test(flavor = "multi_thread")]
async fn non_lance_target_rejects_extra_columns() {
    let rt = runtime("by-name-delta").await;
    let location = write_delta(rt.datasets_dir(), "delta_by_name").await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE dt STORED AS DELTA LOCATION '{location}'"
    ))
    .await;

    let error = rt
        .try_sql("INSERT INTO dt BY NAME SELECT 5 AS id, 1 AS extra")
        .await
        .expect_err("a Delta table does not add columns");
    assert!(
        error.to_string().contains(
            "Table 'dt' does not have the columns 'extra'; only Lance tables add columns"
        ),
        "{error}"
    );

    // Without extra columns, the mapping by name works on Delta too.
    rt.sql("INSERT INTO dt BY NAME SELECT 5 AS id").await;
    assert_eq!(count(&rt, "dt").await, 3);
}

/// Write `nc/a.nc` (`temp`) and `nc/b.nc` (`psal`), and create the external
/// table `nc` over them and the Lance table `lance_t (id BIGINT)`.
async fn netcdf_fixture(rt: &TestRuntime) {
    rt.sql(
        "COPY (SELECT CAST(column1 AS DOUBLE) AS temp FROM (VALUES (1), (2))) \
         TO 'tmp:///a.nc' STORED AS NC",
    )
    .await;
    rt.sql(
        "COPY (SELECT CAST(column1 AS DOUBLE) AS psal FROM (VALUES (3), (4), (5))) \
         TO 'tmp:///b.nc' STORED AS NC",
    )
    .await;
    // COPY writes to the tmp store; the external table reads the datasets store.
    let nc_dir = rt.datasets_dir().join("nc");
    std::fs::create_dir_all(&nc_dir).unwrap();
    for file in ["a.nc", "b.nc"] {
        std::fs::copy(rt.tmp_dir().join(file), nc_dir.join(file)).unwrap();
    }
    rt.sql("CREATE EXTERNAL TABLE nc STORED AS NC LOCATION 'nc/*.nc'")
        .await;
    rt.sql(
        "CREATE EXTERNAL TABLE lance_t (id BIGINT) STORED AS LANCE \
         LOCATION 'merged.lance'",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn netcdf_attribute_columns_are_renamed() {
    let rt = runtime("by-name-netcdf-attributes").await;
    netcdf_fixture(&rt).await;
    let source = columns(&rt, "nc").await;
    assert!(source.iter().any(|n| n.contains('.')), "{source:?}");

    let result = rt.sql("INSERT INTO lance_t BY NAME SELECT * FROM nc").await;
    assert_eq!(inserted(&result), 5);
    let names = columns(&rt, "lance_t").await;
    for name in &source {
        let renamed = name.replace('.', "_");
        assert!(names.contains(&renamed), "{renamed} in {names:?}");
    }
    assert!(names.iter().all(|n| !n.contains('.')), "{names:?}");

    // The next insert gets the same names, so no column is added.
    rt.sql("INSERT INTO lance_t BY NAME SELECT * FROM nc").await;
    assert_eq!(columns(&rt, "lance_t").await, names);
    assert_eq!(count(&rt, "lance_t").await, 10);
}

#[tokio::test(flavor = "multi_thread")]
async fn netcdf_source_merges() {
    let rt = runtime("by-name-netcdf").await;
    netcdf_fixture(&rt).await;

    let result = rt
        .sql("INSERT INTO lance_t BY NAME SELECT temp, psal FROM nc")
        .await;
    assert_eq!(inserted(&result), 5);
    assert_eq!(count(&rt, "lance_t").await, 5);
    let names = columns(&rt, "lance_t").await;
    assert!(
        names.iter().any(|n| n == "temp") && names.iter().any(|n| n == "psal"),
        "{names:?}"
    );
    // A row of a.nc has no `psal`, and a row of b.nc has no `temp`.
    let only_temp = rt
        .sql("SELECT count(*) FROM lance_t WHERE temp IS NOT NULL AND psal IS NULL")
        .await;
    let only_psal = rt
        .sql("SELECT count(*) FROM lance_t WHERE psal IS NOT NULL AND temp IS NULL")
        .await;
    assert_eq!(scalar_i64(&only_temp), 2);
    assert_eq!(scalar_i64(&only_psal), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn dotted_names_are_renamed() {
    let rt = runtime("by-name-dotted").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    rt.sql("INSERT INTO t BY NAME SELECT 1 AS a, 2 AS \"x.y\"")
        .await;
    assert_eq!(columns(&rt, "t").await, vec!["a", "x_y"]);
    rt.sql("INSERT INTO t BY NAME SELECT 3 AS \"x.y\"").await;
    let batches = rt.sql("SELECT x_y FROM t ORDER BY x_y").await;
    let x_y = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((x_y.value(0), x_y.value(1)), (2, 3));

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT 4 AS \"x.y\", 5 AS x_y")
        .await
        .expect_err("two source columns get one name");
    assert!(
        error
            .to_string()
            .contains("The source has the column 'x_y' more than one time"),
        "{error}"
    );
    assert_eq!(count(&rt, "t").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn int32_column_widens_to_int64() {
    let rt = restartable_runtime("by-name-widen-int", |b| b).await;
    rt.sql("CREATE TABLE t (c INT)").await;
    rt.sql("INSERT INTO t VALUES (1)").await;

    rt.sql("INSERT INTO t BY NAME SELECT CAST(3000000000 AS BIGINT) AS c")
        .await;

    let rt = rt.restart().await;
    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Int64
    );
    let batches = rt.sql("SELECT c FROM t ORDER BY c").await;
    let c = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!((c.value(0), c.value(1)), (1, 3_000_000_000));
}

#[tokio::test(flavor = "multi_thread")]
async fn float32_widens_to_float64() {
    let rt = runtime("by-name-widen-float").await;
    rt.sql("CREATE TABLE t (c REAL)").await;
    rt.sql("INSERT INTO t VALUES (1.5)").await;

    rt.sql("INSERT INTO t BY NAME SELECT CAST(0.1 AS DOUBLE) AS c")
        .await;

    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Float64
    );
    let batches = rt.sql("SELECT c FROM t ORDER BY c").await;
    let c = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!((c.value(0), c.value(1)), (0.1, 1.5));
}

#[tokio::test(flavor = "multi_thread")]
async fn type_change_with_index_fails_before_change() {
    let rt = runtime("by-name-widen-index").await;
    rt.sql("CREATE TABLE t (c INT, d BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1, 1)").await;
    rt.sql("CREATE INDEX ON t (c)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT CAST(3000000000 AS BIGINT) AS c, 2 AS z")
        .await
        .expect_err("an index blocks the type change");
    assert!(
        error.to_string().contains(
            "Column 'c' has the index 't_c_idx'; drop it before BY NAME changes the type to Int64"
        ),
        "{error}"
    );
    assert_eq!(columns(&rt, "t").await, vec!["c", "d"]);
    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Int32
    );
    assert_eq!(count(&rt, "t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn type_change_that_lance_refuses_fails_before_change() {
    let rt = runtime("by-name-widen-refused").await;
    rt.sql("CREATE TABLE t (c BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT 1.5 AS c, 2 AS z")
        .await
        .expect_err("Lance does not change an integer to a float");
    assert!(
        error
            .to_string()
            .contains("Lance cannot change column 'c' from Int64 to Float64"),
        "{error}"
    );
    assert_eq!(columns(&rt, "t").await, vec!["c"]);
    assert_eq!(count(&rt, "t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn non_lance_target_keeps_type() {
    let rt = runtime("by-name-delta-type").await;
    let location = write_delta(rt.datasets_dir(), "delta_keeps_type").await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE dt STORED AS DELTA LOCATION '{location}'"
    ))
    .await;

    rt.sql("INSERT INTO dt BY NAME SELECT CAST(7 AS BIGINT) AS id")
        .await;
    assert_eq!(count(&rt, "dt").await, 3);
    assert_eq!(
        schema(&rt, "dt").await.field(0).data_type(),
        &DataType::Int32
    );

    let result = rt
        .try_sql("INSERT INTO dt BY NAME SELECT CAST(3000000000 AS BIGINT) AS id")
        .await;
    assert!(
        result.is_err(),
        "a strict cast fails on a value out of range"
    );
    assert_eq!(count(&rt, "dt").await, 3);
    assert_eq!(
        schema(&rt, "dt").await.field(0).data_type(),
        &DataType::Int32
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_shows_type_changes() {
    let rt = runtime("by-name-explain-widen").await;
    rt.sql("CREATE TABLE t (c INT)").await;

    let batches = rt
        .sql("EXPLAIN INSERT INTO t BY NAME SELECT CAST(1 AS BIGINT) AS c")
        .await;
    let plan = column_strings(&batches, 1).join("\n");
    assert!(plan.contains("planned widen=[c: Int32 -> Int64]"), "{plan}");
    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Int32
    );
    assert_eq!(count(&rt, "t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn failed_append_keeps_the_new_columns() {
    let rt = runtime("by-name-failed-append").await;
    rt.sql("CREATE TABLE t (a BIGINT NOT NULL)").await;
    rt.sql("INSERT INTO t VALUES (1)").await;

    let result = rt
        .try_sql("INSERT INTO t BY NAME SELECT CAST(NULL AS BIGINT) AS a, 2 AS z")
        .await;
    assert!(result.is_err(), "a NULL does not go into a NOT NULL column");

    // The widening is its own version; the append writes no rows.
    assert_eq!(columns(&rt, "t").await, vec!["a", "z"]);
    assert_eq!(count(&rt, "t").await, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn by_name_into_pinned_table_fails_before_widening() {
    let rt = runtime("by-name-pinned").await;
    rt.sql(
        "CREATE EXTERNAL TABLE w (a BIGINT) STORED AS LANCE \
         LOCATION 'pinned.lance'",
    )
    .await;
    rt.sql("INSERT INTO w VALUES (1)").await;
    rt.sql(
        "CREATE EXTERNAL TABLE p STORED AS LANCE LOCATION 'pinned.lance' \
         OPTIONS ('version' '2')",
    )
    .await;

    let error = rt
        .try_sql("INSERT INTO p BY NAME SELECT 1 AS a, 2 AS y")
        .await
        .expect_err("a pinned table is read-only");
    assert!(
        error
            .to_string()
            .contains("Lance table 'p' is pinned to version 2 and is read-only"),
        "{error}"
    );
    // A new table on the location reads the schema of the latest version.
    rt.sql("DROP TABLE w").await;
    rt.sql("CREATE EXTERNAL TABLE w STORED AS LANCE LOCATION 'pinned.lance'")
        .await;
    assert_eq!(columns(&rt, "w").await, vec!["a"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_does_not_widen() {
    let rt = restartable_runtime("by-name-explain", |b| b).await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let batches = rt
        .sql("EXPLAIN INSERT INTO t BY NAME SELECT 1 AS a, 2 AS w")
        .await;
    let plan = column_strings(&batches, 1).join("\n");
    assert!(plan.contains("InsertByNameExec"), "{plan}");
    assert!(plan.contains("add=[w"), "{plan}");

    assert_eq!(columns(&rt, "t").await, vec!["a"]);
    let rt = rt.restart().await;
    assert_eq!(columns(&rt, "t").await, vec!["a"]);
    assert_eq!(count(&rt, "t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_analyze_by_name_is_a_parse_error() {
    let rt = runtime("by-name-explain-analyze").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let result = rt
        .try_sql("EXPLAIN ANALYZE INSERT INTO t BY NAME SELECT 1 AS a")
        .await;
    assert!(result.is_err());
    assert_eq!(count(&rt, "t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_caller_that_is_not_a_super_user_is_refused() {
    let rt = runtime("by-name-unprivileged").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let result = rt
        .try_sql_as(
            "INSERT INTO t BY NAME SELECT 1 AS a, 2 AS w",
            unprivileged(),
        )
        .await;
    let error = result.expect_err("only a super user writes");
    assert!(error.to_string().contains("super-user"), "{error}");
    let explained = rt
        .try_sql_as(
            "EXPLAIN INSERT INTO t BY NAME SELECT 1 AS a, 2 AS w",
            unprivileged(),
        )
        .await;
    let error = explained.expect_err("only a super user explains a write");
    assert!(error.to_string().contains("super-user"), "{error}");
    assert_eq!(columns(&rt, "t").await, vec!["a"]);
    assert_eq!(count(&rt, "t").await, 0);
}

/// Permits each plan and records the tables that it reads.
struct ReadRecorder(Mutex<Vec<String>>);

#[async_trait::async_trait]
impl PlanValidator for ReadRecorder {
    fn caller(&self) -> &str {
        "recorder"
    }

    fn limits(&self) -> QueryLimits {
        QueryLimits::default()
    }

    async fn validate(&self, facts: &PlanFacts<'_>) -> anyhow::Result<()> {
        let mut seen = self.0.lock().unwrap();
        for read in facts.reads() {
            if let PlanRead::Table(table) = read {
                seen.push(table.name().to_string());
            }
        }
        Ok(())
    }

    fn check_table(&self, _table: &TableFacts) -> anyhow::Result<()> {
        Ok(())
    }

    fn can_read_path(&self, _path: &str) -> bool {
        true
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn the_validator_sees_the_source_reads() {
    let rt = runtime("by-name-reads").await;
    rt.sql("CREATE TABLE source (a BIGINT)").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let recorder = Arc::new(ReadRecorder(Mutex::new(Vec::new())));
    rt.sql_as(
        "INSERT INTO t BY NAME SELECT a FROM source",
        recorder.clone(),
    )
    .await;

    let seen = recorder.0.lock().unwrap().clone();
    assert!(seen.iter().any(|name| name == "source"), "{seen:?}");
}
