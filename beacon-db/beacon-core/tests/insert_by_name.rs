//! `INSERT INTO t BY NAME <query>`: columns match by name, values cast to the
//! table type, and the table schema does not change.

mod common;

use std::path::Path;
use std::sync::{Arc, Mutex};

use arrow::array::{Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, SchemaRef};
use beacon_core::plan_validator::{PlanFacts, PlanRead, PlanValidator, QueryLimits, TableFacts};
use common::{
    column_strings, runtime, scalar_i64, total_rows, unprivileged, write_file, TestRuntime,
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
async fn extra_column_is_an_error() {
    let rt = runtime("by-name-extra").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;
    rt.sql("INSERT INTO t VALUES (1), (2)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT 3 AS a, 1.5 AS x")
        .await
        .expect_err("the table does not have the column x");
    assert!(
        error
            .to_string()
            .contains("Table 't' does not have the columns 'x'"),
        "{error}"
    );
    assert_eq!(columns(&rt, "t").await, vec!["a"]);
    assert_eq!(count(&rt, "t").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn casts_to_table_type() {
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
async fn wider_source_keeps_the_table_type() {
    let rt = runtime("by-name-wider").await;
    rt.sql("CREATE TABLE t (c INT)").await;
    rt.sql("INSERT INTO t VALUES (1)").await;

    rt.sql("INSERT INTO t BY NAME SELECT CAST(7 AS BIGINT) AS c")
        .await;
    assert_eq!(count(&rt, "t").await, 2);
    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Int32
    );

    let result = rt
        .try_sql("INSERT INTO t BY NAME SELECT CAST(3000000000 AS BIGINT) AS c")
        .await;
    assert!(
        result.is_err(),
        "a strict cast fails on a value out of range"
    );
    assert_eq!(count(&rt, "t").await, 2);
    assert_eq!(
        schema(&rt, "t").await.field(0).data_type(),
        &DataType::Int32
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
        .try_sql("INSERT INTO t BY NAME SELECT to_timestamp_seconds(32503680000) AS ts")
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
async fn uncastable_fails() {
    let rt = runtime("by-name-uncastable").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT named_struct('f', 1) AS a")
        .await
        .expect_err("a struct does not cast to BIGINT");
    assert!(
        error.to_string().contains("Column 'a': cannot cast"),
        "{error}"
    );
    assert_eq!(count(&rt, "t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_source_inserts_nothing() {
    let rt = runtime("by-name-empty").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let result = rt
        .sql("INSERT INTO t BY NAME SELECT 1 AS a WHERE false")
        .await;
    assert_eq!(inserted(&result), 0);
    assert_eq!(count(&rt, "t").await, 0);
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
async fn missing_not_null_column_fails() {
    let rt = runtime("by-name-missing-not-null").await;
    rt.sql("CREATE TABLE t (a BIGINT NOT NULL, b BIGINT)").await;

    let error = rt
        .try_sql("INSERT INTO t BY NAME SELECT 1 AS b")
        .await
        .expect_err("the source does not have the NOT NULL column a");
    assert!(
        error
            .to_string()
            .contains("Column 'a' is not nullable and the source does not have it"),
        "{error}"
    );
    assert_eq!(count(&rt, "t").await, 0);
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
async fn delta_target_rejects_extra_columns() {
    let rt = runtime("by-name-delta").await;
    let location = write_delta(rt.datasets_dir(), "delta_by_name").await;
    rt.sql(&format!(
        "CREATE EXTERNAL TABLE dt STORED AS DELTA LOCATION '{location}'"
    ))
    .await;

    let error = rt
        .try_sql("INSERT INTO dt BY NAME SELECT 5 AS id, 1 AS extra")
        .await
        .expect_err("the Delta table does not have the column extra");
    assert!(
        error
            .to_string()
            .contains("Table 'dt' does not have the columns 'extra'"),
        "{error}"
    );

    rt.sql("INSERT INTO dt BY NAME SELECT 5 AS id").await;
    assert_eq!(count(&rt, "dt").await, 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_target_keeps_type() {
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
}

/// Write `nc/a.nc` (`temp`) and `nc/b.nc` (`psal`), and create the external
/// table `nc` over them and the table `lance_t (id BIGINT, temp DOUBLE, psal DOUBLE)`.
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
    rt.sql("CREATE TABLE lance_t (id BIGINT, temp DOUBLE, psal DOUBLE)")
        .await;
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
    assert_eq!(columns(&rt, "lance_t").await, vec!["id", "temp", "psal"]);
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
async fn netcdf_attribute_columns_are_extra() {
    let rt = runtime("by-name-netcdf-attributes").await;
    netcdf_fixture(&rt).await;
    let source = columns(&rt, "nc").await;
    let dotted = source
        .iter()
        .find(|n| n.contains('.'))
        .unwrap_or_else(|| panic!("an attribute column in {source:?}"))
        .clone();

    let error = rt
        .try_sql("INSERT INTO lance_t BY NAME SELECT * FROM nc")
        .await
        .expect_err("the table does not have the attribute columns");
    let message = error.to_string();
    assert!(
        message.contains("Table 'lance_t' does not have the columns"),
        "{message}"
    );
    assert!(message.contains(&format!("'{dotted}'")), "{message}");
    assert_eq!(count(&rt, "lance_t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_shows_the_mapping() {
    let rt = runtime("by-name-explain").await;
    rt.sql("CREATE TABLE t (c BIGINT, d BIGINT)").await;

    let batches = rt
        .sql("EXPLAIN INSERT INTO t BY NAME SELECT CAST(1 AS INT) AS c")
        .await;
    let plan = column_strings(&batches, 1).join("\n");
    assert!(plan.contains("InsertByNameExec"), "{plan}");
    assert!(plan.contains("cast=[c: Int32->Int64], null=[d]"), "{plan}");
    assert_eq!(count(&rt, "t").await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn explain_shows_a_mapping_error() {
    let rt = runtime("by-name-explain-error").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    let result = rt
        .try_sql("EXPLAIN INSERT INTO t BY NAME SELECT 1 AS a, 2 AS w")
        .await;
    let error = result.expect_err("EXPLAIN gives the mapping error");
    assert!(
        error
            .to_string()
            .contains("Table 't' does not have the columns 'w'"),
        "{error}"
    );
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
        .try_sql_as("INSERT INTO t BY NAME SELECT 1 AS a", unprivileged())
        .await;
    let error = result.expect_err("only a super user writes");
    assert!(error.to_string().contains("super-user"), "{error}");
    let explained = rt
        .try_sql_as(
            "EXPLAIN INSERT INTO t BY NAME SELECT 1 AS a",
            unprivileged(),
        )
        .await;
    let error = explained.expect_err("only a super user explains a write");
    assert!(error.to_string().contains("super-user"), "{error}");
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
