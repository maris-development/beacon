//! SQL macro functions: `CREATE [OR REPLACE] FUNCTION`, `DROP FUNCTION` and
//! `SHOW CREATE FUNCTION`.
//!
//! These tests pin the results against the Rust mapping, the pushdown of an
//! inlined body to Parquet, the restart, and the refused bodies, names and users.

mod common;

use std::sync::Arc;

use arrow::array::{Array, AsArray};
use beacon_core::AuthIdentity;
use common::{column_strings, restartable_runtime, runtime, scalar_i64, total_rows, TestRuntime};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;

/// The `map_wod_quality_flag` mapping of `beacon-functions`, as a SQL function.
const WOD_FLAG_SQL: &str = "CREATE FUNCTION wod_flag_sql(flag BIGINT) RETURNS VARCHAR \
     RETURN CASE WHEN flag = 0 THEN '1' WHEN flag BETWEEN 1 AND 5 THEN '3' \
     WHEN flag BETWEEN 6 AND 9 THEN '4' END";

/// Every WOD flag, values outside the mapping, and NULL.
const FLAGS: &str = "(SELECT * FROM (VALUES (-1), (0), (1), (2), (3), (4), (5), (6), (7), \
     (8), (9), (10), (11), (NULL)) AS t(flag))";

fn non_super_user() -> AuthIdentity {
    AuthIdentity {
        username: "reader".to_string(),
        roles: vec![],
        is_super_user: false,
    }
}

async fn error_of(rt: &TestRuntime, sql: &str) -> String {
    rt.try_sql(sql)
        .await
        .err()
        .unwrap_or_else(|| panic!("expected an error for `{sql}`"))
        .to_string()
}

/// The one string of a one-row result, or `None` for NULL.
fn optional_string(batches: &[RecordBatch]) -> Option<String> {
    assert_eq!(total_rows(batches), 1, "expected exactly one row");
    let batch = batches.iter().find(|b| b.num_rows() > 0).unwrap();
    let column = batch.column(0).as_string::<i32>();
    (!column.is_null(0)).then(|| column.value(0).to_string())
}

#[tokio::test(flavor = "multi_thread")]
async fn gives_the_same_rows_as_the_rust_mapping() {
    let rt = runtime("fn-rust").await;
    rt.sql(WOD_FLAG_SQL).await;

    let mismatches = rt
        .sql(&format!(
            "SELECT count(*) FROM {FLAGS} \
             WHERE map_wod_quality_flag(flag) IS DISTINCT FROM wod_flag_sql(flag)"
        ))
        .await;
    assert_eq!(scalar_i64(&mismatches), 0);

    let mapped = rt
        .sql(&format!(
            "SELECT wod_flag_sql(flag) FROM {FLAGS} WHERE flag IN (0, 3, 7) ORDER BY flag"
        ))
        .await;
    assert_eq!(column_strings(&mapped, 0), ["1", "3", "4"]);
}

/// A constant argument is folded before the body is inlined, and a narrower
/// argument type is cast to the declared type.
#[tokio::test(flavor = "multi_thread")]
async fn takes_constant_and_narrower_arguments() {
    let rt = runtime("fn-const").await;
    rt.sql(WOD_FLAG_SQL).await;

    assert_eq!(
        optional_string(&rt.sql("SELECT wod_flag_sql(3)").await).as_deref(),
        Some("3")
    );
    assert_eq!(
        optional_string(&rt.sql("SELECT wod_flag_sql(CAST(8 AS INT))").await).as_deref(),
        Some("4")
    );
    assert_eq!(
        optional_string(&rt.sql("SELECT wod_flag_sql(42)").await),
        None
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn functions_take_several_arguments_and_call_other_functions() {
    let rt = runtime("fn-nested").await;
    rt.sql("CREATE FUNCTION add_mul(a BIGINT, b BIGINT, c BIGINT) RETURN (a + b) * c")
        .await;
    rt.sql("CREATE FUNCTION twice_add(a BIGINT, b BIGINT) RETURN add_mul(a, b, 2)")
        .await;
    rt.sql("CREATE FUNCTION answer() RETURNS BIGINT RETURN 42")
        .await;

    assert_eq!(scalar_i64(&rt.sql("SELECT add_mul(1, 2, 3)").await), 9);
    let rows = rt
        .sql("SELECT twice_add(v, 1) FROM (VALUES (CAST(4 AS BIGINT))) AS t(v)")
        .await;
    assert_eq!(scalar_i64(&rows), 10);
    assert_eq!(scalar_i64(&rt.sql("SELECT answer()").await), 42);
}

/// The planner puts the body in place of the call, so the Parquet scan gets the
/// simplified body as its predicate.
#[tokio::test(flavor = "multi_thread")]
async fn the_inlined_body_is_pushed_down_to_parquet() {
    let rt = runtime("fn-pushdown").await;
    let schema = Arc::new(Schema::new(vec![Field::new("flag", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from((0..10).collect::<Vec<i64>>()))],
    )
    .unwrap();
    std::fs::create_dir_all(rt.datasets_dir().join("flags")).unwrap();
    let file = std::fs::File::create(rt.datasets_dir().join("flags/data.parquet")).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    rt.sql("CREATE EXTERNAL TABLE flags STORED AS PARQUET LOCATION 'flags/'")
        .await;
    rt.sql(WOD_FLAG_SQL).await;

    let query = "SELECT flag FROM flags WHERE wod_flag_sql(flag) = '4'";
    let plan = column_strings(&rt.sql(&format!("EXPLAIN {query}")).await, 1).join("\n");
    let scan = plan
        .lines()
        .find(|line| line.contains("DataSourceExec"))
        .unwrap_or_else(|| panic!("no scan in plan:\n{plan}"));
    assert!(scan.contains("predicate="), "plan was:\n{plan}");
    assert!(scan.contains("flag@0 <= 9"), "plan was:\n{plan}");
    assert!(!plan.contains("wod_flag_sql"), "plan was:\n{plan}");

    assert_eq!(total_rows(&rt.sql(query).await), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn functions_and_their_views_survive_a_restart() {
    let rt = restartable_runtime("fn-restart", |b| b).await;
    rt.sql(WOD_FLAG_SQL).await;
    rt.sql("CREATE FUNCTION wod_flag_good(flag BIGINT) RETURN wod_flag_sql(flag) = '1'")
        .await;
    rt.sql(&format!(
        "CREATE VIEW good_flags AS SELECT flag FROM {FLAGS} WHERE wod_flag_good(flag)"
    ))
    .await;
    let definition = column_strings(&rt.sql("SHOW CREATE FUNCTION wod_flag_sql").await, 1);

    let rt = rt.restart().await;

    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM good_flags").await),
        1
    );
    assert_eq!(
        optional_string(&rt.sql("SELECT wod_flag_sql(6)").await).as_deref(),
        Some("4")
    );
    let shown = rt.sql("SHOW CREATE FUNCTION wod_flag_sql").await;
    assert_eq!(column_strings(&shown, 0), ["wod_flag_sql"]);
    assert_eq!(column_strings(&shown, 1), definition);
    assert!(definition[0].starts_with("CREATE FUNCTION wod_flag_sql"));

    rt.sql("DROP FUNCTION wod_flag_good").await;
    let rt = rt.restart().await;
    assert!(rt.try_sql("SELECT wod_flag_good(0)").await.is_err());
    assert_eq!(
        optional_string(&rt.sql("SELECT wod_flag_sql(0)").await).as_deref(),
        Some("1")
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_a_body_that_is_not_one_scalar_expression() {
    let rt = runtime("fn-body").await;
    rt.sql("CREATE TABLE t (a BIGINT)").await;

    for (sql, expected) in [
        ("CREATE FUNCTION f(a BIGINT) RETURN (SELECT 1)", "subquery"),
        (
            "CREATE FUNCTION f(a BIGINT) RETURN a IN (SELECT 1)",
            "subquery",
        ),
        (
            "CREATE FUNCTION f(a BIGINT) RETURN a IN (SELECT a FROM t)",
            "not found",
        ),
        (
            "CREATE FUNCTION f(a BIGINT) RETURN EXISTS (SELECT 1)",
            "subquery",
        ),
        ("CREATE FUNCTION f(a BIGINT) RETURN sum(a)", "aggregate"),
        (
            "CREATE FUNCTION f(a BIGINT) RETURN row_number() OVER ()",
            "window",
        ),
        ("CREATE FUNCTION f(a BIGINT) RETURN b + 1", "invalid body"),
        ("CREATE FUNCTION f(BIGINT) RETURN 1", "needs a name"),
        (
            "CREATE FUNCTION f(a BIGINT) RETURNS DATE RETURN [a]",
            "does not cast",
        ),
        (
            "CREATE FUNCTION f(a BIGINT) LANGUAGE python RETURN a",
            "LANGUAGE",
        ),
        (
            "CREATE FUNCTION f(a BIGINT) AS 'return a'",
            "RETURN <expression>",
        ),
    ] {
        let error = error_of(&rt, sql).await;
        assert!(error.contains(expected), "`{sql}` gave: {error}");
    }

    rt.sql("CREATE FUNCTION g(a BIGINT) RETURN a + 1").await;
    let error = error_of(
        &rt,
        "CREATE OR REPLACE FUNCTION g(a BIGINT) RETURN g(a) + 1",
    )
    .await;
    assert!(error.contains("must not call itself"), "{error}");
    rt.sql("CREATE FUNCTION h(a BIGINT) RETURN g(a) * 2").await;
    let error = error_of(&rt, "CREATE OR REPLACE FUNCTION g(a BIGINT) RETURN h(a)").await;
    assert!(error.contains("must not call itself"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn refuses_a_name_that_a_built_in_function_uses() {
    let rt = runtime("fn-name").await;

    for name in ["map_wod_quality_flag", "abs", "sum", "row_number"] {
        let error = error_of(&rt, &format!("CREATE FUNCTION {name}(a BIGINT) RETURN a")).await;
        assert!(error.contains("built-in function"), "{name} gave: {error}");
        let error = error_of(&rt, &format!("DROP FUNCTION {name}")).await;
        assert!(error.contains("built-in function"), "{name} gave: {error}");
    }
    let error = error_of(&rt, "SHOW CREATE FUNCTION abs").await;
    assert!(error.contains("built-in function"), "{error}");

    let error = error_of(&rt, "CREATE FUNCTION \"a/b\"(a BIGINT) RETURN a").await;
    assert!(error.contains("letters, digits and underscores"), "{error}");
    let error = error_of(&rt, "CREATE FUNCTION public.f(a BIGINT) RETURN a").await;
    assert!(error.contains("must not have a schema"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn or_replace_changes_an_existing_function() {
    let rt = runtime("fn-replace").await;
    rt.sql("CREATE FUNCTION f(a BIGINT) RETURN a + 1").await;

    let error = error_of(&rt, "CREATE FUNCTION f(a BIGINT) RETURN a + 2").await;
    assert!(error.contains("already exists"), "{error}");
    assert_eq!(scalar_i64(&rt.sql("SELECT f(1)").await), 2);

    rt.sql("CREATE OR REPLACE FUNCTION f(a BIGINT) RETURN a + 2")
        .await;
    assert_eq!(scalar_i64(&rt.sql("SELECT f(1)").await), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn drop_function_removes_only_unused_functions() {
    let rt = runtime("fn-drop").await;
    rt.sql("CREATE FUNCTION base(a BIGINT) RETURN a + 1").await;
    rt.sql("CREATE FUNCTION user_of_base(a BIGINT) RETURN base(a) * 2")
        .await;

    let error = error_of(&rt, "DROP FUNCTION base").await;
    assert!(error.contains("used by function 'user_of_base'"), "{error}");

    rt.sql("DROP FUNCTION user_of_base").await;
    rt.sql("DROP FUNCTION base").await;
    assert!(rt.try_sql("SELECT base(1)").await.is_err());

    let error = error_of(&rt, "DROP FUNCTION base").await;
    assert!(error.contains("does not exist"), "{error}");
    rt.sql("DROP FUNCTION IF EXISTS base").await;
}

/// Only the super-user can create or drop a function. Any user can call it.
#[tokio::test(flavor = "multi_thread")]
async fn only_the_super_user_creates_or_drops_a_function() {
    let rt = runtime("fn-user").await;

    for sql in [WOD_FLAG_SQL, "DROP FUNCTION IF EXISTS wod_flag_sql"] {
        let error = rt
            .try_sql_as(sql, non_super_user())
            .await
            .err()
            .unwrap_or_else(|| panic!("`{sql}` should be refused"));
        assert!(error.to_string().contains("super-user"), "{error}");
    }

    rt.sql(WOD_FLAG_SQL).await;
    let rows = rt
        .sql_as(
            &format!("SELECT wod_flag_sql(flag) FROM {FLAGS}"),
            non_super_user(),
        )
        .await;
    assert_eq!(total_rows(&rows), 14);

    let error = rt
        .try_sql_as("DROP FUNCTION wod_flag_sql", non_super_user())
        .await
        .err()
        .expect("a non-super DROP FUNCTION should be refused");
    assert!(error.to_string().contains("super-user"), "{error}");
}

/// `SHOW FUNCTIONS` as a table, one line per row.
async fn show_functions(rt: &TestRuntime, name: &str) -> String {
    let rows = rt.sql(&format!("SHOW FUNCTIONS LIKE '{name}'")).await;
    arrow::util::pretty::pretty_format_batches(&rows)
        .unwrap()
        .to_string()
}

/// `SHOW FUNCTIONS` lists the argument names and types, the return type and the
/// `COMMENT ON FUNCTION` text of a SQL function.
#[tokio::test(flavor = "multi_thread")]
async fn show_functions_lists_the_signature_and_the_comment() {
    let rt = restartable_runtime("fn-show", |b| b).await;
    rt.sql(WOD_FLAG_SQL).await;

    let shown = show_functions(&rt, "wod_flag_sql").await;
    for expected in ["[flag]", "[Int64]", "String", "wod_flag_sql(flag)"] {
        assert!(
            shown.contains(expected),
            "{expected} missing from:\n{shown}"
        );
    }

    rt.sql("COMMENT ON FUNCTION wod_flag_sql IS 'WOD quality flag to SeaDataNet L20'")
        .await;
    // OR REPLACE keeps the comment.
    rt.sql(&WOD_FLAG_SQL.replacen("CREATE", "CREATE OR REPLACE", 1))
        .await;
    let rt = rt.restart().await;
    let shown = show_functions(&rt, "wod_flag_sql").await;
    assert!(
        shown.contains("WOD quality flag to SeaDataNet L20"),
        "comment missing from:\n{shown}"
    );

    rt.sql("COMMENT ON FUNCTION wod_flag_sql IS NULL").await;
    let shown = show_functions(&rt, "wod_flag_sql").await;
    assert!(!shown.contains("SeaDataNet"), "comment kept in:\n{shown}");

    let error = error_of(&rt, "COMMENT ON FUNCTION abs IS 'x'").await;
    assert!(error.contains("built-in function"), "{error}");
    let error = error_of(&rt, "COMMENT ON FUNCTION missing IS 'x'").await;
    assert!(error.contains("does not exist"), "{error}");
    rt.sql("COMMENT IF EXISTS ON FUNCTION missing IS 'x'").await;
}
