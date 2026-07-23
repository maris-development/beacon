//! CSV delimiter handling across the `read_csv` / `read_csv_schema` table
//! functions and `CREATE EXTERNAL TABLE … STORED AS CSV`.
//!
//! The delimiter accepts a bare character (`;`, `|`) or a C-style escape
//! (`\t`, `\n`, `\r`), so tab-separated files work with `'\t'` rather than
//! requiring a raw tab in the SQL. External tables honour the same `OPTIONS
//! ('delimiter' …)` value (which the factory used to ignore entirely).

mod common;

use common::{column_strings, runtime, scalar_i64, total_rows, write_file, TestRuntime};

/// A tab-separated file: with the default comma delimiter it is a single column;
/// with a tab delimiter it is three.
async fn tsv_runtime(tag: &str) -> TestRuntime {
    let rt = runtime(tag).await;
    write_file(
        &rt.datasets_dir().join("t/data.tsv"),
        "id\tname\tdepth\n1\ta\t10\n2\tb\t20\n",
    );
    rt
}

#[tokio::test(flavor = "multi_thread")]
async fn read_csv_accepts_a_tab_escape_delimiter() {
    let rt = tsv_runtime("csv-delim-read-tab").await;

    // `'\t'` (backslash-t in the SQL) is resolved to a tab: three columns, two rows.
    let batches = rt.sql("SELECT * FROM read_csv('t/data.tsv', '\\t')").await;
    assert_eq!(total_rows(&batches), 2);
    let schema = batches[0].schema();
    let cols: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(cols, vec!["id", "name", "depth"]);

    // The value in the second column of the first row is the tab-split field.
    assert_eq!(
        column_strings(
            &rt.sql("SELECT name FROM read_csv('t/data.tsv', '\\t') WHERE id = 1").await,
            0
        ),
        vec!["a"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn without_the_tab_delimiter_the_tsv_is_one_column() {
    let rt = tsv_runtime("csv-delim-default").await;

    // Default comma delimiter: the whole tab-joined line is one column.
    let batches = rt.sql("SELECT * FROM read_csv('t/data.tsv')").await;
    assert_eq!(
        batches[0].schema().fields().len(),
        1,
        "with the default comma delimiter a TSV parses as a single column"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn read_csv_accepts_a_bare_character_delimiter() {
    let rt = runtime("csv-delim-semicolon").await;
    write_file(&rt.datasets_dir().join("s/data.csv"), "a;b;c\n1;2;3\n");

    let cols: Vec<String> = {
        let batches = rt.sql("SELECT * FROM read_csv('s/data.csv', ';')").await;
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    assert_eq!(cols, vec!["a", "b", "c"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn read_csv_schema_accepts_the_delimiter() {
    let rt = tsv_runtime("csv-delim-schema").await;

    // The schema function forwards the delimiter to the reader.
    let names = column_strings(
        &rt.sql("SELECT column_name FROM read_csv_schema('t/data.tsv', '\\t') ORDER BY column_name")
            .await,
        0,
    );
    assert_eq!(names, vec!["depth", "id", "name"]);

    // Without the delimiter it collapses to one column.
    assert_eq!(
        scalar_i64(&rt.sql("SELECT count(*) FROM read_csv_schema('t/data.tsv')").await),
        1,
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn external_csv_table_honours_the_delimiter_option() {
    let rt = tsv_runtime("csv-delim-external").await;

    rt.sql(
        "CREATE EXTERNAL TABLE tsv STORED AS CSV LOCATION 't/' OPTIONS ('delimiter' '\\t')",
    )
    .await;

    // The tab delimiter took effect: three columns, two rows.
    assert_eq!(scalar_i64(&rt.sql("SELECT count(*) FROM tsv").await), 2);
    let cols: Vec<String> = {
        let batches = rt.sql("SELECT * FROM tsv LIMIT 1").await;
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    };
    assert_eq!(cols, vec!["id", "name", "depth"]);
    assert_eq!(
        column_strings(&rt.sql("SELECT name FROM tsv WHERE id = 2").await, 0),
        vec!["b"]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn external_csv_table_without_delimiter_defaults_to_comma() {
    let rt = tsv_runtime("csv-delim-external-default").await;

    rt.sql("CREATE EXTERNAL TABLE tsv STORED AS CSV LOCATION 't/'").await;
    let batches = rt.sql("SELECT * FROM tsv LIMIT 1").await;
    assert_eq!(
        batches[0].schema().fields().len(),
        1,
        "without a delimiter option the TSV is one comma-delimited column"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_delimiter_is_a_clear_error() {
    let rt = tsv_runtime("csv-delim-invalid").await;

    let err = rt
        .try_sql("SELECT * FROM read_csv('t/data.tsv', 'ab')")
        .await
        .err()
        .expect("a multi-character delimiter should be rejected");
    assert!(
        err.to_string().contains("delimiter"),
        "the error should mention the delimiter, got: {err}"
    );
}
