//! A new table or view name holds only ASCII letters, digits, `_` and `-`. A new
//! function name holds only ASCII letters, digits and `_`.
//!
//! Beacon puts names into object-store paths and URIs. A space, `#`, `?`, `/` or
//! `.` changes the meaning of that path, so every `CREATE` refuses such a name.

mod common;

const BAD_NAMES: [&str; 6] = ["odd name", "x#a", "x?a", "x/a", "x.a", "café"];

fn create_statements(name: &str) -> [String; 5] {
    [
        format!(r#"CREATE TABLE "{name}" (id BIGINT)"#),
        format!(r#"CREATE EXTERNAL TABLE "{name}" STORED AS CSV LOCATION 'obs/'"#),
        format!(r#"CREATE VIEW "{name}" AS SELECT 1 AS id"#),
        format!(r#"CREATE MATERIALIZED VIEW "{name}" AS SELECT 1 AS id"#),
        format!(r#"CREATE FUNCTION "{name}"(x BIGINT) RETURNS BIGINT RETURN x + 1"#),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_create_statement_refuses_an_unsafe_name() {
    let rt = common::runtime("object-names-refused").await;
    common::write_file(&rt.datasets_dir().join("obs/a.csv"), "id\n1\n");

    for name in BAD_NAMES {
        for statement in create_statements(name) {
            let message = rt
                .try_sql(&statement)
                .await
                .err()
                .unwrap_or_else(|| panic!("should refuse the name: {statement}"))
                .to_string();
            // The materialized view name is plain text, so `x.a` reads as schema `x`.
            assert!(
                message.contains("letters, digits") || message.contains("resolve schema: x"),
                "the error should say which characters are allowed: {statement}: {message}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_create_statement_accepts_a_safe_name() {
    let rt = common::runtime("object-names-accepted").await;
    common::write_file(&rt.datasets_dir().join("obs/a.csv"), "id\n1\n");

    for (index, statement) in create_statements("Ok_name").iter().enumerate() {
        let statement = statement.replace("Ok_name", &format!("Ok_name_{index}"));
        rt.try_sql(&statement)
            .await
            .unwrap_or_else(|error| panic!("should accept the name: {statement}: {error}"));
    }
    for (index, statement) in create_statements("argo").iter().take(4).enumerate() {
        let statement = statement.replace("argo", &format!("argo-{index}"));
        rt.try_sql(&statement)
            .await
            .unwrap_or_else(|error| panic!("a table or view takes '-': {statement}: {error}"));
    }
}
