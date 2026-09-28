//! A deeply nested SQL expression is refused before planning. DataFusion plans an
//! expression tree recursively, so a deep enough one overflows the thread stack and ends the
//! process.

mod common;

use common::runtime;

/// `SELECT 1+1+...+1` with `terms` operands: a binary tree `terms - 1` levels deep.
fn chain(terms: usize) -> String {
    format!("SELECT {} AS total", vec!["1"; terms].join("+"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_deep_expression_is_an_error_not_a_crash() {
    let rt = runtime("expression-depth-refused").await;

    let err = rt
        .try_sql(&chain(5_000))
        .await
        .expect_err("a 5000-level expression should be refused");

    assert!(
        err.to_string().contains("nesting depth"),
        "the error should name the limit, got: {err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_moderate_expression_still_runs() {
    let rt = runtime("expression-depth-allowed").await;

    let batches = rt.sql(&chain(100)).await;

    assert_eq!(common::scalar_i64(&batches), 100);
}
