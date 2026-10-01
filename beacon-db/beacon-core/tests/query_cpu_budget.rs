//! The CPU budget of a query, end to end through the `Runtime`.
//!
//! The meter itself is unit-tested in `beacon_core::query_cpu`. This suite
//! proves that the runtime selects the budget from the policy, that a
//! super-user has no limit by default, and that a caller can replace the
//! budget for one query.

use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use beacon_core::query::Query;
use beacon_core::query_cpu::CpuBudget;
use beacon_core::runtime::Runtime;
use beacon_core::runtime_builder::RuntimeBuilder;
use beacon_core::AuthIdentity;
use futures::TryStreamExt;

/// A query that needs some tens of milliseconds of CPU time.
const BUSY_SQL: &str = "SELECT count(*) FROM generate_series(1, 20000000) AS t(v) WHERE v % 7 = 3";

/// Far less CPU time than [`BUSY_SQL`] needs.
const TINY: CpuBudget = CpuBudget::Limited(Duration::from_micros(1));

async fn runtime(builder: RuntimeBuilder) -> Runtime {
    // Each runtime gets its own tmp dir: a build sweeps the output files in it.
    let tmp_dir = std::env::temp_dir().join(format!("beacon-cpu-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir).expect("create the runtime's tmp dir");
    builder
        .with_tmp_dir_path(tmp_dir)
        .build()
        .await
        .expect("the runtime should build")
}

async fn drain(
    result: anyhow::Result<beacon_core::query_result::QueryResult>,
) -> anyhow::Result<Vec<RecordBatch>> {
    Ok(result?
        .into_record_stream()?
        .try_collect::<Vec<_>>()
        .await?)
}

async fn run(rt: &Runtime, identity: AuthIdentity) -> anyhow::Result<Vec<RecordBatch>> {
    drain(
        rt.run_query(Query::sql(BUSY_SQL.to_string()), identity)
            .await,
    )
    .await
}

fn assert_over_budget(result: anyhow::Result<Vec<RecordBatch>>) {
    let error = result.expect_err("the query should go over its CPU budget");
    assert!(format!("{error:#}").contains("CPU budget"), "{error:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_a_policy_no_query_has_a_limit() {
    let rt = runtime(RuntimeBuilder::new()).await;
    run(&rt, AuthIdentity::empty())
        .await
        .expect("no limit applies");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_default_budget_limits_only_non_super_users() {
    let rt = runtime(RuntimeBuilder::new().with_query_cpu_budget(TINY)).await;
    assert_over_budget(run(&rt, AuthIdentity::empty()).await);
    run(&rt, AuthIdentity::system())
        .await
        .expect("a super-user has no limit");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_closure_policy_selects_the_budget_per_identity() {
    let policy = |identity: &AuthIdentity| {
        if identity.username == "heavy" {
            CpuBudget::Unlimited
        } else {
            TINY
        }
    };
    let rt = runtime(RuntimeBuilder::new().with_cpu_budget_policy(Arc::new(policy))).await;

    let mut heavy = AuthIdentity::empty();
    heavy.username = "heavy".to_string();
    run(&rt, heavy).await.expect("the policy gives no limit");

    // The policy replaces the default, so here a super-user is limited too.
    assert_over_budget(run(&rt, AuthIdentity::system()).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_budget_for_one_call_replaces_the_policy() {
    let rt = runtime(RuntimeBuilder::new()).await;
    let query = || Query::sql(BUSY_SQL.to_string());

    assert_over_budget(
        drain(
            rt.run_query_with_cpu_budget(query(), AuthIdentity::system(), TINY)
                .await,
        )
        .await,
    );

    let limited = RuntimeBuilder::new().with_query_cpu_budget(TINY);
    let rt = runtime(limited).await;
    drain(
        rt.run_query_with_cpu_budget(query(), AuthIdentity::empty(), CpuBudget::Unlimited)
            .await,
    )
    .await
    .expect("the call replaces the default budget");
}
