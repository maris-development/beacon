//! Per-query CPU accounting and CPU budgets.
//!
//! Tokio counts CPU time per worker thread, not per task, and all queries share
//! the same workers. So this module measures a query from its own code: it reads
//! the thread CPU clock before and after each poll of the query, and adds the
//! difference to one [`QueryCpuMeter`] for that query.
//!
//! [`meter_plan`] wraps every node of a physical plan in a [`CpuMeteredExec`].
//! DataFusion spawns tasks for some operators (`RepartitionExec`,
//! `CoalescePartitionsExec`), and each of those tasks polls a child stream. That
//! child is wrapped too, so the work in the spawned task is counted.
//! [`QueryCpuMeter::meter_future`] counts the planning work.
//!
//! A poll inside a poll on the same thread counts once, at the outermost level.
//!
//! When a query uses more than its [`CpuBudget`], the next poll fails with
//! [`DataFusionError::ResourcesExhausted`]. The check happens at poll
//! boundaries, so one long poll can go over the budget before the query stops.
//!
//! Work that a query hands to other threads is not counted: `spawn_blocking`
//! closures, object store I/O threads and threads inside native libraries.

use std::any::Any;
use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use beacon_auth::{AuthContext, AuthIdentity};
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::OrderingRequirements;
use datafusion::physical_plan::execution_plan::{CardinalityEffect, InvariantLevel};
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, PlanProperties,
};
use futures::{Stream, StreamExt};

/// How much CPU time one query may use.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CpuBudget {
    /// No limit. The query is still measured.
    #[default]
    Unlimited,
    /// The query fails when its CPU time goes over this value.
    Limited(Duration),
}

impl CpuBudget {
    /// A budget of `millis` milliseconds. `0` means [`CpuBudget::Unlimited`].
    pub fn from_millis(millis: u64) -> Self {
        if millis == 0 {
            Self::Unlimited
        } else {
            Self::Limited(Duration::from_millis(millis))
        }
    }

    /// The limit, or `None` when the budget is unlimited.
    pub fn limit(&self) -> Option<Duration> {
        match self {
            Self::Unlimited => None,
            Self::Limited(limit) => Some(*limit),
        }
    }
}

/// Selects the [`CpuBudget`] for a query from the identity that runs it.
///
/// The runtime calls the policy once for each query, so a policy can read live
/// data, such as a quota table or the tier of an API key. A closure
/// `Fn(&AuthIdentity) -> CpuBudget` is also a policy.
pub trait CpuBudgetPolicy: Send + Sync {
    fn budget_for(&self, identity: &AuthIdentity) -> CpuBudget;
}

impl<F> CpuBudgetPolicy for F
where
    F: Fn(&AuthIdentity) -> CpuBudget + Send + Sync,
{
    fn budget_for(&self, identity: &AuthIdentity) -> CpuBudget {
        self(identity)
    }
}

/// The default policy. It reads the budget from the roles of the identity.
///
/// 1. A super-user has no limit.
/// 2. Else the `query_cpu_limit_ms` setting of the roles applies, set with
///    `ALTER ROLE <role> SET query_cpu_limit_ms = <n>`. The most generous role
///    wins, and `0` means no limit.
/// 3. Else [`Self::default_budget`] applies.
///
/// The policy reads the live roles, so an `ALTER ROLE` applies from the next
/// query.
pub struct RoleCpuBudgetPolicy {
    auth: Arc<AuthContext>,
    default_budget: CpuBudget,
}

impl RoleCpuBudgetPolicy {
    pub fn new(auth: Arc<AuthContext>, default_budget: CpuBudget) -> Self {
        Self {
            auth,
            default_budget,
        }
    }
}

impl CpuBudgetPolicy for RoleCpuBudgetPolicy {
    fn budget_for(&self, identity: &AuthIdentity) -> CpuBudget {
        if identity.is_super_user {
            return CpuBudget::Unlimited;
        }
        match self.auth.query_cpu_limit_ms(&identity.roles) {
            Some(millis) => CpuBudget::from_millis(millis),
            None => self.default_budget,
        }
    }
}

/// The CPU time one query has used, and the budget it runs under.
///
/// Shared by every node and task of the query. Cheap to update: one atomic add
/// for each outermost poll.
#[derive(Debug)]
pub struct QueryCpuMeter {
    used_nanos: AtomicU64,
    budget: CpuBudget,
}

impl QueryCpuMeter {
    pub fn new(budget: CpuBudget) -> Arc<Self> {
        Arc::new(Self {
            used_nanos: AtomicU64::new(0),
            budget,
        })
    }

    pub fn budget(&self) -> CpuBudget {
        self.budget
    }

    /// The CPU time the query has used until now.
    pub fn used(&self) -> Duration {
        Duration::from_nanos(self.used_nanos.load(Ordering::Relaxed))
    }

    /// An error when the query has used more than its budget.
    pub fn check(&self) -> Result<(), CpuBudgetExceeded> {
        match self.budget.limit() {
            Some(limit) if self.used() > limit => Err(CpuBudgetExceeded {
                limit,
                used: self.used(),
            }),
            _ => Ok(()),
        }
    }

    /// Runs `poll` and adds the CPU time of this thread during `poll`.
    ///
    /// When this thread already measures an outer poll, `poll` runs without a
    /// second measurement, so the time counts once.
    pub fn time<R>(&self, poll: impl FnOnce() -> R) -> R {
        if POLL_TIMED.with(Cell::get) {
            return poll();
        }
        let _timer = PollTimer::start(self);
        poll()
    }

    /// Wraps `future` so that its polls count against this meter.
    ///
    /// The future fails with [`CpuBudgetExceeded`] at the first poll after the
    /// budget is used up.
    pub fn meter_future<F, T>(self: &Arc<Self>, future: F) -> CpuMeteredFuture<F>
    where
        F: Future<Output = anyhow::Result<T>>,
    {
        CpuMeteredFuture {
            future: Box::pin(future),
            meter: self.clone(),
        }
    }

    fn add(&self, elapsed: Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        self.used_nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

/// The error of a query that used more CPU time than its budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CpuBudgetExceeded {
    pub limit: Duration,
    pub used: Duration,
}

impl fmt::Display for CpuBudgetExceeded {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "query exceeded its CPU budget: used {:?} of {:?}",
            self.used, self.limit
        )
    }
}

impl std::error::Error for CpuBudgetExceeded {}

impl From<CpuBudgetExceeded> for DataFusionError {
    fn from(error: CpuBudgetExceeded) -> Self {
        DataFusionError::ResourcesExhausted(error.to_string())
    }
}

thread_local! {
    /// True while this thread measures a poll.
    static POLL_TIMED: Cell<bool> = const { Cell::new(false) };
}

/// Measures one outermost poll. The drop records the time, also on a panic.
struct PollTimer<'a> {
    meter: &'a QueryCpuMeter,
    start: Option<Duration>,
}

impl<'a> PollTimer<'a> {
    fn start(meter: &'a QueryCpuMeter) -> Self {
        POLL_TIMED.with(|timed| timed.set(true));
        Self {
            meter,
            start: thread_cpu_time(),
        }
    }
}

impl Drop for PollTimer<'_> {
    fn drop(&mut self) {
        POLL_TIMED.with(|timed| timed.set(false));
        if let (Some(start), Some(end)) = (self.start, thread_cpu_time()) {
            self.meter.add(end.saturating_sub(start));
        }
    }
}

/// The CPU time this thread has used. `None` when the platform cannot tell.
#[cfg(unix)]
fn thread_cpu_time() -> Option<Duration> {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `time` is a valid, writable timespec for the call to fill.
    let result = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
    (result == 0).then(|| Duration::new(time.tv_sec as u64, time.tv_nsec as u32))
}

/// No thread CPU clock here, so the meter stays at zero.
#[cfg(not(unix))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

/// A future whose polls count against a [`QueryCpuMeter`].
pub struct CpuMeteredFuture<F> {
    future: Pin<Box<F>>,
    meter: Arc<QueryCpuMeter>,
}

impl<F, T> Future for CpuMeteredFuture<F>
where
    F: Future<Output = anyhow::Result<T>>,
{
    type Output = anyhow::Result<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Err(error) = this.meter.check() {
            return Poll::Ready(Err(error.into()));
        }
        let future = &mut this.future;
        this.meter.time(|| future.as_mut().poll(cx))
    }
}

/// Wraps every node of `plan` in a [`CpuMeteredExec`] that counts against
/// `meter`.
///
/// A node that cannot take new children keeps its subtree without a wrapper
/// below it. The query then still runs, with less CPU time counted.
pub fn meter_plan(
    plan: Arc<dyn ExecutionPlan>,
    meter: &Arc<QueryCpuMeter>,
) -> Arc<dyn ExecutionPlan> {
    let children: Vec<Arc<dyn ExecutionPlan>> = plan
        .children()
        .into_iter()
        .map(|child| meter_plan(child.clone(), meter))
        .collect();
    let plan = if children.is_empty() {
        plan
    } else {
        match plan.clone().with_new_children(children) {
            Ok(rebuilt) => rebuilt,
            Err(error) => {
                tracing::debug!(node = plan.name(), %error, "node not CPU metered below");
                plan
            }
        }
    };
    Arc::new(CpuMeteredExec {
        inner: plan,
        meter: meter.clone(),
    })
}

/// The node below a [`CpuMeteredExec`], or `plan` itself when it is not one.
///
/// Use it before a downcast on a metered plan.
pub fn unwrap_metered(plan: &dyn ExecutionPlan) -> &dyn ExecutionPlan {
    match plan.as_any().downcast_ref::<CpuMeteredExec>() {
        Some(metered) => metered.inner.as_ref(),
        None => plan,
    }
}

/// A node that counts the CPU time of the stream of the node it wraps.
///
/// The wrapper is transparent: its name, display, metrics, properties and
/// children are those of the wrapped node. A walk over the plan, such as the
/// metrics collection or `EXPLAIN`, therefore sees the original plan shape.
/// Only a downcast sees the wrapper, so use [`unwrap_metered`] before one.
///
/// Apply it after physical optimization. It returns no pushdown or repartition
/// rewrites of its own.
#[derive(Debug)]
pub struct CpuMeteredExec {
    inner: Arc<dyn ExecutionPlan>,
    meter: Arc<QueryCpuMeter>,
}

impl CpuMeteredExec {
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

impl DisplayAs for CpuMeteredExec {
    fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        self.inner.fmt_as(format, f)
    }
}

impl ExecutionPlan for CpuMeteredExec {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn check_invariants(&self, check: InvariantLevel) -> DataFusionResult<()> {
        self.inner.check_invariants(check)
    }

    // The per-child methods describe the children of the wrapped node, because
    // `children` returns those children.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.inner.required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.inner.required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: self.inner.clone().with_new_children(children)?,
            meter: self.meter.clone(),
        }))
    }

    fn reset_state(self: Arc<Self>) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: self.inner.clone().reset_state()?,
            meter: self.meter.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.meter.check()?;
        // Some nodes do real work in `execute`, before the first poll.
        let stream = self.meter.time(|| self.inner.execute(partition, context))?;
        Ok(Box::pin(CpuMeteredStream {
            inner: stream,
            meter: self.meter.clone(),
            failed: false,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.inner.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> DataFusionResult<Statistics> {
        self.inner.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.inner.supports_limit_pushdown()
    }

    fn fetch(&self) -> Option<usize> {
        self.inner.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.inner.cardinality_effect()
    }
}

/// The stream of a [`CpuMeteredExec`].
struct CpuMeteredStream {
    inner: SendableRecordBatchStream,
    meter: Arc<QueryCpuMeter>,
    /// Set after the budget error, so the stream ends instead of repeating it.
    failed: bool,
}

impl Stream for CpuMeteredStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.failed {
            return Poll::Ready(None);
        }
        if let Err(error) = this.meter.check() {
            this.failed = true;
            return Poll::Ready(Some(Err(error.into())));
        }
        let inner = &mut this.inner;
        this.meter.time(|| inner.poll_next_unpin(cx))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for CpuMeteredStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use datafusion::physical_plan::{collect, displayable};
    use datafusion::prelude::SessionContext;

    use super::*;

    /// Keeps this thread busy for about `duration` of CPU time.
    fn spin(duration: Duration) {
        let start = Instant::now();
        let mut value = 0u64;
        while start.elapsed() < duration {
            value = std::hint::black_box(value.wrapping_mul(31).wrapping_add(7));
        }
    }

    /// A query that needs some tens of milliseconds of CPU time.
    const BUSY_SQL: &str = "SELECT count(*) FROM generate_series(1, 20000000) AS t(v) \
                            WHERE v % 7 = 3";

    async fn physical_plan(ctx: &SessionContext, sql: &str) -> Arc<dyn ExecutionPlan> {
        ctx.sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }

    #[test]
    fn a_nested_poll_counts_once() {
        let meter = QueryCpuMeter::new(CpuBudget::Unlimited);
        let busy = Duration::from_millis(50);
        meter.time(|| meter.time(|| spin(busy)));
        let used = meter.used();
        assert!(used >= busy / 2, "used {used:?}");
        assert!(used < busy * 3 / 2, "counted twice: used {used:?}");
    }

    #[test]
    fn the_budget_check_fails_only_past_the_limit() {
        let meter = QueryCpuMeter::new(CpuBudget::Limited(Duration::from_millis(20)));
        assert!(meter.check().is_ok());
        meter.time(|| spin(Duration::from_millis(40)));
        let error = meter.check().unwrap_err();
        assert_eq!(error.limit, Duration::from_millis(20));
        assert!(error.to_string().contains("CPU budget"), "{error}");
    }

    #[test]
    fn an_unlimited_budget_never_fails() {
        let meter = QueryCpuMeter::new(CpuBudget::Unlimited);
        meter.time(|| spin(Duration::from_millis(10)));
        assert!(meter.check().is_ok());
    }

    #[test]
    fn zero_millis_is_unlimited() {
        assert_eq!(CpuBudget::from_millis(0), CpuBudget::Unlimited);
        assert_eq!(
            CpuBudget::from_millis(1500),
            CpuBudget::Limited(Duration::from_millis(1500))
        );
    }

    fn identity_with(roles: &[&str]) -> AuthIdentity {
        let mut identity = AuthIdentity::empty();
        identity.roles = roles.iter().map(|role| role.to_string()).collect();
        identity
    }

    #[tokio::test]
    async fn the_role_policy_reads_the_role_setting() {
        use beacon_auth::{BasicAuthProvider, QUERY_CPU_LIMIT_MS};

        let auth = Arc::new(AuthContext::new(Arc::new(BasicAuthProvider::new())));
        for role in ["analyst", "free", "plain"] {
            auth.create_role(role).await.unwrap();
        }
        let setting = QUERY_CPU_LIMIT_MS;
        auth.set_role_setting("analyst", setting, "60000")
            .await
            .unwrap();
        auth.set_role_setting("free", setting, "0").await.unwrap();

        let default = CpuBudget::Limited(Duration::from_secs(5));
        let policy = RoleCpuBudgetPolicy::new(auth.clone(), default);
        let limited = |secs| CpuBudget::Limited(Duration::from_secs(secs));

        assert_eq!(
            policy.budget_for(&AuthIdentity::system()),
            CpuBudget::Unlimited
        );
        assert_eq!(policy.budget_for(&identity_with(&["analyst"])), limited(60));
        assert_eq!(
            policy.budget_for(&identity_with(&["analyst", "free"])),
            CpuBudget::Unlimited
        );
        assert_eq!(policy.budget_for(&identity_with(&["plain"])), default);
        assert_eq!(policy.budget_for(&AuthIdentity::empty()), default);

        // The policy reads the live role, so a change applies to the next query.
        auth.set_role_setting("analyst", setting, "1000")
            .await
            .unwrap();
        assert_eq!(policy.budget_for(&identity_with(&["analyst"])), limited(1));
        auth.reset_role_setting("analyst", setting).await.unwrap();
        assert_eq!(policy.budget_for(&identity_with(&["analyst"])), default);
    }

    #[test]
    fn a_closure_is_a_policy() {
        let policy = |identity: &AuthIdentity| {
            if identity.roles.iter().any(|role| role == "premium") {
                CpuBudget::Limited(Duration::from_secs(60))
            } else {
                CpuBudget::Limited(Duration::from_secs(1))
            }
        };
        let mut premium = AuthIdentity::empty();
        premium.roles.push("premium".to_string());
        assert_eq!(
            policy.budget_for(&premium),
            CpuBudget::Limited(Duration::from_secs(60))
        );
        assert_eq!(
            policy.budget_for(&AuthIdentity::empty()),
            CpuBudget::Limited(Duration::from_secs(1))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_wrapped_plan_looks_like_the_original() {
        let ctx = SessionContext::new();
        let plan = physical_plan(&ctx, BUSY_SQL).await;
        let metered = meter_plan(plan.clone(), &QueryCpuMeter::new(CpuBudget::Unlimited));
        assert_eq!(
            displayable(metered.as_ref()).indent(true).to_string(),
            displayable(plan.as_ref()).indent(true).to_string()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_query_counts_the_work_of_its_spawned_tasks() {
        let ctx = SessionContext::new();
        let plan = physical_plan(&ctx, BUSY_SQL).await;
        let meter = QueryCpuMeter::new(CpuBudget::Unlimited);
        let metered = meter_plan(plan, &meter);
        let batches = collect(metered, ctx.task_ctx()).await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        assert!(meter.used() > Duration::ZERO, "no CPU time counted");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_query_over_its_budget_fails() {
        let ctx = SessionContext::new();
        let plan = physical_plan(&ctx, BUSY_SQL).await;
        let meter = QueryCpuMeter::new(CpuBudget::Limited(Duration::from_micros(1)));
        let error = collect(meter_plan(plan, &meter), ctx.task_ctx())
            .await
            .unwrap_err();
        assert!(error.to_string().contains("CPU budget"), "{error}");
    }

    #[tokio::test]
    async fn a_metered_future_fails_after_its_budget() {
        let meter = QueryCpuMeter::new(CpuBudget::Limited(Duration::from_millis(5)));
        let result = meter
            .meter_future(async {
                spin(Duration::from_millis(20));
                tokio::task::yield_now().await;
                Ok(())
            })
            .await;
        let error = result.unwrap_err();
        assert!(error.to_string().contains("CPU budget"), "{error}");
    }
}
