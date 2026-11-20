//! `table_statistics('<table>')`: the file statistics of one external table, as SQL.
//!
//! The function returns the statistics table of the table: one row for each
//! file, with the file version, the outcome of the analysis and one struct
//! column with `min`, `max` and `null_count` for each statistics column.
//!
//! ```sql
//! SELECT _path, _row_count, "TEMP".min, "TEMP".max FROM table_statistics('obs');
//! SELECT _path FROM table_statistics('obs') WHERE _failed;
//! ```

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use beacon_file_statistics::FileStatistics;
use datafusion::{
    catalog::{Session, TableFunctionArgs, TableFunctionImpl, TableProvider},
    common::{plan_datafusion_err, plan_err, Result as DFResult},
    logical_expr::{Expr, TableProviderFilterPushDown, TableType},
    physical_plan::ExecutionPlan,
};

/// The name `table_statistics('<table>')` is registered under.
pub const TABLE_STATISTICS_FUNCTION: &str = "table_statistics";

/// `table_statistics('<table>')`: the statistics table of an external table.
///
/// The table name is the name of the `CREATE EXTERNAL TABLE` statement.
#[derive(Debug, Default)]
pub(crate) struct TableStatisticsFunc;

impl TableFunctionImpl for TableStatisticsFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        let table = match args.exprs() {
            [Expr::Literal(value, _)] => value.try_as_str().flatten(),
            _ => None,
        };
        let Some(table) = table else {
            return plan_err!(
                "{TABLE_STATISTICS_FUNCTION} takes one table name: \
                 {TABLE_STATISTICS_FUNCTION}('obs')"
            );
        };
        let Some(engine) = args.session().config().get_extension::<FileStatistics>() else {
            return plan_err!(
                "file statistics are not enabled on this runtime; set BEACON_FILE_STATS_ENABLE=true"
            );
        };
        let provider = engine
            .statistics_table(table)
            .map_err(|error| plan_datafusion_err!("{error:#}"))?;
        Ok(Arc::new(TableStatisticsTable(provider)))
    }
}

/// The provider `table_statistics(...)` returns.
///
/// A newtype, so the authorization walk can find it: a function has no schema
/// in the plan, so the metadata gate on the schema name does not see it.
#[derive(Debug)]
pub(crate) struct TableStatisticsTable(Arc<dyn TableProvider>);

#[async_trait::async_trait]
impl TableProvider for TableStatisticsTable {
    fn schema(&self) -> SchemaRef {
        self.0.schema()
    }

    /// `View`: the engine writes the statistics, SQL only reads them.
    fn table_type(&self) -> TableType {
        TableType::View
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.0.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.0.scan(state, projection, filters, limit).await
    }
}
