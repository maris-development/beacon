use std::sync::Arc;

use beacon_formats::bbf::source::BBFSource;
use beacon_query::{Query, output::QueryOutputFile};
use datafusion::{
    catalog::memory::DataSourceExec, datasource::physical_plan::FileScanConfig,
    prelude::SessionContext,
};

use crate::metrics::MetricsTracker;

/// Represents the planned query, including its ID, metrics tracker, physical plan, and output buffer.
pub struct BeaconQueryPlan {
    /// Unique identifier for the query.
    pub query_id: uuid::Uuid,
    /// Tracks metrics for the query execution.
    pub metrics_tracker: Arc<MetricsTracker>,
    /// The physical execution plan for the query.
    pub physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    /// Optional output file for the query results. Otherwise, results are streamed back directly to the user.
    pub output_file: Option<QueryOutputFile>,
}

/// Plans a query by parsing, optimizing, and generating a physical plan.
/// Also wraps file scans to track metrics.
///
/// # Arguments
/// * `session_ctx` - DataFusion session context.
/// * `data_lake` - Reference to the data lake.
/// * `query` - The query to plan.
///
/// # Returns
/// * `BeaconQueryPlan` containing the planned query and associated metadata.
pub async fn plan_query(
    session_ctx: Arc<SessionContext>,
    data_lake: &beacon_data_lake::DataLake,
    query: Query,
) -> anyhow::Result<BeaconQueryPlan> {
    let query_id = uuid::Uuid::new_v4();
    let state = session_ctx.state();

    // Serialize the query for metrics tracking.
    let query_json_value = serde_json::to_value(&query).unwrap();

    // Parse the query into a logical plan.
    let parsed_plan = beacon_query::parser::Parser::parse(&session_ctx, data_lake, query).await?;

    // Optimize the logical plan.
    let optimized_plan = state.optimize(&parsed_plan.datafusion_plan)?;

    // Create the physical plan.
    let physical_plan = state.create_physical_plan(&optimized_plan).await?;

    // Initialize metrics tracker.
    let metrics_tracker = MetricsTracker::new(query_json_value, query_id);

    // Wrap file scans in the physical plan to track file paths.
    let tracked_physical_plan = wrap_file_scans(physical_plan.clone(), metrics_tracker.clone());

    // Set plans in the metrics tracker for logging and analysis.
    metrics_tracker.set_logical_plan(&parsed_plan.datafusion_plan);
    metrics_tracker.set_optimized_logical_plan(&optimized_plan);
    metrics_tracker.set_physical_plan(tracked_physical_plan.clone());

    Ok(BeaconQueryPlan {
        query_id,
        metrics_tracker,
        physical_plan: tracked_physical_plan,
        output_file: parsed_plan.output_file,
    })
}

/// Recursively wraps file scan nodes in the physical plan to track file paths for metrics.
///
/// # Arguments
/// * `plan` - The physical execution plan node.
/// * `tracker` - Metrics tracker to record file paths.
///
/// # Returns
/// * Modified execution plan with file scan nodes tracked.
fn wrap_file_scans(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    tracker: Arc<MetricsTracker>,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    // Check if the node is a DataSourceExec (file scan).
    if let Some(data_source) = plan.as_any().downcast_ref::<DataSourceExec>() {
        // Attempt to downcast to FileScanConfig to access file paths.
        if let Some(file_scan_config) = data_source
            .data_source()
            .as_any()
            .downcast_ref::<FileScanConfig>()
        {
            // Handle BBF Source Differently as it encompasses multiple files
            if let Some(bbf_source) = file_scan_config
                .file_source()
                .as_any()
                .downcast_ref::<BBFSource>()
            {
                let tracer = tracker.get_as_file_tracer();
                bbf_source.set_file_tracer(tracer);
            } else {
                // Collect file paths from all file groups.
                let files = file_scan_config
                    .file_groups
                    .iter()
                    .flat_map(|group| group.iter())
                    .map(|f| f.object_meta.location.to_string())
                    .collect::<Vec<_>>();
                tracker.add_file_paths(files);
            }
        } else {
            tracing::warn!("DataSourceExec is not a FileScanConfig. File paths are not logged.");
        }
    }

    // Recursively process child nodes.
    let new_children: Vec<_> = plan
        .children()
        .into_iter()
        .map(|c| wrap_file_scans(c.clone(), tracker.clone()))
        .collect();

    // Attempt to create a new plan node with updated children.
    plan.clone().with_new_children(new_children).unwrap_or(plan)
}
