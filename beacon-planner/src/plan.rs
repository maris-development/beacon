use std::sync::Arc;

use beacon_query::{Query, output::QueryOutputFile};
use beacon_sources::{netcdf_format::NetCDFExec, odv_format::OdvExec};
use datafusion::{
    datasource::physical_plan::{ArrowExec, CsvExec, ParquetExec},
    physical_plan::filter::FilterExec,
    prelude::SessionContext,
};

use crate::metrics::MetricsTracker;

pub async fn plan_query(
    session_ctx: Arc<SessionContext>,
    query: Query,
) -> anyhow::Result<BeaconQueryPlan> {
    let query_id = uuid::Uuid::new_v4();
    let state = session_ctx.state();
    // Parse the query to a logical plan
    let query_json_value = serde_json::to_value(&query).unwrap();

    let parsed_plan = beacon_query::parser::Parser::parse(&session_ctx, query).await?;
    let optimized_plan = state.optimize(&parsed_plan.datafusion_plan)?;
    let physical_plan = state.create_physical_plan(&optimized_plan).await?;
    let metrics_tracker = MetricsTracker::new(query_json_value, query_id);

    // FilterExec
    let tracked_physical_plan = wrap_file_scans(physical_plan.clone(), metrics_tracker.clone());

    metrics_tracker.set_logical_plan(&parsed_plan.datafusion_plan);
    metrics_tracker.set_optimized_logical_plan(&optimized_plan);
    metrics_tracker.set_physical_plan(tracked_physical_plan.clone());

    Ok(BeaconQueryPlan {
        query_id,
        metrics_tracker,
        physical_plan: tracked_physical_plan,
        output_buffer: parsed_plan.output,
    })
}

fn wrap_file_scans(
    plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    tracker: Arc<MetricsTracker>,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    // This function is a placeholder for wrapping file scans with the metrics tracker
    // The actual implementation would depend on the specific requirements of the metrics tracking
    if let Some(csv) = plan.as_any().downcast_ref::<CsvExec>() {
        let files = csv
            .base_config()
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|f| f.object_meta.location.to_string())
            .collect::<Vec<_>>();

        tracker.add_file_paths(files);
    } else if let Some(parquet) = plan.as_any().downcast_ref::<ParquetExec>() {
        let files = parquet
            .base_config()
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|f| f.object_meta.location.to_string())
            .collect::<Vec<_>>();

        tracker.add_file_paths(files);
    } else if let Some(arrow) = plan.as_any().downcast_ref::<ArrowExec>() {
        let files = arrow
            .base_config()
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|f| f.object_meta.location.to_string())
            .collect::<Vec<_>>();

        tracker.add_file_paths(files);
    } else if let Some(odv) = plan.as_any().downcast_ref::<OdvExec>() {
        let files = odv
            .file_scan_config()
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|f| f.object_meta.location.to_string())
            .collect::<Vec<_>>();

        tracker.add_file_paths(files);
    } else if let Some(netcdf) = plan.as_any().downcast_ref::<NetCDFExec>() {
        let files = netcdf
            .file_scan_config()
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|f| f.object_meta.location.to_string())
            .collect::<Vec<_>>();

        tracker.add_file_paths(files);
    }
    // Recurse into children
    let new_children: Vec<_> = plan
        .children()
        .into_iter()
        .map(|c| wrap_file_scans(c.clone(), tracker.clone()))
        .collect();

    plan.clone().with_new_children(new_children).unwrap_or(plan)
}

pub struct BeaconQueryPlan {
    pub query_id: uuid::Uuid,
    pub metrics_tracker: Arc<MetricsTracker>,
    pub physical_plan: Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    pub output_buffer: QueryOutputFile,
}
