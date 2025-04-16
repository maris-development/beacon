use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use datafusion::physical_plan::{stream::RecordBatchStreamAdapter, DisplayAs, ExecutionPlan};
use futures::StreamExt;

#[derive(Debug)]
pub struct RenameExec {
    input_plan: Arc<dyn ExecutionPlan>,
    plan_properties: datafusion::physical_plan::PlanProperties,
}

impl RenameExec {
    pub fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        renamed_schema: SchemaRef,
    ) -> anyhow::Result<Self> {
        //Validate that the renamed schema has the same number of columns and data types as the input plan
        if input_plan.schema().fields().len() != renamed_schema.fields().len() {
            return Err(anyhow::anyhow!(
                "Input plan and renamed schema have different number of columns"
            ));
        }

        for (i, field) in input_plan.schema().fields().iter().enumerate() {
            if field.data_type() != renamed_schema.field(i).data_type() {
                return Err(anyhow::anyhow!(
                    "Input plan and renamed schema have different data types for column {}",
                    field.name()
                ));
            }
        }

        let input_props = input_plan.properties();
        let eq_props = input_props
            .eq_properties
            .clone()
            .with_new_schema(renamed_schema.clone())
            .unwrap();
        let plan_properties = input_props.clone().with_eq_properties(eq_props);

        Ok(Self {
            input_plan,
            plan_properties,
        })
    }
}

impl DisplayAs for RenameExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(f, "RenameExec: {}", self.input_plan.name())
            }
            datafusion::physical_plan::DisplayFormatType::Verbose => {
                write!(f, "RenameExec: {}", self.input_plan.name())
            }
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for RenameExec {
    fn name(&self) -> &str {
        "RenameExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self.clone())
        } else {
            Err(datafusion::error::DataFusionError::Internal(
                "RenameExec does not have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let mut stream = self.input_plan.execute(partition, context.clone())?;

        let schema = self.plan_properties.eq_properties.schema().clone();
        let stream = async_stream::try_stream! {
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let columns = batch.columns().to_vec();
                let renamed_batch = arrow::record_batch::RecordBatch::try_new(
                    schema.clone(),
                    columns,
                )?;
                yield renamed_batch;
            }
        };

        let adapter = RecordBatchStreamAdapter::new(
            self.plan_properties.eq_properties.schema().clone(),
            stream,
        );

        Ok(Box::pin(adapter))
    }
}
