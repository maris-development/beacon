//! `ErddapTable`: the DataFusion provider over one ERDDAP dataset.

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::Result;
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;

use crate::client::ErddapClient;
use crate::definition::{ErddapTableDefinition, ResolvedDataset};
use crate::exec::ErddapExec;
use crate::info::DatasetInfo;
use crate::location::{ErddapLocation, Protocol};
use crate::options::ErddapOptions;
use crate::tabledap;

/// A table over one ERDDAP tabledap dataset.
#[derive(Debug)]
pub struct ErddapTable {
    definition: ErddapTableDefinition,
    location: ErddapLocation,
    client: Arc<ErddapClient>,
    schema: SchemaRef,
}

impl ErddapTable {
    /// Validate the definition and resolve the dataset when it is not pinned yet.
    pub async fn try_new(mut definition: ErddapTableDefinition) -> anyhow::Result<Self> {
        let location = ErddapLocation::parse(&definition.location)?;
        anyhow::ensure!(
            location.protocol == Protocol::Tabledap,
            "ERDDAP griddap datasets are not supported yet; use a tabledap dataset URL"
        );
        let options = ErddapOptions::from_map(&definition.options)?;
        let client = Arc::new(ErddapClient::new(Duration::from_secs(
            options.request_timeout_secs,
        ))?);
        let resolved = match definition.resolved.take() {
            Some(resolved) => resolved,
            // The cause goes into the message: callers show only the top-level text.
            None => resolve(&client, &location).await.map_err(|e| {
                anyhow::anyhow!(
                    "failed to read ERDDAP dataset {}: {e:#}",
                    definition.location
                )
            })?,
        };
        let schema = resolved.schema.clone();
        definition.resolved = Some(resolved);
        Ok(Self {
            definition,
            location,
            client,
            schema,
        })
    }

    /// The definition, with `resolved` filled in.
    pub fn definition(&self) -> &ErddapTableDefinition {
        &self.definition
    }

    /// The parsed dataset URL.
    pub fn location(&self) -> &ErddapLocation {
        &self.location
    }

    fn tabledap_exec(
        &self,
        projected: SchemaRef,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> ErddapExec {
        let mut vars: Vec<String> = projected
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        if vars.is_empty() {
            // count(*) still needs one column to count rows of.
            vars.push(self.schema.field(0).name().clone());
        }
        let constraints: Vec<String> = filters
            .iter()
            .filter_map(|f| tabledap::translate(f, &self.schema))
            .flatten()
            .collect();
        let query = tabledap::request_query(&vars, &constraints);
        let url = self.location.data_url("parquet", &query);
        ErddapExec::new(self.client.clone(), vec![url], projected, limit)
    }
}

async fn resolve(
    client: &ErddapClient,
    location: &ErddapLocation,
) -> anyhow::Result<ResolvedDataset> {
    let info = DatasetInfo::parse(&client.get_bytes(&location.info_url()).await?)?;
    let schema = info.tabledap_schema()?;
    Ok(ResolvedDataset { info, schema })
}

#[async_trait::async_trait]
impl TableProvider for ErddapTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn get_table_definition(&self) -> Option<&str> {
        self.definition.definition.as_deref()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|f| {
                if tabledap::translate(f, &self.schema).is_some() {
                    TableProviderFilterPushDown::Inexact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let projected = match projection {
            Some(p) => Arc::new(self.schema.project(p)?),
            None => self.schema.clone(),
        };
        Ok(Arc::new(self.tabledap_exec(projected, filters, limit)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::fixture::{FixtureServer, Route};

    fn definition(location: String) -> ErddapTableDefinition {
        ErddapTableDefinition {
            name: "t".into(),
            location,
            options: HashMap::new(),
            definition: None,
            resolved: None,
        }
    }

    #[tokio::test]
    async fn rejects_griddap_without_a_request() {
        let server = FixtureServer::start(vec![]).await;
        let err = ErddapTable::try_new(definition(format!(
            "{}/griddap/erdHadISST",
            server.erddap_url()
        )))
        .await
        .unwrap_err()
        .to_string();
        assert_eq!(
            err,
            "ERDDAP griddap datasets are not supported yet; use a tabledap dataset URL"
        );
        assert!(server.requests().is_empty());
    }

    #[tokio::test]
    async fn resolves_once_and_then_uses_the_pinned_schema() {
        let server = FixtureServer::start(vec![Route::file(
            "/erddap/info/b/index.json",
            "tabledap_info.json",
        )])
        .await;
        let table = ErddapTable::try_new(definition(format!("{}/tabledap/b", server.erddap_url())))
            .await
            .unwrap();
        let pinned = table.definition().clone();
        assert_eq!(pinned.resolved.as_ref().unwrap().schema, table.schema());
        assert_eq!(table.schema().field(0).name(), "cruise_id");
        assert_eq!(server.requests().len(), 1);

        let again = ErddapTable::try_new(pinned).await.unwrap();
        assert_eq!(again.schema(), table.schema());
        assert_eq!(
            server.requests().len(),
            1,
            "a pinned definition makes no request"
        );
    }
}
