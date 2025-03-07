use std::sync::Arc;

use beacon_query::InnerQuery;
use datafusion::{
    dataframe::DataFrameWriteOptions, execution::SessionState, prelude::SessionContext,
};
use deltalake::{DeltaOps, DeltaTable};

use super::table::BeaconTable;
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ParquetIndexedTable {
    table_name: String,
    query: InnerQuery,
}

#[typetag::serde]
#[async_trait::async_trait]
impl BeaconTable for ParquetIndexedTable {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    async fn as_table(
        &self,
        session_state: Arc<SessionState>,
    ) -> Arc<dyn datafusion::catalog::TableProvider> {
        todo!()
    }

    async fn rebuild(&self, session_state: Arc<SessionContext>) -> anyhow::Result<()> {
        //Parse the query and rebuild the table
        let beacon_plan = beacon_query::parser::Parser::parse_to_logical_plan(
            session_state.as_ref(),
            self.query.clone(),
        )
        .await?;

        let df = session_state.execute_logical_plan(beacon_plan).await?;

        //Write the df to a parquet file in the tables directory
        let path = format!(
            "/{}/{}/{}.parquet",
            beacon_config::TABLES_DIR_PREFIX.to_string(),
            self.table_name,
            self.table_name
        );

        df.write_parquet(&path, DataFrameWriteOptions::default(), None)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use deltalake::DeltaTableBuilder;

    use super::*;

    #[tokio::test]
    async fn test_name() {
        // let mut table = DeltaTableBuilder::from_uri("./my_table")
        //     .without_files()
        //     .build()
        //     .unwrap();

        // DeltaOps().create().await.unwrap().

        // table.
        // table
        //     .get_file_uris()
        //     .unwrap()
        //     .into_iter()
        //     .for_each(|x| println!("{}", x));
    }
}
