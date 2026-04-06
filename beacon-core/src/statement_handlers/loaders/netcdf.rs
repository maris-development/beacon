use async_trait::async_trait;
use beacon_arrow_netcdf::reader::NetCDFArrowReader;
use beacon_atlas::prelude::Dataset;
use beacon_common::listing_url::parse_listing_table_url;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};

use crate::statement_handlers::{context::HandlerContext, traits::IngestFormatLoader};

pub(crate) struct NetcdfIngestFormatLoader;

#[async_trait]
impl IngestFormatLoader for NetcdfIngestFormatLoader {
    fn format_name(&self) -> &'static str {
        "netcdf"
    }

    async fn load(
        &self,
        context: &HandlerContext,
        glob_pattern: &str,
    ) -> anyhow::Result<BoxStream<'static, anyhow::Result<Dataset>>> {
        let session_ctx = context.session_ctx();
        let session_state = session_ctx.state();
        let object_store_url = context.data_object_store_url();
        let store = session_ctx.runtime_env().object_store(&object_store_url)?;
        let datasets_store = beacon_object_storage::get_datasets_object_store().await;
        let listing_table_url = parse_listing_table_url(&object_store_url, glob_pattern)?;

        let objects: Vec<_> = listing_table_url
            .list_all_files(&session_state, &store, "")
            .await?
            .try_collect()
            .await?;

        let datasets = objects.into_iter().map(move |obj| {
            let file_path = datasets_store.translate_netcdf_url_path(&obj.location)?;
            let reader = NetCDFArrowReader::new(&file_path)?;
            let arrays = reader.read_columns::<Vec<_>>(None)?;
            let schema = reader.schema();
            let dataset = Dataset::new(&file_path, schema, arrays)?;
            Ok::<_, anyhow::Error>(dataset)
        });

        Ok(futures::stream::iter(datasets).boxed())
    }
}
