use std::{collections::HashMap, path::Path, sync::Arc};

use arrow::datatypes::SchemaRef;
use beacon_common::listing_url::parse_listing_table_url;
use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use bytes::Bytes;
use datafusion::{
    catalog::TableProvider, datasource::listing::ListingTableUrl, error::DataFusionError,
    execution::object_store::ObjectStoreUrl, prelude::SessionContext,
};
use futures::{StreamExt, stream::BoxStream};

use crate::files::{collection::FileCollection, temp_output_file::TempOutputFile};

pub struct FileManager {
    session_context: Arc<SessionContext>,
    data_directory_store_url: ObjectStoreUrl,
    file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
}

impl FileManager {
    pub fn new(
        session_context: Arc<SessionContext>,
        data_directory_store_url: ObjectStoreUrl,
        file_formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    ) -> Self {
        Self {
            session_context,
            data_directory_store_url,
            file_formats,
        }
    }

    #[inline(always)]
    pub fn try_create_listing_url(
        &self,
        path: String,
    ) -> datafusion::error::Result<ListingTableUrl> {
        parse_listing_table_url(&self.data_directory_store_url, &path)
    }

    pub fn data_object_store_url(&self) -> ObjectStoreUrl {
        self.data_directory_store_url.clone()
    }

    pub fn try_create_temp_output_file(&self, extension: &str) -> TempOutputFile {
        Self::create_temp_output_file(extension)
    }

    pub fn create_temp_output_file(extension: &str) -> TempOutputFile {
        TempOutputFile::new(extension)
    }

    pub async fn list_datasets(
        &self,
        offset: Option<usize>,
        limit: Option<usize>,
        pattern: Option<String>,
    ) -> datafusion::error::Result<Vec<DatasetMetadata>> {
        let state = self.session_context.state();
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(self.data_directory_store_url.clone())?;

        let listing_url =
            self.try_create_listing_url(pattern.unwrap_or_else(|| "*".to_string()))?;

        let mut objects = Vec::new();
        let mut entry_stream = listing_url
            .list_all_files(&state, &object_store, "")
            .await?;

        while let Some(entry) = entry_stream.next().await {
            if let Ok(entry) = entry {
                objects.push(entry);
            }
        }

        let mut datasets = vec![];

        for file_format in self.file_formats.iter() {
            let format_datasets = file_format.discover_datasets(&objects)?;
            datasets.extend(format_datasets);
        }

        // Keep current pagination semantics to avoid behavior regressions.
        let start = offset.unwrap_or(0);
        let end = limit.map(|l| start + l).unwrap_or(datasets.len());
        let datasets = datasets.into_iter().skip(start).take(end - start).collect();

        Ok(datasets)
    }

    pub async fn list_dataset_schema(
        &self,
        file_pattern: &str,
    ) -> datafusion::error::Result<SchemaRef> {
        let session_state = self.session_context.state();
        let extension = if file_pattern.ends_with("zarr.json") {
            "zarr.json".to_string()
        } else {
            match Path::new(file_pattern).extension() {
                Some(ext) => ext.to_string_lossy().to_string(),
                None => {
                    return Err(DataFusionError::Plan(format!(
                        "No file extension found for {}. No file type information available.",
                        file_pattern
                    )));
                }
            }
        };

        tracing::debug!("Interpreted file extension: {}", extension);
        let listing_url = self.try_create_listing_url(file_pattern.to_string())?;

        let file_format_factory = session_state
            .get_file_format_factory(&extension)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("No file format reader found for {}", extension))
            })?;
        let file_format = file_format_factory.create(&session_state, &HashMap::new())?;
        tracing::debug!("Using file format: {:?}", file_format);

        let file_collection =
            FileCollection::new(&session_state, file_format, vec![listing_url]).await?;

        Ok(file_collection.schema())
    }

    pub async fn upload_file<S>(
        &self,
        file_path: &str,
        mut stream: S,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        S: futures::Stream<Item = Result<Bytes, Box<dyn std::error::Error + Send + Sync>>> + Unpin,
    {
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.data_directory_store_url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let upload_path = object_store::path::Path::from(file_path);
        let mut writer = object_store
            .put_multipart(&upload_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            writer
                .put_part(bytes.into())
                .await
                .map_err(|e: object_store::Error| Box::new(e))?;
        }

        writer
            .complete()
            .await
            .map_err(|e: object_store::Error| Box::new(e))?;

        Ok(())
    }

    pub async fn download_file(
        &self,
        file_path: &str,
    ) -> Result<
        BoxStream<'static, Result<Bytes, Box<dyn std::error::Error + Send + Sync>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.data_directory_store_url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let file_path = object_store::path::Path::from(file_path);
        let get_result = object_store
            .get(&file_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let stream = get_result.into_stream();
        let file_stream = Box::pin(stream.map(|result| {
            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        }));

        Ok(file_stream)
    }

    pub async fn delete_file(
        &self,
        file_path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&self.data_directory_store_url)
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        let file_path = object_store::path::Path::from(file_path);
        object_store
            .delete(&file_path)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        Ok(())
    }
}
