//! From objects to datasets, as the listing streams.

use std::sync::Arc;

use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::error::Result;
use futures::stream::{BoxStream, StreamExt};
use object_store::ObjectMeta;

/// Turn a walk into dataset rows, one object at a time.
///
/// Every format is asked about every object, so two formats that claim one
/// object produce two rows. Each row carries its object's size and timestamp.
/// An error in the walk or in a format is an error row.
pub fn classify(
    formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    objects: BoxStream<'static, Result<ObjectMeta>>,
) -> BoxStream<'static, Result<DatasetMetadata>> {
    objects
        .flat_map(move |object| futures::stream::iter(datasets_of(&formats, object)))
        .boxed()
}

/// The rows every format finds in one object.
fn datasets_of(
    formats: &[Arc<dyn FileFormatFactoryExt>],
    object: Result<ObjectMeta>,
) -> Vec<Result<DatasetMetadata>> {
    let object = match object {
        Ok(object) => object,
        Err(e) => return vec![Err(e)],
    };
    let mut rows = Vec::new();
    for format in formats {
        match format.discover_datasets(std::slice::from_ref(&object)) {
            Ok(found) => rows.extend(found.into_iter().map(|mut dataset| {
                dataset.size = Some(object.size);
                dataset.last_modified = Some(object.last_modified);
                Ok(dataset)
            })),
            Err(e) => rows.push(Err(e)),
        }
    }
    rows
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::Arc;

    use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
    use datafusion::{
        catalog::Session,
        common::GetExt,
        datasource::file_format::{FileFormat, FileFormatFactory},
        error::{DataFusionError, Result},
    };
    use futures::stream::{BoxStream, StreamExt, TryStreamExt};
    use object_store::{ObjectMeta, path::Path};

    use super::classify;

    fn object(path: &str, size: u64) -> ObjectMeta {
        ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            size,
            e_tag: None,
            version: None,
        }
    }

    fn objects(paths: &[(&str, u64)]) -> BoxStream<'static, Result<ObjectMeta>> {
        let all: Vec<Result<ObjectMeta>> =
            paths.iter().map(|(p, s)| Ok(object(p, *s))).collect();
        futures::stream::iter(all).boxed()
    }

    async fn rows(
        formats: Vec<Arc<dyn FileFormatFactoryExt>>,
        objects: BoxStream<'static, Result<ObjectMeta>>,
    ) -> Vec<DatasetMetadata> {
        classify(formats, objects)
            .try_collect()
            .await
            .expect("the listing succeeds")
    }

    fn paths(rows: &[DatasetMetadata]) -> Vec<&str> {
        rows.iter().map(|r| r.file_path.as_str()).collect()
    }

    /// Claims every object with extension `ext`.
    #[derive(Debug)]
    struct ExtFactory(&'static str);

    impl GetExt for ExtFactory {
        fn get_ext(&self) -> String {
            self.0.to_string()
        }
    }

    impl FileFormatFactory for ExtFactory {
        fn create(
            &self,
            _state: &dyn Session,
            _options: &HashMap<String, String>,
        ) -> Result<Arc<dyn FileFormat>> {
            unimplemented!("a listing never creates a format")
        }
        fn default(&self) -> Arc<dyn FileFormat> {
            unimplemented!("a listing never creates a format")
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    impl FileFormatFactoryExt for ExtFactory {
        fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
            Ok(objects
                .iter()
                .filter(|o| o.location.extension() == Some(self.0))
                .map(|o| DatasetMetadata::new(o.location.to_string(), self.0.to_string()))
                .collect())
        }
        fn file_format_name(&self) -> String {
            self.0.to_string()
        }
    }

    #[tokio::test]
    async fn rows_leave_with_their_object_metadata() {
        let got = rows(
            vec![Arc::new(ExtFactory("foo"))],
            objects(&[("a.foo", 3), ("b.txt", 1), ("c/d.foo", 5)]),
        )
        .await;
        assert_eq!(paths(&got), vec!["a.foo", "c/d.foo"]);
        assert_eq!(got[0].size, Some(3));
        assert_eq!(got[1].size, Some(5));
        assert!(got[0].last_modified.is_some());
    }

    #[tokio::test]
    async fn two_formats_that_claim_one_object_give_two_rows() {
        let got = rows(
            vec![Arc::new(ExtFactory("foo")), Arc::new(ExtFactory("foo"))],
            objects(&[("a.foo", 1)]),
        )
        .await;
        assert_eq!(paths(&got), vec!["a.foo", "a.foo"]);
    }

    #[tokio::test]
    async fn a_listing_error_reaches_the_rows() {
        let walk = futures::stream::iter(vec![
            Ok(object("a.foo", 1)),
            Err(DataFusionError::Execution("the store went away".to_string())),
        ])
        .boxed();
        let err = classify(vec![Arc::new(ExtFactory("foo"))], walk)
            .try_collect::<Vec<_>>()
            .await
            .expect_err("the error propagates");
        assert!(err.to_string().contains("the store went away"), "{err}");
    }
}
