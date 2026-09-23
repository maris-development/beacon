//! From objects to datasets, as the listing streams.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beacon_datafusion_ext::format_ext::{DatasetMetadata, FileFormatFactoryExt};
use datafusion::error::Result;
use futures::stream::{BoxStream, StreamExt};
use object_store::ObjectMeta;

/// Objects held per format, in the order of the formats.
type Held = Arc<Mutex<Vec<Vec<ObjectMeta>>>>;

/// Turn a walk into dataset rows.
///
/// Every format is asked about every object, so two formats that claim one
/// object produce two rows. An object a format holds is judged with the others
/// it holds after the walk. Each row carries its object's size and timestamp.
/// An error in the walk or in a format is an error row.
pub fn classify(
    formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    objects: BoxStream<'static, Result<ObjectMeta>>,
) -> BoxStream<'static, Result<DatasetMetadata>> {
    let held: Held = Arc::new(Mutex::new(vec![Vec::new(); formats.len()]));
    let formats = Arc::new(formats);

    let walk = {
        let (formats, held) = (Arc::clone(&formats), Arc::clone(&held));
        objects.flat_map(move |object| futures::stream::iter(walk_one(&formats, &held, object)))
    };
    // Runs once the walk has ended, so it sees every held object.
    let tail = futures::stream::once(async move {
        let held = std::mem::take(&mut *held.lock().expect("no poisoned listing"));
        let rows: Vec<_> = formats
            .iter()
            .zip(held)
            .filter(|(_, objects)| !objects.is_empty())
            .flat_map(|(format, objects)| judge(format.as_ref(), &objects))
            .collect();
        futures::stream::iter(rows)
    })
    .flatten();

    walk.chain(tail).boxed()
}

/// Hold `object` for the formats that keep it, and judge it for the rest.
fn walk_one(
    formats: &[Arc<dyn FileFormatFactoryExt>],
    held: &Held,
    object: Result<ObjectMeta>,
) -> Vec<Result<DatasetMetadata>> {
    let object = match object {
        Ok(object) => object,
        Err(e) => return vec![Err(e)],
    };
    let mut rows = Vec::new();
    for (i, format) in formats.iter().enumerate() {
        if format.holds(&object) {
            held.lock().expect("no poisoned listing")[i].push(object.clone());
        } else {
            rows.extend(judge(format.as_ref(), std::slice::from_ref(&object)));
        }
    }
    rows
}

/// The datasets `format` finds in `objects`, each with its object's metadata.
fn judge(format: &dyn FileFormatFactoryExt, objects: &[ObjectMeta]) -> Vec<Result<DatasetMetadata>> {
    let by_path: HashMap<&str, &ObjectMeta> =
        objects.iter().map(|o| (o.location.as_ref(), o)).collect();
    match format.discover_datasets(objects) {
        Ok(found) => found
            .into_iter()
            .map(|mut dataset| {
                if let Some(object) = by_path.get(dataset.file_path.as_str()) {
                    dataset.size = Some(object.size);
                    dataset.last_modified = Some(object.last_modified);
                }
                Ok(dataset)
            })
            .collect(),
        Err(e) => vec![Err(e)],
    }
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

    /// Holds every `marker` and keeps the outermost of each tree.
    #[derive(Debug)]
    struct MarkerFactory;

    impl GetExt for MarkerFactory {
        fn get_ext(&self) -> String {
            "marked".to_string()
        }
    }

    impl FileFormatFactory for MarkerFactory {
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

    impl FileFormatFactoryExt for MarkerFactory {
        fn holds(&self, object: &ObjectMeta) -> bool {
            object.location.filename() == Some("marker")
        }
        fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
            let markers: Vec<&ObjectMeta> = objects.iter().filter(|o| self.holds(o)).collect();
            let dir = |o: &ObjectMeta| o.location.to_string().trim_end_matches("marker").to_string();
            let dirs: Vec<String> = markers.iter().map(|o| dir(o)).collect();
            Ok(markers
                .into_iter()
                .filter(|o| !dirs.iter().any(|d| d != &dir(o) && dir(o).starts_with(d.as_str())))
                .map(|o| DatasetMetadata::new(o.location.to_string(), "marked".to_string()))
                .collect())
        }
        fn file_format_name(&self) -> String {
            "marked".to_string()
        }
    }

    #[tokio::test]
    async fn held_objects_are_judged_together_after_the_walk() {
        let got = rows(
            vec![Arc::new(MarkerFactory), Arc::new(ExtFactory("foo"))],
            objects(&[
                ("x/marker", 7),
                ("x/inner/marker", 1),
                ("x/data.bin", 9),
                ("a.foo", 1),
            ]),
        )
        .await;
        assert_eq!(paths(&got), vec!["a.foo", "x/marker"]);
        assert_eq!(got[1].size, Some(7));
        assert!(got[1].last_modified.is_some());
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
