//! From objects to datasets, as the listing streams.
//!
//! A format judges a listing in one of two ways. See
//! [`Discovery`]. A per-object format answers as each object passes. A deferred
//! format names the objects it needs, the stream holds those and only those,
//! and the format judges them together when the walk ends. So the stream holds
//! a store's markers, never the store.

use std::sync::{Arc, Mutex};

use beacon_datafusion_ext::format_ext::{DatasetMetadata, Discovery, FileFormatFactoryExt};
use beacon_datafusion_ext::listing_factory::enrich_with_object_metadata;
use datafusion::error::Result;
use futures::stream::{BoxStream, StreamExt};
use object_store::ObjectMeta;

/// A deferred format and the objects held for it so far.
struct Held {
    format: Arc<dyn FileFormatFactoryExt>,
    candidate: fn(&ObjectMeta) -> bool,
    objects: Vec<ObjectMeta>,
}

/// Turn a walk into dataset rows, each format judging its own way.
///
/// Every format is asked about every object, as [`ListingFactory::list_datasets`]
/// does over a finished listing, so two formats that claim one object produce
/// two rows there and here alike. An error in the walk is an error row, and the
/// walk is not resumed after it.
///
/// [`ListingFactory::list_datasets`]: beacon_datafusion_ext::listing_factory::ListingFactory::list_datasets
pub fn classify(
    formats: Vec<Arc<dyn FileFormatFactoryExt>>,
    objects: BoxStream<'static, Result<ObjectMeta>>,
) -> BoxStream<'static, Result<DatasetMetadata>> {
    let mut per_object = Vec::new();
    let mut held = Vec::new();
    for format in formats {
        match format.discovery() {
            Discovery::PerObject => per_object.push(format),
            Discovery::Deferred { candidate } => held.push(Held {
                format,
                candidate,
                objects: Vec::new(),
            }),
        }
    }
    // Shared between the walk and the tail. The lock is never held across an
    // await, and the tail runs only after the walk has ended.
    let held = Arc::new(Mutex::new(held));
    let held_for_tail = Arc::clone(&held);

    let walk = objects
        .map(move |object| -> Vec<Result<DatasetMetadata>> {
            let object = match object {
                Ok(object) => object,
                Err(e) => return vec![Err(e)],
            };
            for h in held.lock().expect("no poisoned listing").iter_mut() {
                if (h.candidate)(&object) {
                    h.objects.push(object.clone());
                }
            }
            per_object
                .iter()
                .filter_map(|format| format.classify_object(&object))
                .map(Ok)
                .collect()
        })
        .flat_map(futures::stream::iter);

    let tail = futures::stream::once(async move {
        let held = std::mem::take(&mut *held_for_tail.lock().expect("no poisoned listing"));
        let mut rows = Vec::new();
        for h in held {
            match h.format.discover_datasets(&h.objects) {
                Ok(mut datasets) => {
                    enrich_with_object_metadata(&mut datasets, &h.objects);
                    rows.extend(datasets.into_iter().map(Ok));
                }
                Err(e) => rows.push(Err(e)),
            }
        }
        futures::stream::iter(rows)
    })
    .flatten();

    walk.chain(tail).boxed()
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use beacon_datafusion_ext::format_ext::{DatasetMetadata, Discovery, FileFormatFactoryExt};
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

    /// Claims every `.foo` object, one at a time.
    #[derive(Debug)]
    struct FooFactory;

    impl GetExt for FooFactory {
        fn get_ext(&self) -> String {
            "foo".to_string()
        }
    }

    impl FileFormatFactory for FooFactory {
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

    impl FileFormatFactoryExt for FooFactory {
        fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
            Ok(objects
                .iter()
                .filter(|o| o.location.extension() == Some("foo"))
                .map(|o| DatasetMetadata::new(o.location.to_string(), "foo".to_string()))
                .collect())
        }
        fn file_format_name(&self) -> String {
            "foo".to_string()
        }
    }

    fn is_marker(object: &ObjectMeta) -> bool {
        object.location.filename() == Some("marker")
    }

    /// The paths each `discover_datasets` call was handed.
    type Seen = Arc<Mutex<Vec<Vec<String>>>>;

    /// Claims the outermost `marker` of each tree, which needs every marker at
    /// once. Records what each call was handed.
    #[derive(Debug)]
    struct MarkerFactory {
        seen: Seen,
    }

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
        fn discovery(&self) -> Discovery {
            Discovery::Deferred {
                candidate: is_marker,
            }
        }
        fn discover_datasets(&self, objects: &[ObjectMeta]) -> Result<Vec<DatasetMetadata>> {
            self.seen
                .lock()
                .unwrap()
                .push(objects.iter().map(|o| o.location.to_string()).collect());
            let dirs: Vec<String> = objects
                .iter()
                .filter(|o| is_marker(o))
                .map(|o| o.location.to_string().trim_end_matches("/marker").to_string())
                .collect();
            Ok(objects
                .iter()
                .filter(|o| is_marker(o))
                .filter(|o| {
                    let dir = o.location.to_string().trim_end_matches("/marker").to_string();
                    !dirs.iter().any(|d| d != &dir && dir.starts_with(&format!("{d}/")))
                })
                .map(|o| DatasetMetadata::new(o.location.to_string(), "marked".to_string()))
                .collect())
        }
        fn file_format_name(&self) -> String {
            "marked".to_string()
        }
    }

    fn marker_factory() -> (Arc<MarkerFactory>, Seen) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(MarkerFactory {
                seen: Arc::clone(&seen),
            }),
            seen,
        )
    }

    /// A per-object format answers as each object passes, with that object's
    /// size and timestamp on the row.
    #[tokio::test]
    async fn per_object_rows_leave_with_their_metadata() {
        let got = rows(
            vec![Arc::new(FooFactory)],
            objects(&[("a.foo", 3), ("b.txt", 1), ("c/d.foo", 5)]),
        )
        .await;
        assert_eq!(paths(&got), vec!["a.foo", "c/d.foo"]);
        assert_eq!(got[0].size, Some(3));
        assert_eq!(got[1].size, Some(5));
        assert!(got[0].last_modified.is_some());
    }

    /// A deferred format sees its candidates together, once, and nothing else.
    #[tokio::test]
    async fn a_deferred_format_judges_its_candidates_together() {
        let (factory, seen) = marker_factory();
        let got = rows(
            vec![factory],
            objects(&[
                ("x/marker", 1),
                ("x/inner/marker", 1),
                ("x/data.bin", 9),
                ("y/marker", 1),
            ]),
        )
        .await;
        assert_eq!(paths(&got), vec!["x/marker", "y/marker"], "the inner marker is not a dataset");

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "one call, at the end");
        assert_eq!(
            seen[0],
            vec!["x/marker", "x/inner/marker", "y/marker"],
            "only the candidates are held"
        );
    }

    /// A deferred row still names its object, so it carries that object's size
    /// and timestamp like a per-object row does.
    #[tokio::test]
    async fn a_deferred_row_carries_its_object_metadata() {
        let (factory, _) = marker_factory();
        let got = rows(vec![factory], objects(&[("x/marker", 7)])).await;
        assert_eq!(got[0].size, Some(7));
        assert!(got[0].last_modified.is_some());
    }

    /// Deferred rows follow the walk. A per-object row that arrives after the
    /// marker still leaves first.
    #[tokio::test]
    async fn deferred_rows_come_after_the_walk() {
        let (factory, _) = marker_factory();
        let got = rows(
            vec![factory, Arc::new(FooFactory)],
            objects(&[("x/marker", 1), ("a.foo", 1)]),
        )
        .await;
        assert_eq!(paths(&got), vec!["a.foo", "x/marker"]);
    }

    /// A failure in the walk is a failure of the listing. It must not end the
    /// rows quietly.
    #[tokio::test]
    async fn a_listing_error_reaches_the_rows() {
        let walk = futures::stream::iter(vec![
            Ok(object("a.foo", 1)),
            Err(DataFusionError::Execution("the store went away".to_string())),
        ])
        .boxed();
        let err = classify(vec![Arc::new(FooFactory)], walk)
            .try_collect::<Vec<_>>()
            .await
            .expect_err("the error propagates");
        assert!(err.to_string().contains("the store went away"), "{err}");
    }
}
