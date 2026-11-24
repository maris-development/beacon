//! Zarr v3 stores on the Beacon ND framework (`beacon-nd-dataset`).

use std::collections::HashSet;
use std::sync::Arc;

use beacon_nd_dataset::{
    Attributes, DatasetSchema, Dimension, Dimensions, Error, NdDataset, NdVariable, Result,
};
use indexmap::IndexMap;
use zarrs::group::Group;
use zarrs_storage::AsyncReadableListableStorageTraits;

use attributes::attributes_of;
use variable::ZarrVariable;

mod attributes;
// The statistics of a later change use the time bounds of this module.
#[allow(dead_code)]
mod decode;
#[cfg(test)]
mod testing;
mod variable;

/// A Zarr array over an async store that can read and list.
pub type ZarrArray = zarrs::array::Array<dyn AsyncReadableListableStorageTraits>;

/// One Zarr leaf group as an [`NdDataset`].
///
/// Each child array with a supported type is a variable.
#[derive(Debug)]
pub struct ZarrDataset {
    schema: DatasetSchema,
}

impl NdDataset for ZarrDataset {
    fn name(&self) -> &str {
        self.schema.name()
    }

    fn dimensions(&self) -> &Dimensions {
        self.schema.dimensions()
    }

    fn attributes(&self) -> &Attributes {
        self.schema.attributes()
    }

    fn variable_names(&self) -> Vec<Arc<str>> {
        self.schema.variable_names()
    }

    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        self.schema.variable(name)
    }

    fn invented_dimensions(&self) -> &[Arc<str>] {
        self.schema.invented_dimensions()
    }
}

/// Open one leaf group. Reads only metadata.
///
/// The variables are the child arrays, sorted by name. An axis without a name
/// gets the first free name of `dim_{i}`, `{array}_dim_{i}`, `{array}_dim_{i}_2`, ..
/// A free name is no name of `dimension_names` and has no other size so far.
/// An array of an unsupported type is not a variable.
///
/// `Error::Format` when the group does not open. `Error::Invalid` when a
/// dimension has two sizes.
pub async fn open_group(
    storage: Arc<dyn AsyncReadableListableStorageTraits>,
    group_path: &str,
) -> Result<ZarrDataset> {
    let group = Group::async_open(storage, group_path)
        .await
        .map_err(|e| Error::format(format!("failed to open the Zarr group '{group_path}': {e}")))?;
    let arrays = group.async_child_arrays().await.map_err(|e| {
        Error::format(format!(
            "failed to list the arrays of the Zarr group '{group_path}': {e}"
        ))
    })?;
    let mut arrays: Vec<(String, ZarrArray)> = arrays
        .into_iter()
        .map(|array| (array_name(group.path().as_str(), &array), array))
        .collect();
    arrays.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut used = UsedDimensions::new(arrays.iter().map(|(_, array)| array));
    let mut variables: Vec<Arc<dyn NdVariable>> = Vec::with_capacity(arrays.len());
    for (name, array) in arrays {
        let axes = used.axes(&name, &array);
        let variable = Dimensions::try_new(
            axes.iter()
                .map(|axis| Dimension::new(axis.name.as_str(), axis.size)),
        )
        .map_err(|e| e.to_string())
        .and_then(|dimensions| ZarrVariable::try_new(Arc::new(array), &name, dimensions));
        match variable {
            Ok(variable) => {
                used.add(&name, &axes).map_err(|reason| Error::Invalid {
                    dataset: group_path.to_string(),
                    reason,
                })?;
                variables.push(Arc::new(variable));
            }
            Err(reason) => tracing::warn!("Skipping zarr array '{name}': {reason}"),
        }
    }
    let schema = DatasetSchema::try_new(group_path, used.dimensions()?, variables)?
        .with_attributes(attributes_of(group.attributes()))
        .with_invented_dimensions(used.invented);
    Ok(ZarrDataset { schema })
}

/// The name of a child array: its node path without the group path and the leading `/`.
fn array_name(group_path: &str, array: &ZarrArray) -> String {
    let path = array.path().as_str();
    let name = path.strip_prefix(group_path).unwrap_or(path);
    name.strip_prefix('/').unwrap_or(name).to_string()
}

/// One axis of an array with its dimension name.
struct Axis {
    name: String,
    size: usize,
    /// True when the reader gave the name.
    invented: bool,
}

/// The dimensions of the variables so far, in first-use order.
struct UsedDimensions {
    /// Each name in the `dimension_names` of an array of the group.
    real: HashSet<String>,
    /// The size of each dimension and the array that used it first.
    sizes: IndexMap<String, (usize, String)>,
    /// The names that the reader gave, in first-use order.
    invented: Vec<Arc<str>>,
}

impl UsedDimensions {
    /// No dimension yet. `arrays` gives the real names: each array of the group, also an unsupported one.
    fn new<'a>(arrays: impl IntoIterator<Item = &'a ZarrArray>) -> Self {
        let real = arrays
            .into_iter()
            .filter_map(|array| array.dimension_names().as_ref())
            .flatten()
            .flatten()
            .cloned()
            .collect();
        Self {
            real,
            sizes: IndexMap::new(),
            invented: Vec::new(),
        }
    }

    /// The axes of the array `name`, with a name for each axis.
    ///
    /// A name that the reader gives is never a real name, and has no other size so far.
    fn axes(&self, name: &str, array: &ZarrArray) -> Vec<Axis> {
        let names = array.dimension_names().as_deref().unwrap_or_default();
        array
            .shape()
            .iter()
            .enumerate()
            .map(|(i, &size)| {
                let size = size as usize;
                if let Some(Some(given)) = names.get(i) {
                    return Axis {
                        name: given.clone(),
                        size,
                        invented: false,
                    };
                }
                let dimension = [format!("dim_{i}"), format!("{name}_dim_{i}")]
                    .into_iter()
                    .chain((2..).map(|k| format!("{name}_dim_{i}_{k}")))
                    .find(|candidate| self.is_free(candidate, size))
                    .expect("only a finite number of names is taken");
                Axis {
                    name: dimension,
                    size,
                    invented: true,
                }
            })
            .collect()
    }

    /// True when the reader can give `candidate` to an axis of `size`.
    fn is_free(&self, candidate: &str, size: usize) -> bool {
        !self.real.contains(candidate)
            && self
                .sizes
                .get(candidate)
                .is_none_or(|&(known, _)| known == size)
    }

    /// Add the axes of the array `name`. `Err(reason)` when an axis has another size.
    fn add(&mut self, name: &str, axes: &[Axis]) -> std::result::Result<(), String> {
        for axis in axes {
            if let Some((known, first)) = self.sizes.get(&axis.name)
                && *known != axis.size
            {
                return Err(format!(
                    "the dimension '{}' has the size {known} in '{first}' and the size {} in '{name}'",
                    axis.name, axis.size
                ));
            }
        }
        for axis in axes {
            self.sizes
                .entry(axis.name.clone())
                .or_insert_with(|| (axis.size, name.to_string()));
            if axis.invented && !self.invented.iter().any(|known| **known == axis.name) {
                self.invented.push(Arc::from(axis.name.as_str()));
            }
        }
        Ok(())
    }

    /// The dimensions in first-use order.
    fn dimensions(&self) -> Result<Dimensions> {
        Ok(Dimensions::try_new(self.sizes.iter().map(
            |(name, &(size, _))| Dimension::new(name.as_str(), size),
        ))?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, TimeUnit};
    use beacon_nd_dataset::{Error, NdDataset, attribute_columns, validate};
    use object_store::local::LocalFileSystem;
    use serde_json::json;
    use zarrs_object_store::AsyncObjectStore;

    use super::testing::{TestStore, write_array, write_group, write_unsupported_array};
    use super::*;

    /// An empty group "/g" in a new store.
    async fn group() -> TestStore {
        let store = TestStore::new();
        write_group(&store, "/g", json!({})).await;
        store
    }

    /// Write the `f64` array `/g/{name}` with one chunk.
    async fn array(store: &TestStore, name: &str, shape: &[u64], dims: &[Option<&str>]) {
        let values = vec![0.0f64; shape.iter().product::<u64>() as usize];
        let path = format!("/g/{name}");
        write_array(store, &path, shape, shape, dims, json!({}), values).await;
    }

    async fn open(store: &TestStore) -> beacon_nd_dataset::Result<ZarrDataset> {
        open_group(store.storage().inner(), "/g").await
    }

    fn names(names: Vec<Arc<str>>) -> Vec<String> {
        names.iter().map(|name| name.to_string()).collect()
    }

    /// The dimension names of the variable `name`.
    fn axes(dataset: &ZarrDataset, name: &str) -> Vec<String> {
        let variable = dataset.variable(name).unwrap();
        variable
            .dimensions()
            .names()
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    #[tokio::test]
    async fn the_variables_are_sorted_by_name() {
        let store = group().await;
        array(&store, "b", &[2], &[Some("x")]).await;
        array(&store, "a", &[2], &[Some("x")]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(names(dataset.variable_names()), ["a", "b"]);
    }

    #[tokio::test]
    async fn the_dimensions_follow_first_use() {
        let store = group().await;
        array(&store, "a", &[3], &[Some("time")]).await;
        array(&store, "b", &[2, 3], &[Some("lat"), Some("time")]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(dataset.dimensions().names(), ["time", "lat"]);
        assert!(dataset.invented_dimensions().is_empty());
    }

    #[tokio::test]
    async fn a_named_dimension_with_two_sizes_is_invalid() {
        let store = group().await;
        array(&store, "a", &[3], &[Some("time")]).await;
        array(&store, "b", &[4], &[Some("time")]).await;
        let error = open(&store).await.unwrap_err();
        let Error::Invalid { dataset, reason } = error else {
            panic!("{error:?}");
        };
        assert_eq!(dataset, "/g");
        assert_eq!(
            reason,
            "the dimension 'time' has the size 3 in 'a' and the size 4 in 'b'"
        );
    }

    #[tokio::test]
    async fn axes_with_no_name_share_dim_i_when_the_size_agrees() {
        let store = group().await;
        array(&store, "a", &[3], &[None]).await;
        array(&store, "b", &[3], &[None]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(axes(&dataset, "a"), ["dim_0"]);
        assert_eq!(axes(&dataset, "b"), ["dim_0"]);
        assert_eq!(names(dataset.invented_dimensions().to_vec()), ["dim_0"]);
    }

    #[tokio::test]
    async fn an_axis_with_no_name_and_another_size_gets_the_array_name() {
        let store = group().await;
        array(&store, "a", &[3], &[None]).await;
        array(&store, "b", &[4], &[None]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(axes(&dataset, "a"), ["dim_0"]);
        assert_eq!(axes(&dataset, "b"), ["b_dim_0"]);
        assert_eq!(
            names(dataset.invented_dimensions().to_vec()),
            ["dim_0", "b_dim_0"]
        );
    }

    #[tokio::test]
    async fn an_axis_with_no_name_does_not_take_a_real_name() {
        let store = group().await;
        array(&store, "a", &[3], &[None]).await;
        array(&store, "b", &[4], &[Some("dim_0")]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(axes(&dataset, "a"), ["a_dim_0"]);
        assert_eq!(axes(&dataset, "b"), ["dim_0"]);
        assert_eq!(names(dataset.invented_dimensions().to_vec()), ["a_dim_0"]);
    }

    #[tokio::test]
    async fn an_axis_with_no_name_after_a_real_name_gets_the_array_name() {
        let store = group().await;
        array(&store, "a", &[3], &[Some("dim_0")]).await;
        array(&store, "b", &[4], &[None]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(axes(&dataset, "a"), ["dim_0"]);
        assert_eq!(axes(&dataset, "b"), ["b_dim_0"]);
        assert_eq!(names(dataset.invented_dimensions().to_vec()), ["b_dim_0"]);
    }

    #[tokio::test]
    async fn a_mixed_array_keeps_its_real_name() {
        let store = group().await;
        array(&store, "m", &[2, 3], &[None, Some("dim_0")]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(axes(&dataset, "m"), ["m_dim_0", "dim_0"]);
    }

    #[tokio::test]
    async fn an_array_with_a_repeated_axis_name_is_skipped() {
        let store = group().await;
        array(&store, "a", &[3], &[Some("y")]).await;
        array(&store, "r", &[2, 2], &[Some("x"), Some("x")]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(names(dataset.variable_names()), ["a"]);
        assert_eq!(dataset.dimensions().names(), ["y"]);
    }

    #[tokio::test]
    async fn an_unsupported_array_is_not_a_variable() {
        let store = group().await;
        array(&store, "a", &[3], &[Some("x")]).await;
        write_unsupported_array(&store, "/g/c", &[4]).await;
        let dataset = open(&store).await.unwrap();
        assert_eq!(names(dataset.variable_names()), ["a"]);
        assert_eq!(dataset.dimensions().names(), ["x"]);
        assert!(dataset.invented_dimensions().is_empty());
    }

    #[tokio::test]
    async fn a_group_with_no_array_opens() {
        let store = TestStore::new();
        write_group(&store, "/g", json!({"title": "x"})).await;
        let dataset = open(&store).await.unwrap();
        assert!(dataset.variable_names().is_empty());
        let columns = attribute_columns(Arc::new(dataset));
        let schema = beacon_nd_scan::file_schema(columns.as_ref());
        let fields: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(fields, [".title"]);
    }

    #[tokio::test]
    async fn a_missing_group_names_the_path() {
        let error = open_group(TestStore::new().storage().inner(), "/missing")
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Format(_)), "{error:?}");
        assert!(error.to_string().contains("/missing"), "{error}");
    }

    #[tokio::test]
    async fn the_example_store_opens() {
        let local = LocalFileSystem::new_with_prefix("./test_files").unwrap();
        let storage = Arc::new(AsyncObjectStore::new(local));
        let dataset = open_group(storage, "/gridded-example.zarr").await.unwrap();
        let variables = names(dataset.variable_names());
        for name in ["lat", "lon", "time", "analysed_sst"] {
            assert!(variables.iter().any(|v| v == name), "{variables:?}");
        }
        let time = dataset.variable("time").unwrap();
        assert_eq!(
            time.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        validate(&dataset).unwrap();
    }
}
