//! Tests of the NetCDF opener.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Float64Array};
use beacon_nd_array::arrow::array::ndarray_to_arrow_array;
use beacon_nd_dataset::{validate, ChunkGrid, Error, NdDataset, Region};
use beacon_nd_scan::NdFileOpener;

use super::NetcdfOpener;
use crate::datafusion::test_util::{
    local_object, test_file, write_cf_netcdf, write_regular_netcdf, TestVariable, GRIDDED_FILE,
    WOD_FILE,
};
use crate::oxcdf_reader::{open_file, read_arrays};

async fn open(path: &Path) -> beacon_nd_dataset::Result<Arc<dyn NdDataset>> {
    let (store, object) = local_object(path).await;
    NetcdfOpener.open(store, &object).await
}

fn names(dataset: &dyn NdDataset) -> Vec<String> {
    dataset
        .variable_names()
        .iter()
        .map(|name| name.to_string())
        .collect()
}

/// The names that are not attribute columns.
fn file_variables(dataset: &dyn NdDataset) -> Vec<String> {
    names(dataset)
        .into_iter()
        .filter(|name| !name.contains('.'))
        .collect()
}

/// The keys of `read_arrays` for the file at `path`.
async fn old_keys(path: &Path) -> Vec<String> {
    let (store, object) = local_object(path).await;
    let file = Arc::new(open_file(store, &object).await.unwrap());
    read_arrays(&file).unwrap().keys().cloned().collect()
}

#[tokio::test]
async fn the_format_name_is_netcdf() {
    assert_eq!(NetcdfOpener.format_name(), "netcdf");
}

#[tokio::test]
async fn the_gridded_file_passes_validate() {
    let dataset = open(&test_file(GRIDDED_FILE)).await.unwrap();
    validate(dataset.as_ref()).unwrap();
    assert!(dataset.name().ends_with(GRIDDED_FILE), "{}", dataset.name());
}

#[tokio::test]
async fn a_ragged_file_is_unsupported() {
    let error = open(&test_file(WOD_FILE)).await.unwrap_err();
    assert!(matches!(error, Error::Unsupported { .. }), "{error:?}");
    assert!(error.to_string().contains("ragged dataset"), "{error}");
}

#[tokio::test]
async fn the_variables_are_sorted_by_name() {
    let dataset = open(&test_file(GRIDDED_FILE)).await.unwrap();
    let variables = file_variables(dataset.as_ref());
    let mut sorted = variables.clone();
    sorted.sort();
    assert_eq!(variables, sorted);
    assert!(variables.len() > 1);
}

#[tokio::test]
async fn the_attribute_columns_are_present() {
    let path = test_file(GRIDDED_FILE);
    let dataset = open(&path).await.unwrap();
    let names = names(dataset.as_ref());
    let attributes: Vec<String> = old_keys(&path)
        .await
        .into_iter()
        .filter(|key| key.contains('.'))
        .collect();
    assert!(!attributes.is_empty());
    for key in &attributes {
        assert!(names.contains(key), "{key} is not a variable");
    }
}

#[tokio::test]
async fn a_plain_file_keeps_its_types_and_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_regular_netcdf(dir.path(), "plain.nc", 5);
    let dataset = open(&path).await.unwrap();

    let (store, object) = local_object(&path).await;
    let file = Arc::new(open_file(store, &object).await.unwrap());
    let old = read_arrays(&file).unwrap();
    let mut count = 0;
    for (name, array) in old.iter().filter(|(name, _)| !name.contains('.')) {
        let expected = ndarray_to_arrow_array(array.as_ref()).await.unwrap();
        let variable = dataset.variable(name).unwrap();
        assert_eq!(variable.data_type(), expected.data_type(), "{name}");
        let read = variable
            .read(&Region::full(variable.dimensions()))
            .await
            .unwrap();
        assert_eq!(read.values().as_ref(), expected.as_ref(), "{name}");
        count += 1;
    }
    assert!(count > 0);
}

#[tokio::test]
async fn an_unlimited_dimension_takes_the_longest_variable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unlimited.nc");
    write_cf_netcdf(
        &path,
        &[("t", None)],
        vec![
            TestVariable {
                name: "a",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from(vec![1.0, 2.0])),
                attributes: vec![],
            },
            TestVariable {
                name: "b",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from(vec![4.0, 5.0, 6.0])),
                attributes: vec![],
            },
        ],
    );
    let dataset = open(&path).await.unwrap();
    validate(dataset.as_ref()).unwrap();
    assert_eq!(dataset.dimensions().shape(), vec![3]);
    for name in ["a", "b"] {
        let variable = dataset.variable(name).unwrap();
        assert_eq!(variable.dimensions().shape(), vec![3], "{name}");
    }
}

#[tokio::test]
async fn an_empty_unlimited_dimension_gives_no_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.nc");
    write_cf_netcdf(
        &path,
        &[("t", None)],
        vec![TestVariable {
            name: "a",
            dimensions: vec!["t"],
            values: Arc::new(Float64Array::from(Vec::<f64>::new())),
            attributes: vec![],
        }],
    );
    let dataset = open(&path).await.unwrap();
    assert_eq!(dataset.dimensions().shape(), vec![0]);
    assert_eq!(ChunkGrid::try_new(dataset.as_ref(), 8192).unwrap().len(), 0);
}

#[tokio::test]
async fn a_variable_that_cannot_be_modeled_is_left_out() {
    // The oxcdf writer makes no type that the variable refuses, so a repeated dimension stands in.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("repeated.nc");
    write_cf_netcdf(
        &path,
        &[("x", Some(2))],
        vec![
            TestVariable {
                name: "a",
                dimensions: vec!["x"],
                values: Arc::new(Float64Array::from(vec![1.0, 2.0])),
                attributes: vec![],
            },
            TestVariable {
                name: "m",
                dimensions: vec!["x", "x"],
                values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
                attributes: vec![],
            },
        ],
    );
    let (store, object) = local_object(&path).await;
    let file = open_file(store, &object).await.unwrap();
    assert_eq!(file.variable("m").unwrap().dimensions, ["x", "x"]);

    let dataset = open(&path).await.unwrap();
    assert_eq!(file_variables(dataset.as_ref()), ["a"]);
}
