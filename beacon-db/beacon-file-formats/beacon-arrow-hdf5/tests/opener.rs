//! Tests of the HDF5 opener on the ND contract.

#[path = "support/mod.rs"]
mod support;

use arrow::datatypes::{Float32Type, Int32Type, Int64Type, Int8Type};
use beacon_arrow_hdf5::ReadOptions;
use support::{dimension_names, open, open_with, store, try_open_with, values};

const NESTED_FILE: &str = "nested-groups.h5";
const COMPOUND_FILE: &str = "compound.h5";
const LIST_ATTRIBUTE_FILE: &str = "list-attribute.h5";

fn names(dataset: &dyn beacon_nd_dataset::NdDataset) -> Vec<String> {
    dataset
        .variable_names()
        .iter()
        .map(|name| name.to_string())
        .collect()
}

#[tokio::test]
async fn a_nested_dataset_is_named_by_its_path() {
    let dataset = open(NESTED_FILE).await;
    let names = names(dataset.as_ref());
    for name in [
        "station_id",
        "observations/temperature",
        "observations/salinity",
        "observations/qc/flag",
    ] {
        assert!(names.iter().any(|n| n == name), "{name} in {names:?}");
    }
    assert!(dataset.variable("temperature").is_none());
    assert!(dataset.variable("flag").is_none());
}

#[tokio::test]
async fn a_nested_dataset_reads_its_values() {
    let dataset = open(NESTED_FILE).await;
    assert_eq!(
        values::<Int32Type>(dataset.as_ref(), "station_id").await,
        [Some(11), Some(22), Some(33)]
    );
    assert_eq!(
        values::<Float32Type>(dataset.as_ref(), "observations/temperature").await,
        (0..12).map(|v| Some(v as f32)).collect::<Vec<_>>()
    );
    assert_eq!(
        values::<Int8Type>(dataset.as_ref(), "observations/qc/flag").await,
        (0..12).map(|v| Some(v as i8)).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn every_group_shares_one_name_per_length() {
    let dataset = open(NESTED_FILE).await;
    assert_eq!(
        dimension_names(dataset.as_ref(), "observations/qc/flag"),
        ["phony_len_3", "phony_len_4"]
    );
    assert_eq!(
        dimension_names(dataset.as_ref(), "station_id"),
        ["phony_len_3"]
    );
    let dimensions: Vec<(String, usize)> = dataset
        .dimensions()
        .iter()
        .map(|d| (d.name().to_string(), d.size()))
        .collect();
    assert_eq!(dimensions.len(), 2, "{dimensions:?}");
    assert!(dimensions.contains(&("phony_len_3".to_string(), 3)));
    assert!(dimensions.contains(&("phony_len_4".to_string(), 4)));
}

#[tokio::test]
async fn the_names_of_the_reader_survive_when_the_unification_is_off() {
    let options = ReadOptions {
        unify_phony_dimensions: false,
        ..ReadOptions::default()
    };
    let dataset = open_with(NESTED_FILE, options).await;
    assert_eq!(
        dimension_names(dataset.as_ref(), "observations/temperature"),
        ["phony_dim_2", "phony_dim_3"]
    );
    assert_eq!(
        dimension_names(dataset.as_ref(), "observations/qc/flag"),
        ["phony_dim_0", "phony_dim_1"]
    );
    assert_eq!(
        dimension_names(dataset.as_ref(), "station_id"),
        ["phony_dim_4"]
    );
}

#[tokio::test]
async fn attribute_columns_keep_their_names() {
    let nested = open(NESTED_FILE).await;
    assert!(nested.variable(".title").is_some());
    assert!(nested.variable("title").is_none());
    assert_eq!(
        values::<Int64Type>(nested.as_ref(), ".version").await,
        [Some(2)]
    );
    assert!(nested.variable("observations/.units").is_some());

    let compound = open(COMPOUND_FILE).await;
    assert!(compound.variable("measurements.description").is_some());
}

#[tokio::test]
async fn a_list_attribute_gives_no_column() {
    let dataset = open(LIST_ATTRIBUTE_FILE).await;
    assert!(dataset.variable(".bounds").is_none());
    assert!(dataset.variable(".version").is_some());
    assert!(dataset.variable("value").is_some());
}

#[tokio::test]
async fn a_compound_dataset_gives_its_members() {
    let dataset = open(COMPOUND_FILE).await;
    assert!(dataset.variable("measurements/station").is_some());
    assert!(dataset.variable("measurements/note").is_none());
    assert!(dataset.variable("measurements").is_none());
}

#[tokio::test]
async fn the_dataset_name_is_the_object_path() {
    let dataset = open(NESTED_FILE).await;
    assert_eq!(dataset.name(), NESTED_FILE);
}

#[tokio::test]
async fn variable_names_are_sorted() {
    let dataset = open(NESTED_FILE).await;
    let names = names(dataset.as_ref());
    let data: Vec<&String> = names.iter().filter(|n| !n.contains('.')).collect();
    let mut sorted = data.clone();
    sorted.sort();
    assert_eq!(data, sorted);
}

#[tokio::test]
async fn a_missing_object_is_an_error() {
    use beacon_arrow_hdf5::Hdf5Opener;
    use beacon_nd_scan::NdFileOpener;
    let meta = object_store::ObjectMeta {
        location: object_store::path::Path::from("nope.h5"),
        last_modified: chrono::DateTime::from_timestamp(0, 0).unwrap(),
        size: 10,
        e_tag: None,
        version: None,
    };
    assert!(Hdf5Opener::new(ReadOptions::default())
        .open(store(), &meta)
        .await
        .is_err());
    let _ = try_open_with;
}
