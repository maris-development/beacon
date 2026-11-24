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

// ── The OptoDAS convention ─────────────────────────────────────────────────

const OPTODAS_FILE: &str = "optodas.h5";
const OPTODAS_GAP_FILE: &str = "optodas-gap.h5";

fn optodas() -> ReadOptions {
    ReadOptions {
        convention: beacon_arrow_hdf5::Hdf5Convention::OptoDas,
        ..ReadOptions::default()
    }
}

#[tokio::test]
async fn the_payload_axes_get_the_names_of_the_file() {
    let dataset = open_with(OPTODAS_FILE, optodas()).await;
    assert_eq!(
        dimension_names(dataset.as_ref(), "data"),
        ["time", "distance"]
    );
}

#[tokio::test]
async fn the_time_coordinate_counts_in_nanoseconds() {
    use arrow::datatypes::{DataType, TimeUnit, TimestampNanosecondType};
    let dataset = open_with(OPTODAS_FILE, optodas()).await;
    let time = dataset.variable("time").expect("a time column");
    assert_eq!(
        time.data_type(),
        &DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    assert_eq!(dimension_names(dataset.as_ref(), "time"), ["time"]);
    // 2026-03-28T12:00:00Z, then 8 ms per sample, six of them.
    let start = 1_774_699_200_000_000_000i64;
    assert_eq!(
        values::<TimestampNanosecondType>(dataset.as_ref(), "time").await,
        (0..6)
            .map(|step| Some(start + step * 8_000_000))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn the_distance_coordinate_is_in_metres() {
    use arrow::datatypes::Float64Type;
    let dataset = open_with(OPTODAS_FILE, optodas()).await;
    assert_eq!(dimension_names(dataset.as_ref(), "distance"), ["distance"]);
    assert_eq!(
        values::<Float64Type>(dataset.as_ref(), "distance").await,
        [Some(0.0), Some(5.0), Some(10.0), Some(15.0)]
    );
}

#[tokio::test]
async fn the_payload_is_scaled() {
    use arrow::datatypes::{DataType, Float64Type};
    let dataset = open_with(OPTODAS_FILE, optodas()).await;
    let data = dataset.variable("data").unwrap();
    assert_eq!(data.data_type(), &DataType::Float64);
    let read = values::<Float64Type>(dataset.as_ref(), "data").await;
    assert_eq!(read[..4], [Some(0.0), Some(0.5), Some(1.0), Some(1.5)]);
}

#[tokio::test]
async fn missing_samples_give_no_time_column() {
    let dataset = open_with(OPTODAS_GAP_FILE, optodas()).await;
    assert!(dataset.variable("time").is_none());
    assert!(dataset.variable("distance").is_some());
    assert!(dataset.variable("data").is_some());
}

#[tokio::test]
async fn the_convention_is_off_by_default() {
    use arrow::datatypes::DataType;
    let dataset = open(OPTODAS_FILE).await;
    assert!(dataset.variable("time").is_none());
    assert_eq!(
        dataset.variable("data").unwrap().data_type(),
        &DataType::Int16
    );
}

#[tokio::test]
async fn a_file_of_another_layout_reads_plainly_with_the_convention() {
    let dataset = open_with("instrument.h5", optodas()).await;
    assert!(dataset.variable("time").is_none());
}

/// A compound shorter than its dimension reads its records, then nulls.
#[tokio::test]
async fn a_short_compound_pads_with_nulls() {
    use arrow::datatypes::Float64Type;
    let dataset = open("short-compound.h5").await;
    assert_eq!(dimension_names(dataset.as_ref(), "g/rec/a"), ["x"]);
    assert_eq!(
        values::<Int32Type>(dataset.as_ref(), "g/rec/a").await,
        [Some(1), Some(2), Some(3), None, None]
    );
    assert_eq!(
        values::<Float64Type>(dataset.as_ref(), "g/rec/b").await,
        [Some(1.5), Some(2.5), Some(3.5), None, None]
    );
}
