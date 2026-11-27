//! Tests of one NetCDF variable on the ND contract, read through the opener.

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, FixedSizeBinaryArray, Float64Array, Int16Array,
};
use arrow::datatypes::{DataType, Float64Type};
use beacon_arrow_netcdf::nd::NetcdfOpener;
use beacon_nd_dataset::{Error, NdDataset, NdVariable, Region};
use beacon_nd_scan::NdFileOpener;
use oxcdf::AttributeValue;

mod common;
use common::{local_object, test_file, write_cf_netcdf, TestVariable, GRIDDED_FILE};

async fn open(path: &Path) -> Arc<dyn NdDataset> {
    let (store, object) = local_object(path).await;
    NetcdfOpener.open(store, &object).await.unwrap()
}

fn variable(dataset: &Arc<dyn NdDataset>, name: &str) -> Arc<dyn NdVariable> {
    dataset
        .variable(name)
        .unwrap_or_else(|| panic!("the dataset holds '{name}'"))
}

/// Write one file with `variables` and open the variable `name`.
async fn written(
    dimensions: &[(&str, Option<usize>)],
    variables: Vec<TestVariable>,
    name: &str,
) -> Arc<dyn NdVariable> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cf.nc");
    write_cf_netcdf(&path, dimensions, variables);
    variable(&open(&path).await, name)
}

fn strings(array: &ArrayRef) -> Vec<Option<&str>> {
    array.as_string::<i32>().iter().collect()
}

#[tokio::test]
async fn a_region_reads_the_decoded_values() {
    let temp = written(
        &[("x", Some(4))],
        vec![TestVariable {
            name: "temp",
            dimensions: vec!["x"],
            values: Arc::new(Int16Array::from(vec![10, 20, 30, 40])),
            attributes: vec![("scale_factor", AttributeValue::Double(0.5))],
        }],
        "temp",
    )
    .await;
    assert_eq!(temp.data_type(), &DataType::Float64);

    let read = temp.read(&Region::new(vec![1], vec![2])).await.unwrap();
    assert_eq!(read.dims().shape(), vec![2]);
    let values = read.values().as_primitive::<Float64Type>();
    assert_eq!(values, &Float64Array::from(vec![10.0, 15.0]));
}

#[tokio::test]
async fn a_rank_zero_variable_reads_one_value() {
    let depth = written(
        &[],
        vec![TestVariable {
            name: "depth",
            dimensions: vec![],
            values: Arc::new(Float64Array::from(vec![7.5])),
            attributes: vec![],
        }],
        "depth",
    )
    .await;
    assert_eq!(depth.dimensions().rank(), 0);

    let read = depth.read(&Region::full(depth.dimensions())).await.unwrap();
    assert_eq!(read.dims().rank(), 0);
    let values = read.values().as_primitive::<Float64Type>();
    assert_eq!(values, &Float64Array::from(vec![7.5]));
}

#[tokio::test]
async fn a_fixed_char_array_reads_one_string_per_row() {
    let rows = FixedSizeBinaryArray::try_from_iter([b"ab\0\0", b"cd  "].into_iter()).unwrap();
    let name = written(
        &[("n", Some(2)), ("strlen", Some(4))],
        vec![TestVariable {
            name: "name",
            dimensions: vec!["n", "strlen"],
            values: Arc::new(rows),
            attributes: vec![],
        }],
        "name",
    )
    .await;
    assert_eq!(name.data_type(), &DataType::Utf8);
    assert_eq!(name.dimensions().names(), vec!["n"]);
    assert_eq!(name.chunk_shape().map(<[usize]>::len), Some(1));

    let read = name.read(&Region::full(name.dimensions())).await.unwrap();
    assert_eq!(strings(read.values()), [Some("ab"), Some("cd")]);
}

#[tokio::test]
async fn a_char_array_without_a_length_axis_reads_bytes() {
    let flag = written(
        &[("n", Some(3))],
        vec![TestVariable {
            name: "flag",
            dimensions: vec!["n"],
            values: Arc::new(BinaryArray::from_vec(vec![b"x", b"y", b"z"])),
            attributes: vec![],
        }],
        "flag",
    )
    .await;
    assert_eq!(flag.dimensions().names(), vec!["n"]);

    let read = flag.read(&Region::new(vec![1], vec![2])).await.unwrap();
    assert_eq!(strings(read.values()), [Some("y"), Some("z")]);
}

#[tokio::test]
async fn a_string_fill_becomes_null() {
    // The oxcdf writer refuses a fill on a netCDF string variable, so a char variable carries it.
    let code = written(
        &[("n", Some(3))],
        vec![TestVariable {
            name: "code",
            dimensions: vec!["n"],
            values: Arc::new(BinaryArray::from_vec(vec![b"a", b"-", b"b"])),
            attributes: vec![("_FillValue", AttributeValue::Str("-".to_string()))],
        }],
        "code",
    )
    .await;

    let read = code.read(&Region::full(code.dimensions())).await.unwrap();
    assert_eq!(strings(read.values()), [Some("a"), None, Some("b")]);
}

#[tokio::test]
async fn a_short_variable_pads_with_nulls() {
    let b = written(
        &[("t", None)],
        vec![
            TestVariable {
                name: "a",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                attributes: vec![],
            },
            TestVariable {
                name: "b",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from(vec![4.0, 5.0])),
                attributes: vec![],
            },
        ],
        "b",
    )
    .await;
    assert_eq!(b.dimensions().shape(), vec![3]);

    let read = b.read(&Region::new(vec![0], vec![3])).await.unwrap();
    let values = read.values().as_primitive::<Float64Type>();
    assert_eq!(
        values,
        &Float64Array::from(vec![Some(4.0), Some(5.0), None])
    );

    let past = b.read(&Region::new(vec![2], vec![1])).await.unwrap();
    assert_eq!(past.values().null_count(), 1);
}

#[tokio::test]
async fn a_short_two_dimensional_variable_pads_in_c_order() {
    let v = written(
        &[("t", None), ("x", Some(2))],
        vec![
            TestVariable {
                name: "a",
                dimensions: vec!["t"],
                values: Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
                attributes: vec![],
            },
            TestVariable {
                name: "v",
                dimensions: vec!["t", "x"],
                values: Arc::new(Float64Array::from(vec![10.0, 11.0, 20.0, 21.0])),
                attributes: vec![],
            },
        ],
        "v",
    )
    .await;
    assert_eq!(v.dimensions().shape(), vec![3, 2]);

    let read = v.read(&Region::full(v.dimensions())).await.unwrap();
    assert_eq!(read.dims().shape(), vec![3, 2]);
    let values = read.values().as_primitive::<Float64Type>();
    let expected = [Some(10.0), Some(11.0), Some(20.0), Some(21.0), None, None];
    assert_eq!(values, &Float64Array::from(expected.to_vec()));

    let corner = v.read(&Region::new(vec![1, 1], vec![2, 1])).await.unwrap();
    let values = corner.values().as_primitive::<Float64Type>();
    assert_eq!(values, &Float64Array::from(vec![Some(21.0), None]));
}

#[tokio::test]
async fn a_region_outside_the_variable_is_invalid() {
    let dataset = open(&test_file(GRIDDED_FILE)).await;
    let temp = variable(&dataset, "analysed_sst");
    let mut shape = temp.dimensions().shape();
    shape[0] += 1;
    let error = temp
        .read(&Region::new(vec![0; shape.len()], shape))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidRegion { .. }), "{error:?}");
}

#[tokio::test]
async fn the_read_type_is_the_declared_type() {
    let dataset = open(&test_file(GRIDDED_FILE)).await;
    let mut count = 0;
    // A name with a dot is an attribute column, not a file variable.
    for name in dataset.variable_names() {
        if name.contains('.') {
            continue;
        }
        let variable = variable(&dataset, &name);
        let read = variable
            .read(&Region::full(variable.dimensions()))
            .await
            .unwrap();
        assert_eq!(
            read.data_type(),
            variable.data_type(),
            "{}",
            variable.name()
        );
        assert_eq!(read.dims().names(), variable.dimensions().names());
        count += 1;
    }
    assert!(count > 1);
}

/// Build the `NetcdfVariable` of `path` under the column name `column`.
async fn built(
    path: &Path,
    variable: &str,
    column: &str,
) -> beacon_arrow_netcdf::nd::NetcdfVariable {
    use beacon_arrow_netcdf::dimensions::PhonyDimensions;
    use beacon_arrow_netcdf::nd::{stored_length, NetcdfVariable};

    let (store, object) = local_object(path).await;
    let file = Arc::new(
        beacon_arrow_netcdf::oxcdf_reader::open_file(store, &object)
            .await
            .unwrap(),
    );
    let phony = PhonyDimensions::of_file(&file);
    let bound = file.variable(variable).unwrap();
    let sizes: indexmap::IndexMap<String, usize> = phony
        .apply(&bound.dimensions)
        .into_iter()
        .zip(stored_length(&bound))
        .collect();
    NetcdfVariable::try_new(
        Arc::from(column),
        Arc::from("test"),
        file.clone(),
        &bound,
        &phony,
        &sizes,
    )
    .unwrap()
}

fn write_raw_temp(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("raw.nc");
    write_cf_netcdf(
        &path,
        &[("x", Some(4))],
        vec![TestVariable {
            name: "temp",
            dimensions: vec!["x"],
            values: Arc::new(Int16Array::from(vec![10, 20, 30, 40])),
            attributes: vec![],
        }],
    );
    path
}

#[tokio::test]
async fn a_variable_takes_the_column_name_it_is_given() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_raw_temp(dir.path());
    let temp = built(&path, "temp", "grp/temp").await;
    assert_eq!(temp.name(), "grp/temp");

    let read = temp.read(&Region::full(temp.dimensions())).await.unwrap();
    let values = read.values().as_primitive::<arrow::datatypes::Int16Type>();
    assert_eq!(values, &Int16Array::from(vec![10, 20, 30, 40]));
}

#[tokio::test]
async fn a_scale_factor_decodes_the_values() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_raw_temp(dir.path());
    let temp = built(&path, "temp", "temp").await.with_scale_factor(0.5);
    assert_eq!(temp.data_type(), &DataType::Float64);

    let read = temp.read(&Region::full(temp.dimensions())).await.unwrap();
    let values = read.values().as_primitive::<Float64Type>();
    assert_eq!(values, &Float64Array::from(vec![5.0, 10.0, 15.0, 20.0]));
}
