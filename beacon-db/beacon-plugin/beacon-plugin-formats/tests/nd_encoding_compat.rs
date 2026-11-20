//! Plugins encode nd batches with `beacon-nd-encoding`; Beacon decodes them with its own
//! copy in `beacon_datafusion_ext::nd`. These tests keep the two copies in agreement.

use std::sync::Arc;

use arrow::array::{Float64Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};

use beacon_datafusion_ext::nd as beacon;
use beacon_nd_encoding as plugin;

fn plugin_batch() -> plugin::NdRecordBatch {
    let dims = |spec: &[(&str, usize)]| {
        plugin::Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| plugin::Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    };
    let schema = Arc::new(Schema::new(vec![
        Field::new("time", DataType::Int32, true),
        Field::new("sst", DataType::Float64, true),
    ]));
    let time =
        plugin::NdArrowArray::try_new(Arc::new(Int32Array::from(vec![7, 8])), dims(&[("time", 2)]))
            .unwrap();
    let sst = plugin::NdArrowArray::try_new(
        Arc::new(Float64Array::from(vec![0.0, 0.1, 0.2, 1.0, 1.1, 1.2])),
        dims(&[("time", 2), ("lat", 3)]),
    )
    .unwrap();
    plugin::NdRecordBatch::try_new(schema, vec![time, sst], dims(&[("time", 2), ("lat", 3)]))
        .unwrap()
}

#[test]
fn the_extension_name_is_the_same() {
    assert_eq!(
        plugin::encoding::ND_EXTENSION_NAME,
        beacon::encoding::ND_EXTENSION_NAME
    );
}

#[test]
fn the_encoded_field_is_the_same() {
    for data_type in [DataType::Int32, DataType::Float64, DataType::Utf8] {
        assert_eq!(
            plugin::encoding::nd_encoded_field("x", &data_type),
            beacon::encoding::nd_encoded_field("x", &data_type)
        );
    }
}

#[test]
fn beacon_decodes_a_plugin_batch() {
    let original = plugin_batch();
    let encoded = plugin::encoding::encode_nd_record_batch(&original).unwrap();
    let decoded = beacon::encoding::decode_nd_record_batch(&encoded).unwrap();
    assert_eq!(decoded.target().shape(), vec![2, 3]);
    for (actual, expected) in decoded.columns().iter().zip(original.columns()) {
        assert_eq!(actual.dims().shape(), expected.dims().shape());
        assert_eq!(actual.values().as_ref(), expected.values().as_ref());
    }
}

#[test]
fn a_plugin_decodes_a_beacon_batch() {
    let encoded = plugin::encoding::encode_nd_record_batch(&plugin_batch()).unwrap();
    let beacon_batch = beacon::encoding::decode_nd_record_batch(&encoded).unwrap();
    let re_encoded = beacon::encoding::encode_nd_record_batch(&beacon_batch).unwrap();
    assert_eq!(re_encoded, encoded);
    let decoded = plugin::encoding::decode_nd_record_batch(&re_encoded).unwrap();
    assert_eq!(decoded.num_rows(), 6);
}
