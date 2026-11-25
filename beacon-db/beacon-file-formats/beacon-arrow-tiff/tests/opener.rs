//! One TIFF file through the ND contract: bands, coordinates and tags.

mod support;

use arrow::array::AsArray;
use arrow::datatypes::{
    DataType, Float32Type, Float64Type, Int16Type, Int32Type, UInt8Type, UInt16Type,
};
use beacon_nd_dataset::{Error, NdDataset, Region};
use support::{assert_grid, fixture_bytes, open, open_bytes, read, read_all};

/// Tiles (0,0) and (2,1) of the sparse fixtures hold data. The other tiles are sparse.
fn sparse_tiled_value(row: usize, col: usize, fill: Option<i16>) -> Option<i16> {
    match (row / 16, col / 16) {
        (0, 0) => Some(7),
        (1, 2) => Some(5),
        _ => fill,
    }
}

#[tokio::test]
async fn sparse_tiles_read_as_null() {
    for fixture in ["sparse_tiled_i16.tif", "sparse_tiled_bigtiff_i16.tif"] {
        let dataset = open(fixture).await;
        let values = read_all::<Int16Type>(dataset.as_ref(), "band.0").await;
        assert_grid(&values, 64, 64, |r, c| sparse_tiled_value(r, c, None));
    }
}

#[tokio::test]
async fn sparse_tiles_read_as_zero_without_nodata() {
    let dataset = open("sparse_tiled_no_nodata_i16.tif").await;
    let values = read_all::<Int16Type>(dataset.as_ref(), "band.0").await;
    assert_grid(&values, 64, 64, |r, c| sparse_tiled_value(r, c, Some(0)));
}

#[tokio::test]
async fn subset_across_sparse_and_full_tiles() {
    let dataset = open("sparse_tiled_i16.tif").await;
    let region = Region::new(vec![10, 12], vec![20, 30]);
    let values = read::<Int16Type>(dataset.as_ref(), "band.0", &region)
        .await
        .unwrap();
    assert_grid(&values, 30, 20, |r, c| {
        sparse_tiled_value(r + 10, c + 12, None)
    });
}

#[tokio::test]
async fn sparse_strips_read_as_null() {
    let dataset = open("sparse_stripped_i16.tif").await;
    let values = read_all::<Int16Type>(dataset.as_ref(), "band.0").await;
    assert_grid(&values, 64, 64, |r, _| (8..16).contains(&r).then_some(3));
}

#[tokio::test]
async fn planar_band_reads_when_another_band_is_sparse() {
    let dataset = open("planar_partly_sparse_u8.tif").await;
    let first = read_all::<UInt8Type>(dataset.as_ref(), "band.0").await;
    assert_grid(&first, 32, 32, |r, c| Some(((r * 100 + c) % 200) as u8));
    let second = read_all::<UInt8Type>(dataset.as_ref(), "band.1").await;
    assert_grid(&second, 32, 32, |r, c| (r < 16 && c >= 16).then_some(9));
}

#[tokio::test]
async fn big_endian_tiles_with_predictor_and_edge_tiles() {
    let dataset = open("big_endian_predictor_tiled_u16.tif").await;
    let values = read_all::<UInt16Type>(dataset.as_ref(), "band.0").await;
    assert_grid(&values, 50, 37, |r, c| Some((r * 100 + c) as u16));
}

#[tokio::test]
async fn packbits_tiles() {
    let dataset = open("packbits_tiled_u16.tif").await;
    let values = read_all::<UInt16Type>(dataset.as_ref(), "band.0").await;
    assert_grid(&values, 50, 37, |r, c| Some((r * 100 + c) as u16));
}

#[tokio::test]
async fn float_predictor_strips_with_short_last_strip() {
    let dataset = open("float_predictor_stripped_f32.tif").await;
    for index in 0..3 {
        let values = read_all::<Float32Type>(dataset.as_ref(), &format!("band.{index}")).await;
        assert_grid(&values, 20, 37, |r, c| {
            Some((r * 100 + c + index * 10000) as f32 / 4.0)
        });
    }
    // The last strip holds rows 32..37 only.
    let region = Region::new(vec![30, 5], vec![7, 10]);
    let tail = read::<Float32Type>(dataset.as_ref(), "band.2", &region)
        .await
        .unwrap();
    assert_grid(&tail, 10, 7, |r, c| {
        Some(((r + 30) * 100 + c + 5 + 20000) as f32 / 4.0)
    });
}

#[tokio::test]
async fn horizontal_predictor_strips() {
    let dataset = open("int_predictor_stripped_i32.tif").await;
    let values = read_all::<Int32Type>(dataset.as_ref(), "band.0").await;
    assert_grid(&values, 33, 21, |r, c| Some((r * 100 + c) as i32 - 1000));
}

/// Issue #525: both raster types give the pixel center, the same as CF and `read_netcdf`.
#[tokio::test]
async fn coordinates_are_pixel_centers_for_both_raster_types() {
    for fixture in ["pixel_is_area.tif", "pixel_is_point.tif"] {
        let dataset = open(fixture).await;
        let lon = read_all::<Float64Type>(dataset.as_ref(), "geo.lon").await;
        let lat = read_all::<Float64Type>(dataset.as_ref(), "geo.lat").await;
        let some = |v: &[f64]| v.iter().copied().map(Some).collect::<Vec<_>>();
        assert_eq!(lon, some(&[0.5, 1.5, 2.5, 3.5]));
        assert_eq!(lat, some(&[2.5, 1.5, 0.5]));
    }
}

/// The type and the first value of a rank-0 variable.
async fn scalar(dataset: &dyn NdDataset, name: &str) -> (DataType, String) {
    let variable = dataset
        .variable(name)
        .unwrap_or_else(|| panic!("missing {name}"));
    assert_eq!(variable.dimensions().rank(), 0, "{name} is a scalar");
    let array = variable.read(&Region::new(vec![], vec![])).await.unwrap();
    let text = arrow::util::display::array_value_to_string(array.values(), 0).unwrap();
    (variable.data_type().clone(), text)
}

#[tokio::test]
async fn the_tags_keep_their_names_and_types() {
    let dataset = open("test.tif").await;
    let ds = dataset.as_ref();
    assert_eq!(
        scalar(ds, "image.width").await,
        (DataType::UInt32, "1287".into())
    );
    assert_eq!(
        scalar(ds, "image.height").await,
        (DataType::UInt32, "380".into())
    );
    assert_eq!(
        scalar(ds, "image.samples_per_pixel").await,
        (DataType::UInt16, "1".into())
    );
    assert_eq!(
        scalar(ds, "image.bits_per_sample").await,
        (DataType::UInt16, "32".into())
    );

    let band = ds.variable("band.0").unwrap();
    assert_eq!(band.data_type(), &DataType::Float32);
    let dims: Vec<&str> = band.dimensions().iter().map(|d| d.name()).collect();
    assert_eq!(dims, vec!["y", "x"]);

    let lat = read_all::<Float64Type>(ds, "geo.lat").await;
    assert_eq!(lat.len(), 380);
    assert!(
        (lat[0].unwrap() - 30.1875).abs() < 1e-8,
        "lat[0]={:?}",
        lat[0]
    );
    let lon = read_all::<Float64Type>(ds, "geo.lon").await;
    assert_eq!(lon.len(), 1287);
    assert!(
        (lon[0].unwrap() - -17.291_666_030_883_79).abs() < 1e-8,
        "lon[0]={:?}",
        lon[0]
    );

    let names = ds.variable_names();
    let mut sorted = names.clone();
    sorted.sort();
    assert_eq!(names, sorted, "the variables are sorted by name");
}

#[tokio::test]
async fn the_tile_and_geo_tags_keep_their_types() {
    let dataset = open("sparse_tiled_i16.tif").await;
    let ds = dataset.as_ref();
    assert_eq!(
        scalar(ds, "image.tile_width").await,
        (DataType::UInt32, "16".into())
    );
    assert_eq!(
        scalar(ds, "image.tile_height").await,
        (DataType::UInt32, "16".into())
    );
    assert_eq!(
        scalar(ds, "image.tile_count_x").await,
        (DataType::UInt64, "4".into())
    );
    assert_eq!(
        scalar(ds, "image.tile_count_y").await,
        (DataType::UInt64, "4".into())
    );
    assert_eq!(
        scalar(ds, "geo.epsg").await,
        (DataType::UInt16, "28992".into())
    );
    assert_eq!(
        scalar(ds, "geo.crs").await,
        (DataType::Utf8, "EPSG:28992".into())
    );
    assert_eq!(
        scalar(ds, "geo.nodata").await,
        (DataType::Utf8, "-32767".into())
    );
}

/// Regression: an LZW-compressed, stripped float32 GeoTIFF must decode each strip.
///
/// 64x48, gradient 15.0..19.0, and the top-left 8x8 block is nodata.
#[tokio::test]
async fn lzw_stripped_geotiff_decodes() {
    let dataset = open("synthetic_lzw_stripped_f32.tif").await;
    let values = read_all::<Float32Type>(dataset.as_ref(), "band.0").await;
    assert_eq!(values.len(), 64 * 48);
    assert_eq!(
        values.iter().filter(|v| v.is_none()).count(),
        64,
        "the nodata block is null"
    );
    for value in values.iter().flatten() {
        assert!(
            (15.0 - 1e-3..=19.0 + 1e-3).contains(value),
            "{value} is outside the gradient"
        );
    }
    assert!(values[0].is_none());
    assert!(
        (values[8].unwrap() - 15.507_936).abs() < 1e-4,
        "values[8]={:?}",
        values[8]
    );
}

#[tokio::test]
async fn a_truncated_file_is_an_error() {
    let bytes = fixture_bytes("sparse_tiled_i16.tif");
    for length in [8, 64, 200, 400] {
        let result = async {
            let dataset = open_bytes(bytes[..length].to_vec()).await?;
            let band = dataset.variable("band.0").expect("band.0");
            band.read(&Region::new(vec![0, 0], vec![64, 64])).await
        };
        assert!(
            result.await.is_err(),
            "a file cut at {length} bytes must fail"
        );
    }
}

/// Set the SHORT value of `tag` in the first IFD of a little-endian classic TIFF.
fn patch_short_tag(bytes: &mut [u8], tag: u16, value: u16) {
    assert_eq!(&bytes[..4], b"II*\0", "a little-endian classic TIFF");
    let ifd = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let count = u16::from_le_bytes(bytes[ifd..ifd + 2].try_into().unwrap()) as usize;
    let entry = (0..count)
        .map(|i| ifd + 2 + i * 12)
        .find(|&at| u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap()) == tag)
        .unwrap_or_else(|| panic!("tag {tag} is not in the first IFD"));
    bytes[entry + 8..entry + 10].copy_from_slice(&value.to_le_bytes());
}

/// An unsupported layout gives an error that tells the reason.
#[tokio::test]
async fn a_layout_error_tells_the_reason() {
    const BITS_PER_SAMPLE: u16 = 258;
    let mut bytes = fixture_bytes("int_predictor_stripped_i32.tif");
    patch_short_tag(&mut bytes, BITS_PER_SAMPLE, 12);
    let error = open_bytes(bytes)
        .await
        .expect_err("12 bits per sample is not supported");
    let message = error.to_string();
    assert!(
        message.contains("Failed to read TIFF block layout")
            && message.contains("12 bits per sample"),
        "unexpected error: {message}"
    );
}

#[tokio::test]
async fn a_missing_object_is_an_error() {
    use beacon_nd_scan::NdFileOpener;
    use object_store::{ObjectMeta, path::Path};
    let store = std::sync::Arc::new(object_store::memory::InMemory::new());
    let meta = ObjectMeta {
        location: Path::from("tests/does-not-exist.tif"),
        last_modified: Default::default(),
        size: 100,
        e_tag: None,
        version: None,
    };
    let err = beacon_arrow_tiff::TiffOpener
        .open(store, &meta)
        .await
        .expect_err("a missing object is an error");
    assert!(err.to_string().contains("TIFF"), "unexpected error: {err}");
}

#[tokio::test]
async fn a_region_outside_the_band_is_invalid() {
    let dataset = open("sparse_tiled_i16.tif").await;
    let band = dataset.variable("band.0").unwrap();
    let err = band
        .read(&Region::new(vec![60, 0], vec![10, 64]))
        .await
        .expect_err("rows 60..70 are outside a 64-row band");
    assert!(
        matches!(err, Error::InvalidRegion { .. }),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn a_band_has_the_chunk_shape_of_its_tiles() {
    let dataset = open("sparse_tiled_i16.tif").await;
    let band = dataset.variable("band.0").unwrap();
    assert_eq!(band.chunk_shape(), Some(&[16usize, 16][..]));
    let values = band
        .read(&Region::new(vec![0, 0], vec![2, 2]))
        .await
        .unwrap();
    assert_eq!(values.values().as_primitive::<Int16Type>().len(), 4);
}
