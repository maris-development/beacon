//! High-level NetCDF reader that produces [`Dataset`] values.
//!
//! The entry point is [`open_dataset`], which opens a NetCDF file, converts
//! every variable and attribute into a lazy [`NdArrayD`] wrapper, and returns
//! the result as a [`Dataset`].
//!
//! # CF conventions
//!
//! The reader applies CF conventions automatically:
//!
//! - Variables whose last dimension name starts with `string`, `strlen`, or
//!   `strnlen` are decoded as UTF-8 strings instead of raw characters.
//! - Variables with a `units` attribute that follows the CF time pattern
//!   (e.g. `"days since 1950-01-01"`) are decoded as nanosecond timestamps.
//!
//! # Naming conventions
//!
//! - Each NetCDF variable becomes a named array in the dataset.
//! - Variable attributes are surfaced as additional arrays using the dotted
//!   name convention `"variable_name.attribute_name"`.
//! - Global file attributes use their bare name.
//!
//! # Example
//!
//! ```no_run
//! # async fn example() -> anyhow::Result<()> {
//! let dataset = beacon_arrow_netcdf::reader::open_dataset("data.nc").await?;
//! for name in dataset.get_array_names() {
//!     println!("{name}");
//! }
//! # Ok(())
//! # }
//! ```

use std::{collections::HashMap, path::Path, sync::Arc};

use beacon_nd_array::{dataset::Dataset, NdArrayD};
use indexmap::IndexMap;
use netcdf::AttributeValue;

use crate::compat;

// ─── Public API ────────────────────────────────────────────────────────────

/// Open a NetCDF file and return its contents as a [`Dataset`].
///
/// This is the primary entry point for reading NetCDF files.  It discovers
/// every variable and attribute in the file, wraps each one in a lazy
/// [`NdArrayD`] backend, and assembles them into a [`Dataset`].
///
/// Variable data is **not** read eagerly — the underlying backends fetch
/// data on demand when individual arrays are accessed.
///
/// # Errors
///
/// Returns an error if the file cannot be opened or if any variable uses an
/// unsupported NetCDF type.
pub async fn open_dataset<P: AsRef<Path>>(path: P) -> anyhow::Result<Dataset> {
    let name = path.as_ref().to_string_lossy().to_string();
    let arrays = read_arrays(&path)?;
    Ok(Dataset::new(name, arrays).await)
}

/// Read all variables and attributes from a NetCDF file into an ordered map
/// of lazy ND arrays.
///
/// The returned map is sorted by key for deterministic iteration order.
/// This is the lower-level building block used by [`open_dataset`]; prefer
/// that function unless you need the raw [`IndexMap`] before constructing a
/// [`Dataset`].
///
/// # Errors
///
/// Returns an error if the file cannot be opened or if any variable uses an
/// unsupported NetCDF type.
pub fn read_arrays<P: AsRef<Path>>(path: P) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
    let file = netcdf::open(&path)?;
    let file_ref = Arc::new(file);

    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // ── Variables and their per-variable attributes ──────────────────────
    for variable in file_ref.variables() {
        let variable_attributes = variable_attribute_values(&variable)?;

        if let Ok(variable_array) = compat::variable_to_nd_array(
            file_ref.clone(),
            &variable.name(),
            variable_attributes.clone(),
        ) {
            arrays.insert(variable.name(), variable_array);
        }

        for (attr_name, attr_value) in &variable_attributes {
            let full_name = format!("{}.{}", variable.name(), attr_name);
            if let Ok(array) = compat::attribute_to_nd_array(&full_name, attr_value.clone()) {
                arrays.insert(full_name, array);
            }
        }
    }

    // ── Global file attributes ──────────────────────────────────────────
    for (attr_name, attr_value) in global_attribute_values(&file_ref)? {
        if let Ok(array) = compat::attribute_to_nd_array(&attr_name, attr_value) {
            arrays.insert(attr_name, array);
        }
    }

    // Deterministic ordering by name.
    arrays.sort_keys();

    Ok(arrays)
}

// ─── Private helpers ───────────────────────────────────────────────────────

/// Collect every attribute of a NetCDF *file* into a map.
fn global_attribute_values(file: &netcdf::File) -> anyhow::Result<HashMap<String, AttributeValue>> {
    let mut values = HashMap::new();
    for attribute in file.attributes() {
        values.insert(attribute.name().to_string(), attribute.value()?);
    }
    Ok(values)
}

/// Collect every attribute of a NetCDF *variable* into a map.
fn variable_attribute_values(
    variable: &netcdf::Variable,
) -> anyhow::Result<HashMap<String, AttributeValue>> {
    let mut values = HashMap::new();
    for attribute in variable.attributes() {
        values.insert(attribute.name().to_string(), attribute.value()?);
    }
    Ok(values)
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use beacon_nd_array::{
        datatypes::{NdArrayDataType, TimestampNanosecond},
        NdArray,
    };
    use tempfile::Builder;

    use super::*;
    use crate::NcChar;

    // ── NetCDF file factory helpers ────────────────────────────────────

    /// Create a temp NetCDF file with a single `f64` variable and an
    /// optional `units` attribute (used to trigger CF-time decoding).
    fn make_f64_nc(var_name: &str, values: &[f64], units: Option<&str>) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", values.len()).unwrap();
        let mut var = nc.add_variable::<f64>(var_name, &["obs"]).unwrap();
        var.put_values(values, netcdf::Extents::All).unwrap();
        if let Some(u) = units {
            var.put_attribute("units", u).unwrap();
        }
        drop(nc);
        tmp
    }

    /// Create a temp NetCDF file with a single `i32` variable.
    fn make_i32_nc(var_name: &str, values: &[i32]) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", values.len()).unwrap();
        let mut var = nc.add_variable::<i32>(var_name, &["obs"]).unwrap();
        var.put_values(values, netcdf::Extents::All).unwrap();
        drop(nc);
        tmp
    }

    /// Create a temp NetCDF file with a 2-D char variable (`obs × strlen`)
    /// following the CF fixed-size string convention.
    fn make_char_nc(var_name: &str, strings: &[&str], str_len: usize) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", strings.len()).unwrap();
        let strlen_dim = format!("strlen{str_len}");
        nc.add_dimension(&strlen_dim, str_len).unwrap();
        let mut var = nc
            .add_variable::<NcChar>(var_name, &["obs", &strlen_dim])
            .unwrap();
        let mut buf: Vec<NcChar> = Vec::with_capacity(strings.len() * str_len);
        for s in strings {
            let bytes = s.as_bytes();
            for i in 0..str_len {
                buf.push(NcChar(*bytes.get(i).unwrap_or(&b'\0')));
            }
        }
        var.put_values::<NcChar, _>(&buf, netcdf::Extents::All)
            .unwrap();
        drop(nc);
        tmp
    }

    /// Create a temp NetCDF file with two numeric variables on the same
    /// dimension.
    fn make_multi_var_nc(
        f64_name: &str,
        f64_values: &[f64],
        i32_name: &str,
        i32_values: &[i32],
    ) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("obs", f64_values.len()).unwrap();
        {
            let mut var = nc.add_variable::<f64>(f64_name, &["obs"]).unwrap();
            var.put_values(f64_values, netcdf::Extents::All).unwrap();
        }
        {
            let mut var = nc.add_variable::<i32>(i32_name, &["obs"]).unwrap();
            var.put_values(i32_values, netcdf::Extents::All).unwrap();
        }
        drop(nc);
        tmp
    }

    /// Create a temp file with three variables of different ranks sharing
    /// the leading dimension `n_obs`.
    fn make_mixed_dim_nc(n_obs: usize, n_param: usize, n_calib: usize) -> tempfile::NamedTempFile {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        let mut nc = netcdf::create(tmp.path()).unwrap();
        nc.add_dimension("n_obs", n_obs).unwrap();
        nc.add_dimension("n_param", n_param).unwrap();
        nc.add_dimension("n_calib", n_calib).unwrap();
        {
            let mut v = nc.add_variable::<f64>("obs1d", &["n_obs"]).unwrap();
            let data: Vec<f64> = (0..n_obs).map(|i| i as f64).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        {
            let mut v = nc
                .add_variable::<f32>("obs2d", &["n_obs", "n_param"])
                .unwrap();
            let data: Vec<f32> = (0..n_obs * n_param).map(|i| i as f32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        {
            let mut v = nc
                .add_variable::<i32>("obs3d", &["n_obs", "n_calib", "n_param"])
                .unwrap();
            let data: Vec<i32> = (0..(n_obs * n_calib * n_param) as i32).collect();
            v.put_values(&data, netcdf::Extents::All).unwrap();
        }
        drop(nc);
        tmp
    }

    // ── read_arrays() ──────────────────────────────────────────────────

    #[test]
    fn read_arrays_opens_valid_file() {
        let tmp = make_f64_nc("lat", &[1.0, 2.0, 3.0], None);
        read_arrays(tmp.path()).expect("should open a valid NetCDF file");
    }

    #[test]
    fn read_arrays_returns_error_for_nonexistent_file() {
        assert!(read_arrays("/nonexistent/path/file.nc").is_err());
    }

    #[test]
    fn read_arrays_keys_are_sorted() {
        let tmp = make_multi_var_nc("zz_var", &[1.0], "aa_var", &[2]);
        let arrays = read_arrays(tmp.path()).unwrap();
        let keys: Vec<_> = arrays.keys().cloned().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "keys should be in sorted order");
    }

    // ── open_dataset() ─────────────────────────────────────────────────

    #[tokio::test]
    async fn open_dataset_creates_dataset_with_all_arrays() {
        let tmp = make_multi_var_nc("temp", &[1.0, 2.0], "salt", &[3, 4]);
        let dataset = open_dataset(tmp.path()).await.unwrap();
        assert!(dataset.get_array("temp").is_some());
        assert!(dataset.get_array("salt").is_some());
    }

    #[tokio::test]
    async fn open_dataset_returns_error_for_nonexistent_file() {
        assert!(open_dataset("/nonexistent.nc").await.is_err());
    }

    #[tokio::test]
    async fn open_dataset_name_matches_path() {
        let tmp = make_f64_nc("v", &[1.0], None);
        let dataset = open_dataset(tmp.path()).await.unwrap();
        assert_eq!(dataset.name, tmp.path().to_string_lossy().to_string());
    }

    // ── Data type mapping ──────────────────────────────────────────────

    #[test]
    fn f64_variable_has_f64_datatype() {
        let tmp = make_f64_nc("temperature", &[20.0, 21.0], None);
        let arrays = read_arrays(tmp.path()).unwrap();
        assert_eq!(arrays["temperature"].datatype(), NdArrayDataType::F64);
    }

    #[test]
    fn i32_variable_has_i32_datatype() {
        let tmp = make_i32_nc("cycle", &[1, 2, 3]);
        let arrays = read_arrays(tmp.path()).unwrap();
        assert_eq!(arrays["cycle"].datatype(), NdArrayDataType::I32);
    }

    #[test]
    fn char_variable_maps_to_string_datatype() {
        let tmp = make_char_nc("station", &["ABC", "DEF"], 8);
        let arrays = read_arrays(tmp.path()).unwrap();
        assert_eq!(arrays["station"].datatype(), NdArrayDataType::String);
    }

    #[test]
    fn cf_time_variable_maps_to_timestamp_datatype() {
        let tmp = make_f64_nc("time", &[0.0, 1.0], Some("days since 1950-01-01"));
        let arrays = read_arrays(tmp.path()).unwrap();
        assert_eq!(
            arrays["time"].datatype(),
            NdArrayDataType::Timestamp,
            "CF time variable should be decoded as Timestamp"
        );
    }

    // ── Attribute surfacing ────────────────────────────────────────────

    #[test]
    fn variable_attribute_surfaced_as_dotted_name() {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_dimension("obs", 2).unwrap();
            let mut var = nc.add_variable::<f64>("pres", &["obs"]).unwrap();
            var.put_values(&[1.0f64, 2.0], netcdf::Extents::All)
                .unwrap();
            var.put_attribute("units", "dbar").unwrap();
        }
        let arrays = read_arrays(tmp.path()).unwrap();
        assert!(arrays.contains_key("pres.units"));
        assert_eq!(arrays["pres.units"].datatype(), NdArrayDataType::String);
    }

    #[test]
    fn global_attribute_surfaced_by_name() {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        {
            let mut nc = netcdf::create(tmp.path()).unwrap();
            nc.add_attribute("Conventions", "CF-1.6").unwrap();
        }
        let arrays = read_arrays(tmp.path()).unwrap();
        assert!(arrays.contains_key("Conventions"));
    }

    // ── Data correctness (via NdArray downcasting) ─────────────────────

    #[test]
    fn f64_values_read_correctly() {
        let expected = [1.5f64, 2.5, 3.5];
        let tmp = make_f64_nc("depth", &expected, None);
        let arrays = read_arrays(tmp.path()).unwrap();

        let typed = arrays["depth"]
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .unwrap();
        let raw = futures::executor::block_on(typed.clone_into_raw_vec());
        for (i, &exp) in expected.iter().enumerate() {
            assert!(
                (raw[i] - exp).abs() < 1e-10,
                "mismatch at index {i}: got {}, expected {exp}",
                raw[i]
            );
        }
    }

    #[test]
    fn i32_values_read_correctly() {
        let expected = [10i32, 20, 30];
        let tmp = make_i32_nc("cycle", &expected);
        let arrays = read_arrays(tmp.path()).unwrap();

        let typed = arrays["cycle"]
            .as_any()
            .downcast_ref::<NdArray<i32>>()
            .unwrap();
        let raw = futures::executor::block_on(typed.clone_into_raw_vec());
        for (i, &exp) in expected.iter().enumerate() {
            assert_eq!(raw[i], exp, "mismatch at index {i}");
        }
    }

    #[test]
    fn cf_time_values_decoded_to_nanoseconds() {
        let tmp = make_f64_nc("time", &[0.0, 1.0], Some("days since 1970-01-01T00:00:00Z"));
        let arrays = read_arrays(tmp.path()).unwrap();

        let typed = arrays["time"]
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .unwrap();
        let raw = futures::executor::block_on(typed.clone_into_raw_vec());
        assert_eq!(raw[0].0, 0, "epoch offset 0 should map to 0 ns");
        let one_day_ns: i64 = 86_400 * 1_000_000_000;
        assert!(
            (raw[1].0 - one_day_ns).abs() < 1_000,
            "1 day should be ~86400e9 ns, got {}",
            raw[1].0
        );
    }

    #[test]
    fn char_variable_decoded_as_string() {
        let tmp = make_char_nc("station_id", &["ALPHA", "BETA"], 8);
        let arrays = read_arrays(tmp.path()).unwrap();

        let typed = arrays["station_id"]
            .as_any()
            .downcast_ref::<NdArray<String>>()
            .unwrap();
        let raw = futures::executor::block_on(typed.clone_into_raw_vec());
        assert_eq!(raw[0], "ALPHA");
        assert_eq!(raw[1], "BETA");
    }

    // ── Shape ──────────────────────────────────────────────────────────

    #[test]
    fn shape_matches_variable_length() {
        let tmp = make_f64_nc("lon", &[1.0, 2.0, 3.0, 4.0, 5.0], None);
        let arrays = read_arrays(tmp.path()).unwrap();
        assert_eq!(arrays["lon"].shape(), &[5]);
    }

    #[test]
    fn mixed_dim_column_shapes() {
        let (n_obs, n_param, n_calib) = (5, 3, 2);
        let tmp = make_mixed_dim_nc(n_obs, n_param, n_calib);
        let arrays = read_arrays(tmp.path()).unwrap();

        assert_eq!(arrays["obs1d"].shape(), &[n_obs]);
        assert_eq!(arrays["obs2d"].shape(), &[n_obs, n_param]);
        assert_eq!(arrays["obs3d"].shape(), &[n_obs, n_calib, n_param]);
    }

    // ── Dataset dimensions ─────────────────────────────────────────────

    #[tokio::test]
    async fn dataset_captures_dimensions() {
        let tmp = make_f64_nc("lat", &[1.0, 2.0, 3.0], None);
        let dataset = open_dataset(tmp.path()).await.unwrap();
        assert_eq!(dataset.dimensions.get("obs"), Some(&3));
    }

    #[tokio::test]
    async fn dataset_empty_for_dimless_file() {
        let tmp = Builder::new().suffix(".nc").tempfile().unwrap();
        netcdf::create(tmp.path()).unwrap();
        let dataset = open_dataset(tmp.path()).await.unwrap();
        assert!(dataset.dimensions.is_empty());
        assert!(dataset.arrays.is_empty());
    }

    // ── Edge cases ─────────────────────────────────────────────────────

    #[test]
    fn missing_key_not_in_arrays() {
        let tmp = make_f64_nc("temp", &[20.0], None);
        let arrays = read_arrays(tmp.path()).unwrap();
        assert!(!arrays.contains_key("nonexistent"));
    }

    #[test]
    fn mixed_dim_file_contains_all_variables() {
        let tmp = make_mixed_dim_nc(4, 3, 2);
        let arrays = read_arrays(tmp.path()).unwrap();
        assert!(arrays.contains_key("obs1d"));
        assert!(arrays.contains_key("obs2d"));
        assert!(arrays.contains_key("obs3d"));
        // Three variables, no attributes -> exactly 3 entries.
        assert_eq!(arrays.len(), 3);
    }

    // ── Remote integration (requires network, skipped by default) ──────

    #[tokio::test]
    #[ignore = "requires network access"]
    async fn remote_argo_file_opens_as_dataset() {
        let path = "https://s3.eu-west-3.amazonaws.com/argo-gdac-sandbox/pub/dac/aoml/13857/13857_prof.nc#mode=bytes";
        let dataset = open_dataset(path).await.unwrap();
        assert!(!dataset.arrays.is_empty());
        assert!(!dataset.dimensions.is_empty());
    }

    // ── Ragged dataset → record batch stream ───────────────────────────

    mod ragged_record_batch {
        use super::*;
        use arrow::array::{Array, Float32Array, Float64Array, StringArray};
        use beacon_nd_array::{
            arrow::batch::any_dataset_as_record_batch_stream,
            dataset::{AnyDataset, CfDatasetType},
        };
        use futures::TryStreamExt;

        const WOD_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/test_files/wod_ctd_1964.nc");

        #[tokio::test]
        async fn wod_file_detected_as_ragged() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            assert!(matches!(ds.cf_dataset_type(), CfDatasetType::Ragged));
        }

        #[tokio::test]
        async fn any_dataset_wraps_as_ragged() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();
            assert!(any.is_ragged());
        }

        #[tokio::test]
        async fn stream_produces_one_batch_per_cast() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            // The file contains 47 casts.
            assert_eq!(batches.len(), 47);
        }

        #[tokio::test]
        async fn all_batches_share_the_same_schema() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            let schema = batches[0].schema();
            for (i, batch) in batches.iter().enumerate() {
                assert_eq!(batch.schema(), schema, "batch {i} has a different schema");
            }
        }

        #[tokio::test]
        async fn schema_excludes_row_size_variables() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            let schema = batches[0].schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();

            assert!(!field_names.contains(&"z_row_size"));
            assert!(!field_names.contains(&"Temperature_row_size"));
            assert!(!field_names.contains(&"Salinity_row_size"));
            assert!(!field_names.contains(&"z_row_size.sample_dimension"));
            assert!(!field_names.contains(&"Temperature_row_size.sample_dimension"));
            assert!(!field_names.contains(&"Salinity_row_size.sample_dimension"));
        }

        #[tokio::test]
        async fn schema_includes_observation_variables() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            let schema = batches[0].schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();

            assert!(field_names.contains(&"z"));
            assert!(field_names.contains(&"Temperature"));
            assert!(field_names.contains(&"Salinity"));
        }

        #[tokio::test]
        async fn schema_includes_instance_variables() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            let schema = batches[0].schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();

            assert!(field_names.contains(&"lat"));
            assert!(field_names.contains(&"lon"));
            assert!(field_names.contains(&"wod_unique_cast"));
        }

        #[tokio::test]
        async fn total_obs_row_count_across_batches() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            // z_obs=418, Temperature_obs=418, Salinity_obs=416.
            // The max obs dim per cast determines row count.
            // Total rows >= max(418, 418, 416) = 418 (at minimum).
            let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert!(
                total_rows >= 418,
                "total rows ({total_rows}) should be at least 418"
            );
        }

        #[tokio::test]
        async fn instance_vars_repeated_to_obs_length() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            // For each batch, lat should have the same value in every row
            // (it's an instance variable repeated to match obs count).
            for (i, batch) in batches.iter().enumerate() {
                if batch.num_rows() == 0 {
                    continue;
                }
                let lat = batch
                    .column_by_name("lat")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap();
                let first = lat.value(0);
                for row in 1..lat.len() {
                    assert_eq!(
                        lat.value(row),
                        first,
                        "cast {i}: lat should be constant across all rows"
                    );
                }
            }
        }

        #[tokio::test]
        async fn shorter_obs_dim_null_padded() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            // Salinity has 416 total obs while z and Temperature have 418.
            // So at least one cast must have fewer Salinity obs than the max,
            // resulting in null padding. Check that the total non-null Salinity
            // values across all casts is <= 416.
            let total_salinity_valid: usize = batches
                .iter()
                .map(|b| {
                    let col = b.column_by_name("Salinity").unwrap();
                    col.len() - col.null_count()
                })
                .sum();
            assert!(
                total_salinity_valid <= 416,
                "total non-null Salinity ({total_salinity_valid}) should be <= 416"
            );
        }

        #[tokio::test]
        async fn global_attrs_present_and_repeated() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            let schema = batches[0].schema();
            let field_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert!(field_names.contains(&"Conventions"));

            // Verify the global attribute is repeated in each row.
            for batch in &batches {
                if batch.num_rows() == 0 {
                    continue;
                }
                let conv = batch
                    .column_by_name("Conventions")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap();
                for row in 0..conv.len() {
                    assert_eq!(conv.value(row), "CF-1.6");
                }
            }
        }

        #[tokio::test]
        async fn observation_data_is_not_all_null() {
            let ds = open_dataset(WOD_PATH).await.unwrap();
            let any = AnyDataset::try_from_dataset(ds).await.unwrap();

            let batches: Vec<_> = any_dataset_as_record_batch_stream(any)
                .try_collect()
                .await
                .unwrap();

            // z (depth) should have real values in most casts.
            let total_z_valid: usize = batches
                .iter()
                .map(|b| {
                    let col = b.column_by_name("z").unwrap();
                    col.len() - col.null_count()
                })
                .sum();
            assert!(
                total_z_valid > 0,
                "z should have at least some non-null values"
            );

            // Temperature should have real values.
            let total_temp_valid: usize = batches
                .iter()
                .map(|b| {
                    let col = b.column_by_name("Temperature").unwrap();
                    col.len() - col.null_count()
                })
                .sum();
            assert!(
                total_temp_valid > 0,
                "Temperature should have non-null values"
            );
        }
    }
}
