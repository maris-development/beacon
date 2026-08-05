//! High-level [`oxcdf`] reader that produces [`AnyDataset`] values.
//!
//! The entry point is [`open_dataset`]. It opens an object through
//! [`object_store`], turns every variable and attribute into a lazy
//! [`NdArrayD`] wrapper, and returns the result as an [`AnyDataset`].
//!
//! This is the [`oxcdf`] twin of [`crate::reader`]. The two follow the same
//! conventions, so a table keeps its schema when the reader changes.
//!
//! # CF conventions
//!
//! - A variable whose last dimension name starts with `string`, `strlen` or
//!   `strnlen` decodes as UTF-8 strings instead of raw characters.
//! - A variable with a `units` attribute in the CF time form (for example
//!   `"days since 1950-01-01"`) decodes as nanosecond timestamps.
//! - `scale_factor` and `add_offset` decode a packed variable to `f64`.
//!
//! # Naming conventions
//!
//! - Each variable becomes a named array in the dataset.
//! - A variable attribute becomes an array named `"variable_name.attribute_name"`.
//! - A global attribute becomes an array named `".attribute_name"`.
//!
//! # Groups
//!
//! Only the root group is read, which is what netcdf-c's `File::variables`
//! returns. Both readers therefore report the same variables.
//!
//! # Example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(store: Arc<dyn object_store::ObjectStore>) -> anyhow::Result<()> {
//! use object_store::path::Path;
//!
//! let any =
//!     beacon_arrow_netcdf::oxcdf_reader::open_dataset(store, Path::from("data.nc")).await?;
//! for name in any.dataset().arrays.keys() {
//!     println!("{name}");
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use beacon_nd_array::{
    dataset::{AnyDataset, Dataset},
    NdArrayD,
};
use indexmap::IndexMap;
use object_store::{path::Path, ObjectStore};
use oxcdf::AsyncNetcdfFile;

use crate::oxcdf_reader::compat;

/// Open a NetCDF object from `store` and return its contents as an
/// [`AnyDataset`].
///
/// The reader fetches byte ranges. It never copies the whole object, so an
/// object in S3, GCS or Azure needs no local file.
///
/// Variable data is **not** read eagerly. The backends fetch data on demand
/// when individual arrays are accessed.
///
/// # Errors
///
/// Returns an error when the object cannot be opened, or when a variable uses a
/// type this reader does not support.
pub async fn open_dataset(store: Arc<dyn ObjectStore>, path: Path) -> anyhow::Result<AnyDataset> {
    let name = path.to_string();
    let file = Arc::new(AsyncNetcdfFile::open_store(store, path).await?);
    let arrays = read_arrays(&file)?;
    let dataset = Dataset::new(name, arrays).await;
    AnyDataset::try_from_dataset(dataset).await
}

/// Read every variable and attribute of an opened file into an ordered map of
/// lazy ND arrays.
///
/// The map is sorted by key, so iteration order is stable. This is the
/// lower-level building block behind [`open_dataset`].
///
/// # Errors
///
/// Returns an error when an attribute of the file cannot be read. A variable
/// this reader cannot model is skipped, as it is on the netcdf-c path.
pub fn read_arrays(
    file: &Arc<AsyncNetcdfFile>,
) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // ── Variables and their per-variable attributes ──────────────────────
    for info in &file.root().variables {
        let Some(variable) = file.variable(&info.path) else {
            continue;
        };

        if let Ok(array) = compat::variable_to_nd_array(file.clone(), &variable) {
            arrays.insert(info.name.clone(), array);
        }

        for attribute in variable.attributes() {
            let full_name = format!("{}.{}", info.name, attribute.name);
            if let Ok(array) = compat::attribute_to_nd_array(&full_name, &attribute.value) {
                arrays.insert(full_name, array);
            }
        }
    }

    // ── Global file attributes ──────────────────────────────────────────
    for attribute in file.attributes() {
        let key = format!(".{}", attribute.name);
        if let Ok(array) = compat::attribute_to_nd_array(&key, &attribute.value) {
            arrays.insert(key, array);
        }
    }

    // Deterministic ordering by name.
    arrays.sort_keys();

    Ok(arrays)
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::{
        datatypes::{NdArrayDataType, TimestampNanosecond},
        NdArray,
    };
    use object_store::local::LocalFileSystem;

    const WOD_FILE: &str = "wod_ctd_1964.nc";
    const GRIDDED_FILE: &str = "gridded-example.nc";

    /// A store rooted at this crate's `test_files` directory.
    fn test_store() -> Arc<dyn ObjectStore> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        Arc::new(LocalFileSystem::new_with_prefix(root).expect("test_files store"))
    }

    async fn open(file: &str) -> AnyDataset {
        open_dataset(test_store(), Path::from(file))
            .await
            .unwrap_or_else(|e| panic!("open {file}: {e}"))
    }

    /// The same file, through the netcdf-c reader, for a side-by-side check.
    async fn open_with_netcdf_c(file: &str) -> AnyDataset {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("test_files")
            .join(file);
        crate::reader::open_dataset(path).await.unwrap()
    }

    // ── Opening ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn open_dataset_reads_a_ragged_file() {
        let any = open(WOD_FILE).await;
        assert!(any.is_ragged(), "the WOD file is a ragged dataset");
        assert!(!any.dataset().arrays.is_empty());
    }

    #[tokio::test]
    async fn open_dataset_reads_a_gridded_file() {
        let any = open(GRIDDED_FILE).await;
        assert!(any.get_array("analysed_sst").is_some());
    }

    #[tokio::test]
    async fn open_dataset_returns_an_error_for_a_missing_object() {
        assert!(open_dataset(test_store(), Path::from("nope.nc"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn dataset_name_is_the_object_path() {
        let any = open(WOD_FILE).await;
        assert_eq!(any.name(), WOD_FILE);
    }

    // ── Parity with the netcdf-c reader ────────────────────────────────

    /// Both readers must surface the same arrays. A table keeps its schema
    /// when the reader flag changes only if this holds.
    #[tokio::test]
    async fn both_readers_surface_the_same_arrays() {
        for file in [WOD_FILE, GRIDDED_FILE] {
            let rust = open(file).await;
            let c = open_with_netcdf_c(file).await;

            let rust_keys: Vec<&String> = rust.dataset().arrays.keys().collect();
            let c_keys: Vec<&String> = c.dataset().arrays.keys().collect();
            assert_eq!(rust_keys, c_keys, "array names differ for {file}");

            for key in rust_keys {
                assert_eq!(
                    rust.get_array(key).unwrap().datatype(),
                    c.get_array(key).unwrap().datatype(),
                    "data type of '{key}' differs for {file}"
                );
                assert_eq!(
                    rust.get_array(key).unwrap().shape(),
                    c.get_array(key).unwrap().shape(),
                    "shape of '{key}' differs for {file}"
                );
            }
        }
    }

    /// Values, not only names. `z` is a plain `f32` observation variable.
    #[tokio::test]
    async fn both_readers_read_the_same_f32_values() {
        let rust = open(WOD_FILE).await;
        let c = open_with_netcdf_c(WOD_FILE).await;

        let rust_values = rust
            .get_array("z")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .unwrap()
            .clone_into_raw_vec()
            .await;
        let c_values = c
            .get_array("z")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f32>>()
            .unwrap()
            .clone_into_raw_vec()
            .await;
        assert_eq!(rust_values, c_values);
    }

    /// The decoded `analysed_sst` values of a dataset.
    async fn sst_values(dataset: &AnyDataset) -> Vec<f64> {
        dataset
            .get_array("analysed_sst")
            .unwrap()
            .as_any()
            .downcast_ref::<NdArray<f64>>()
            .unwrap()
            .clone_into_raw_vec()
            .await
    }

    // ── CF decoding ────────────────────────────────────────────────────

    /// `analysed_sst` is packed `int16` with `scale_factor` and `add_offset`,
    /// so it decodes to `f64`. The decoded values match the netcdf-c path, and
    /// the real (non-fill) cells sit in a physical kelvin range.
    #[tokio::test]
    async fn scale_and_offset_decode_to_f64() {
        let any = open(GRIDDED_FILE).await;
        let c = open_with_netcdf_c(GRIDDED_FILE).await;

        let array = any.get_array("analysed_sst").unwrap();
        assert_eq!(array.datatype(), NdArrayDataType::F64);

        let rust_values = sst_values(&any).await;
        assert_eq!(
            rust_values,
            sst_values(&c).await,
            "decoded values must match"
        );

        // The fill cells decode to the packed fill run through the same
        // arithmetic, so take the maximum to reach a real measurement.
        let warmest = rust_values.iter().copied().fold(f64::MIN, f64::max);
        assert!(
            (200.0..400.0).contains(&warmest),
            "decoded SST must be a kelvin value, got {warmest}"
        );
    }

    /// A `units` attribute in the CF time form decodes to a timestamp.
    #[tokio::test]
    async fn cf_time_decodes_to_a_timestamp() {
        let any = open(GRIDDED_FILE).await;
        let array = any.get_array("time").unwrap();
        assert_eq!(array.datatype(), NdArrayDataType::Timestamp);

        let typed = array
            .as_any()
            .downcast_ref::<NdArray<TimestampNanosecond>>()
            .unwrap();
        let raw = typed.clone_into_raw_vec().await;
        assert!(!raw.is_empty());
    }

    /// A char variable with a trailing length dimension decodes as strings,
    /// and the length axis stays out of the shape.
    #[tokio::test]
    async fn fixed_size_char_variable_decodes_as_strings() {
        let any = open(WOD_FILE).await;
        let c = open_with_netcdf_c(WOD_FILE).await;

        // Find a string array that the netcdf-c reader also reports as strings.
        let name = c
            .dataset()
            .arrays
            .iter()
            .find(|(_, array)| array.datatype() == NdArrayDataType::String)
            .map(|(name, _)| name.clone())
            .expect("the WOD file holds a string variable");

        let array = any.get_array(&name).unwrap();
        assert_eq!(array.datatype(), NdArrayDataType::String);
        assert_eq!(array.shape(), c.get_array(&name).unwrap().shape());
    }

    // ── Attributes ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_global_attribute_gets_a_leading_dot() {
        let any = open(WOD_FILE).await;
        assert!(any.get_array(".Conventions").is_some());
        assert!(any.get_array("Conventions").is_none());
    }

    #[tokio::test]
    async fn a_variable_attribute_gets_a_dotted_name() {
        let any = open(GRIDDED_FILE).await;
        assert!(any.get_array("analysed_sst.units").is_some());
    }

    #[tokio::test]
    async fn array_names_are_sorted() {
        let file = Arc::new(
            AsyncNetcdfFile::open_store(test_store(), Path::from(WOD_FILE))
                .await
                .unwrap(),
        );
        let keys: Vec<String> = read_arrays(&file).unwrap().keys().cloned().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
