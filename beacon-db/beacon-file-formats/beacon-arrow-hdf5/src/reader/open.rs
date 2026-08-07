//! High-level HDF5 reader that produces [`AnyDataset`] values.
//!
//! The entry point is [`open_dataset`]. It opens an object through
//! [`object_store`], turns every dataset and attribute into a lazy
//! [`NdArrayD`] wrapper, and returns the result as an [`AnyDataset`].
//!
//! # Naming conventions
//!
//! - A dataset in the root group becomes an array under its own name, which is
//!   what the netCDF reader gives it.
//! - A dataset inside a group keeps its path: `observations/qc/flag`.
//! - An attribute of a dataset becomes an array named `"dataset.attribute"`.
//! - A global attribute becomes an array named `".attribute"`.
//! - An attribute of a group becomes an array named `"group/.attribute"`.
//! - A member of a compound dataset becomes an array named `"dataset/member"`.
//!
//! The first four rules are the netCDF reader's, so a NetCDF-4 file gives the
//! same array names on either backend.
//!
//! # Conventions
//!
//! Dimension names come from the HDF5 dimension scales a NetCDF-4 writer
//! attaches. A file with no scales gets `phony_dim_0`, `phony_dim_1` and so on,
//! one per axis. The CF rules — `scale_factor` / `add_offset` packing, a CF
//! `units` string, a fixed-size char string — are applied by
//! [`beacon_arrow_netcdf::oxcdf_reader::compat`], which both readers share.
//!
//! # A known limit of the attribute decoder
//!
//! A version-1 object header pads every message to 8 bytes, and the declared
//! message size includes that padding. [`oxcdf`] takes the whole message body
//! as the attribute value, so the padding becomes data: a scalar attribute
//! narrower than 8 bytes decodes as two values, and `int32[3]` as four. Beacon
//! drops such an attribute rather than surface a wrong one, so it is missing
//! from the arrays.
//!
//! netcdf-c writes version-2 object headers, so a netCDF-4 file is unaffected
//! and the netCDF reader never meets this. It bites a plain HDF5 file written
//! with the earliest library version — h5py's default — which is exactly the
//! population this reader exists for. The fix belongs in [`oxcdf`]; see
//! <https://github.com/robinskil/oxcdf/issues/1>.
//!
//! # Example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(store: Arc<dyn object_store::ObjectStore>) -> anyhow::Result<()> {
//! use object_store::path::Path;
//!
//! let any = beacon_arrow_hdf5::reader::open_dataset(store, Path::from("data.h5")).await?;
//! for name in any.dataset().arrays.keys() {
//!     println!("{name}");
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use beacon_arrow_netcdf::oxcdf_reader::compat;
use beacon_nd_array::{
    dataset::{AnyDataset, Dataset},
    NdArrayD,
};
use indexmap::IndexMap;
use object_store::{path::Path, ObjectStore};
use oxcdf::{netcdf::NcGroup, AsyncNetcdfFile, AsyncVariable};

use crate::reader::compound;

/// Open an HDF5 object from `store` and return its contents as an
/// [`AnyDataset`].
///
/// The reader fetches byte ranges. It never copies the whole object, so an
/// object in S3, GCS or Azure needs no local file.
///
/// Dataset values are **not** read eagerly. The backends fetch data on demand
/// when individual arrays are accessed.
///
/// # Errors
///
/// Returns an error when the object cannot be opened. A dataset this reader
/// cannot model is skipped rather than failing the open, which is what the
/// netcdf-c path does too.
pub async fn open_dataset(store: Arc<dyn ObjectStore>, path: Path) -> anyhow::Result<AnyDataset> {
    let name = path.to_string();
    let file = Arc::new(AsyncNetcdfFile::open_store(store, path).await?);
    let arrays = read_arrays(&file)?;
    let dataset = Dataset::new(name, arrays).await;
    AnyDataset::try_from_dataset(dataset).await
}

/// Read every dataset and attribute of an opened file into an ordered map of
/// lazy ND arrays.
///
/// Every group is walked, depth first. The map is sorted by key, so iteration
/// order is stable. This is the lower-level building block behind
/// [`open_dataset`].
///
/// # Errors
///
/// Returns an error when an attribute of the file cannot be read. A dataset
/// this reader cannot model is skipped and logged.
pub fn read_arrays(
    file: &Arc<AsyncNetcdfFile>,
) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // ── Datasets and their attributes, in every group ────────────────────
    for variable in file.variables() {
        let name = array_name(&variable);

        match compat::variable_to_nd_array(file.clone(), &variable) {
            Ok(array) => {
                arrays.insert(name.clone(), array);
            }
            // The netCDF data model does not cover the type. A compound
            // dataset is the case this crate exists for, so try the HDF5 one.
            Err(_) if compound::members_of(variable.datatype()).is_some() => {
                match compound::member_arrays(file.clone(), &variable, &name) {
                    Ok(members) => arrays.extend(members),
                    Err(error) => tracing::warn!(dataset = %name, "{error}"),
                }
            }
            Err(error) => {
                tracing::debug!(dataset = %name, "skipping a dataset this reader does not model: {error}")
            }
        }

        for attribute in variable.attributes() {
            let full_name = format!("{name}.{}", attribute.name);
            if let Ok(array) = compat::attribute_to_nd_array(&full_name, &attribute.value) {
                arrays.insert(full_name, array);
            }
        }
    }

    // ── Global attributes, then those of every nested group ──────────────
    for attribute in file.attributes() {
        let key = format!(".{}", attribute.name);
        if let Ok(array) = compat::attribute_to_nd_array(&key, &attribute.value) {
            arrays.insert(key, array);
        }
    }
    for group in file.groups() {
        read_group_attributes(group, &mut arrays);
    }

    // Deterministic ordering by name.
    arrays.sort_keys();

    Ok(arrays)
}

/// The array name of one variable.
///
/// A dataset in the root group keeps its plain name, which is what the netCDF
/// reader gives it. A dataset inside a group keeps its path, so two groups can
/// hold the same name.
fn array_name(variable: &AsyncVariable<'_>) -> String {
    let path = variable.path.trim_start_matches('/');
    if path.is_empty() {
        variable.name.clone()
    } else {
        path.to_string()
    }
}

/// Add the attributes of `group` and of every group inside it.
fn read_group_attributes(group: &NcGroup, arrays: &mut IndexMap<String, Arc<dyn NdArrayD>>) {
    let path = group.path.trim_start_matches('/');
    for attribute in &group.attributes {
        let key = format!("{path}/.{}", attribute.name);
        if let Ok(array) = compat::attribute_to_nd_array(&key, &attribute.value) {
            arrays.insert(key, array);
        }
    }
    for child in &group.groups {
        read_group_attributes(child, arrays);
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use beacon_nd_array::{datatypes::NdArrayDataType, NdArray};
    use object_store::local::LocalFileSystem;

    const NESTED_FILE: &str = "nested-groups.h5";
    const COMPOUND_FILE: &str = "compound.h5";

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

    async fn values<T: beacon_nd_array::datatypes::NdArrayType>(
        dataset: &AnyDataset,
        name: &str,
    ) -> Vec<T> {
        dataset
            .get_array(name)
            .unwrap_or_else(|| panic!("array '{name}' is missing"))
            .as_any()
            .downcast_ref::<NdArray<T>>()
            .unwrap_or_else(|| panic!("array '{name}' has another type"))
            .clone_into_raw_vec()
            .await
    }

    // ── Nested groups ──────────────────────────────────────────────────

    /// A dataset inside a group reads, under its path.
    #[tokio::test]
    async fn a_nested_dataset_is_named_by_its_path() {
        let any = open(NESTED_FILE).await;
        let names: Vec<&String> = any.dataset().arrays.keys().collect();

        assert!(any.get_array("station_id").is_some(), "{names:?}");
        assert!(
            any.get_array("observations/temperature").is_some(),
            "{names:?}"
        );
        assert!(
            any.get_array("observations/salinity").is_some(),
            "{names:?}"
        );
        assert!(any.get_array("observations/qc/flag").is_some(), "{names:?}");
        // The leaf name alone must not resolve, or two groups would collide.
        assert!(any.get_array("temperature").is_none());
        assert!(any.get_array("flag").is_none());
    }

    #[tokio::test]
    async fn a_nested_dataset_reads_its_values() {
        let any = open(NESTED_FILE).await;

        assert_eq!(values::<i32>(&any, "station_id").await, vec![11, 22, 33]);
        assert_eq!(
            values::<f32>(&any, "observations/temperature").await,
            (0..12).map(|v| v as f32).collect::<Vec<f32>>()
        );
        assert_eq!(
            values::<i8>(&any, "observations/qc/flag").await,
            (0..12).map(|v| v as i8).collect::<Vec<i8>>()
        );
    }

    #[tokio::test]
    async fn a_nested_dataset_reports_its_shape() {
        let any = open(NESTED_FILE).await;
        assert_eq!(any.get_array("station_id").unwrap().shape(), vec![3]);
        assert_eq!(
            any.get_array("observations/temperature").unwrap().shape(),
            vec![3, 4]
        );
    }

    /// A file with no dimension scales gets phony axis names, one per axis.
    /// Two datasets of the same rank then share them, so they broadcast.
    #[tokio::test]
    async fn a_plain_file_gets_phony_dimension_names() {
        let any = open(NESTED_FILE).await;
        assert_eq!(
            any.get_array("observations/temperature")
                .unwrap()
                .dimensions(),
            vec!["phony_dim_0".to_string(), "phony_dim_1".to_string()]
        );
        assert_eq!(
            any.get_array("observations/salinity").unwrap().dimensions(),
            any.get_array("observations/temperature")
                .unwrap()
                .dimensions()
        );
    }

    // ── Attributes ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn a_global_attribute_gets_a_leading_dot() {
        let any = open(NESTED_FILE).await;
        assert!(any.get_array(".title").is_some());
        assert!(any.get_array("title").is_none());
        assert_eq!(values::<i64>(&any, ".version").await, vec![2]);
    }

    #[tokio::test]
    async fn a_group_attribute_is_named_by_its_group_path() {
        let any = open(NESTED_FILE).await;
        assert!(any.get_array("observations/.units").is_some());
    }

    #[tokio::test]
    async fn a_dataset_attribute_gets_a_dotted_name() {
        let any = open(COMPOUND_FILE).await;
        assert!(any.get_array("measurements.description").is_some());
    }

    // ── Compound datasets ──────────────────────────────────────────────

    /// Every fixed-width member becomes its own array, named `dataset/member`.
    #[tokio::test]
    async fn a_compound_dataset_expands_into_one_array_per_member() {
        let any = open(COMPOUND_FILE).await;

        assert_eq!(
            values::<i32>(&any, "measurements/station").await,
            vec![1, 2, 3, 4]
        );
        assert_eq!(
            values::<f32>(&any, "measurements/depth").await,
            vec![0.0, 10.0, 20.0, 30.0]
        );
        assert_eq!(
            values::<f64>(&any, "measurements/temp").await,
            vec![12.5, 11.25, 10.0, 9.75]
        );
        assert_eq!(
            values::<String>(&any, "measurements/label").await,
            vec![
                "alpha".to_string(),
                "beta".to_string(),
                "gamma".to_string(),
                "delta".to_string()
            ]
        );
    }

    /// The members carry the dataset's shape and axis names, so a query joins
    /// them by row the way it joins two ordinary variables.
    #[tokio::test]
    async fn a_compound_member_keeps_the_shape_of_its_dataset() {
        let any = open(COMPOUND_FILE).await;
        let station = any.get_array("measurements/station").unwrap();
        let index = any.get_array("index").unwrap();

        assert_eq!(station.shape(), vec![4]);
        assert_eq!(station.dimensions(), index.dimensions());
        assert_eq!(station.datatype(), NdArrayDataType::I32);
    }

    /// A variable-length member holds a heap pointer, not a value. It is
    /// skipped; the members around it still read.
    #[tokio::test]
    async fn a_member_the_reader_cannot_model_is_skipped() {
        let any = open(COMPOUND_FILE).await;
        assert!(any.get_array("measurements/note").is_none());
        assert!(any.get_array("measurements/station").is_some());
    }

    /// The compound dataset itself is not an array — only its members are.
    #[tokio::test]
    async fn the_compound_dataset_itself_is_not_an_array() {
        let any = open(COMPOUND_FILE).await;
        assert!(any.get_array("measurements").is_none());
    }

    // ── Opening ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn open_dataset_returns_an_error_for_a_missing_object() {
        assert!(open_dataset(test_store(), Path::from("nope.h5"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn dataset_name_is_the_object_path() {
        let any = open(NESTED_FILE).await;
        assert_eq!(any.name(), NESTED_FILE);
    }

    #[tokio::test]
    async fn array_names_are_sorted() {
        let file = Arc::new(
            AsyncNetcdfFile::open_store(test_store(), Path::from(NESTED_FILE))
                .await
                .unwrap(),
        );
        let keys: Vec<String> = read_arrays(&file).unwrap().keys().cloned().collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
