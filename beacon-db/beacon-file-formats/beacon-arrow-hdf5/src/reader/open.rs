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
//! attaches. A file with no scales gets the dimensions netCDF invents for it,
//! and [`oxcdf`] names them as `ncdump` does: one counter over the whole file,
//! and one `phony_dim_N` for every axis of one length inside one group. Two
//! groups would then never share a name, so a dataset of one group would not
//! broadcast against a dataset of another.
//!
//! [`beacon_arrow_netcdf::dimensions`] renames them: every invented axis of one
//! length becomes `phony_len_<length>`, whatever group holds it. An empty or
//! growable axis keeps the name [`oxcdf`] gave it, and a named dimension is
//! never touched. The CF rules — `scale_factor` / `add_offset` packing, a CF
//! `units` string, a fixed-size char string — are applied by
//! [`beacon_arrow_netcdf::oxcdf_reader::compat`], which both readers share.
//!
//! # Example
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn example(store: Arc<dyn object_store::ObjectStore>) -> anyhow::Result<()> {
//! use object_store::path::Path;
//!
//! let any =
//!     beacon_arrow_hdf5::reader::open_dataset(store, Path::from("data.h5"), Default::default())
//!         .await?;
//! for name in any.dataset().arrays.keys() {
//!     println!("{name}");
//! }
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use beacon_arrow_netcdf::dimensions::PhonyDimensions;
use beacon_arrow_netcdf::oxcdf_reader::compat;
use beacon_nd_array::{
    dataset::{AnyDataset, Dataset},
    NdArrayD,
};
use indexmap::IndexMap;
use object_store::{path::Path, ObjectStore};
use oxcdf::{netcdf::NcGroup, AsyncNetcdfFile, AsyncVariable};

use crate::conventions;
use crate::reader::compound;
use crate::{Hdf5Convention, ReadOptions};

/// Open an HDF5 object from `store` and return its contents as an
/// [`AnyDataset`].
///
/// [`ReadOptions::unify_phony_dimensions`] renames every invented dimension by
/// its length, so two groups of one file broadcast against each other. Clear it
/// to keep the names the reader gave, one per length per group.
///
/// [`ReadOptions::convention`] reads a vendor layout on top of the container.
/// It is [`Hdf5Convention::None`] by default, and then no file is inspected for
/// one. See [`crate::conventions`].
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
pub async fn open_dataset(
    store: Arc<dyn ObjectStore>,
    path: Path,
    options: ReadOptions,
) -> anyhow::Result<AnyDataset> {
    let name = path.to_string();
    let file = Arc::new(AsyncNetcdfFile::open_store(store, path).await?);

    // The file names no dimension, so netCDF invented one per length per group.
    // Give every length one name instead, or a dataset of one group would not
    // broadcast against a dataset of another. See
    // [`beacon_arrow_netcdf::dimensions`].
    let mut phony = PhonyDimensions::of_file(&file);
    if !options.unify_phony_dimensions {
        phony = phony.without_renames();
    }
    // The merge is a heuristic: two axes of one length in two groups become one
    // dimension, whatever they count. Say which, so a join nobody asked for can
    // be traced back to it.
    phony.log_merges(&name);

    // A convention names the axes it recognises. It joins the map above, so the
    // arrays below are built with the names it gives and nothing is rebuilt.
    // `Hdf5Convention::None` is the default, and reads no byte of the file.
    let convention = match options.convention {
        Hdf5Convention::None => None,
        Hdf5Convention::OptoDas => conventions::optodas::detect(&file, &phony).await?,
    };
    if let Some(convention) = &convention {
        phony = phony.rename(convention.axis_names());
    }

    let mut arrays = read_arrays(&file, &phony)?;

    // The convention adds what the file describes but does not store.
    if let Some(convention) = &convention {
        convention.decorate(&file, &phony, &mut arrays).await?;
    }
    // Which dimensions the file itself names decides how `SELECT *` picks its
    // grid. A file that names none is an instrument file, and its payload is
    // the largest array, not the most common one.
    let dataset = Dataset::new(name, arrays)
        .await
        .with_invented_dimensions(phony.invented_names().iter().cloned());
    AnyDataset::try_from_dataset(dataset).await
}

/// Read every dataset and attribute of an opened file into an ordered map of
/// lazy ND arrays.
///
/// Every group is walked, depth first. The map is sorted by key, so iteration
/// order is stable. This is the lower-level building block behind
/// [`open_dataset`], which builds `phony` and records what it invented.
///
/// # Errors
///
/// Returns an error when an attribute of the file cannot be read. A dataset
/// this reader cannot model is skipped and logged.
pub fn read_arrays(
    file: &Arc<AsyncNetcdfFile>,
    phony: &PhonyDimensions,
) -> anyhow::Result<IndexMap<String, Arc<dyn NdArrayD>>> {
    let mut arrays: IndexMap<String, Arc<dyn NdArrayD>> = IndexMap::new();

    // ── Datasets and their attributes, in every group ────────────────────
    for variable in file.variables() {
        let name = array_name(&variable);

        match compat::variable_to_nd_array(file.clone(), &variable, phony) {
            Ok(array) => {
                arrays.insert(name.clone(), array);
            }
            // The netCDF data model does not cover the type. A compound
            // dataset is the case this crate exists for, so try the HDF5 one.
            Err(_) if compound::members_of(variable.datatype()).is_some() => {
                match compound::member_arrays(file.clone(), &variable, &name, phony) {
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
        open_dataset(test_store(), Path::from(file), ReadOptions::default())
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

    /// A file with no dimension scales gets the dimensions netCDF invents. Every
    /// axis of one length then carries one name, so two datasets of one group
    /// broadcast against each other.
    #[tokio::test]
    async fn a_plain_file_gets_phony_dimension_names() {
        let any = open(NESTED_FILE).await;
        let temperature = any
            .get_array("observations/temperature")
            .unwrap()
            .dimensions();

        assert_eq!(
            temperature,
            vec!["phony_len_3".to_string(), "phony_len_4".to_string()]
        );
        assert_eq!(
            any.get_array("observations/salinity").unwrap().dimensions(),
            temperature
        );
    }

    /// [`oxcdf`] names the axes of one group apart from those of another, as
    /// `ncdump` does. Beacon renames them by length, so a dataset of a nested
    /// group broadcasts against one of the root.
    #[tokio::test]
    async fn every_group_shares_one_name_per_length() {
        let any = open(NESTED_FILE).await;

        // `qc` is a group of its own, and `station_id` sits in the root. All
        // three axes 3 long now carry one name.
        assert_eq!(
            any.get_array("observations/qc/flag").unwrap().dimensions(),
            vec!["phony_len_3".to_string(), "phony_len_4".to_string()]
        );
        assert_eq!(
            any.get_array("station_id").unwrap().dimensions(),
            vec!["phony_len_3".to_string()]
        );

        // One dimension per name, so the dataset broadcasts as one table.
        assert_eq!(any.dataset().dimensions.get("phony_len_3"), Some(&3));
        assert_eq!(any.dataset().dimensions.get("phony_len_4"), Some(&4));
        assert_eq!(any.dataset().dimensions.len(), 2);
    }

    /// The unification is a setting. Turned off, the reader reports the names
    /// [`oxcdf`] gave, one per length per group.
    #[tokio::test]
    async fn the_names_of_the_reader_survive_when_the_unification_is_off() {
        let file = Arc::new(
            AsyncNetcdfFile::open_store(test_store(), Path::from(NESTED_FILE))
                .await
                .unwrap(),
        );
        let arrays =
            read_arrays(&file, &PhonyDimensions::of_file(&file).without_renames()).unwrap();

        assert_eq!(
            arrays["observations/temperature"].dimensions(),
            vec!["phony_dim_2".to_string(), "phony_dim_3".to_string()]
        );
        assert_eq!(
            arrays["observations/qc/flag"].dimensions(),
            vec!["phony_dim_0".to_string(), "phony_dim_1".to_string()]
        );
        assert_eq!(
            arrays["station_id"].dimensions(),
            vec!["phony_dim_4".to_string()]
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
        assert!(
            open_dataset(test_store(), Path::from("nope.h5"), ReadOptions::default())
                .await
                .is_err()
        );
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
        let keys: Vec<String> = read_arrays(&file, &PhonyDimensions::of_file(&file))
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted);
    }
}
