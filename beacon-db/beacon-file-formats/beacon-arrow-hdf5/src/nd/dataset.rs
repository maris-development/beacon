//! The metadata of one HDF5 file on the ND contract.
//!
//! Every group is walked. A root dataset keeps its name, a nested one its path
//! (`observations/qc/flag`), and a compound member is `dataset/member`.
//! `attribute_columns` adds `dataset.attr` and `.attr`. This module adds the
//! two attribute columns it cannot see: `group/.attr`, and `dataset.attr` of a
//! compound dataset.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use beacon_arrow_netcdf::dimensions::PhonyDimensions;
use beacon_arrow_netcdf::nd::{attributes_of, stored_length, NetcdfVariable};
use beacon_nd_dataset::{
    Attributes, DatasetSchema, Dimension, Dimensions, MemoryVariable, NdArrowArray, NdVariable,
    Result,
};
use indexmap::IndexMap;
use oxcdf::netcdf::NcGroup;
use oxcdf::{AsyncNetcdfFile, AsyncVariable};

use super::compound::{is_compound, member_variables};

/// The dataset of every variable of `file`, plus `extra`, sorted by name.
///
/// A file variable wins over an `extra` variable of the same name.
/// `scaled` names one column and the scale factor that unpacks it.
pub(crate) fn dataset_of(
    name: &str,
    file: &Arc<AsyncNetcdfFile>,
    phony: &PhonyDimensions,
    extra: Vec<Arc<dyn NdVariable>>,
    scaled: Option<(&str, f64)>,
) -> Result<DatasetSchema> {
    let dataset: Arc<str> = Arc::from(name);
    let bound = file.variables();
    let sizes = dimension_sizes(file, phony, &bound, &extra);

    let mut variables: Vec<Arc<dyn NdVariable>> = Vec::new();
    for variable in &bound {
        let column = column_name(variable);
        if is_compound(variable) {
            match member_variables(file, variable, &column, phony, &sizes) {
                Ok(members) => variables.extend(
                    members
                        .into_iter()
                        .map(|member| Arc::new(member) as Arc<dyn NdVariable>),
                ),
                Err(reason) => {
                    tracing::warn!(variable = %column, %reason, "skip an HDF5 compound dataset")
                }
            }
            // `attribute_columns` sees only the members, so the dataset attributes go here.
            variables.extend(attribute_variables(
                &column,
                ".",
                &attributes_of(variable.attributes()),
            )?);
            continue;
        }
        match NetcdfVariable::try_new(
            Arc::from(column.as_str()),
            dataset.clone(),
            file.clone(),
            variable,
            phony,
            &sizes,
        ) {
            Ok(variable) => variables.push(Arc::new(match scaled {
                Some((scaled, scale)) if scaled == column => variable.with_scale_factor(scale),
                _ => variable,
            })),
            Err(reason) => {
                tracing::debug!(variable = %column, %reason, "skip an HDF5 dataset")
            }
        }
    }
    for group in file.groups() {
        group_attribute_variables(group, &mut variables)?;
    }
    variables.extend(extra);
    // A stable sort keeps a file variable before an `extra` one of the same name.
    variables.sort_by(|a, b| a.name().cmp(b.name()));
    let variables = unique_by_name(variables);

    let dimensions = Dimensions::try_new(
        sizes
            .iter()
            .map(|(name, &size)| Dimension::new(name.as_str(), size)),
    )?;
    let mut invented: Vec<Arc<str>> = phony
        .invented_names()
        .iter()
        .map(|name| Arc::from(name.as_str()))
        .collect();
    invented.sort();
    Ok(DatasetSchema::try_new(name, dimensions, variables)?
        .with_attributes(attributes_of(file.attributes()))
        .with_invented_dimensions(invented))
}

/// The column name of one dataset: its path without the leading `/`, or its name at the root.
fn column_name(variable: &AsyncVariable<'_>) -> String {
    let path = variable.path.trim_start_matches('/');
    if path.is_empty() {
        variable.name.clone()
    } else {
        path.to_string()
    }
}

/// The size of each dimension: the longest stored length of the variables on it.
fn dimension_sizes(
    file: &AsyncNetcdfFile,
    phony: &PhonyDimensions,
    variables: &[AsyncVariable<'_>],
    extra: &[Arc<dyn NdVariable>],
) -> IndexMap<String, usize> {
    let mut used: IndexMap<String, usize> = IndexMap::new();
    for variable in variables {
        let names = phony.apply(&variable.dimensions);
        for (name, length) in names.into_iter().zip(stored_length(variable)) {
            let size = used.entry(name).or_insert(0);
            *size = (*size).max(length);
        }
    }

    let mut sizes = IndexMap::with_capacity(used.len());
    for dimension in file.dimensions() {
        for name in phony.apply(std::slice::from_ref(&dimension.name)) {
            let size = used.get(&name).copied().unwrap_or(dimension.len as usize);
            sizes.entry(name).or_insert(size);
        }
    }
    for (name, size) in used {
        sizes.entry(name).or_insert(size);
    }
    for variable in extra {
        for dimension in variable.dimensions().iter() {
            sizes
                .entry(dimension.name().to_string())
                .or_insert(dimension.size());
        }
    }
    sizes
}

/// Add `path/.attr` for the attributes of `group` and of every group inside it.
fn group_attribute_variables(
    group: &NcGroup,
    variables: &mut Vec<Arc<dyn NdVariable>>,
) -> Result<()> {
    let path = group.path.trim_start_matches('/');
    variables.extend(attribute_variables(
        path,
        "/.",
        &attributes_of(&group.attributes),
    )?);
    for child in &group.groups {
        group_attribute_variables(child, variables)?;
    }
    Ok(())
}

/// One rank-0 variable `prefix{separator}attr` per attribute that is not a list.
fn attribute_variables(
    prefix: &str,
    separator: &str,
    attributes: &Attributes,
) -> Result<Vec<Arc<dyn NdVariable>>> {
    attributes
        .iter()
        .filter(|(_, value)| !is_list(value))
        .map(|(attribute, value)| {
            let array = NdArrowArray::try_new_scalar(value.clone())?;
            let name = format!("{prefix}{separator}{attribute}");
            Ok(Arc::new(MemoryVariable::new(name, array)) as Arc<dyn NdVariable>)
        })
        .collect()
}

/// True for an attribute with more than one value.
fn is_list(value: &ArrayRef) -> bool {
    matches!(
        value.data_type(),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
    )
}

/// The variables with the first of each name. A warning names each duplicate.
fn unique_by_name(variables: Vec<Arc<dyn NdVariable>>) -> Vec<Arc<dyn NdVariable>> {
    let mut seen: HashSet<String> = HashSet::new();
    variables
        .into_iter()
        .filter(|variable| {
            let first = seen.insert(variable.name().to_string());
            if !first {
                tracing::warn!(variable = %variable.name(), "skip a second HDF5 variable with the same column name");
            }
            first
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use beacon_nd_dataset::{Dimension, Dimensions, MemoryVariable, NdArrowArray, NdVariable};

    use super::unique_by_name;

    fn variable(name: &str, value: i32) -> Arc<dyn NdVariable> {
        let dimensions = Dimensions::try_new([Dimension::new("x", 1)]).unwrap();
        let array =
            NdArrowArray::try_new(Arc::new(Int32Array::from(vec![value])), dimensions).unwrap();
        Arc::new(MemoryVariable::new(name, array))
    }

    #[test]
    fn a_duplicate_column_name_keeps_the_first() {
        let kept = unique_by_name(vec![
            variable("a/b", 1),
            variable("a/b", 2),
            variable("c", 3),
        ]);
        let names: Vec<&str> = kept.iter().map(|v| v.name()).collect();
        assert_eq!(names, ["a/b", "c"]);
    }

    #[tokio::test]
    async fn a_column_of_the_file_wins_over_an_extra_variable() {
        use arrow::datatypes::DataType;
        use beacon_arrow_netcdf::dimensions::PhonyDimensions;
        use beacon_nd_dataset::NdDataset;
        use object_store::{local::LocalFileSystem, path::Path, ObjectStore};
        use oxcdf::AsyncNetcdfFile;

        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test_files");
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(root).unwrap());
        let file = Arc::new(
            AsyncNetcdfFile::open_store(store, Path::from("optodas.h5"))
                .await
                .unwrap(),
        );
        let phony = PhonyDimensions::of_file(&file);
        let extra = vec![variable("fileVersion", -1)];
        let dataset = super::dataset_of("optodas.h5", &file, &phony, extra, None).unwrap();
        let version = dataset.variable("fileVersion").unwrap();
        assert_eq!(
            version.dimensions().rank(),
            0,
            "the rank-0 file dataset stays"
        );
        assert_eq!(version.data_type(), &DataType::Int32);
    }
}
