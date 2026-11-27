//! The metadata of one NetCDF file on the ND contract.

use std::sync::Arc;

use beacon_nd_dataset::{DatasetSchema, Dimension, Dimensions, NdVariable, Result};
use indexmap::IndexMap;
use oxcdf::{AsyncNetcdfFile, AsyncVariable};

use super::attributes::attributes_of;
use super::variable::{stored_length, NetcdfVariable};
use crate::dimensions::PhonyDimensions;

/// The dataset of the root variables of `file`, sorted by name.
///
/// A variable that the rules cannot model is left out, with a debug log.
pub(crate) fn dataset_of(name: &str, file: &Arc<AsyncNetcdfFile>) -> Result<DatasetSchema> {
    let phony = PhonyDimensions::of_file(file);
    let mut variables: Vec<AsyncVariable<'_>> = file
        .root()
        .variables
        .iter()
        .filter_map(|info| {
            let variable = file.variable(&info.path);
            if variable.is_none() {
                tracing::debug!(variable = %info.name, "skip a NetCDF variable that the file cannot bind");
            }
            variable
        })
        .collect();
    variables.sort_by(|a, b| a.name.cmp(&b.name));

    let dataset: Arc<str> = Arc::from(name);
    let sizes = dimension_sizes(file, &phony, &variables);
    let modeled = modeled(variables.iter().map(|variable| {
        (
            variable.name.clone(),
            NetcdfVariable::try_new(
                Arc::from(variable.name.as_str()),
                dataset.clone(),
                file.clone(),
                variable,
                &phony,
                &sizes,
            ),
        )
    }))
    .into_iter()
    .map(|variable| Arc::new(variable) as Arc<dyn NdVariable>)
    .collect();

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
    Ok(DatasetSchema::try_new(name, dimensions, modeled)?
        .with_attributes(attributes_of(file.attributes()))
        .with_invented_dimensions(invented))
}

/// The size of each dimension, in root order: the longest stored length of the
/// variables on it, or the file size for a dimension that no variable uses.
fn dimension_sizes(
    file: &AsyncNetcdfFile,
    phony: &PhonyDimensions,
    variables: &[AsyncVariable<'_>],
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
    sizes
}

/// The values of `results` that are `Ok`. A debug log names each `Err` and its reason.
fn modeled<V>(
    results: impl IntoIterator<Item = (String, std::result::Result<V, String>)>,
) -> Vec<V> {
    results
        .into_iter()
        .filter_map(|(name, result)| match result {
            Ok(value) => Some(value),
            Err(reason) => {
                tracing::debug!(variable = %name, %reason, "skip a NetCDF variable");
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::modeled;

    #[test]
    fn a_variable_with_no_arrow_type_is_left_out() {
        let results = vec![
            ("a".to_string(), Ok(1)),
            (
                "b".to_string(),
                Err("the NetCDF type Vlen(Int(4)) has no Arrow type".to_string()),
            ),
            ("c".to_string(), Ok(3)),
        ];
        assert_eq!(modeled(results), vec![1, 3]);
    }
}
