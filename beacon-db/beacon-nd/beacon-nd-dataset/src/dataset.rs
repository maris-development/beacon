//! The contract that a format implements for one opened file.

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::DataType;
use async_trait::async_trait;
use nd_arrow_array::{Attributes, Dimensions, NdArrowArray};

use crate::{Error, Region, Result};

/// One variable of an opened file.
///
/// The metadata methods do no I/O.
#[async_trait]
pub trait NdVariable: Send + Sync + Debug {
    /// The name of the variable.
    fn name(&self) -> &str;
    /// The Arrow type that `read` returns.
    fn data_type(&self) -> &DataType;
    /// The dimensions, in C order, with their sizes.
    fn dimensions(&self) -> &Dimensions;
    /// The native chunk shape. `None` for a variable without chunks.
    fn chunk_shape(&self) -> Option<&[usize]>;
    /// The attributes of the variable.
    fn attributes(&self) -> &Attributes;
    /// Read one region of this variable.
    ///
    /// The result has the type of `data_type`, the dimension names of the
    /// variable and the sizes of `region.shape`. Missing data is null. The
    /// format applies its fill value and its decode. A region outside the
    /// variable gives `Error::InvalidRegion`. Only this method does I/O.
    async fn read(&self, region: &Region) -> Result<NdArrowArray>;
}

/// One opened file of an ND format.
///
/// The metadata methods do no I/O.
pub trait NdDataset: Send + Sync + Debug {
    /// The name for errors and logs. Usually the file path.
    fn name(&self) -> &str;
    /// Every dimension of the file, with its size.
    ///
    /// A dimension has one size in the dataset. Each variable that uses it has that size.
    fn dimensions(&self) -> &Dimensions;
    /// The global attributes.
    fn attributes(&self) -> &Attributes;
    /// The variable names. The order is the same in each call.
    ///
    /// The order is the same at each open of the same file. It decides the tie
    /// of `default_grid` and the tie of the grid driver.
    fn variable_names(&self) -> Vec<Arc<str>>;
    /// One variable. `None` when the file does not have it.
    ///
    /// The returned variable has the given name.
    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>>;
    /// The dimensions that the reader gave a name. Default: none.
    fn invented_dimensions(&self) -> &[Arc<str>] {
        &[]
    }
}

/// `Invalid` when the dataset breaks a rule of the contract.
///
/// The checks: each variable name is unique, `variable(name)` returns a
/// variable with that name, each dimension of a variable is a dataset
/// dimension with the same size, and each chunk shape has the rank of its
/// variable.
pub fn validate(dataset: &dyn NdDataset) -> Result<()> {
    let names = dataset.variable_names();
    for (name, variable) in names.iter().zip(variables(dataset)?) {
        if variable.name() != &**name {
            return Err(invalid(
                dataset,
                format!("the name '{name}' gives the variable '{}'", variable.name()),
            ));
        }
        check_variable(dataset.dimensions(), variable.as_ref())
            .map_err(|reason| invalid(dataset, reason))?;
    }
    Ok(())
}

/// Each variable of `variable_names()`, in that order.
///
/// `Invalid` when a name is not unique or gives no variable.
pub(crate) fn variables(dataset: &dyn NdDataset) -> Result<Vec<Arc<dyn NdVariable>>> {
    let names = dataset.variable_names();
    let mut seen = HashSet::with_capacity(names.len());
    let mut variables = Vec::with_capacity(names.len());
    for name in &names {
        if !seen.insert(&**name) {
            return Err(invalid(dataset, not_unique(name)));
        }
        let Some(variable) = dataset.variable(name) else {
            return Err(invalid(
                dataset,
                format!("the variable '{name}' is not available"),
            ));
        };
        variables.push(variable);
    }
    Ok(variables)
}

/// The dimensions of `dataset` that one of `variables` uses, in dataset order.
pub(crate) fn used_dimensions(
    dataset: &dyn NdDataset,
    variables: &[Arc<dyn NdVariable>],
) -> Result<Dimensions> {
    Ok(Dimensions::try_new(
        dataset
            .dimensions()
            .iter()
            .filter(|dimension| {
                variables
                    .iter()
                    .any(|variable| variable.dimensions().contains(dimension.name()))
            })
            .cloned(),
    )?)
}

/// The reason when two variables have the name `name`.
pub(crate) fn not_unique(name: &str) -> String {
    format!("the variable name '{name}' is not unique")
}

/// The reason when a chunk shape does not have the rank of its variable.
pub(crate) fn wrong_chunk_rank(variable: &dyn NdVariable, chunk_rank: usize) -> String {
    format!(
        "the variable '{}' has rank {} and a chunk shape of rank {chunk_rank}",
        variable.name(),
        variable.dimensions().rank()
    )
}

/// An `Invalid` error of `dataset`.
fn invalid(dataset: &dyn NdDataset, reason: String) -> Error {
    Error::Invalid {
        dataset: dataset.name().to_string(),
        reason,
    }
}

/// The reason when the variable does not fit the dataset dimensions.
fn check_variable(
    dimensions: &Dimensions,
    variable: &dyn NdVariable,
) -> std::result::Result<(), String> {
    let name = variable.name();
    for dimension in variable.dimensions().iter() {
        match dimensions.iter().find(|d| d.name() == dimension.name()) {
            None => {
                return Err(format!(
                    "the variable '{name}' uses the unknown dimension '{}'",
                    dimension.name()
                ));
            }
            Some(known) if known.size() != dimension.size() => {
                return Err(format!(
                    "the variable '{name}' gives the dimension '{}' the size {}, the dataset gives it the size {}",
                    dimension.name(),
                    dimension.size(),
                    known.size()
                ));
            }
            Some(_) => {}
        }
    }
    if let Some(chunk_shape) = variable.chunk_shape()
        && chunk_shape.len() != variable.dimensions().rank()
    {
        return Err(wrong_chunk_rank(variable, chunk_shape.len()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::testing::{dims, meta};

    /// A dataset that lists `names` and resolves only the name "a".
    #[derive(Debug)]
    struct Listed {
        names: Vec<&'static str>,
        dimensions: Dimensions,
        attributes: Attributes,
    }

    impl Listed {
        fn new(names: Vec<&'static str>) -> Self {
            Self {
                names,
                dimensions: dims(&[("n", 2)]),
                attributes: Attributes::new(),
            }
        }
    }

    impl NdDataset for Listed {
        fn name(&self) -> &str {
            "ds"
        }
        fn dimensions(&self) -> &Dimensions {
            &self.dimensions
        }
        fn attributes(&self) -> &Attributes {
            &self.attributes
        }
        fn variable_names(&self) -> Vec<Arc<str>> {
            self.names.iter().map(|n| Arc::from(*n)).collect()
        }
        fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
            (name == "a")
                .then(|| Arc::new(meta("a", DataType::Float64, &[("n", 2)])) as Arc<dyn NdVariable>)
        }
    }

    fn assert_invalid(result: Result<impl Debug>) {
        assert!(
            matches!(result, Err(Error::Invalid { ref dataset, .. }) if dataset == "ds"),
            "{result:?}"
        );
    }

    #[test]
    fn a_name_listed_two_times_is_invalid() {
        let dataset = Listed::new(vec!["a", "a"]);
        assert_invalid(variables(&dataset));
        assert_invalid(validate(&dataset));
    }

    #[test]
    fn a_name_that_gives_no_variable_is_invalid() {
        let dataset = Listed::new(vec!["a", "b"]);
        assert_invalid(variables(&dataset));
        assert_invalid(validate(&dataset));
    }

    #[test]
    fn the_variables_follow_the_name_order() {
        let variables = variables(&Listed::new(vec!["a"])).unwrap();
        assert_eq!(variables.len(), 1);
        assert_eq!(variables[0].name(), "a");
    }
}
