//! The contract that a format implements for one opened file.

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::datatypes::DataType;
use async_trait::async_trait;
use nd_arrow_array::{Attributes, Dimensions, NdArrowArray};

use crate::{Error, Region, Result};

/// One variable of an opened file.
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
    async fn read(&self, region: &Region) -> Result<NdArrowArray>;
}

/// One opened file of an ND format.
pub trait NdDataset: Send + Sync + Debug {
    /// The name for errors and logs. Usually the file path.
    fn name(&self) -> &str;
    /// Every dimension of the file, with its size.
    fn dimensions(&self) -> &Dimensions;
    /// The global attributes.
    fn attributes(&self) -> &Attributes;
    /// The variable names. The order is the same in each call.
    fn variable_names(&self) -> Vec<Arc<str>>;
    /// One variable. `None` when the file does not have it.
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
    let invalid = |reason: String| Error::Invalid {
        dataset: dataset.name().to_string(),
        reason,
    };
    let mut seen = HashSet::new();
    for name in dataset.variable_names() {
        if !seen.insert(name.clone()) {
            return Err(invalid(format!("the variable name '{name}' is not unique")));
        }
        let Some(variable) = dataset.variable(&name) else {
            return Err(invalid(format!("the variable '{name}' is not available")));
        };
        if variable.name() != &*name {
            return Err(invalid(format!(
                "the name '{name}' gives the variable '{}'",
                variable.name()
            )));
        }
        check_variable(dataset.dimensions(), variable.as_ref()).map_err(invalid)?;
    }
    Ok(())
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
    if let Some(chunk_shape) = variable.chunk_shape() {
        let rank = variable.dimensions().rank();
        if chunk_shape.len() != rank {
            return Err(format!(
                "the variable '{name}' has rank {rank} and a chunk shape of rank {}",
                chunk_shape.len()
            ));
        }
    }
    Ok(())
}
