use serde::{Deserialize, Serialize};

use std::collections::HashSet;

use crate::error::NdArrayError;

/// Logical dimensions for an N-dimensional array.
///
/// `Dimensions` is intentionally lightweight: it carries a list of named axes and their sizes.
/// A scalar is represented as [`Dimensions::Scalar`] (shape `[]`), and therefore always has a
/// flat size of `1`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Dimensions {
    /// A scalar value (rank 0, shape `[]`).
    Scalar,
    /// A rank-N array with named axes.
    MultiDimensional(Vec<Dimension>),
}

impl Dimensions {
    /// Construct dimensions from an explicit list of axes.
    ///
    /// An empty list is normalized to [`Dimensions::Scalar`].
    pub fn new(dimensions: Vec<Dimension>) -> Self {
        if dimensions.is_empty() {
            Dimensions::Scalar
        } else {
            Dimensions::MultiDimensional(dimensions)
        }
    }

    /// Construct scalar dimensions (rank 0).
    pub fn new_scalar() -> Self {
        Dimensions::Scalar
    }

    /// Validate dimension invariants.
    ///
    /// Rules:
    /// - Scalars have no axes and are always valid.
    /// - Non-scalar arrays must have a non-empty name per axis.
    /// - Dimension names must be unique.
    pub fn validate(&self) -> Result<(), NdArrayError> {
        let dims = match self {
            Dimensions::Scalar => return Ok(()),
            Dimensions::MultiDimensional(dims) => dims,
        };

        let mut seen: HashSet<&str> = HashSet::with_capacity(dims.len());
        for d in dims {
            if d.name().trim().is_empty() {
                return Err(NdArrayError::InvalidDimensions(
                    "dimension names must be non-empty".to_string(),
                ));
            }
            if !seen.insert(d.name()) {
                return Err(NdArrayError::InvalidDimensions(format!(
                    "duplicate dimension name: {}",
                    d.name()
                )));
            }
        }

        Ok(())
    }

    /// Return the number of logical axes.
    pub fn num_dims(&self) -> usize {
        match self {
            Dimensions::Scalar => 0,
            Dimensions::MultiDimensional(dims) => dims.len(),
        }
    }

    /// Return the number of elements in the flattened storage representation.
    ///
    /// For scalars, this returns `1`.
    pub fn total_flat_size(&self) -> usize {
        match self {
            Dimensions::Scalar => 1,
            Dimensions::MultiDimensional(dims) => dims.iter().map(|dim| dim.size()).product(),
        }
    }

    /// Returns `true` if this represents a scalar.
    pub fn is_scalar(&self) -> bool {
        matches!(self, Dimensions::Scalar)
    }

    /// Return the list of axes for non-scalar dimensions.
    pub fn as_multi_dimensional(&self) -> Option<&Vec<Dimension>> {
        if let Dimensions::MultiDimensional(dims) = self {
            Some(dims)
        } else {
            None
        }
    }

    /// Return the shape as a list of sizes.
    ///
    /// Scalars are represented as an empty shape `[]`.
    pub fn shape(&self) -> Vec<usize> {
        match self {
            Dimensions::Scalar => vec![],
            Dimensions::MultiDimensional(dims) => dims.iter().map(|d| d.size()).collect(),
        }
    }

    // Intentionally no `from_shape` helper.
    //
    // Dimensions in this crate are always *named*, and broadcasting is name-aligned.
    // Construct named dimensions explicitly via `Dimensions::new(vec![Dimension::try_new(...)? , ...])`.
}

impl From<Vec<Dimension>> for Dimensions {
    fn from(dims: Vec<Dimension>) -> Self {
        Dimensions::new(dims)
    }
}

/// A single axis of an ND array.
///
/// The `name` is used for xarray-style named broadcasting.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Dimension {
    name: String,
    size: usize,
}

impl Dimension {
    /// Construct a new dimension.
    ///
    /// Dimension names are mandatory and must be non-empty.
    pub fn try_new(name: impl Into<String>, size: usize) -> Result<Self, NdArrayError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(NdArrayError::InvalidDimensions(
                "dimension names must be non-empty".to_string(),
            ));
        }
        Ok(Self { name, size })
    }

    pub(crate) fn new_unchecked(name: String, size: usize) -> Self {
        Self { name, size }
    }

    /// Dimension name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Dimension size.
    pub fn size(&self) -> usize {
        self.size
    }
}

impl From<(&str, usize)> for Dimension {
    fn from((name, size): (&str, usize)) -> Self {
        Self::new_unchecked(name.to_string(), size)
    }
}
