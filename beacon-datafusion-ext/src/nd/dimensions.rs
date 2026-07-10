//! Named dimensions for N-dimensional Arrow arrays.
//!
//! A [`Dimensions`] value describes the logical shape of an [`crate::nd::NdArrowArray`]
//! in C-order (row-major, first dimension outermost). Dimension names drive
//! xarray-style broadcasting: axes are aligned by name, never by position.

use std::fmt;
use std::sync::Arc;

use datafusion::common::plan_err;
use datafusion::error::Result;

/// A single named axis with a fixed size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dimension {
    name: Arc<str>,
    size: usize,
}

impl Dimension {
    pub fn new(name: impl Into<Arc<str>>, size: usize) -> Self {
        Self {
            name: name.into(),
            size,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn name_arc(&self) -> Arc<str> {
        self.name.clone()
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl fmt::Display for Dimension {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.size)
    }
}

/// An ordered list of named axes, C-order. Cheap to clone.
///
/// Rank 0 (no axes) describes a scalar: one logical element.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Dimensions {
    dims: Arc<[Dimension]>,
}

impl Dimensions {
    /// Create dimensions, validating that axis names are non-empty and unique.
    pub fn try_new(dims: Vec<Dimension>) -> Result<Self> {
        for (i, dim) in dims.iter().enumerate() {
            if dim.name().is_empty() {
                return plan_err!("nd dimension at axis {i} has an empty name");
            }
            if dims[..i].iter().any(|d| d.name() == dim.name()) {
                return plan_err!("duplicate nd dimension name '{}'", dim.name());
            }
        }
        Ok(Self { dims: dims.into() })
    }

    /// Rank-0 dimensions describing a scalar.
    pub fn scalar() -> Self {
        Self::default()
    }

    pub fn rank(&self) -> usize {
        self.dims.len()
    }

    pub fn is_scalar(&self) -> bool {
        self.dims.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Dimension> {
        self.dims.iter()
    }

    pub fn get(&self, axis: usize) -> &Dimension {
        &self.dims[axis]
    }

    pub fn shape(&self) -> Vec<usize> {
        self.dims.iter().map(|d| d.size()).collect()
    }

    /// Total number of logical elements (1 for a scalar, 0 if any axis is empty).
    pub fn num_elements(&self) -> usize {
        self.dims.iter().map(|d| d.size()).product()
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.dims.iter().position(|d| d.name() == name)
    }

    /// C-order (row-major) strides, in elements.
    pub fn c_strides(&self) -> Vec<usize> {
        let mut strides = vec![0usize; self.rank()];
        let mut acc = 1usize;
        for axis in (0..self.rank()).rev() {
            strides[axis] = acc;
            acc = acc.saturating_mul(self.dims[axis].size());
        }
        strides
    }
}

impl fmt::Display for Dimensions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, dim) in self.dims.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{dim}")?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dims(spec: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(
            spec.iter()
                .map(|(name, size)| Dimension::new(*name, *size))
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn strides_are_c_order() {
        let d = dims(&[("time", 2), ("lat", 3), ("lon", 4)]);
        assert_eq!(d.c_strides(), vec![12, 4, 1]);
        assert_eq!(d.num_elements(), 24);
    }

    #[test]
    fn scalar_has_one_element() {
        let d = Dimensions::scalar();
        assert_eq!(d.rank(), 0);
        assert_eq!(d.num_elements(), 1);
        assert!(d.c_strides().is_empty());
    }

    #[test]
    fn duplicate_names_rejected() {
        let result = Dimensions::try_new(vec![
            Dimension::new("x", 2),
            Dimension::new("x", 3),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn empty_name_rejected() {
        assert!(Dimensions::try_new(vec![Dimension::new("", 2)]).is_err());
    }
}
