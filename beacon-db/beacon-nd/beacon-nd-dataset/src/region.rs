//! A box inside one variable.

use nd_arrow_array::Dimensions;

use crate::{Error, Result};

/// A box inside one variable: a start and a length for each axis.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Region {
    /// The first index on each axis.
    pub start: Vec<usize>,
    /// The length on each axis.
    pub shape: Vec<usize>,
}

impl Region {
    /// A region with this start and shape.
    pub fn new(start: Vec<usize>, shape: Vec<usize>) -> Self {
        Self { start, shape }
    }

    /// The whole variable.
    pub fn full(dimensions: &Dimensions) -> Self {
        Self::new(vec![0; dimensions.rank()], dimensions.shape())
    }

    /// The element count: the product of `shape`. 1 for rank 0.
    ///
    /// `usize::MAX` when the product does not fit in `usize`.
    pub fn len(&self) -> usize {
        self.shape
            .iter()
            .fold(1usize, |count, &length| count.saturating_mul(length))
    }

    /// True when the region holds no element.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of axes.
    pub fn rank(&self) -> usize {
        self.shape.len()
    }

    /// `InvalidRegion` when the region does not lie inside `dimensions`.
    pub fn check(&self, variable: &str, dimensions: &Dimensions) -> Result<()> {
        let invalid = |reason: String| Error::InvalidRegion {
            variable: variable.to_string(),
            reason,
        };
        let rank = dimensions.rank();
        if self.start.len() != rank || self.shape.len() != rank {
            return Err(invalid(format!(
                "the region has start rank {} and shape rank {}, the variable has rank {rank}",
                self.start.len(),
                self.shape.len()
            )));
        }
        for (axis, dimension) in dimensions.iter().enumerate() {
            let (start, length) = (self.start[axis], self.shape[axis]);
            // An end past usize::MAX is also outside the variable.
            match start.checked_add(length) {
                Some(end) if end <= dimension.size() => {}
                _ => {
                    return Err(invalid(format!(
                        "axis {axis} ('{}'): start {start} and length {length} pass the size {}",
                        dimension.name(),
                        dimension.size()
                    )));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use nd_arrow_array::Dimension;

    use super::*;

    fn dims(sizes: &[(&str, usize)]) -> Dimensions {
        Dimensions::try_new(sizes.iter().map(|(n, s)| Dimension::new(*n, *s))).unwrap()
    }

    #[test]
    fn full_covers_the_variable() {
        let region = Region::full(&dims(&[("time", 2), ("lat", 3)]));
        assert_eq!(region, Region::new(vec![0, 0], vec![2, 3]));
        assert_eq!(region.len(), 6);
    }

    #[test]
    fn a_rank_0_region_holds_one_element() {
        let region = Region::full(&Dimensions::scalar());
        assert_eq!(region.rank(), 0);
        assert_eq!(region.len(), 1);
    }

    #[test]
    fn the_len_of_a_huge_region_saturates() {
        let region = Region::new(vec![0, 0], vec![1 << 40, 1 << 40]);
        assert_eq!(region.len(), usize::MAX);
    }

    #[test]
    fn check_accepts_a_region_inside_the_variable() {
        let d = dims(&[("time", 2), ("lat", 3)]);
        assert!(Region::new(vec![1, 1], vec![1, 2]).check("v", &d).is_ok());
    }

    #[test]
    fn check_rejects_a_wrong_rank_a_region_past_the_end_and_an_overflow() {
        let d = dims(&[("time", 2), ("lat", 3)]);
        for region in [
            Region::new(vec![0], vec![2]),
            Region::new(vec![0, 2], vec![2, 2]),
            Region::new(vec![usize::MAX, 0], vec![2, 1]),
            Region::new(vec![0, 0], vec![2]),
        ] {
            let error = region.check("v", &d).unwrap_err();
            assert!(matches!(error, Error::InvalidRegion { ref variable, .. } if variable == "v"));
        }
    }
}
