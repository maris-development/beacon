//! The grid and column views of a dataset.

use std::collections::HashSet;
use std::sync::Arc;

use indexmap::IndexSet;
use nd_arrow_array::{Attributes, Dimensions};

use crate::dataset::{used_dimensions, variables};
use crate::{Error, NdDataset, NdVariable, Result};

/// The grid that a scan reads from each file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GridSelection {
    /// The grid of `default_grid`, for each file.
    PerFile,
    /// These dimension names, for each file.
    ///
    /// An empty list keeps only the rank-0 variables.
    Given(Vec<String>),
}

/// A dataset that shows a part of the variables of a wrapped dataset.
#[derive(Debug)]
struct View {
    inner: Arc<dyn NdDataset>,
    dimensions: Dimensions,
    names: IndexSet<Arc<str>>,
}

impl NdDataset for View {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn dimensions(&self) -> &Dimensions {
        &self.dimensions
    }

    fn attributes(&self) -> &Attributes {
        self.inner.attributes()
    }

    fn variable_names(&self) -> Vec<Arc<str>> {
        self.names.iter().cloned().collect()
    }

    fn variable(&self, name: &str) -> Option<Arc<dyn NdVariable>> {
        if self.names.contains(name) {
            self.inner.variable(name)
        } else {
            None
        }
    }

    fn invented_dimensions(&self) -> &[Arc<str>] {
        self.inner.invented_dimensions()
    }
}

/// The variables whose dimensions are all in the grid, and the dimensions they use.
///
/// The result has only the dimensions that a kept variable uses, in the
/// order of the dataset, not the order of the grid. `PerFile` keeps each
/// variable when `default_grid` gives `None`. It then returns the same `Arc`
/// when a variable uses each dimension of the dataset.
/// A dimension of the grid that the dataset does not have gives `Error::OffGrid`.
/// A name that is not unique or gives no variable gives `Error::Invalid`.
pub fn select_grid(
    dataset: Arc<dyn NdDataset>,
    grid: &GridSelection,
) -> Result<Arc<dyn NdDataset>> {
    let variables = variables(dataset.as_ref())?;
    let grid = match grid {
        GridSelection::PerFile => default_grid(dataset.as_ref()),
        GridSelection::Given(names) => Some(names.clone()),
    };
    let Some(grid) = grid else {
        // Each variable fits, so only an unused dimension can go.
        let dimensions = used_dimensions(dataset.as_ref(), &variables)?;
        if dimensions.rank() == dataset.dimensions().rank() {
            return Ok(dataset);
        }
        let names = dataset.variable_names().into_iter().collect();
        return Ok(Arc::new(View {
            inner: dataset,
            dimensions,
            names,
        }));
    };
    if let Some(unknown) = grid.iter().find(|n| !dataset.dimensions().contains(n)) {
        return Err(Error::OffGrid {
            dataset: dataset.name().to_string(),
            reason: format!("the file has no dimension '{unknown}'"),
            grid,
        });
    }
    let (kept, removed): (Vec<_>, Vec<_>) = variables.into_iter().partition(|variable| {
        variable
            .dimensions()
            .iter()
            .all(|d| grid.iter().any(|g| g == d.name()))
    });
    // Keep only the dimensions of a kept variable, so that no orphan inflates the grid.
    let dimensions = used_dimensions(dataset.as_ref(), &kept)?;
    if !removed.is_empty() {
        let removed: Vec<&str> = removed.iter().map(|v| v.name()).collect();
        tracing::debug!(
            "{}: the grid {grid:?} removes the variables {removed:?}",
            dataset.name()
        );
    }
    let names = kept.iter().map(|v| Arc::from(v.name())).collect();
    Ok(Arc::new(View {
        inner: dataset,
        dimensions,
        names,
    }))
}

/// The variables whose name is in `names`, in the order of the dataset.
///
/// The view keeps all dimensions. An unknown name has no effect.
pub fn select_columns(dataset: Arc<dyn NdDataset>, names: &[&str]) -> Arc<dyn NdDataset> {
    let wanted: HashSet<&str> = names.iter().copied().collect();
    let kept = dataset
        .variable_names()
        .into_iter()
        .filter(|name| wanted.contains(&**name))
        .collect();
    Arc::new(View {
        dimensions: dataset.dimensions().clone(),
        inner: dataset,
        names: kept,
    })
}

/// The key of a candidate grid. Tuples compare in order, and a larger key wins.
type GridKey = (bool, u128, u128, u128);

/// The grid for a scan with no given grid.
///
/// `None` when each variable broadcasts onto the variable of the highest
/// rank. Else the grid is the dimension list of one variable.
/// Each candidate gets a key, and a later candidate wins only with a strictly
/// larger key: (1) more than 0 rows, (2) the count of variables of rank 1 or
/// more that the grid keeps, (3) the count of variables with exactly this
/// dimension list, (4) the row count of the grid.
///
/// When the reader invented each dimension of the file, the row count moves
/// to the second place, before the two counts.
pub fn default_grid(dataset: &dyn NdDataset) -> Option<Vec<String>> {
    let variables: Vec<Vec<String>> = dataset
        .variable_names()
        .iter()
        .filter_map(|name| dataset.variable(name))
        .map(|v| {
            v.dimensions()
                .names()
                .into_iter()
                .map(String::from)
                .collect()
        })
        .collect();

    // `max_by_key` gives the last variable of the highest rank.
    let max_dims = variables.iter().max_by_key(|dims| dims.len())?;
    let needs_narrowing = variables
        .iter()
        .any(|dims| !dims.iter().all(|d| max_dims.contains(d)));
    if !needs_narrowing {
        return None;
    }

    let mut candidates: Vec<&Vec<String>> = Vec::new();
    for dims in &variables {
        if !dims.is_empty() && !candidates.contains(&dims) {
            candidates.push(dims);
        }
    }

    // Rank-0 variables fit each grid, so they do not count.
    let score = |c: &[String]| -> u128 {
        variables
            .iter()
            .filter(|dims| !dims.is_empty() && dims.iter().all(|d| c.contains(d)))
            .count() as u128
    };
    let exact_count =
        |c: &[String]| -> u128 { variables.iter().filter(|dims| *dims == c).count() as u128 };
    let grid_rows = |c: &[String]| -> u128 {
        c.iter()
            .map(|d| dataset.dimensions().size_of(d).unwrap_or(0) as u128)
            .product()
    };
    let invented = dataset.invented_dimensions();
    let names_no_dimension = !invented.is_empty()
        && dataset
            .dimensions()
            .iter()
            .all(|d| invented.iter().any(|i| &**i == d.name()));

    let key = |c: &[String]| -> GridKey {
        let rows = grid_rows(c);
        if names_no_dimension {
            (rows > 0, rows, score(c), exact_count(c))
        } else {
            (rows > 0, score(c), exact_count(c), rows)
        }
    };

    let mut best: Option<(GridKey, &Vec<String>)> = None;
    for candidate in candidates {
        let candidate_key = key(candidate);
        // An equal key keeps the earlier candidate.
        if best
            .as_ref()
            .is_none_or(|(best_key, _)| candidate_key > *best_key)
        {
            best = Some((candidate_key, candidate));
        }
    }
    best.map(|(_, dims)| dims.clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::memory::testing::{dataset, dims, meta, ramp};
    use crate::{ChunkGrid, DatasetSchema, Error, MemoryDataset, NdDataset, validate};

    fn grid(names: &[&str]) -> Option<Vec<String>> {
        Some(names.iter().map(|n| n.to_string()).collect())
    }

    fn given(names: &[&str]) -> GridSelection {
        GridSelection::Given(names.iter().map(|n| n.to_string()).collect())
    }

    fn names(view: &Arc<dyn NdDataset>) -> Vec<String> {
        view.variable_names()
            .iter()
            .map(|n| n.to_string())
            .collect()
    }

    fn invented(names: &[&str]) -> Vec<Arc<str>> {
        names.iter().map(|n| Arc::from(*n)).collect()
    }

    /// temp(time,lat,lon), time, lat, lon, lat_bnds(lat,nv), title().
    fn cf_bounds_dataset() -> MemoryDataset {
        dataset(vec![
            ramp("temp", &[("time", 2), ("lat", 3), ("lon", 4)]),
            ramp("time", &[("time", 2)]),
            ramp("lat", &[("lat", 3)]),
            ramp("lon", &[("lon", 4)]),
            ramp("lat_bnds", &[("lat", 3), ("nv", 2)]),
            ramp("title", &[]),
        ])
    }

    // The tests of `default_grid`.

    #[test]
    fn default_dims_already_safe_returns_none() {
        let ds = dataset(vec![
            ramp("grid", &[("x", 1), ("y", 1)]),
            ramp("scale", &[("y", 1)]),
        ]);
        assert_eq!(default_grid(&ds), None);
    }

    #[test]
    fn default_dims_picks_group_with_most_variables() {
        let ds = dataset(vec![
            ramp("temp", &[("x", 1), ("y", 1), ("z", 1)]),
            ramp("bnds", &[("y", 1), ("nv", 1)]),
            ramp("x", &[("x", 1)]),
            ramp("y", &[("y", 1)]),
            ramp("z", &[("z", 1)]),
        ]);
        assert_eq!(default_grid(&ds), grid(&["x", "y", "z"]));
    }

    #[test]
    fn default_dims_tie_prefers_larger_volume() {
        let ds = dataset(vec![
            ramp("small", &[("c", 2), ("d", 2)]),
            ramp("big", &[("a", 10), ("b", 10)]),
        ]);
        assert_eq!(default_grid(&ds), grid(&["a", "b"]));
    }

    fn unnamed_file_variables() -> Vec<crate::MemoryVariable> {
        vec![
            ramp("data", &[("len_1250", 1250), ("len_23250", 23250)]),
            ramp("meta_a", &[("len_3", 3)]),
            ramp("meta_b", &[("len_3", 3)]),
            ramp("meta_c", &[("len_3", 3)]),
        ]
    }

    #[test]
    fn default_dims_of_an_unnamed_file_follow_the_volume() {
        let named = dataset(unnamed_file_variables());
        assert_eq!(default_grid(&named), grid(&["len_3"]));

        let unnamed = dataset(unnamed_file_variables()).with_invented_dimensions(invented(&[
            "len_1250",
            "len_23250",
            "len_3",
        ]));
        assert_eq!(default_grid(&unnamed), grid(&["len_1250", "len_23250"]));
    }

    #[test]
    fn one_named_dimension_keeps_the_variable_count_first() {
        let ds = dataset(vec![
            ramp("data", &[("time", 1250), ("len_23250", 23250)]),
            ramp("meta_a", &[("len_3", 3)]),
            ramp("meta_b", &[("len_3", 3)]),
            ramp("meta_c", &[("len_3", 3)]),
        ])
        .with_invented_dimensions(invented(&["len_23250", "len_3"]));
        assert_eq!(default_grid(&ds), grid(&["len_3"]));
    }

    #[test]
    fn an_unnamed_file_still_skips_an_empty_grid() {
        let ds = dataset(vec![
            ramp("growing", &[("len_0", 0), ("len_9", 9)]),
            ramp("payload", &[("len_4", 4)]),
        ])
        .with_invented_dimensions(invented(&["len_0", "len_9", "len_4"]));
        assert_eq!(default_grid(&ds), grid(&["len_4"]));
    }

    #[test]
    fn default_dims_skips_empty_grid_for_nonempty_alternative() {
        let ds = dataset(vec![
            ramp("history_a", &[("hist", 0), ("prof", 5)]),
            ramp("history_b", &[("hist", 0), ("prof", 5)]),
            ramp("history_c", &[("hist", 0), ("prof", 5)]),
            ramp("pres", &[("prof", 5), ("lev", 3)]),
        ]);
        assert_eq!(default_grid(&ds), grid(&["prof", "lev"]));
    }

    #[test]
    fn default_dims_tie_on_count_prefers_native_data_grid() {
        let ds = dataset(vec![
            ramp("pres", &[("prof", 1), ("levels", 1)]),
            ramp("temp", &[("prof", 1), ("levels", 1)]),
            ramp("psal", &[("prof", 1), ("levels", 1)]),
            ramp("history_date", &[("hist", 1), ("prof", 1), ("time", 1)]),
            ramp("history_institution", &[("hist", 1), ("prof", 1)]),
            ramp("date_creation", &[("time", 1)]),
            ramp("juld", &[("prof", 1)]),
        ]);
        assert_eq!(default_grid(&ds), grid(&["prof", "levels"]));
    }

    #[test]
    fn default_dims_scalars_and_attributes_do_not_sway_choice() {
        let ds = dataset(vec![
            ramp("temp", &[("prof", 1), ("levels", 1)]),
            ramp("psal", &[("prof", 1), ("levels", 1)]),
            ramp("history_date", &[("hist", 1), ("prof", 1), ("time", 1)]),
            ramp("temp.units", &[]),
            ramp("psal.units", &[]),
            ramp(".Conventions", &[]),
            ramp(".title", &[]),
        ]);
        assert_eq!(default_grid(&ds), grid(&["prof", "levels"]));
    }

    #[test]
    fn default_dims_full_tie_is_deterministic_first_encountered() {
        let ds = dataset(vec![
            ramp("first", &[("a", 1), ("b", 1)]),
            ramp("second", &[("c", 1), ("d", 1)]),
        ]);
        assert_eq!(default_grid(&ds), grid(&["a", "b"]));
    }

    #[test]
    fn default_dims_all_scalar_returns_none() {
        let ds = dataset(vec![ramp("attr", &[])]);
        assert_eq!(default_grid(&ds), None);
    }

    #[test]
    fn default_dims_empty_dataset_returns_none() {
        let ds = dataset(vec![]);
        assert_eq!(default_grid(&ds), None);
    }

    // The tests of `select_grid` with a given grid.

    #[test]
    fn project_with_dimensions_drops_incompatible_variables() {
        let ds = dataset(vec![
            ramp("temp", &[("x", 2), ("y", 3)]),
            ramp("y_bnds", &[("y", 3), ("nv", 2)]),
            ramp("y", &[("y", 3)]),
        ]);
        let view = select_grid(Arc::new(ds), &given(&["x", "y"])).unwrap();
        assert_eq!(names(&view), ["temp", "y"]);
    }

    #[test]
    fn project_with_dimensions_prunes_orphaned_dimensions() {
        let ds = dataset(vec![
            ramp("temp", &[("x", 2), ("y", 3)]),
            ramp("y_bnds", &[("y", 3), ("nv", 2)]),
        ]);
        assert!(ds.dimensions().contains("nv"));
        let view = select_grid(Arc::new(ds), &given(&["x", "y"])).unwrap();
        assert_eq!(view.dimensions().names(), vec!["x", "y"]);
    }

    #[test]
    fn project_with_dimensions_keeps_original_dimension_order() {
        let ds = dataset(vec![ramp("temp", &[("x", 2), ("y", 3), ("z", 4)])]);
        let view = select_grid(Arc::new(ds), &given(&["z", "x", "y"])).unwrap();
        assert_eq!(view.dimensions().names(), vec!["x", "y", "z"]);
    }

    #[test]
    fn project_with_dimensions_rejects_unknown_dimension() {
        let ds = dataset(vec![ramp("temp", &[("x", 1)])]);
        let error = select_grid(Arc::new(ds), &given(&["nope"])).unwrap_err();
        assert!(matches!(error, Error::OffGrid { .. }));
        assert!(error.to_string().contains("nope"), "{error}");
    }

    // The new tests.

    #[test]
    fn per_file_narrows_a_file_that_does_not_broadcast() {
        let ds = Arc::new(cf_bounds_dataset());
        let view = select_grid(ds, &GridSelection::PerFile).unwrap();
        assert_eq!(names(&view), ["temp", "time", "lat", "lon", "title"]);
        assert_eq!(view.dimensions().names(), vec!["time", "lat", "lon"]);
    }

    #[test]
    fn per_file_returns_a_broadcast_safe_file_as_it_is() {
        let ds: Arc<dyn NdDataset> = Arc::new(dataset(vec![
            ramp("temp", &[("time", 2), ("lat", 3)]),
            ramp("lat", &[("lat", 3)]),
        ]));
        let view = select_grid(ds.clone(), &GridSelection::PerFile).unwrap();
        assert!(Arc::ptr_eq(&ds, &view));
    }

    #[test]
    fn per_file_drops_a_dimension_that_no_variable_uses() {
        let time: Arc<dyn NdVariable> = Arc::new(meta("time", DataType::Float64, &[("time", 5)]));
        let ds =
            DatasetSchema::try_new("test.nd", dims(&[("time", 5), ("nv", 2)]), vec![time]).unwrap();
        let view = select_grid(Arc::new(ds), &GridSelection::PerFile).unwrap();
        assert_eq!(view.dimensions().names(), vec!["time"]);
        let count = select_columns(view, &[]);
        let grid = ChunkGrid::try_new(count.as_ref(), 8192).unwrap();
        assert_eq!(grid.chunks().map(|chunk| chunk.len()).sum::<usize>(), 5);
    }

    #[test]
    fn an_empty_given_grid_keeps_only_rank_0_variables() {
        let view =
            select_grid(Arc::new(cf_bounds_dataset()), &GridSelection::Given(vec![])).unwrap();
        assert_eq!(names(&view), ["title"]);
        assert_eq!(view.dimensions().rank(), 0);
    }

    #[test]
    fn a_given_grid_that_the_file_does_not_have_is_off_grid() {
        let error = select_grid(
            Arc::new(cf_bounds_dataset()),
            &GridSelection::Given(vec!["depth".into()]),
        )
        .unwrap_err();
        assert!(matches!(error, Error::OffGrid { ref grid, .. } if grid == &["depth".to_string()]));
        assert!(error.to_string().contains("depth"));
    }

    #[test]
    fn a_view_returns_the_variable_of_the_wrapped_dataset() {
        let ds = cf_bounds_dataset();
        let view = select_grid(Arc::new(ds.clone()), &GridSelection::PerFile).unwrap();
        assert!(view.variable("lat_bnds").is_none());
        assert!(view.variable("temp").is_some());
        validate(view.as_ref()).unwrap();
    }

    #[test]
    #[tracing_test::traced_test]
    fn the_removed_variables_are_logged_one_time() {
        select_grid(Arc::new(cf_bounds_dataset()), &GridSelection::PerFile).unwrap();
        logs_assert(|lines: &[&str]| {
            match lines.iter().filter(|l| l.contains("lat_bnds")).count() {
                1 => Ok(()),
                n => Err(format!("expected 1 line, found {n}")),
            }
        });
    }

    #[test]
    fn select_columns_keeps_the_named_variables_in_file_order() {
        let view = select_columns(Arc::new(cf_bounds_dataset()), &["lon", "nope", "temp"]);
        assert_eq!(names(&view), ["temp", "lon"]);
        assert_eq!(view.dimensions().rank(), 4);
    }

    #[test]
    fn select_columns_with_no_name_keeps_no_variable_and_all_dimensions() {
        let view = select_columns(Arc::new(cf_bounds_dataset()), &[]);
        assert!(view.variable_names().is_empty());
        assert_eq!(view.dimensions().names(), vec!["time", "lat", "lon", "nv"]);
    }

    #[test]
    fn a_grid_with_no_rows_loses_to_a_grid_with_rows() {
        // a(n=0, m=2), b(n=0, m=2), c(k=3): the larger score is on the empty grid.
        let ds = dataset(vec![
            ramp("a", &[("n", 0), ("m", 2)]),
            ramp("b", &[("n", 0), ("m", 2)]),
            ramp("c", &[("k", 3)]),
        ]);
        assert_eq!(default_grid(&ds), Some(vec!["k".to_string()]));
    }
}
