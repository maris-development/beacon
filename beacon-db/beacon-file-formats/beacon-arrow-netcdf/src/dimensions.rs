//! Unify the dimensions netCDF invents for a file that names none.
//!
//! # Why a file needs this
//!
//! A plain HDF5 file holds no dimension scale, so no axis of it has a name.
//! netCDF needs one for every axis, so it invents `phony_dim_N`. [`oxcdf`] hands
//! those names out the way `ncdump` does: one counter over the whole file, and
//! one name for every axis of one length **inside one group**.
//!
//! That rule keeps two groups apart. A DAS file puts its payload in the root
//! group, `data(1250, 23250)`, and the description of each channel in another,
//! `header/channels(23250)`. The two 23250-long axes get two names, so beacon's
//! ND engine treats them as two dimensions. The channel description then does
//! not broadcast against the payload, and a `SELECT` of both either drops one of
//! them or fails.
//!
//! # What this module does
//!
//! It gives every invented dimension of one length the same name,
//! `phony_len_<length>`, whatever group it sits in. The two axes above become
//! one, so the payload and the channel description join by row.
//!
//! A named dimension is never touched. A NetCDF-4 file therefore reads exactly
//! as it did.
//!
//! # The rules
//!
//! 1. Rename an invented dimension only. A dimension the file names keeps it.
//! 2. Leave an empty axis alone. netCDF gives each one a dimension of its own,
//!    because it has no fixed dimension of length zero.
//! 3. Leave a growable axis alone. Its length is what it holds today, so two
//!    growable axes of one length are equal by accident, not by design.
//! 4. Never put one dimension on two axes of one variable. The second axis
//!    keeps the name [`oxcdf`] gave it, which is the rule netCDF itself follows.
//! 5. Leave an axis alone when the file already names another dimension
//!    `phony_len_<length>`, so a rename never shadows a real name.
//!
//! # The trade-off
//!
//! Rule 1 merges two axes of one length even when they describe different
//! things. That is a heuristic. It is the only one available, because a plain
//! HDF5 file records nothing else about an axis. A query of two such columns
//! joins them by row instead of dropping one.
//!
//! [`PhonyDimensions::merges`] reports every merge, and
//! [`PhonyDimensions::log_merges`] writes one debug line for each, so a join
//! nobody asked for can be traced to the rule that made it.

use std::collections::{BTreeMap, HashMap, HashSet};

use oxcdf::{netcdf::NcGroup, AsyncNetcdfFile};

/// The prefix netCDF puts on a dimension it invents.
///
/// netcdf-c writes the same names, so the netcdf-c reader recognises its own
/// invented dimensions by this prefix. The [`oxcdf`] reader does not need it:
/// that reader reports `is_phony` for each dimension.
pub const INVENTED_PREFIX: &str = "phony_dim_";

/// One axis, as the rules above see it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Axis {
    /// The dimension name the reader gave the axis.
    pub name: String,
    /// The number of elements on the axis.
    pub len: u64,
    /// Whether the axis can grow.
    pub is_unlimited: bool,
    /// Whether the reader invented the dimension because the file names none.
    pub is_phony: bool,
}

impl Axis {
    /// One axis of the netcdf-c reader, which reports no `is_phony` flag.
    ///
    /// netcdf-c invents the same `phony_dim_N` names, so the prefix identifies
    /// them. A file that names a dimension `phony_dim_0` itself is treated as
    /// invented, which costs nothing: the name carries no meaning either way.
    pub fn from_name(name: impl Into<String>, len: u64, is_unlimited: bool) -> Self {
        let name = name.into();
        let is_phony = name.starts_with(INVENTED_PREFIX);
        Self {
            name,
            len,
            is_unlimited,
            is_phony,
        }
    }
}

/// The name every invented dimension of `len` elements shares.
fn unified_name(len: u64) -> String {
    format!("phony_len_{len}")
}

/// The renames one file needs, built once per open.
///
/// [`PhonyDimensions::none`] renames nothing, for a caller that wants the names
/// the reader gave.
#[derive(Debug, Clone, Default)]
pub struct PhonyDimensions {
    /// The name the reader gave, and the name every axis of that length shares.
    renames: HashMap<String, String>,
    /// Every name an invented dimension can end up under.
    ///
    /// That is the unified name and the name the reader gave, because rules 2
    /// to 5 each hand an axis its own name back. `SELECT *` reads this to tell
    /// a file that names its axes from one that names none; see
    /// [`beacon_nd_array::dataset::Dataset::invented_dimensions`]. It asks
    /// whether a name is invented, so holding both spellings is right.
    invented: HashSet<String>,
    /// The same dimensions under the names the reader gave, for
    /// [`PhonyDimensions::without_renames`].
    invented_as_given: HashSet<String>,
}

impl PhonyDimensions {
    /// Rename nothing.
    pub fn none() -> Self {
        Self::default()
    }

    /// The renames of one open file, over every group of it.
    pub fn of_file(file: &AsyncNetcdfFile) -> Self {
        let mut axes = Vec::new();
        collect_axes(file.root(), &mut axes);
        Self::of_axes(axes)
    }

    /// The renames for a set of axes.
    ///
    /// Every axis of the file must be present. Rule 5 needs the named ones to
    /// know which names are already taken.
    pub fn of_axes(axes: impl IntoIterator<Item = Axis>) -> Self {
        let axes: Vec<Axis> = axes.into_iter().collect();

        // Rule 5: a name the file uses itself is out of reach.
        let taken: HashSet<&str> = axes
            .iter()
            .filter(|axis| !axis.is_phony)
            .map(|axis| axis.name.as_str())
            .collect();

        let mut renames = HashMap::new();
        let mut invented = HashSet::new();
        let mut invented_as_given = HashSet::new();
        for axis in &axes {
            // Rule 1.
            if !axis.is_phony {
                continue;
            }
            invented_as_given.insert(axis.name.clone());
            // The name the reader gave counts as invented whatever happens
            // below: rule 4 hands it back to any axis whose unified name an
            // earlier axis of the same variable already holds.
            invented.insert(axis.name.clone());

            // Rules 2, 3 and 5: the axis keeps the name the reader gave it.
            let unified = unified_name(axis.len);
            if axis.is_unlimited || axis.len == 0 || taken.contains(unified.as_str()) {
                continue;
            }
            invented.insert(unified.clone());
            renames.insert(axis.name.clone(), unified);
        }

        Self {
            renames,
            invented,
            invented_as_given,
        }
    }

    /// This without its renames, for a reader told not to unify.
    ///
    /// Every axis is still invented, and [`PhonyDimensions::invented_names`]
    /// still reports it. Only the names go back to the ones the reader gave.
    pub fn without_renames(self) -> Self {
        Self {
            renames: HashMap::new(),
            invented: self.invented_as_given.clone(),
            invented_as_given: self.invented_as_given,
        }
    }

    /// The name of every invented dimension, as the arrays report it.
    pub fn invented_names(&self) -> &HashSet<String> {
        &self.invented
    }

    /// The axes that became one dimension, and the dimension they became.
    ///
    /// A rename of one axis is not news: the name changes and nothing else. Two
    /// axes under one name is the heuristic at work, and a query that selects a
    /// column of each joins them row by row. Only a file of several groups can
    /// produce one, because [`oxcdf`] already shares an axis by length inside
    /// one group.
    ///
    /// Sorted, so two reads of one file report the same thing.
    pub fn merges(&self) -> Vec<(String, Vec<String>)> {
        let mut by_target: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for (from, to) in &self.renames {
            by_target
                .entry(to.as_str())
                .or_default()
                .push(from.as_str());
        }
        by_target
            .into_iter()
            .filter(|(_, sources)| sources.len() > 1)
            .map(|(target, mut sources)| {
                sources.sort_unstable();
                (
                    target.to_string(),
                    sources.into_iter().map(String::from).collect(),
                )
            })
            .collect()
    }

    /// Say which axes became one dimension, once for the file `label` names.
    ///
    /// The merge is a heuristic, and a wrong one costs a query a join it did
    /// not ask for. This is the only place that shows it happened. See
    /// [`PhonyDimensions::merges`].
    pub fn log_merges(&self, label: &str) {
        for (dimension, sources) in self.merges() {
            tracing::debug!(
                file = %label,
                "dimension '{dimension}' covers {sources:?}, which the file keeps apart. \
                 A query that selects a column of each joins them row by row."
            );
        }
    }

    /// This with a second layer of renames on top.
    ///
    /// `extra` maps the name an axis carries **after** the unification to the
    /// name it should carry instead, so a caller works from what
    /// [`PhonyDimensions::apply`] would give it. A convention layer names the
    /// axes of a file it recognises this way, and a hand-written map does the
    /// same.
    ///
    /// A renamed axis stays invented. The name changes; where it came from does
    /// not, and `SELECT *` reads the provenance rather than the spelling. See
    /// [`beacon_nd_array::dataset::Dataset::invented_dimensions`].
    pub fn rename(mut self, extra: &HashMap<String, String>) -> Self {
        if extra.is_empty() {
            return self;
        }

        // An axis this already renames takes the new name instead.
        for target in self.renames.values_mut() {
            if let Some(renamed) = extra.get(target.as_str()) {
                *target = renamed.clone();
            }
        }
        // An axis it does not rename is still invented, and `extra` may name it
        // — an empty or growable axis reaches this way.
        for (from, to) in extra {
            if self.invented.contains(from) && !self.renames.values().any(|held| held == to) {
                self.renames.insert(from.clone(), to.clone());
            }
        }

        for name in extra.values() {
            self.invented.insert(name.clone());
        }
        self
    }

    /// Whether this renames anything.
    pub fn is_empty(&self) -> bool {
        self.renames.is_empty()
    }

    /// How many dimensions this renames.
    pub fn len(&self) -> usize {
        self.renames.len()
    }

    /// The dimension names of one variable, after the renames.
    ///
    /// Rule 4 applies here: an axis whose new name an earlier axis of the same
    /// variable already holds keeps the name the reader gave it.
    pub fn apply(&self, dimensions: &[String]) -> Vec<String> {
        if self.renames.is_empty() {
            return dimensions.to_vec();
        }

        let mut out: Vec<String> = Vec::with_capacity(dimensions.len());
        for name in dimensions {
            match self.renames.get(name) {
                Some(unified) if !out.iter().any(|held| held == unified) => {
                    out.push(unified.clone())
                }
                _ => out.push(name.clone()),
            }
        }
        out
    }
}

/// Add the dimensions of `group` and of every group inside it.
fn collect_axes(group: &NcGroup, out: &mut Vec<Axis>) {
    for dimension in &group.dimensions {
        out.push(Axis {
            name: dimension.name.clone(),
            len: dimension.len,
            is_unlimited: dimension.is_unlimited,
            is_phony: dimension.is_phony,
        });
    }
    for child in &group.groups {
        collect_axes(child, out);
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn phony(name: &str, len: u64) -> Axis {
        Axis {
            name: name.to_string(),
            len,
            is_unlimited: false,
            is_phony: true,
        }
    }

    fn named(name: &str, len: u64) -> Axis {
        Axis {
            name: name.to_string(),
            len,
            is_unlimited: false,
            is_phony: false,
        }
    }

    fn apply(axes: Vec<Axis>, dimensions: &[&str]) -> Vec<String> {
        let names: Vec<String> = dimensions.iter().map(|d| d.to_string()).collect();
        PhonyDimensions::of_axes(axes).apply(&names)
    }

    /// Two groups hold an axis of one length. Both get one name, so the two
    /// variables broadcast.
    #[test]
    fn two_groups_share_one_name_per_length() {
        let axes = vec![phony("phony_dim_33", 23250), phony("phony_dim_8", 23250)];
        assert_eq!(
            apply(axes.clone(), &["phony_dim_33"]),
            vec!["phony_len_23250".to_string()]
        );
        assert_eq!(
            apply(axes, &["phony_dim_8"]),
            vec!["phony_len_23250".to_string()]
        );
    }

    /// Two lengths stay two dimensions.
    #[test]
    fn two_lengths_stay_apart() {
        let axes = vec![phony("phony_dim_32", 1250), phony("phony_dim_33", 23250)];
        assert_eq!(
            apply(axes, &["phony_dim_32", "phony_dim_33"]),
            vec!["phony_len_1250".to_string(), "phony_len_23250".to_string()]
        );
    }

    /// A dimension the file names is never renamed.
    #[test]
    fn a_named_dimension_is_left_alone() {
        let axes = vec![named("time", 10), named("depth", 10)];
        assert_eq!(
            apply(axes, &["time", "depth"]),
            vec!["time".to_string(), "depth".to_string()]
        );
    }

    /// A file that names some axes and not others keeps the names it has.
    #[test]
    fn a_named_axis_survives_beside_an_invented_one() {
        let axes = vec![named("time", 10), phony("phony_dim_0", 10)];
        assert_eq!(
            apply(axes, &["time", "phony_dim_0"]),
            vec!["time".to_string(), "phony_len_10".to_string()]
        );
    }

    /// netCDF has no fixed dimension of length zero, so each empty axis keeps
    /// the dimension of its own that the reader gave it.
    #[test]
    fn an_empty_axis_keeps_its_name() {
        let axes = vec![phony("phony_dim_2", 0), phony("phony_dim_4", 0)];
        assert_eq!(
            apply(axes, &["phony_dim_2"]),
            vec!["phony_dim_2".to_string()]
        );
    }

    /// A growable axis holds what it holds today, so two of one length are
    /// equal by accident.
    #[test]
    fn a_growable_axis_keeps_its_name() {
        let axes = vec![
            Axis {
                name: "phony_dim_1".to_string(),
                len: 12,
                is_unlimited: true,
                is_phony: true,
            },
            phony("phony_dim_7", 12),
        ];
        assert_eq!(
            apply(axes, &["phony_dim_1"]),
            vec!["phony_dim_1".to_string()]
        );
    }

    /// netCDF never puts one dimension on two axes of one variable, so the
    /// second axis of one length keeps the name the reader gave it.
    #[test]
    fn one_variable_never_holds_one_dimension_twice() {
        let axes = vec![phony("phony_dim_0", 4), phony("phony_dim_1", 4)];
        assert_eq!(
            apply(axes, &["phony_dim_0", "phony_dim_1"]),
            vec!["phony_len_4".to_string(), "phony_dim_1".to_string()]
        );
    }

    /// A rename never shadows a name the file uses itself.
    #[test]
    fn a_real_name_is_never_shadowed() {
        let axes = vec![named("phony_len_5", 5), phony("phony_dim_0", 5)];
        assert_eq!(
            apply(axes, &["phony_dim_0"]),
            vec!["phony_dim_0".to_string()]
        );
    }

    /// The netcdf-c reader reports no flag, so the prefix identifies an
    /// invented dimension.
    #[test]
    fn the_netcdf_c_reader_recognises_its_own_names() {
        assert!(Axis::from_name("phony_dim_3", 8, false).is_phony);
        assert!(!Axis::from_name("time", 8, false).is_phony);
        assert_eq!(
            PhonyDimensions::of_axes(vec![Axis::from_name("phony_dim_3", 8, false)])
                .apply(&["phony_dim_3".to_string()]),
            vec!["phony_len_8".to_string()]
        );
    }

    /// A file with no invented dimension pays nothing and changes nothing.
    #[test]
    fn a_file_that_names_every_axis_renames_nothing() {
        let phony = PhonyDimensions::of_axes(vec![named("time", 3)]);
        assert!(phony.is_empty());
        assert_eq!(phony.apply(&["time".to_string()]), vec!["time".to_string()]);
    }

    /// A merge is two axes under one name. A rename of one axis alone is not.
    #[test]
    fn it_reports_the_axes_that_became_one() {
        let merged = PhonyDimensions::of_axes(vec![
            phony("phony_dim_33", 23250),
            phony("phony_dim_8", 23250),
            phony("phony_dim_3", 23250),
            // 1250 long, and alone at that length.
            phony("phony_dim_32", 1250),
        ]);
        assert_eq!(
            merged.merges(),
            vec![(
                "phony_len_23250".to_string(),
                vec![
                    "phony_dim_3".to_string(),
                    "phony_dim_33".to_string(),
                    "phony_dim_8".to_string(),
                ]
            )],
            "the axis that is alone at its length is a rename, not a merge"
        );
    }

    /// A file of one group holds one axis per length already, so it merges
    /// nothing.
    #[test]
    fn one_group_reports_no_merge() {
        let single =
            PhonyDimensions::of_axes(vec![phony("phony_dim_0", 6), phony("phony_dim_1", 4)]);
        assert!(single.merges().is_empty());
    }

    /// `none` is the identity.
    #[test]
    fn none_renames_nothing() {
        let names = vec!["phony_dim_0".to_string()];
        assert_eq!(PhonyDimensions::none().apply(&names), names);
    }
}
