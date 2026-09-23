//! Pure discovery logic: turn a flat list of classified files into candidate
//! external tables, detecting Hive-style partitions.
//!
//! Everything here is a pure function over `DatasetMetadata` (the output of
//! `FileManager::list_datasets`, which already classifies each object by format).
//! No object store, no session — so it is exhaustively unit-testable.

use std::collections::BTreeMap;

use beacon_datafusion_ext::format_ext::DatasetMetadata;

use super::definition::{CrawlerDefinition, TableNaming};

/// A discovered table-to-be: a group of same-format files sharing a base prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateTable {
    /// Format identifier / `file_type` (e.g. `parquet`, `nc`).
    pub format: String,
    /// The filename extension the files share, as the files spell it (e.g. `nc4`).
    pub extension: String,
    /// Base prefix relative to the datasets store, without trailing slash (may be empty).
    pub base: String,
    /// Hive partition column names, in path order. Empty when not partitioned.
    pub partition_cols: Vec<String>,
    /// Whether files live in sub-directories (so the listing glob must recurse).
    pub recursive: bool,
    /// Number of files grouped into this candidate.
    pub file_count: usize,
}

impl CandidateTable {
    /// Datasets-store-relative location (with glob) for `CREATE EXTERNAL TABLE`.
    ///
    /// Partitioned/nested layouts get a recursive `**/*.<ext>` glob; flat layouts get
    /// `*.<ext>`. An explicit glob is always supplied so the non-recursive default
    /// glob is never applied (which would miss partition sub-directories).
    pub fn location(&self) -> String {
        let glob = if self.recursive {
            format!("**/*.{}", self.extension)
        } else {
            format!("*.{}", self.extension)
        };
        if self.base.is_empty() {
            glob
        } else {
            format!("{}/{glob}", self.base)
        }
    }
}

/// Result of analysing a single file path.
struct PathAnalysis {
    base: String,
    partition_keys: Vec<String>,
    partitioned: bool,
}

/// Split a datasets-relative path into (base prefix, Hive partition keys).
///
/// `argo/year=2024/month=01/f.parquet` -> base `argo`, keys `[year, month]`.
/// `argo/2024/f.parquet`               -> base `argo/2024`, keys `[]` (flat).
fn analyze_path(file_path: &str) -> PathAnalysis {
    let mut segments: Vec<&str> = file_path.split('/').collect();
    // Drop the filename; only directory segments matter.
    segments.pop();

    let first_partition = segments.iter().position(|s| s.contains('='));

    match first_partition {
        None => PathAnalysis {
            base: segments.join("/"),
            partition_keys: Vec::new(),
            partitioned: false,
        },
        Some(idx) => {
            let base = segments[..idx].join("/");
            let partition_keys = segments[idx..]
                .iter()
                .filter_map(|s| s.split_once('=').map(|(k, _)| k.to_string()))
                .collect();
            PathAnalysis {
                base,
                partition_keys,
                partitioned: true,
            }
        }
    }
}

/// The filename extensions the crawler accepts for each format identifier.
///
/// A format that has no entry accepts only the extension equal to its identifier.
pub type FormatExtensions = BTreeMap<String, Vec<String>>;

/// The on-disk extension of a path's filename, as the file spells it, if any.
fn file_extension(file_path: &str) -> Option<&str> {
    let filename = file_path.rsplit('/').next().unwrap_or(file_path);
    filename
        .rsplit_once('.')
        .map(|(_, ext)| ext)
        .filter(|ext| !ext.is_empty())
}

/// Whether `format` accepts files with `extension`, in any case.
fn accepts_extension(extensions: &FormatExtensions, format: &str, extension: &str) -> bool {
    match extensions.get(format) {
        Some(known) => known.iter().any(|ext| ext.eq_ignore_ascii_case(extension)),
        None => format.eq_ignore_ascii_case(extension),
    }
}

/// Accumulator while grouping files by `(format, extension, base)`.
struct Group {
    format: String,
    extension: String,
    base: String,
    partitioned: bool,
    /// `Some` while all partitioned members agree on the ordered key list; `None`
    /// once an inconsistency is seen (then partition columns are dropped).
    keys: Option<Vec<String>>,
    file_count: usize,
}

/// Group classified files into candidate tables.
///
/// A file is only crawled when `extensions` lists its on-disk extension for its
/// `format` — this cleanly excludes marker/dir-based formats (zarr/atlas markers
/// are `*.json`) and prevents geoparquet from double-claiming `*.parquet` files.
/// Skipped files are returned for reporting.
pub fn group_into_tables(
    datasets: &[DatasetMetadata],
    def: &CrawlerDefinition,
    extensions: &FormatExtensions,
) -> (Vec<CandidateTable>, Vec<String>) {
    let mut groups: BTreeMap<(String, String, String), Group> = BTreeMap::new();
    let mut skipped: Vec<String> = Vec::new();

    for ds in datasets {
        let format = ds.format.to_lowercase();

        if let Some(filter) = &def.format_filter {
            if !filter.contains(&format) {
                continue;
            }
        }

        let extension = match file_extension(&ds.file_path) {
            Some(ext) if accepts_extension(extensions, &format, ext) => ext.to_string(),
            _ => {
                skipped.push(ds.file_path.clone());
                continue;
            }
        };

        let analysis = if def.detect_partitions {
            analyze_path(&ds.file_path)
        } else {
            // Partition detection off: treat the whole directory as the base.
            let mut segments: Vec<&str> = ds.file_path.split('/').collect();
            segments.pop();
            PathAnalysis {
                base: segments.join("/"),
                partition_keys: Vec::new(),
                partitioned: false,
            }
        };

        let entry = groups
            .entry((format.clone(), extension.clone(), analysis.base.clone()))
            .or_insert_with(|| Group {
                format: format.clone(),
                extension,
                base: analysis.base.clone(),
                partitioned: false,
                keys: Some(Vec::new()),
                file_count: 0,
            });

        entry.file_count += 1;
        if analysis.partitioned {
            entry.partitioned = true;
            match &entry.keys {
                // First partitioned member seeds the key list.
                Some(existing) if existing.is_empty() => {
                    entry.keys = Some(analysis.partition_keys);
                }
                // Subsequent members must agree, else drop partition columns.
                Some(existing) if *existing != analysis.partition_keys => {
                    entry.keys = None;
                }
                _ => {}
            }
        }
    }

    let candidates = groups
        .into_values()
        .map(|g| CandidateTable {
            format: g.format,
            extension: g.extension,
            base: g.base,
            partition_cols: if g.partitioned {
                g.keys.unwrap_or_default()
            } else {
                Vec::new()
            },
            recursive: g.partitioned,
            file_count: g.file_count,
        })
        .collect();

    (candidates, skipped)
}

/// Slugify a path leaf into a safe SQL table identifier.
fn slugify(input: &str) -> String {
    let s: String = input
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .collect();
    s.trim_matches('_').to_string()
}

/// The leaf component of a base prefix (`data/argo_floats` -> `argo_floats`).
fn leaf(base: &str) -> &str {
    base.rsplit('/').find(|s| !s.is_empty()).unwrap_or("")
}

/// Assign unique table names to candidates, applying the crawler's naming policy.
///
/// Collisions (two bases share a leaf, or differ only by extension) are disambiguated
/// by appending the extension and then a numeric suffix, so names stay deterministic.
pub fn assign_table_names(candidates: &[CandidateTable], def: &CrawlerDefinition) -> Vec<String> {
    let mut used: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut names = Vec::with_capacity(candidates.len());

    for cand in candidates {
        let leaf_slug = slugify(leaf(&cand.base));
        let stem = if leaf_slug.is_empty() {
            slugify(&def.name)
        } else {
            match def.table_naming {
                TableNaming::LeafPrefix => leaf_slug,
                TableNaming::CrawlerPrefixed => format!("{}_{}", slugify(&def.name), leaf_slug),
            }
        };

        // Deterministic disambiguation: prefer the bare stem, then stem_<extension>,
        // then a numeric suffix.
        let mut name = stem.clone();
        if used.contains(&name) {
            name = format!("{stem}_{}", cand.extension);
        }
        let base_name = name.clone();
        let mut n = 2;
        while used.contains(&name) {
            name = format!("{base_name}_{n}");
            n += 1;
        }

        used.insert(name.clone());
        names.push(name);
    }

    names
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ds(path: &str, format: &str) -> DatasetMetadata {
        DatasetMetadata::new(path.to_string(), format.to_string())
    }

    /// netCDF has aliases. Every other format falls back to its own name.
    fn extensions() -> FormatExtensions {
        FormatExtensions::from([(
            "nc".to_string(),
            ["nc", "nc3", "nc4", "netcdf"].map(String::from).to_vec(),
        )])
    }

    fn def() -> CrawlerDefinition {
        CrawlerDefinition {
            name: "c".to_string(),
            target_prefix: "".to_string(),
            format_filter: None,
            table_naming: TableNaming::LeafPrefix,
            detect_partitions: true,
            schedule_secs: None,
            event_driven: false,
            options: Default::default(),
        }
    }

    #[test]
    fn detects_hive_partitions() {
        let datasets = vec![
            ds("argo/year=2024/month=01/a.parquet", "parquet"),
            ds("argo/year=2024/month=02/b.parquet", "parquet"),
        ];
        let (cands, skipped) = group_into_tables(&datasets, &def(), &extensions());
        assert!(skipped.is_empty());
        assert_eq!(cands.len(), 1);
        let c = &cands[0];
        assert_eq!(c.base, "argo");
        assert_eq!(c.partition_cols, vec!["year".to_string(), "month".to_string()]);
        assert!(c.recursive);
        assert_eq!(c.file_count, 2);
        assert_eq!(c.location(), "argo/**/*.parquet");
    }

    #[test]
    fn flat_layout_has_no_partitions() {
        let datasets = vec![ds("argo/a.parquet", "parquet"), ds("argo/b.parquet", "parquet")];
        let (cands, _) = group_into_tables(&datasets, &def(), &extensions());
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].base, "argo");
        assert!(cands[0].partition_cols.is_empty());
        assert!(!cands[0].recursive);
        assert_eq!(cands[0].location(), "argo/*.parquet");
    }

    #[test]
    fn file_at_root() {
        let datasets = vec![ds("a.parquet", "parquet")];
        let (cands, _) = group_into_tables(&datasets, &def(), &extensions());
        assert_eq!(cands[0].base, "");
        assert_eq!(cands[0].location(), "*.parquet");
    }

    #[test]
    fn splits_by_format() {
        let datasets = vec![ds("d/a.parquet", "parquet"), ds("d/b.csv", "csv")];
        let (cands, _) = group_into_tables(&datasets, &def(), &extensions());
        assert_eq!(cands.len(), 2);
    }

    #[test]
    fn skips_marker_and_overlapping_formats() {
        // zarr marker (.json != zarr), geoparquet (.parquet != geoparquet)
        let datasets = vec![
            ds("d/foo.zarr/zarr.json", "zarr"),
            ds("d/g.parquet", "geoparquet"),
        ];
        let (cands, skipped) = group_into_tables(&datasets, &def(), &extensions());
        assert!(cands.is_empty());
        assert_eq!(skipped.len(), 2);
    }

    /// An Atlas collection is one file, `data.atlas`, so its extension is its
    /// format and the crawler can build a table over it. That is the difference
    /// from Zarr, whose store is a directory behind a `zarr.json`.
    ///
    /// Each collection lives in its own directory, and tables group by
    /// directory, so each becomes its own table. Several collections in one
    /// table is what an external table over a glob is for.
    #[test]
    fn an_atlas_collection_is_crawlable() {
        let datasets = vec![
            ds("d/january/data.atlas", "atlas"),
            ds("d/february/data.atlas", "atlas"),
        ];
        let (cands, skipped) = group_into_tables(&datasets, &def(), &extensions());
        assert!(skipped.is_empty(), "{skipped:?}");
        assert_eq!(cands.len(), 2, "one per directory");
        assert!(cands.iter().all(|table| table.format == "atlas"));
    }

    /// The glob keeps the file's own extension, so it matches the `.nc4` files.
    #[test]
    fn a_format_alias_is_crawlable() {
        let datasets = vec![ds("merra/a.nc4", "nc"), ds("merra/b.nc4", "nc")];
        let (cands, skipped) = group_into_tables(&datasets, &def(), &extensions());
        assert!(skipped.is_empty(), "{skipped:?}");
        assert_eq!(cands.len(), 1);
        assert_eq!(cands[0].format, "nc");
        assert_eq!(cands[0].location(), "merra/*.nc4");
    }

    /// One glob cannot match two extensions, so each extension gets its own table.
    #[test]
    fn each_extension_of_a_format_gets_its_own_table() {
        let datasets = vec![ds("d/a.nc", "nc"), ds("d/b.nc4", "nc")];
        let (cands, _) = group_into_tables(&datasets, &def(), &extensions());
        let locations: Vec<String> = cands.iter().map(CandidateTable::location).collect();
        assert_eq!(locations, ["d/*.nc", "d/*.nc4"]);
        assert_eq!(assign_table_names(&cands, &def()), ["d", "d_nc4"]);
    }

    /// The glob keeps the case of the file name, so a case-sensitive store matches it.
    #[test]
    fn an_upper_case_extension_keeps_its_case_in_the_glob() {
        let datasets = vec![ds("d/A.NC4", "nc")];
        let (cands, _) = group_into_tables(&datasets, &def(), &extensions());
        assert_eq!(cands[0].location(), "d/*.NC4");
    }

    #[test]
    fn an_extension_that_the_format_does_not_list_is_skipped() {
        let datasets = vec![ds("d/a.ncml", "nc"), ds("d/b.tsv", "csv")];
        let (cands, skipped) = group_into_tables(&datasets, &def(), &extensions());
        assert!(cands.is_empty());
        assert_eq!(skipped, ["d/a.ncml", "d/b.tsv"]);
    }

    #[test]
    fn inconsistent_partition_keys_drop_to_flat_recursive() {
        let datasets = vec![
            ds("d/year=2024/a.parquet", "parquet"),
            ds("d/region=eu/b.parquet", "parquet"),
        ];
        let (cands, _) = group_into_tables(&datasets, &def(), &extensions());
        assert_eq!(cands.len(), 1);
        assert!(cands[0].partition_cols.is_empty());
        assert!(cands[0].recursive); // still recursive so nested files are listed
        assert_eq!(cands[0].location(), "d/**/*.parquet");
    }

    #[test]
    fn format_filter_excludes() {
        let mut d = def();
        d.format_filter = Some(vec!["nc".to_string()]);
        let datasets = vec![ds("d/a.parquet", "parquet")];
        let (cands, _) = group_into_tables(&datasets, &d, &extensions());
        assert!(cands.is_empty());
    }

    #[test]
    fn detect_partitions_off_keeps_full_dir() {
        let mut d = def();
        d.detect_partitions = false;
        let datasets = vec![ds("argo/year=2024/a.parquet", "parquet")];
        let (cands, _) = group_into_tables(&datasets, &d, &extensions());
        assert_eq!(cands[0].base, "argo/year=2024");
        assert!(cands[0].partition_cols.is_empty());
    }

    #[test]
    fn names_leaf_and_prefixed() {
        let cands = vec![CandidateTable {
            format: "parquet".to_string(),
            extension: "parquet".to_string(),
            base: "data/argo_floats".to_string(),
            partition_cols: vec![],
            recursive: false,
            file_count: 1,
        }];
        assert_eq!(assign_table_names(&cands, &def()), vec!["argo_floats".to_string()]);

        let mut prefixed = def();
        prefixed.name = "ocean".to_string();
        prefixed.table_naming = TableNaming::CrawlerPrefixed;
        assert_eq!(
            assign_table_names(&cands, &prefixed),
            vec!["ocean_argo_floats".to_string()]
        );
    }

    /// A candidate rooted at the datasets store has no leaf to name it after, so
    /// it falls back to the crawler's own (slugified) name — an empty table name
    /// would be rejected by the catalog.
    #[test]
    fn root_candidates_are_named_after_the_crawler() {
        let cands = vec![CandidateTable {
            format: "parquet".to_string(),
            extension: "parquet".to_string(),
            base: String::new(),
            partition_cols: vec![],
            recursive: false,
            file_count: 1,
        }];
        let mut d = def();
        d.name = "Argo Floats!".to_string();
        assert_eq!(assign_table_names(&cands, &d), vec!["argo_floats".to_string()]);
    }

    /// Two candidates that share a leaf *and* a format cannot be disambiguated by
    /// appending the format, so the numeric suffix is the last resort. Names must
    /// stay unique and deterministic — they are used to register catalog tables.
    #[test]
    fn same_leaf_and_format_falls_back_to_a_numeric_suffix() {
        let candidate = |base: &str| CandidateTable {
            format: "parquet".to_string(),
            extension: "parquet".to_string(),
            base: base.to_string(),
            partition_cols: vec![],
            recursive: false,
            file_count: 1,
        };
        let cands = vec![candidate("a/data"), candidate("b/data"), candidate("c/data")];
        assert_eq!(
            assign_table_names(&cands, &def()),
            vec![
                "data".to_string(),
                "data_parquet".to_string(),
                "data_parquet_2".to_string()
            ]
        );
    }

    #[test]
    fn names_disambiguate_collisions() {
        let cands = vec![
            CandidateTable {
                format: "parquet".to_string(),
                extension: "parquet".to_string(),
                base: "a/data".to_string(),
                partition_cols: vec![],
                recursive: false,
                file_count: 1,
            },
            CandidateTable {
                format: "csv".to_string(),
                extension: "csv".to_string(),
                base: "b/data".to_string(),
                partition_cols: vec![],
                recursive: false,
                file_count: 1,
            },
        ];
        let names = assign_table_names(&cands, &def());
        assert_eq!(names[0], "data");
        assert_ne!(names[1], "data");
        assert_eq!(names.iter().collect::<std::collections::HashSet<_>>().len(), 2);
    }
}
