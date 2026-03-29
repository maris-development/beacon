use std::path::PathBuf;

/// Expand a glob expression into sorted file paths.
pub(crate) fn collect_glob_paths(glob_pattern: &str) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in glob::glob(glob_pattern)? {
        match entry {
            Ok(path) if path.is_file() => paths.push(path),
            Ok(_) => {}
            Err(err) => eprintln!("Skipping invalid glob entry: {err}"),
        }
    }

    anyhow::ensure!(!paths.is_empty(), "glob matched no files: {glob_pattern}");

    paths.sort();
    Ok(paths)
}

/// Keep only NetCDF files from an input path list.
pub(crate) fn only_netcdf_paths(
    paths: Vec<PathBuf>,
    fail_fast: bool,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut filtered = Vec::with_capacity(paths.len());
    for path in paths {
        let is_netcdf = path
            .extension()
            .and_then(|ext| ext.to_str())
            .is_some_and(|ext| ext.eq_ignore_ascii_case("nc"));

        if is_netcdf {
            filtered.push(path);
        } else if fail_fast {
            anyhow::bail!(
                "unsupported file '{}'; only .nc is currently supported",
                path.display()
            );
        } else {
            eprintln!(
                "Skipping unsupported file '{}'; only .nc is currently supported",
                path.display()
            );
        }
    }

    anyhow::ensure!(
        !filtered.is_empty(),
        "no supported input files after filtering (.nc)"
    );

    Ok(filtered)
}
