use std::path::{Path, PathBuf};

pub(crate) fn find_in<P: AsRef<Path>>(glob: &str, in_dir: P) -> anyhow::Result<Vec<PathBuf>> {
    let in_dir = std::fs::canonicalize(in_dir.as_ref())?;

    let mut paths = Vec::new();
    for entry in glob::glob(glob)? {
        let path = entry?;
        // Canonicalize the path to resolve any symlinks
        let path = path.canonicalize()?;

        if path.starts_with(&in_dir) {
            paths.push(path);
        } else {
            return Err(anyhow::anyhow!("Path is not allowed"));
        }
    }
    Ok(paths)
}
