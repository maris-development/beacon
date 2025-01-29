use std::path::{Path, PathBuf};

/// Check if the path contains any symbolic links.
pub(crate) fn contains_symlink(path: &Path) -> Result<bool, String> {
    let mut current_path = PathBuf::new();
    for component in path.components() {
        current_path.push(component);
        if let Ok(metadata) = std::fs::symlink_metadata(&current_path) {
            if metadata.file_type().is_symlink() {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Check if the requested path is allowed and does not use symlinks.
pub(crate) fn is_path_allowed(requested_path: &str, allowed_folder: &str) -> Result<bool, String> {
    // Convert both the requested path and the allowed folder to absolute paths
    let requested_path = std::fs::canonicalize(requested_path)
        .map_err(|e| format!("Failed to canonicalize requested path: {}", e))?;
    let allowed_folder = std::fs::canonicalize(allowed_folder)
        .map_err(|e| format!("Failed to canonicalize allowed folder: {}", e))?;

    // Check if the requested path starts with the allowed folder
    if !requested_path.starts_with(&allowed_folder) {
        return Ok(false);
    }

    // Ensure that the requested path does not contain any symlinks
    if contains_symlink(&PathBuf::from(requested_path))? {
        return Ok(false); // Reject if a symlink is found
    }

    Ok(true)
}

pub(crate) fn find_in(glob: &str, in_folder: &str) -> anyhow::Result<Vec<PathBuf>> {
    let in_folder = std::fs::canonicalize(in_folder)?;

    let mut paths = Vec::new();
    for entry in glob::glob(glob)? {
        let path = entry?;
        // Canonicalize the path to resolve any symlinks
        let path = path.canonicalize()?;

        if path.starts_with(in_folder.as_os_str()) {
            paths.push(path);
        }
    }
    Ok(paths)
}
