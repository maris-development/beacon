//! Installed plugins in the plugins directory: one `<name>/` folder per plugin,
//! with its `install.json`.

use std::path::Path;

use anyhow::Context as _;
use serde::{Deserialize, Serialize};

use crate::manifest::valid_name;

/// The file in a plugin folder that records the install.
pub const RECORD_FILE: &str = "install.json";
/// The name prefix of an unfinished install or uninstall folder.
pub const TEMP_PREFIX: &str = ".tmp-";

/// What `install.json` records about an installed plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstallRecord {
    pub name: String,
    pub version: String,
    pub source: String,
    /// Lowercase hex SHA-256 of the installed archive.
    pub sha256: String,
    /// RFC 3339 time of the install.
    pub installed_at: String,
}

/// What the plugins directory holds: readable records and the folders whose record is bad.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Listing {
    /// In name order.
    pub records: Vec<InstallRecord>,
    /// Folder name and the reason, in name order.
    pub broken: Vec<(String, String)>,
}

/// Reads the `install.json` of the plugin folder `dir`.
pub fn read_record(dir: &Path) -> anyhow::Result<InstallRecord> {
    let bytes = std::fs::read(dir.join(RECORD_FILE))
        .with_context(|| format!("cannot read {RECORD_FILE}"))?;
    serde_json::from_slice(&bytes).with_context(|| format!("cannot parse {RECORD_FILE}"))
}

/// Writes `record` as the `install.json` of the plugin folder `dir`.
pub fn write_record(dir: &Path, record: &InstallRecord) -> anyhow::Result<()> {
    std::fs::write(dir.join(RECORD_FILE), serde_json::to_vec_pretty(record)?)
        .with_context(|| format!("failed to write the record of plugin '{}'", record.name))
}

/// Every plugin folder in `plugins_dir`. A bad record is logged and listed as broken.
///
/// Temp folders, files and folders with no valid plugin name are skipped.
pub fn list(plugins_dir: &Path) -> anyhow::Result<Listing> {
    let entries = match std::fs::read_dir(plugins_dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Listing::default()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to list {}", plugins_dir.display()))
        }
    };
    let mut listing = Listing::default();
    for entry in entries {
        let entry = entry.with_context(|| format!("failed to list {}", plugins_dir.display()))?;
        let folder = entry.file_name().to_string_lossy().into_owned();
        if folder.starts_with(TEMP_PREFIX) || !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        if !valid_name(&folder) {
            tracing::warn!(
                "Skipping {}: the folder name is not a plugin name",
                entry.path().display()
            );
            continue;
        }
        let parsed = read_record(&entry.path()).and_then(|record| {
            anyhow::ensure!(
                record.name == folder,
                "the record names plugin '{}', the folder is '{folder}'",
                record.name
            );
            Ok(record)
        });
        match parsed {
            Ok(record) => listing.records.push(record),
            Err(error) => {
                tracing::error!("Bad plugin record in {}: {error:#}", entry.path().display());
                listing.broken.push((folder, format!("{error:#}")));
            }
        }
    }
    listing.records.sort_by(|a, b| a.name.cmp(&b.name));
    listing.broken.sort();
    Ok(listing)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &str) -> InstallRecord {
        InstallRecord {
            name: name.to_string(),
            version: "0.1.0".to_string(),
            source: "file:///x".to_string(),
            sha256: "ab".repeat(32),
            installed_at: "2026-10-24T00:00:00Z".to_string(),
        }
    }

    fn put(root: &Path, folder: &str, record: &InstallRecord) {
        let dir = root.join(folder);
        std::fs::create_dir_all(&dir).unwrap();
        write_record(&dir, record).unwrap();
    }

    #[test]
    fn list_reports_a_record_in_another_folder_as_broken() {
        let root = tempfile::tempdir().unwrap();
        put(root.path(), "alpha", &record("alpha"));
        put(root.path(), "other", &record("alpha"));
        let listing = list(root.path()).unwrap();
        assert_eq!(listing.records, vec![record("alpha")]);
        assert_eq!(listing.broken.len(), 1);
        assert_eq!(listing.broken[0].0, "other");
        assert!(listing.broken[0].1.contains("folder is 'other'"));
    }

    #[test]
    fn list_skips_temp_folders_files_and_foreign_names() {
        let root = tempfile::tempdir().unwrap();
        put(root.path(), "zeta", &record("zeta"));
        put(root.path(), "alpha", &record("alpha"));
        put(root.path(), ".tmp-123", &record("tmp"));
        std::fs::create_dir(root.path().join("Not-A-Name")).unwrap();
        std::fs::write(root.path().join("notes.txt"), "x").unwrap();
        std::fs::create_dir(root.path().join("empty")).unwrap();
        let listing = list(root.path()).unwrap();
        let names: Vec<_> = listing.records.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, ["alpha", "zeta"]);
        assert_eq!(listing.broken.len(), 1, "{:?}", listing.broken);
        assert_eq!(listing.broken[0].0, "empty");
        assert_eq!(
            read_record(&root.path().join("zeta")).unwrap(),
            record("zeta")
        );
    }

    #[test]
    fn list_of_a_missing_dir_is_empty() {
        let root = tempfile::tempdir().unwrap();
        assert_eq!(list(&root.path().join("gone")).unwrap(), Listing::default());
    }
}
