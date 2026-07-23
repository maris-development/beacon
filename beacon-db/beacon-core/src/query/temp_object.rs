//! RAII ownership of a single query-output temp file.
//!
//! A [`TempObject`] reserves a unique name under the tmp store's root and owns the
//! file that will be written there, deleting it on drop.

use std::io;
use std::path::{Path, PathBuf};

/// Owns one query-output file on disk and deletes it on drop.
///
/// Unlike `tempfile::NamedTempFile`, the file may be (re)created at this path by a
/// native writer (netcdf-c / ODV) rather than by us — those writers open the path
/// themselves rather than writing through a file handle we hold — so cleanup is
/// keyed on the path, not on an open handle. `object_name` is the single source of
/// truth: the COPY target is `tmp://<object_name>` and the native sink reconstructs
/// `tmp_dir/<object_name>` via the same `output_dir.join(prefix)`, so the write
/// location and the readable path cannot diverge.
#[derive(Debug)]
pub struct TempObject {
    /// `<tmp_dir>/<object_name>` — where the bytes end up, for read-back / size.
    path: PathBuf,
    /// The bare `<object_name>` — the `tmp://` object path the COPY target and the
    /// native sink agree on.
    object_name: String,
}

impl TempObject {
    /// Reserve a unique output name under `tmp_dir` (the tmp store's root).
    ///
    /// `tmp_dir` MUST be the directory the tmp object store
    /// (the tmp URL in [`crate::settings::ObjectStoreUrls`]) is rooted at *and* the NetCDF
    /// factory's `output_dir`, so a COPY to `tmp://<object_name>` lands at
    /// [`Self::path`] regardless of which writer produces it.
    ///
    /// The name is reserved (a unique `beacon_out_<uuid><extension>`), not created:
    /// the COPY writer — object-store or native — creates the file. `extension`
    /// includes the leading dot (e.g. `.csv`), or is empty for none.
    pub fn create_in(tmp_dir: &Path, extension: &str) -> io::Result<Self> {
        // Ensure the destination directory exists so the writer that follows can
        // create the file (matches the old `tempfile_in` behavior of requiring it).
        std::fs::create_dir_all(tmp_dir)?;
        let object_name = format!("beacon_out_{}{}", uuid::Uuid::new_v4().simple(), extension);
        let path = tmp_dir.join(&object_name);
        Ok(Self { path, object_name })
    }

    /// The bare object name — the `tmp://` object path shared by the COPY target and
    /// the native sink.
    pub fn object_path(&self) -> &str {
        &self.object_name
    }

    /// The local filesystem path the bytes are written to and read back from.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempObject {
    fn drop(&mut self) {
        // Best-effort: the file may never have been created (e.g. the COPY failed),
        // or already be gone.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Best-effort removal of crash-orphaned query-output files under `tmp_dir`.
///
/// [`TempObject`]'s `Drop` handles the happy path; a process crash leaves
/// `beacon_out_*` files behind. Called once on runtime build. Logs at debug and
/// never fails the build.
pub fn sweep_stale_outputs(tmp_dir: &Path) {
    let entries = match std::fs::read_dir(tmp_dir) {
        Ok(entries) => entries,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("beacon_out_") {
            match std::fs::remove_file(entry.path()) {
                Ok(()) => tracing::debug!(file = %name, "swept stale query-output file"),
                Err(error) => {
                    tracing::debug!(file = %name, %error, "failed to sweep stale query-output file")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `TempObject` reserves a name but creates no file; writing to its path and
    /// dropping it removes the file — the RAII cleanup the native-writer path relies
    /// on (netcdf-c opens the path itself, so `NamedTempFile` was the wrong tool).
    #[test]
    fn temp_object_deletes_its_path_on_drop() {
        let dir = tempfile::tempdir().expect("temp dir");
        let temp = TempObject::create_in(dir.path(), ".nc").expect("reserve name");
        let path = temp.path().to_path_buf();

        assert_eq!(
            path.parent().unwrap(),
            dir.path(),
            "the reserved path is under the tmp dir"
        );
        assert!(
            temp.object_path().starts_with("beacon_out_"),
            "the object name carries the sweep prefix"
        );
        assert!(!path.exists(), "creation only reserves a name, it writes nothing");

        // A writer (object-store or native) creates the file at this path.
        std::fs::write(&path, b"payload").expect("write output");
        assert!(path.exists());

        drop(temp);
        assert!(!path.exists(), "dropping the TempObject removes the file");
    }

    /// The startup sweep removes crash-orphaned `beacon_out_*` files and leaves
    /// unrelated files alone.
    #[test]
    fn sweep_removes_only_beacon_outputs() {
        let dir = tempfile::tempdir().expect("temp dir");
        let orphan = dir.path().join("beacon_out_deadbeef.nc");
        let keep = dir.path().join("unrelated.txt");
        std::fs::write(&orphan, b"stale").unwrap();
        std::fs::write(&keep, b"keep").unwrap();

        sweep_stale_outputs(dir.path());

        assert!(!orphan.exists(), "orphaned output should be swept");
        assert!(keep.exists(), "unrelated files should be left alone");
    }
}
