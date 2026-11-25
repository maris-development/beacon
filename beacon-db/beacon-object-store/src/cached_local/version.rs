//! The version of a local file: its size and its modification time.

use std::time::SystemTime;

use chrono::{DateTime, Utc};
use object_store::ObjectMeta;
use object_store::path::Path;

/// The version of a local file, as `LocalFileSystem` sees it.
///
/// Two versions are equal when the size and the mtime are equal. The inode is not part of it.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(crate) struct FileVersion {
    /// The file length in bytes.
    pub size: u64,
    /// The mtime in microseconds since the UNIX epoch, 0 before the UNIX epoch.
    pub mtime_micros: u128,
    /// The mtime as UTC.
    pub last_modified: DateTime<Utc>,
}

impl FileVersion {
    /// Reads the version from file metadata, with the same conversions as `LocalFileSystem`.
    pub(crate) fn from_metadata(m: &std::fs::Metadata) -> FileVersion {
        let modified = m
            .modified()
            .expect("Modified file time should be supported on this platform");
        let mtime_micros = modified
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros();
        FileVersion {
            size: m.len(),
            mtime_micros,
            last_modified: modified.into(),
        }
    }

    /// The ETag: the stock `LocalFileSystem` format with inode 0.
    pub(crate) fn e_tag(&self) -> String {
        format!("0-{:x}-{:x}", self.mtime_micros, self.size)
    }

    /// The object metadata of the file at `location`.
    pub(crate) fn object_meta(&self, location: Path) -> ObjectMeta {
        ObjectMeta {
            location,
            last_modified: self.last_modified,
            size: self.size,
            e_tag: Some(self.e_tag()),
            version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use object_store::ObjectStoreExt;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;

    use super::super::CachedLocalOptions;
    use super::*;

    #[tokio::test]
    async fn object_meta_matches_local_file_system() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.bin"), vec![7u8; 5000]).unwrap();
        let stock = LocalFileSystem::new_with_prefix(dir.path()).unwrap();
        let expected = stock.head(&Path::from("a.bin")).await.unwrap();
        let md = std::fs::metadata(dir.path().join("a.bin")).unwrap();
        let ours = FileVersion::from_metadata(&md).object_meta(Path::from("a.bin"));
        assert_eq!(ours.location, expected.location);
        assert_eq!(ours.last_modified, expected.last_modified);
        assert_eq!(ours.size, expected.size);
        assert_eq!(ours.version, expected.version);
        // Stock: "{inode:x}-{mtime:x}-{size:x}". Ours: the same with inode 0.
        let stock_tag = expected.e_tag.unwrap();
        let (_, mtime_size) = stock_tag.split_once('-').unwrap();
        assert_eq!(ours.e_tag.unwrap(), format!("0-{mtime_size}"));
    }

    #[test]
    fn options_validate() {
        assert!(CachedLocalOptions::default().validate().is_ok());
        for bad in [1000, 2048, 3 * 4096, 32 << 20] {
            let o = CachedLocalOptions {
                block_size: bad,
                ..Default::default()
            };
            assert!(o.validate().is_err(), "{bad}");
        }
        for good in [0, 4096, 1 << 20, 16 << 20] {
            let o = CachedLocalOptions {
                block_size: good,
                ..Default::default()
            };
            assert!(o.validate().is_ok(), "{good}");
        }
        let o = CachedLocalOptions {
            max_open_files: 0,
            ..Default::default()
        };
        assert!(o.validate().is_err());
        let o = CachedLocalOptions {
            stream_chunk: 0,
            ..Default::default()
        };
        assert!(o.validate().is_err());
    }

    #[test]
    fn version_follows_size_and_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("a.bin");
        std::fs::write(&p, b"aaaa").unwrap();
        let before_md = std::fs::metadata(&p).unwrap();
        let before = FileVersion::from_metadata(&before_md);
        std::fs::write(&p, b"aaaaa").unwrap(); // size changes
        assert_ne!(
            FileVersion::from_metadata(&std::fs::metadata(&p).unwrap()),
            before
        );
        std::fs::write(&p, b"bbbb").unwrap(); // same size, mtime set back
        let f = std::fs::File::options().write(true).open(&p).unwrap();
        f.set_modified(before_md.modified().unwrap()).unwrap();
        // Known gap (spec 8.2): same size and mtime is the same version.
        assert_eq!(
            FileVersion::from_metadata(&std::fs::metadata(&p).unwrap()),
            before
        );
    }
}
