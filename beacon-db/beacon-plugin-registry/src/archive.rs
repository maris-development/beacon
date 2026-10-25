//! Pack, unpack and checksum plugin archives. Archives come from untrusted sources.

use std::collections::HashMap;
use std::io::Read;
use std::path::{Component, Path, PathBuf};

use anyhow::{bail, Context};
use sha2::{Digest, Sha256};

/// Returns the lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Parses `sha256sum` output into a map of file name to lowercase hex hash.
pub fn parse_sha256sums(text: &str) -> anyhow::Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    for (index, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let (hash, rest) = line
            .split_once(char::is_whitespace)
            .with_context(|| format!("SHA256SUMS line {} is malformed", index + 1))?;
        let name = rest.trim_start();
        let name = name.strip_prefix('*').unwrap_or(name);
        if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) || name.is_empty() {
            bail!("SHA256SUMS line {} is malformed", index + 1);
        }
        map.insert(name.to_string(), hash.to_ascii_lowercase());
    }
    Ok(map)
}

fn check_path(path: &Path) -> anyhow::Result<()> {
    for component in path.components() {
        match component {
            Component::Normal(_) | Component::CurDir => {}
            _ => bail!("archive entry has an unsafe path: {}", path.display()),
        }
    }
    Ok(())
}

fn gz_entries(archive: &[u8]) -> tar::Archive<flate2::read::GzDecoder<&[u8]>> {
    tar::Archive::new(flate2::read::GzDecoder::new(archive))
}

/// Largest compressed archive that [`unpack`] accepts.
pub(crate) const MAX_ARCHIVE_BYTES: u64 = 128 * 1024 * 1024;
/// Largest total of unpacked file bytes that [`unpack`] accepts.
const MAX_UNPACKED_BYTES: u64 = 512 * 1024 * 1024;

/// Unpacks a gzip tar into `dest`. It checks every entry first and writes nothing when one is bad.
pub fn unpack(archive: &[u8], dest: &Path) -> anyhow::Result<()> {
    unpack_with_limit(archive, dest, MAX_UNPACKED_BYTES)
}

fn unpack_with_limit(archive: &[u8], dest: &Path, unpacked_limit: u64) -> anyhow::Result<()> {
    if archive.len() as u64 > MAX_ARCHIVE_BYTES {
        bail!("archive too large: more than {MAX_ARCHIVE_BYTES} bytes");
    }
    let mut unpacked = 0u64;
    // Pass one: validate only.
    for entry in gz_entries(archive)
        .entries()
        .context("cannot read archive")?
    {
        let entry = entry.context("cannot read archive entry")?;
        let path = entry.path().context("archive entry has a bad path")?;
        check_path(&path)?;
        unpacked = unpacked.saturating_add(entry.size());
        if unpacked > unpacked_limit {
            return Err(too_many_unpacked(unpacked_limit));
        }
        match entry.header().entry_type() {
            tar::EntryType::Regular | tar::EntryType::Directory => {}
            tar::EntryType::Symlink | tar::EntryType::Link => {
                bail!("links are not allowed: {}", path.display())
            }
            other => bail!("unsupported entry type {other:?}: {}", path.display()),
        }
    }
    // Pass two: write the checked entries.
    std::fs::create_dir_all(dest)?;
    for entry in gz_entries(archive).entries()? {
        let mut entry = entry?;
        let target = dest.join(entry.path()?);
        if entry.header().entry_type() == tar::EntryType::Directory {
            std::fs::create_dir_all(&target)?;
            continue;
        }
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = std::fs::File::create(&target)?;
        std::io::copy(&mut entry, &mut file)?;
    }
    Ok(())
}

fn too_many_unpacked(limit: u64) -> anyhow::Error {
    anyhow::anyhow!("archive too large: more than {limit} unpacked bytes")
}

fn collect(
    root: &Path,
    dir: &Path,
    out: &mut Vec<(PathBuf, bool)>,
    total: &mut u64,
    limit: u64,
) -> anyhow::Result<()> {
    let mut names = std::fs::read_dir(dir)?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()?;
    names.sort();
    for path in names {
        let metadata = std::fs::symlink_metadata(&path)?;
        let kind = metadata.file_type();
        let rel = path.strip_prefix(root)?.to_path_buf();
        if kind.is_dir() {
            out.push((rel, true));
            collect(root, &path, out, total, limit)?;
        } else if kind.is_file() {
            *total = total.saturating_add(metadata.len());
            if *total > limit {
                return Err(too_many_unpacked(limit));
            }
            out.push((rel, false));
        } else {
            bail!("links are not allowed: {}", path.display());
        }
    }
    Ok(())
}

/// Packs `dir` into a deterministic gzip tar with paths relative to `dir`.
pub fn pack_dir(dir: &Path) -> anyhow::Result<Vec<u8>> {
    pack_dir_with_limits(dir, MAX_UNPACKED_BYTES, MAX_ARCHIVE_BYTES)
}

fn pack_dir_with_limits(
    dir: &Path,
    unpacked_limit: u64,
    archive_limit: u64,
) -> anyhow::Result<Vec<u8>> {
    let mut entries = Vec::new();
    collect(dir, dir, &mut entries, &mut 0, unpacked_limit)?;
    let mut total = 0u64;
    let mut builder = tar::Builder::new(Vec::new());
    for (rel, is_dir) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mtime(0);
        header.set_uid(0);
        header.set_gid(0);
        let name = rel.to_string_lossy().replace('\\', "/");
        if is_dir {
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(0o755);
            header.set_size(0);
            builder.append_data(&mut header, format!("{name}/"), std::io::empty())?;
        } else {
            let mut data = Vec::new();
            // A file can grow after the size check in `collect`.
            std::fs::File::open(dir.join(&rel))?
                .take(unpacked_limit - total + 1)
                .read_to_end(&mut data)?;
            total += data.len() as u64;
            if total > unpacked_limit {
                return Err(too_many_unpacked(unpacked_limit));
            }
            header.set_entry_type(tar::EntryType::Regular);
            header.set_mode(0o644);
            header.set_size(data.len() as u64);
            builder.append_data(&mut header, name, data.as_slice())?;
        }
    }
    let tar_bytes = builder.into_inner()?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gz, &tar_bytes)?;
    let archive = gz.finish()?;
    if archive.len() as u64 > archive_limit {
        bail!("archive too large: more than {archive_limit} bytes");
    }
    Ok(archive)
}

/// Returns the major version of the DataFusion this build links.
pub fn df_major() -> u64 {
    datafusion::DATAFUSION_VERSION
        .split('.')
        .next()
        .and_then(|major| major.parse().ok())
        .expect("DataFusion version starts with a major number")
}

/// Returns the archive file name of a plugin release.
pub fn archive_name(name: &str, version: &semver::Version, native: bool) -> String {
    if native {
        format!(
            "{name}-{version}-df{}-{}.tar.gz",
            df_major(),
            env!("BEACON_TARGET")
        )
    } else {
        format!("{name}-{version}-any.tar.gz")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    // Builds a gzip tar from raw (path, type, link target, data) entries.
    fn raw_archive(entries: &[(&str, tar::EntryType, Option<&str>, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, kind, link, data) in entries {
            let mut header = tar::Header::new_gnu();
            let name = &mut header.as_old_mut().name;
            name.fill(0);
            name[..path.len()].copy_from_slice(path.as_bytes());
            header.set_entry_type(*kind);
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            if let Some(link) = link {
                header.set_link_name(link).unwrap();
            }
            header.set_cksum();
            builder.append(&header, *data).unwrap();
        }
        let tar_bytes = builder.into_inner().unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(&tar_bytes).unwrap();
        gz.finish().unwrap()
    }

    #[test]
    fn pack_then_unpack_gives_the_same_files() {
        let src = tempfile::tempdir().unwrap();
        std::fs::create_dir(src.path().join("sql")).unwrap();
        std::fs::write(src.path().join("beacon-plugin.toml"), b"a = 1").unwrap();
        std::fs::write(src.path().join("sql").join("f.sql"), b"select 1").unwrap();
        let bytes = pack_dir(src.path()).unwrap();
        assert_eq!(bytes, pack_dir(src.path()).unwrap());
        let dest = tempfile::tempdir().unwrap();
        unpack(&bytes, dest.path()).unwrap();
        assert_eq!(
            std::fs::read(dest.path().join("beacon-plugin.toml")).unwrap(),
            b"a = 1"
        );
        assert_eq!(
            std::fs::read(dest.path().join("sql").join("f.sql")).unwrap(),
            b"select 1"
        );
    }

    #[test]
    fn refuses_a_parent_dir_entry() {
        let root = tempfile::tempdir().unwrap();
        let dest = root.path().join("dest");
        std::fs::create_dir(&dest).unwrap();
        let bytes = raw_archive(&[
            ("ok.txt", tar::EntryType::Regular, None, b"x"),
            ("../evil", tar::EntryType::Regular, None, b"x"),
        ]);
        let err = unpack(&bytes, &dest).unwrap_err().to_string();
        assert!(err.contains("unsafe path"), "{err}");
        assert!(!root.path().join("evil").exists());
        assert!(!dest.join("ok.txt").exists());
    }

    #[test]
    fn refuses_an_absolute_entry() {
        let dest = tempfile::tempdir().unwrap();
        let bytes = raw_archive(&[("/etc/x", tar::EntryType::Regular, None, b"x")]);
        let err = unpack(&bytes, dest.path()).unwrap_err().to_string();
        assert!(err.contains("unsafe path"), "{err}");
    }

    #[test]
    fn refuses_a_link_entry() {
        let dest = tempfile::tempdir().unwrap();
        let bytes = raw_archive(&[("l", tar::EntryType::Symlink, Some("/etc/passwd"), b"")]);
        let err = unpack(&bytes, dest.path()).unwrap_err().to_string();
        assert!(err.contains("links are not allowed"), "{err}");
    }

    #[test]
    fn refuses_too_many_unpacked_bytes() {
        let dest = tempfile::tempdir().unwrap();
        let bytes = raw_archive(&[
            ("a.txt", tar::EntryType::Regular, None, b"12345"),
            ("b.txt", tar::EntryType::Regular, None, b"67890"),
        ]);
        let err = unpack_with_limit(&bytes, dest.path(), 8)
            .unwrap_err()
            .to_string();
        assert!(err.contains("archive too large"), "{err}");
        assert!(!dest.path().join("a.txt").exists());
        unpack_with_limit(&bytes, dest.path(), 10).unwrap();
    }

    #[test]
    fn pack_refuses_too_many_bytes() {
        let src = tempfile::tempdir().unwrap();
        std::fs::write(src.path().join("a.txt"), b"12345").unwrap();
        std::fs::write(src.path().join("b.txt"), b"67890").unwrap();
        let err = pack_dir_with_limits(src.path(), 8, MAX_ARCHIVE_BYTES)
            .unwrap_err()
            .to_string();
        assert!(err.contains("archive too large"), "{err}");
        let err = pack_dir_with_limits(src.path(), 10, 16)
            .unwrap_err()
            .to_string();
        assert!(err.contains("archive too large"), "{err}");
        let packed = pack_dir_with_limits(src.path(), 10, MAX_ARCHIVE_BYTES).unwrap();
        assert_eq!(packed, pack_dir(src.path()).unwrap());
    }

    #[test]
    fn parses_both_sha256sums_forms() {
        let h1 = "a".repeat(64);
        let h2 = "B".repeat(64);
        let text = format!("{h1}  one.tar.gz\n{h2} *two.tar.gz\n\n");
        let map = parse_sha256sums(&text).unwrap();
        assert_eq!(map["one.tar.gz"], h1);
        assert_eq!(map["two.tar.gz"], "b".repeat(64));
        assert!(parse_sha256sums("nothex  x").is_err());
    }

    #[test]
    fn sha256_of_empty_input() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn archive_names_follow_the_spec() {
        let v = semver::Version::new(0, 1, 0);
        assert_eq!(
            archive_name("example", &v, true),
            format!("example-0.1.0-df54-{}.tar.gz", env!("BEACON_TARGET"))
        );
        assert_eq!(
            archive_name("example", &v, false),
            "example-0.1.0-any.tar.gz"
        );
    }
}
