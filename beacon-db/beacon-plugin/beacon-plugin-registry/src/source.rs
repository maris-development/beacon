//! The sources of `INSTALL <name> FROM '<source>'`, and how to read their archive.

use std::io::Read as _;
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use bytes::Bytes;

use crate::archive::{df_major, pack_dir, parse_sha256sums, sha256_hex, MAX_ARCHIVE_BYTES};

/// The checksum file next to a release archive.
const SHA256SUMS_FILE: &str = "SHA256SUMS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginSource {
    /// A `.tar.gz` file or an unpacked plugin directory.
    File(PathBuf),
    /// A directory URL that holds `SHA256SUMS` and the archives.
    Https(url::Url),
    /// The release assets of a GitHub tag.
    GitHub {
        owner: String,
        repo: String,
        tag: String,
    },
}

/// A GitHub owner, repository or tag: not empty, no `/` and no `..`.
fn valid_github_part(part: &str) -> bool {
    !part.is_empty()
        && part != "."
        && !part.contains("..")
        && part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

/// True for `127.0.0.1`, `::1` and `localhost`.
fn is_loopback(url: &url::Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        None => false,
    }
}

impl PluginSource {
    /// Parses `file://<path>`, `https://<url>` or `github:<owner>/<repo>@<tag>`.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        if let Some(rest) = text.strip_prefix("github:") {
            let parts = rest
                .split_once('@')
                .and_then(|(path, tag)| path.split_once('/').map(|(o, r)| (o, r, tag)));
            let Some((owner, repo, tag)) =
                parts.filter(|(o, r, t)| [o, r, t].iter().all(|part| valid_github_part(part)))
            else {
                anyhow::bail!("invalid plugin source '{text}': use github:<owner>/<repo>@<tag>");
            };
            return Ok(Self::GitHub {
                owner: owner.to_string(),
                repo: repo.to_string(),
                tag: tag.to_string(),
            });
        }
        let url =
            url::Url::parse(text).with_context(|| format!("invalid plugin source '{text}'"))?;
        match url.scheme() {
            "file" => url
                .to_file_path()
                .map(Self::File)
                .map_err(|()| anyhow::anyhow!("invalid file path in plugin source '{text}'")),
            scheme @ ("https" | "http") => {
                // The text goes to logs, `install.json` and `system.plugins`.
                anyhow::ensure!(
                    url.username().is_empty() && url.password().is_none(),
                    "credentials in a plugin source are not allowed"
                );
                anyhow::ensure!(
                    scheme == "https" || is_loopback(&url),
                    "plain http is allowed only for loopback hosts: use https://"
                );
                Ok(Self::Https(url))
            }
            scheme => anyhow::bail!(
                "unsupported plugin source scheme '{scheme}': use file://, https:// or github:"
            ),
        }
    }
}

/// Reads the body of a URL.
#[async_trait::async_trait]
pub(crate) trait Fetcher: Send + Sync {
    async fn get(&self, url: &url::Url) -> anyhow::Result<Bytes>;
}

/// Fetches with `reqwest`. Only HTTP status 200 succeeds.
pub(crate) struct HttpFetcher {
    client: reqwest::Client,
}

/// Most redirects one fetch follows.
const MAX_REDIRECTS: usize = 10;

/// A redirect goes to `https`, or to a loopback `http` host when the source was loopback.
fn redirect_allowed(source_is_loopback: bool, next: &url::Url) -> bool {
    match next.scheme() {
        "https" => true,
        "http" => source_is_loopback && is_loopback(next),
        _ => false,
    }
}

impl HttpFetcher {
    pub(crate) fn new() -> anyhow::Result<Self> {
        let policy = reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > MAX_REDIRECTS {
                return attempt.error("too many redirects");
            }
            let source_is_loopback = attempt.previous().first().is_some_and(is_loopback);
            if redirect_allowed(source_is_loopback, attempt.url()) {
                attempt.follow()
            } else {
                let error = format!("redirect to {} is not allowed", attempt.url());
                attempt.error(error)
            }
        });
        // No total timeout: a large archive on a slow link must finish.
        let client = reqwest::Client::builder()
            .redirect(policy)
            .connect_timeout(std::time::Duration::from_secs(30))
            .read_timeout(std::time::Duration::from_secs(60))
            .build()
            .context("cannot create the HTTP client")?;
        Ok(Self { client })
    }
}

#[async_trait::async_trait]
impl Fetcher for HttpFetcher {
    async fn get(&self, url: &url::Url) -> anyhow::Result<Bytes> {
        let mut response = self
            .client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("cannot fetch {url}"))?;
        let status = response.status();
        anyhow::ensure!(status == reqwest::StatusCode::OK, "HTTP {status} for {url}");
        let too_large =
            || anyhow::anyhow!("archive too large: more than {MAX_ARCHIVE_BYTES} bytes");
        if response
            .content_length()
            .is_some_and(|length| length > MAX_ARCHIVE_BYTES)
        {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| format!("cannot read {url}"))?
        {
            // The length header can be missing or wrong.
            if body.len() as u64 + chunk.len() as u64 > MAX_ARCHIVE_BYTES {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(body))
    }
}

/// The directory URL, with a trailing `/`, that holds `SHA256SUMS` and the archives.
fn base_url(source: &PluginSource) -> anyhow::Result<url::Url> {
    match source {
        PluginSource::Https(url) => {
            let mut url = url.clone();
            if !url.path().ends_with('/') {
                let path = format!("{}/", url.path());
                url.set_path(&path);
            }
            // A query or fragment would break the file name join.
            url.set_query(None);
            url.set_fragment(None);
            Ok(url)
        }
        PluginSource::GitHub { owner, repo, tag } => url::Url::parse(&format!(
            "https://github.com/{owner}/{repo}/releases/download/{tag}/"
        ))
        .context("invalid GitHub release URL"),
        PluginSource::File(_) => anyhow::bail!("a file source has no base URL"),
    }
}

/// Picks the archive of plugin `name` in the `SHA256SUMS` names: native first, else `any`.
fn pick_archive<'a>(names: impl Iterator<Item = &'a String>, name: &str) -> anyhow::Result<String> {
    let native_suffix = format!("-df{}-{}.tar.gz", df_major(), env!("BEACON_TARGET"));
    let prefix = format!("{name}-");
    let mut versions = std::collections::BTreeSet::new();
    let mut native = None;
    let mut any = None;
    for file in names.filter(|file| file.starts_with(&prefix)) {
        let rest = &file[prefix.len()..];
        // The version part must be a semantic version: `demo-docs-...` is not a `demo` archive.
        let is_version = |version: &str| semver::Version::parse(version).is_ok();
        if let Some(version) = rest.strip_suffix(&native_suffix).filter(|v| is_version(v)) {
            versions.insert(version.to_string());
            native = Some(file.clone());
        } else if let Some(version) = rest.strip_suffix("-any.tar.gz").filter(|v| is_version(v)) {
            versions.insert(version.to_string());
            any = Some(file.clone());
        }
    }
    anyhow::ensure!(
        versions.len() <= 1,
        "{SHA256SUMS_FILE} lists more than one version of {name}"
    );
    native.or(any).with_context(|| {
        format!(
            "no archive for df{}-{} or any",
            df_major(),
            env!("BEACON_TARGET")
        )
    })
}

/// Reads and checks the archive of plugin `name` from a remote release directory.
async fn fetch_remote(
    fetcher: &dyn Fetcher,
    source: &PluginSource,
    name: &str,
) -> anyhow::Result<(Bytes, String)> {
    let base = base_url(source)?;
    let sums_url = base.join(SHA256SUMS_FILE)?;
    let sums = fetcher
        .get(&sums_url)
        .await
        .with_context(|| format!("{SHA256SUMS_FILE} not found at {sums_url}"))?;
    let sums = parse_sha256sums(&String::from_utf8_lossy(&sums))?;
    let file = pick_archive(sums.keys(), name)?;
    let url = base.join(&file)?;
    let archive = fetcher.get(&url).await?;
    let sha256 = sha256_hex(&archive);
    let expected = &sums[&file];
    anyhow::ensure!(
        *expected == sha256,
        "SHA-256 mismatch for {file}: {SHA256SUMS_FILE} says {expected}, the download has {sha256}"
    );
    Ok((archive, sha256))
}

/// Reads the plugin archive of `source` for plugin `name` and returns it with its SHA-256.
pub async fn fetch_archive(source: &PluginSource, name: &str) -> anyhow::Result<(Bytes, String)> {
    match source {
        PluginSource::File(path) => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || read_file_source(&path, MAX_ARCHIVE_BYTES))
                .await
                .context("the archive read stopped")?
        }
        PluginSource::Https(_) | PluginSource::GitHub { .. } => {
            fetch_remote(&HttpFetcher::new()?, source, name).await
        }
    }
}

/// Packs a directory, or reads an archive file and checks it against a sibling `SHA256SUMS`.
fn read_file_source(path: &Path, limit: u64) -> anyhow::Result<(Bytes, String)> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("cannot read plugin source {}", path.display()))?;
    if metadata.is_dir() {
        let archive = pack_dir(path)
            .with_context(|| format!("cannot pack plugin directory {}", path.display()))?;
        let sha256 = sha256_hex(&archive);
        return Ok((Bytes::from(archive), sha256));
    }
    anyhow::ensure!(
        metadata.len() <= limit,
        "archive too large: more than {limit} bytes"
    );
    let mut archive = Vec::new();
    std::fs::File::open(path)
        .with_context(|| format!("cannot read plugin source {}", path.display()))?
        .take(limit + 1)
        .read_to_end(&mut archive)
        .with_context(|| format!("cannot read plugin source {}", path.display()))?;
    // The file can grow after the size check.
    anyhow::ensure!(
        archive.len() as u64 <= limit,
        "archive too large: more than {limit} bytes"
    );
    let sha256 = sha256_hex(&archive);
    let sums = path.with_file_name(SHA256SUMS_FILE);
    if sums.is_file() {
        let file = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let text = std::fs::read_to_string(&sums)
            .with_context(|| format!("cannot read {}", sums.display()))?;
        let expected = parse_sha256sums(&text)?
            .remove(&file)
            .with_context(|| format!("{SHA256SUMS_FILE} has no entry for {file}"))?;
        anyhow::ensure!(
            expected == sha256,
            "SHA-256 mismatch for {file}: {SHA256SUMS_FILE} says {expected}, the file has {sha256}"
        );
    }
    Ok((Bytes::from(archive), sha256))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::archive::archive_name;

    fn plugin_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("beacon-plugin.toml"),
            "[plugin]\nname = \"demo\"",
        )
        .unwrap();
        dir
    }

    /// An archive file in a new directory, with a `SHA256SUMS` line when given.
    fn archive_file(sums: Option<&str>) -> (tempfile::TempDir, PathBuf, Vec<u8>) {
        let archive = pack_dir(plugin_dir().path()).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("demo-0.1.0-any.tar.gz");
        std::fs::write(&path, &archive).unwrap();
        if let Some(sums) = sums {
            std::fs::write(dir.path().join(SHA256SUMS_FILE), sums).unwrap();
        }
        (dir, path, archive)
    }

    #[test]
    fn parses_each_source_kind() {
        let dir = tempfile::tempdir().unwrap();
        let url = url::Url::from_file_path(dir.path()).unwrap();
        assert_eq!(
            PluginSource::parse(url.as_str()).unwrap(),
            PluginSource::File(dir.path().to_path_buf())
        );
        assert!(matches!(
            PluginSource::parse("https://plugins.example.org/demo/0.1.0/").unwrap(),
            PluginSource::Https(_)
        ));
        assert_eq!(
            PluginSource::parse("github:maris-development/beacon-plugin-example@v0.1.0").unwrap(),
            PluginSource::GitHub {
                owner: "maris-development".to_string(),
                repo: "beacon-plugin-example".to_string(),
                tag: "v0.1.0".to_string(),
            }
        );
    }

    #[test]
    fn refuses_bad_sources() {
        for text in [
            "ftp://example.org/x",
            "not a url",
            "github:owner@v1",
            "github:owner/repo",
            "github:owner/re/po@v1",
            "github:owner/..@v1",
        ] {
            assert!(PluginSource::parse(text).is_err(), "{text}");
        }
    }

    /// Serves fixed bodies by URL.
    struct FakeFetcher(HashMap<url::Url, Bytes>);

    #[async_trait::async_trait]
    impl Fetcher for FakeFetcher {
        async fn get(&self, url: &url::Url) -> anyhow::Result<Bytes> {
            self.0
                .get(url)
                .cloned()
                .with_context(|| format!("HTTP 404 for {url}"))
        }
    }

    const BASE: &str = "https://plugins.example.org/demo/";

    fn native_file() -> String {
        archive_name("demo", &semver::Version::new(0, 1, 0), true)
    }

    fn any_file() -> String {
        archive_name("demo", &semver::Version::new(0, 1, 0), false)
    }

    /// A fetcher that serves `SHA256SUMS` for `files` (name, body, listed hash) under `BASE`.
    fn release(files: &[(String, &[u8], Option<String>)]) -> FakeFetcher {
        let mut map = HashMap::new();
        let mut sums = String::new();
        for (file, body, hash) in files {
            let hash = hash.clone().unwrap_or_else(|| sha256_hex(body));
            sums.push_str(&format!("{hash}  {file}\n"));
            map.insert(
                url::Url::parse(&format!("{BASE}{file}")).unwrap(),
                Bytes::copy_from_slice(body),
            );
        }
        map.insert(
            url::Url::parse(&format!("{BASE}SHA256SUMS")).unwrap(),
            Bytes::from(sums),
        );
        FakeFetcher(map)
    }

    async fn fetch(fetcher: &FakeFetcher) -> anyhow::Result<(Bytes, String)> {
        let source = PluginSource::parse(BASE).unwrap();
        fetch_remote(fetcher, &source, "demo").await
    }

    #[tokio::test]
    async fn picks_the_native_archive_for_this_target() {
        let fetcher = release(&[
            (any_file(), b"any".as_slice(), None),
            (native_file(), b"native".as_slice(), None),
        ]);
        let (archive, sha256) = fetch(&fetcher).await.unwrap();
        assert_eq!(archive.as_ref(), b"native");
        assert_eq!(sha256, sha256_hex(b"native"));
    }

    #[tokio::test]
    async fn falls_back_to_the_any_archive() {
        let fetcher = release(&[
            (any_file(), b"any".as_slice(), None),
            (
                "demo-0.1.0-df1-other-target.tar.gz".to_string(),
                b"other".as_slice(),
                None,
            ),
        ]);
        let (archive, _) = fetch(&fetcher).await.unwrap();
        assert_eq!(archive.as_ref(), b"any");
    }

    #[tokio::test]
    async fn no_matching_archive_fails() {
        let fetcher = release(&[(
            "demo-0.1.0-df1-other-target.tar.gz".to_string(),
            b"other".as_slice(),
            None,
        )]);
        let err = fetch(&fetcher).await.unwrap_err().to_string();
        assert!(err.contains("no archive for df"), "{err}");
    }

    #[tokio::test]
    async fn a_longer_plugin_name_is_not_an_archive() {
        let fetcher = release(&[
            (
                "demo-docs-0.1.0-any.tar.gz".to_string(),
                b"docs".as_slice(),
                None,
            ),
            (any_file(), b"any".as_slice(), None),
        ]);
        let (archive, _) = fetch(&fetcher).await.unwrap();
        assert_eq!(archive.as_ref(), b"any");
        let only_docs = release(&[(
            "demo-docs-0.1.0-any.tar.gz".to_string(),
            b"docs".as_slice(),
            None,
        )]);
        let err = fetch(&only_docs).await.unwrap_err().to_string();
        assert!(err.contains("no archive for df"), "{err}");
    }

    #[tokio::test]
    async fn several_versions_fail() {
        let fetcher = release(&[
            (any_file(), b"a".as_slice(), None),
            ("demo-0.2.0-any.tar.gz".to_string(), b"b".as_slice(), None),
        ]);
        let err = fetch(&fetcher).await.unwrap_err().to_string();
        assert!(
            err.contains("SHA256SUMS lists more than one version of demo"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn missing_sha256sums_fails() {
        let fetcher = FakeFetcher(HashMap::new());
        let err = fetch(&fetcher).await.unwrap_err().to_string();
        assert!(err.contains("SHA256SUMS not found"), "{err}");
    }

    #[tokio::test]
    async fn hash_mismatch_fails() {
        let fetcher = release(&[(any_file(), b"any".as_slice(), Some("0".repeat(64)))]);
        let err = fetch(&fetcher).await.unwrap_err().to_string();
        assert!(err.contains("SHA-256 mismatch"), "{err}");
    }

    #[tokio::test]
    async fn github_source_maps_to_the_release_url() {
        let source = PluginSource::parse("github:o/r@v1").unwrap();
        assert_eq!(
            base_url(&source).unwrap().as_str(),
            "https://github.com/o/r/releases/download/v1/"
        );
        let file = any_file();
        let mut map = HashMap::new();
        map.insert(
            url::Url::parse("https://github.com/o/r/releases/download/v1/SHA256SUMS").unwrap(),
            Bytes::from(format!("{}  {file}\n", sha256_hex(b"any"))),
        );
        map.insert(
            url::Url::parse(&format!(
                "https://github.com/o/r/releases/download/v1/{file}"
            ))
            .unwrap(),
            Bytes::from_static(b"any"),
        );
        let (archive, _) = fetch_remote(&FakeFetcher(map), &source, "demo")
            .await
            .unwrap();
        assert_eq!(archive.as_ref(), b"any");
    }

    #[test]
    fn a_directory_url_gets_a_trailing_slash() {
        let source = PluginSource::parse("https://plugins.example.org/demo").unwrap();
        assert_eq!(
            base_url(&source).unwrap().as_str(),
            "https://plugins.example.org/demo/"
        );
    }

    #[test]
    fn redirects_go_only_to_https() {
        let url = |text: &str| url::Url::parse(text).unwrap();
        assert!(redirect_allowed(false, &url("https://cdn.example.org/a")));
        assert!(!redirect_allowed(false, &url("http://cdn.example.org/a")));
        assert!(!redirect_allowed(false, &url("http://127.0.0.1:8080/a")));
        assert!(redirect_allowed(true, &url("http://127.0.0.1:8080/a")));
        assert!(redirect_allowed(true, &url("https://cdn.example.org/a")));
        assert!(!redirect_allowed(true, &url("http://cdn.example.org/a")));
        assert!(!redirect_allowed(true, &url("file:///etc/passwd")));
    }

    #[test]
    fn credentials_and_plain_http_are_refused() {
        let err = PluginSource::parse("https://user:token@example.org/x/")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("credentials in a plugin source are not allowed"),
            "{err}"
        );
        assert!(PluginSource::parse("https://user@example.org/x/").is_err());
        assert!(PluginSource::parse("http://example.org/x/").is_err());
        for host in ["127.0.0.1:8080", "localhost", "[::1]:80"] {
            assert!(PluginSource::parse(&format!("http://{host}/x/")).is_ok());
        }
    }

    #[tokio::test]
    async fn a_directory_is_packed() {
        let dir = plugin_dir();
        let source = PluginSource::File(dir.path().to_path_buf());
        let (archive, sha256) = fetch_archive(&source, "demo").await.unwrap();
        assert_eq!(archive, pack_dir(dir.path()).unwrap());
        assert_eq!(sha256, sha256_hex(&archive));
    }

    #[tokio::test]
    async fn an_archive_is_checked_against_sha256sums() {
        let (_dir, path, archive) = archive_file(None);
        let sums = format!("{}  demo-0.1.0-any.tar.gz\n", sha256_hex(&archive));
        std::fs::write(path.with_file_name(SHA256SUMS_FILE), sums).unwrap();
        let (read, sha256) = fetch_archive(&PluginSource::File(path), "demo")
            .await
            .unwrap();
        assert_eq!(read, archive);
        assert_eq!(sha256, sha256_hex(&archive));
    }

    #[test]
    fn an_archive_without_sha256sums_is_read() {
        let (_dir, path, archive) = archive_file(None);
        let (read, _) = read_file_source(&path, MAX_ARCHIVE_BYTES).unwrap();
        assert_eq!(read, archive);
    }

    #[test]
    fn a_wrong_or_missing_sum_is_refused() {
        let wrong = format!("{}  demo-0.1.0-any.tar.gz\n", "0".repeat(64));
        let (_dir, path, _) = archive_file(Some(&wrong));
        let err = read_file_source(&path, MAX_ARCHIVE_BYTES)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("SHA-256 mismatch for demo-0.1.0-any.tar.gz"),
            "{err}"
        );

        let other = format!("{}  other.tar.gz\n", "0".repeat(64));
        let (_dir, path, _) = archive_file(Some(&other));
        let err = read_file_source(&path, MAX_ARCHIVE_BYTES)
            .unwrap_err()
            .to_string();
        assert!(err.contains("no entry for demo-0.1.0-any.tar.gz"), "{err}");
    }

    #[test]
    fn a_large_file_is_refused_before_the_read() {
        let (_dir, path, archive) = archive_file(None);
        let limit = archive.len() as u64 - 1;
        let err = read_file_source(&path, limit).unwrap_err().to_string();
        assert!(err.contains("archive too large"), "{err}");
    }
}
