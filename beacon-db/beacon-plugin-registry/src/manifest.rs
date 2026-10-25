use anyhow::{bail, Context};
use serde::Deserialize;

/// File name of the manifest at the root of a plugin archive.
pub const MANIFEST_FILE: &str = "beacon-plugin.toml";

/// Parsed `beacon-plugin.toml`.
#[derive(Debug, Clone)]
pub struct PluginManifest {
    pub name: String,
    pub version: semver::Version,
    pub beacon: semver::VersionReq,
    pub native: Option<NativeSection>,
    pub sql: Option<SqlSection>,
}

/// The TOML shape of `beacon-plugin.toml`.
#[derive(Deserialize)]
struct ManifestFile {
    plugin: Option<PluginSection>,
    native: Option<NativeSection>,
    sql: Option<SqlSection>,
}

/// The `[plugin]` table.
#[derive(Deserialize)]
struct PluginSection {
    name: String,
    version: semver::Version,
    beacon: semver::VersionReq,
}

/// The `[native]` table: a Rust crate built as a dynamic library.
#[derive(Debug, Clone, Deserialize)]
pub struct NativeSection {
    #[serde(rename = "crate")]
    pub krate: String,
    pub library: String,
}

/// The `[sql]` table: files with `CREATE FUNCTION` statements.
#[derive(Debug, Clone, Deserialize)]
pub struct SqlSection {
    pub files: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginKind {
    Native,
    Sql,
    NativeSql,
}

impl PluginKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            PluginKind::Native => "native",
            PluginKind::Sql => "sql",
            PluginKind::NativeSql => "native+sql",
        }
    }
}

/// Version of this Beacon build, without the pre-release part.
pub fn beacon_version() -> semver::Version {
    let mut version = semver::Version::parse(env!("CARGO_PKG_VERSION"))
        .expect("CARGO_PKG_VERSION is valid semver");
    version.pre = semver::Prerelease::EMPTY;
    version
}

pub fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('a'..='z'))
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

impl PluginManifest {
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let file: ManifestFile = toml::from_str(text).context("invalid beacon-plugin.toml")?;
        let Some(plugin) = file.plugin else {
            bail!("beacon-plugin.toml needs a [plugin] table with name, version and beacon");
        };
        let manifest = Self {
            name: plugin.name,
            version: plugin.version,
            beacon: plugin.beacon,
            native: file.native,
            sql: file.sql,
        };
        if !valid_name(&manifest.name) {
            bail!(
                "invalid plugin name '{}': use lowercase letters, digits and '_', start with a letter",
                manifest.name
            );
        }
        if manifest.native.is_none() && manifest.sql.is_none() {
            bail!("plugin '{}' needs [native] or [sql]", manifest.name);
        }
        Ok(manifest)
    }

    pub fn kind(&self) -> PluginKind {
        match (&self.native, &self.sql) {
            (Some(_), Some(_)) => PluginKind::NativeSql,
            (Some(_), None) => PluginKind::Native,
            _ => PluginKind::Sql,
        }
    }

    /// Checks the `beacon` range against a Beacon version. Pre-release parts are ignored.
    pub fn check_beacon(&self, beacon: &semver::Version) -> anyhow::Result<()> {
        let mut release = beacon.clone();
        release.pre = semver::Prerelease::EMPTY;
        if !self.beacon.matches(&release) {
            bail!(
                "plugin '{}' requires Beacon {}, this is Beacon {}",
                self.name,
                self.beacon,
                release
            );
        }
        Ok(())
    }

    /// Platform file name of the native library, if the plugin has one.
    pub fn library_file_name(&self) -> Option<String> {
        self.native.as_ref().map(|n| {
            format!(
                "{}{}{}",
                std::env::consts::DLL_PREFIX,
                n.library,
                std::env::consts::DLL_SUFFIX
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EXAMPLE: &str = r#"
[plugin]
name = "bluecloud"
version = "1.2.0"
beacon = ">=2.1, <3"

[native]
crate = "bluecloud-native"
library = "bluecloud_native"

[sql]
files = ["sql/mappings.sql"]
"#;

    #[test]
    fn parses_the_issue_example() {
        let m = PluginManifest::parse(EXAMPLE).unwrap();
        assert_eq!(m.name, "bluecloud");
        assert_eq!(m.version, semver::Version::new(1, 2, 0));
        assert_eq!(m.kind(), PluginKind::NativeSql);
        assert_eq!(m.kind().as_str(), "native+sql");
        assert_eq!(m.native.unwrap().library, "bluecloud_native");
        assert_eq!(m.sql.unwrap().files, vec!["sql/mappings.sql"]);
    }

    #[test]
    fn refuses_a_manifest_without_native_or_sql() {
        let text = "[plugin]\nname = \"x\"\nversion = \"1.0.0\"\nbeacon = \">=2\"\n";
        let err = PluginManifest::parse(text).unwrap_err().to_string();
        assert!(err.contains("needs [native] or [sql]"), "{err}");
    }

    #[test]
    fn refuses_the_top_level_form() {
        let text = EXAMPLE.replace("[plugin]\n", "");
        let err = PluginManifest::parse(&text).unwrap_err().to_string();
        assert!(err.contains("[plugin] table"), "{err}");
    }

    #[test]
    fn refuses_an_invalid_name() {
        let text = EXAMPLE.replace("bluecloud\"\nversion", "Blue-Cloud\"\nversion");
        let err = PluginManifest::parse(&text).unwrap_err().to_string();
        assert!(err.contains("plugin name"), "{err}");
    }

    #[test]
    fn rc_version_matches_a_release_range() {
        let m = PluginManifest::parse(EXAMPLE).unwrap();
        let rc = semver::Version::parse("2.1.0-rc1").unwrap();
        m.check_beacon(&rc).unwrap();
    }

    #[test]
    fn refuses_a_beacon_outside_the_range() {
        let text = EXAMPLE.replace(">=2.1, <3", ">=3");
        let m = PluginManifest::parse(&text).unwrap();
        let err = m
            .check_beacon(&semver::Version::new(2, 1, 0))
            .unwrap_err()
            .to_string();
        assert!(err.contains("requires Beacon >=3"), "{err}");
    }

    #[test]
    fn library_file_name_uses_the_platform_names() {
        let m = PluginManifest::parse(EXAMPLE).unwrap();
        let expected = format!(
            "{}bluecloud_native{}",
            std::env::consts::DLL_PREFIX,
            std::env::consts::DLL_SUFFIX
        );
        assert_eq!(m.library_file_name(), Some(expected));
        let sql_only = PluginManifest::parse(
            "[plugin]\nname = \"x\"\nversion = \"1.0.0\"\nbeacon = \">=2\"\n[sql]\nfiles = [\"a.sql\"]\n",
        )
        .unwrap();
        assert_eq!(sql_only.library_file_name(), None);
    }
}
