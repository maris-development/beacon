//! [`LanceConfig`]: the runtime settings of the managed Lance table engine.

use beacon_datafusion_ext::settings::BeaconOptions;
use datafusion::catalog::Session;
use datafusion::execution::context::SessionConfig;
use lance::dataset::scanner::MaterializationStyle;
use lance_encoding::version::LanceFileVersion;

/// Runtime configuration for managed Lance tables.
///
/// Plain data with sensible defaults; the caller populates it (there is no
/// environment parsing here, so the crate stays reusable and the host decides
/// where the values come from). An empty string means "leave it to Lance", which
/// is what an unset `BEACON_LANCE_*` variable used to mean.
///
/// The first four are *write* settings: they shape the files a `CREATE TABLE` or
/// `INSERT` produces, and never the files already on disk. `materialization` is a
/// read setting and applies to the next scan.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LanceConfig {
    /// Block compression for string and binary columns: `fsst`, `zstd`, `lz4`, or
    /// `none`. Empty leaves them uncompressed.
    pub compression: String,
    /// Block compression for numeric columns: `zstd`, `lz4`, or `none`. Empty
    /// keeps Lance's default.
    pub numeric_compression: String,
    /// File format version for new data: `2.0`, `2.1` or `2.2`. Empty means the
    /// beacon default, `2.2`.
    pub version: String,
    /// Minichunk size in bytes for fixed-width columns. Empty leaves it to Lance.
    pub minichunk: String,
    /// Column materialization on a filtered scan: `late` or `early`. Empty keeps
    /// beacon's projection-width rule.
    pub materialization: String,
}

impl LanceConfig {
    /// The `beacon.lance.*` settings on `config`, or the defaults when the
    /// namespace is absent (a session beacon did not build).
    ///
    /// Read at each write and each scan, never cached, so
    /// `SET beacon.lance.version = '2.1'` applies to the next statement.
    pub fn from_config(config: &SessionConfig) -> Self {
        let lance = BeaconOptions::from_config(config).lance;
        Self {
            compression: lance.compression,
            numeric_compression: lance.numeric_compression,
            version: lance.version,
            minichunk: lance.minichunk,
            materialization: lance.materialization,
        }
    }

    /// [`Self::from_config`] for the `&dyn Session` a `TableProvider` is handed.
    pub fn from_session(session: &dyn Session) -> Self {
        Self::from_config(session.config())
    }

    /// Compression scheme for string columns, or `None` when unset or `none`.
    ///
    /// Compression is applied only to string/binary columns, and only when asked
    /// for. Measured on a 20M-row ClickBench subset:
    ///   * no compression : 5.15GB, string scan  126ms, int scan 12.5ms
    ///   * zstd (all cols): 4.13GB, string scan 1334ms, int scan 42.1ms
    /// Block-compressing numerics is a bad trade (3x slower scans for little
    /// size), so numeric columns are never compressed here regardless.
    pub(crate) fn string_compression(&self) -> Option<&str> {
        non_empty(&self.compression).filter(|v| !v.eq_ignore_ascii_case("none"))
    }

    /// Compression scheme for numeric columns, or `None` for Lance's default.
    ///
    /// From file version 2.2 Lance block-compresses any buffer over 32KB by
    /// default, which shrinks the dataset but slows numeric scans. Setting `none`
    /// opts numeric columns back out while leaving strings compressed.
    pub(crate) fn numeric_compression(&self) -> Option<&str> {
        non_empty(&self.numeric_compression)
    }

    /// File format version for new data.
    ///
    /// Beacon writes 2.2, not Lance's 2.1 default. 2.2 is a stable version (Lance
    /// only treats `>= Next` as unstable) and adds RLE for whole blocks plus
    /// automatic block compression for buffers over 32KB. Measured on a 100M-row
    /// ClickBench table: 27GB -> 21.9GB (-19%) with query time unchanged
    /// (96.99s vs 95.01s over the 43-query suite, within run-to-run noise).
    ///
    /// Appends to an existing table keep that table's own version, so this only
    /// affects newly created tables. An unrecognized value falls back to 2.2
    /// rather than failing a write.
    pub(crate) fn storage_version(&self) -> Option<LanceFileVersion> {
        match non_empty(&self.version) {
            Some("2.0") => Some(LanceFileVersion::V2_0),
            Some("2.1") => Some(LanceFileVersion::V2_1),
            _ => Some(LanceFileVersion::V2_2),
        }
    }

    /// Minichunk size in bytes, as the string Lance's field metadata expects, or
    /// `None` when unset or not a number.
    pub(crate) fn minichunk_size(&self) -> Option<String> {
        non_empty(&self.minichunk)?
            .parse::<i64>()
            .ok()
            .map(|n| n.to_string())
    }

    /// Materialization override, or `None` to keep beacon's projection-width rule.
    pub(crate) fn materialization_style(&self) -> Option<MaterializationStyle> {
        match non_empty(&self.materialization) {
            Some("late") => Some(MaterializationStyle::AllLate),
            Some("early") => Some(MaterializationStyle::AllEarly),
            _ => None,
        }
    }
}

/// The trimmed value, or `None` when it is empty — the "unset" spelling in this
/// config, since a config namespace has no null.
fn non_empty(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then_some(trimmed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_means_unset() {
        let config = LanceConfig::default();
        assert_eq!(config.string_compression(), None);
        assert_eq!(config.numeric_compression(), None);
        assert_eq!(config.minichunk_size(), None);
        assert!(config.materialization_style().is_none());
        // The version is the one setting with a beacon default rather than a
        // Lance one, so "unset" still resolves to 2.2.
        assert_eq!(config.storage_version(), Some(LanceFileVersion::V2_2));
    }

    /// `none` is how an operator turns string compression off, and it must not
    /// reach Lance as a scheme name.
    #[test]
    fn none_disables_string_compression() {
        let config = LanceConfig {
            compression: "none".to_string(),
            ..Default::default()
        };
        assert_eq!(config.string_compression(), None);

        let config = LanceConfig {
            compression: "zstd".to_string(),
            ..Default::default()
        };
        assert_eq!(config.string_compression(), Some("zstd"));
    }

    /// `none` on numerics is meaningful — it opts them out of Lance's automatic
    /// block compression — so unlike strings it is passed through.
    #[test]
    fn none_is_passed_through_for_numeric_compression() {
        let config = LanceConfig {
            numeric_compression: "none".to_string(),
            ..Default::default()
        };
        assert_eq!(config.numeric_compression(), Some("none"));
    }

    #[test]
    fn version_and_minichunk_parse() {
        let config = LanceConfig {
            version: "2.0".to_string(),
            minichunk: " 65536 ".to_string(),
            materialization: "late".to_string(),
            ..Default::default()
        };
        assert_eq!(config.storage_version(), Some(LanceFileVersion::V2_0));
        assert_eq!(config.minichunk_size().as_deref(), Some("65536"));
        assert!(matches!(
            config.materialization_style(),
            Some(MaterializationStyle::AllLate)
        ));

        // A non-numeric minichunk is ignored rather than failing a write.
        let config = LanceConfig {
            minichunk: "big".to_string(),
            ..Default::default()
        };
        assert_eq!(config.minichunk_size(), None);
    }
}
