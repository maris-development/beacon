//! The `OPTIONS` of an ERDDAP table.

use std::collections::HashMap;

use anyhow::{anyhow, bail};

/// Parsed table options. Valid key: `request_timeout_secs`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErddapOptions {
    /// The timeout of each HTTP request.
    pub request_timeout_secs: u64,
}

impl Default for ErddapOptions {
    fn default() -> Self {
        Self {
            request_timeout_secs: 600,
        }
    }
}

impl ErddapOptions {
    /// Parse `OPTIONS`. DataFusion prefixes keys with no dot with `format.`.
    pub fn from_map(options: &HashMap<String, String>) -> anyhow::Result<Self> {
        let mut parsed = Self::default();
        for (key, value) in options {
            let key = key.strip_prefix("format.").unwrap_or(key);
            let slot = match key {
                "request_timeout_secs" => &mut parsed.request_timeout_secs,
                other => {
                    bail!("unknown ERDDAP option '{other}'; valid option: request_timeout_secs")
                }
            };
            let number: u64 = value.trim().parse().map_err(|_| {
                anyhow!("ERDDAP option '{key}' must be a positive integer, got '{value}'")
            })?;
            if number == 0 {
                bail!("ERDDAP option '{key}' must be a positive integer, got '{value}'");
            }
            *slot = number;
        }
        Ok(parsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults_apply_without_options() {
        let o = ErddapOptions::from_map(&HashMap::new()).unwrap();
        assert_eq!(o.request_timeout_secs, 600);
    }

    #[test]
    fn reads_bare_and_prefixed_keys() {
        let o = ErddapOptions::from_map(&map(&[("request_timeout_secs", "5")])).unwrap();
        assert_eq!(o.request_timeout_secs, 5);
        let o = ErddapOptions::from_map(&map(&[("format.request_timeout_secs", "7")])).unwrap();
        assert_eq!(o.request_timeout_secs, 7);
    }

    #[test]
    fn rejects_unknown_zero_and_non_numeric() {
        assert!(
            ErddapOptions::from_map(&map(&[("tls", "true")]))
                .unwrap_err()
                .to_string()
                .contains("tls")
        );
        let err = ErddapOptions::from_map(&map(&[("max_cells_per_request", "5")]))
            .unwrap_err()
            .to_string();
        assert!(err.contains("valid option: request_timeout_secs"), "{err}");
        assert!(ErddapOptions::from_map(&map(&[("request_timeout_secs", "0")])).is_err());
        assert!(ErddapOptions::from_map(&map(&[("request_timeout_secs", "ten")])).is_err());
    }
}
