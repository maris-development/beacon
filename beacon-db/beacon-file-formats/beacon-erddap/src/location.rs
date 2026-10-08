//! The `LOCATION` of an ERDDAP table: the full dataset URL.

use anyhow::{anyhow, ensure};

const EXPECTED: &str = "ERDDAP LOCATION must be 'http(s)://host/erddap/tabledap/<datasetID>' \
                        or 'http(s)://host/erddap/griddap/<datasetID>'";

/// The ERDDAP service a dataset is served by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Protocol {
    Tabledap,
    Griddap,
}

impl Protocol {
    /// The URL path segment of the service.
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Tabledap => "tabledap",
            Protocol::Griddap => "griddap",
        }
    }
}

/// A parsed ERDDAP dataset URL.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ErddapLocation {
    /// The ERDDAP base URL, e.g. `https://host/erddap`, with no trailing slash.
    pub server: String,
    pub protocol: Protocol,
    pub dataset_id: String,
}

impl ErddapLocation {
    /// Parse a dataset URL as copied from the ERDDAP web pages.
    ///
    /// A file extension on the dataset ID and a query string are removed.
    pub fn parse(location: &str) -> anyhow::Result<Self> {
        let url =
            url::Url::parse(location).map_err(|e| anyhow!("{EXPECTED}, got '{location}' ({e})"))?;
        ensure!(
            matches!(url.scheme(), "http" | "https"),
            "{EXPECTED}, got '{location}'"
        );
        let mut segments: Vec<&str> = url.path_segments().map(|s| s.collect()).unwrap_or_default();
        if segments.last() == Some(&"") {
            segments.pop();
        }
        let position = segments
            .iter()
            .rposition(|s| *s == "tabledap" || *s == "griddap")
            .ok_or_else(|| anyhow!("{EXPECTED}, got '{location}'"))?;
        ensure!(
            segments.len() == position + 2,
            "{EXPECTED}, got '{location}'"
        );
        let protocol = if segments[position] == "tabledap" {
            Protocol::Tabledap
        } else {
            Protocol::Griddap
        };
        let dataset_id = segments[position + 1]
            .split('.')
            .next()
            .unwrap_or_default()
            .to_string();
        ensure!(!dataset_id.is_empty(), "{EXPECTED}, got '{location}'");

        let mut server = url.clone();
        server.set_query(None);
        server.set_fragment(None);
        server.set_path(&segments[..position].join("/"));
        Ok(Self {
            server: server.as_str().trim_end_matches('/').to_string(),
            protocol,
            dataset_id,
        })
    }

    /// The dataset metadata endpoint.
    pub fn info_url(&self) -> String {
        format!("{}/info/{}/index.json", self.server, self.dataset_id)
    }

    /// A data request. `query` must already be percent-encoded.
    pub fn data_url(&self, extension: &str, query: &str) -> String {
        let base = format!(
            "{}/{}/{}.{}",
            self.server,
            self.protocol.as_str(),
            self.dataset_id,
            extension
        );
        if query.is_empty() {
            base
        } else {
            format!("{base}?{query}")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_tabledap_url() {
        let l = ErddapLocation::parse("https://host.org/erddap/tabledap/erdGlobecBottle").unwrap();
        assert_eq!(l.server, "https://host.org/erddap");
        assert_eq!(l.protocol, Protocol::Tabledap);
        assert_eq!(l.dataset_id, "erdGlobecBottle");
    }

    #[test]
    fn strips_extension_query_and_trailing_slash() {
        let l =
            ErddapLocation::parse("http://h:8080/erddap/griddap/erdHadISST.html?sst[0]").unwrap();
        assert_eq!(l.server, "http://h:8080/erddap");
        assert_eq!(l.protocol, Protocol::Griddap);
        assert_eq!(l.dataset_id, "erdHadISST");
        let l = ErddapLocation::parse("https://h/erddap/tabledap/x/").unwrap();
        assert_eq!(l.dataset_id, "x");
    }

    #[test]
    fn builds_urls() {
        let l = ErddapLocation::parse("https://h/erddap/griddap/g").unwrap();
        assert_eq!(l.info_url(), "https://h/erddap/info/g/index.json");
        assert_eq!(
            l.data_url("nc", "sst%5B0%5D"),
            "https://h/erddap/griddap/g.nc?sst%5B0%5D"
        );
        assert_eq!(l.data_url("json", ""), "https://h/erddap/griddap/g.json");
    }

    #[test]
    fn rejects_bad_locations() {
        for bad in [
            "erddap/tabledap/x",
            "s3://bucket/erddap/tabledap/x",
            "https://h/erddap/info/x/index.json",
            "https://h/erddap/tabledap/",
            "https://h/erddap/tabledap/x/extra",
        ] {
            let err = ErddapLocation::parse(bad).unwrap_err().to_string();
            assert!(err.contains("/tabledap/<datasetID>"), "{bad}: {err}");
        }
    }
}
