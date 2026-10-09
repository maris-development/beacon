//! HTTP access to an ERDDAP server.

use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use tokio::io::AsyncWriteExt;

const NO_RESULTS: &str = "Your query produced no matching results";
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// An error from an ERDDAP request.
#[derive(Debug, thiserror::Error)]
pub enum ErddapError {
    /// The server answered with a failure status.
    #[error("ERDDAP request {url} failed with HTTP {status}: {message}")]
    Http {
        /// The request URL.
        url: String,
        /// The HTTP status code.
        status: u16,
        /// The message that ERDDAP gave, or an empty string.
        message: String,
    },
    /// The request ran longer than the request timeout.
    #[error(
        "ERDDAP request {url} timed out after {secs} s; raise OPTIONS ('request_timeout_secs' ...)"
    )]
    Timeout {
        /// The request URL.
        url: String,
        /// The timeout in seconds.
        secs: u64,
    },
    /// The request failed before a response arrived, or the body broke off.
    #[error("ERDDAP request {url} failed: {cause}")]
    Transport {
        /// The request URL.
        url: String,
        /// The error and its causes, joined by `: `.
        cause: String,
    },
    /// The temp file could not be written.
    #[error("ERDDAP response could not be written to a temp file: {0}")]
    Io(#[from] std::io::Error),
}

impl From<ErddapError> for datafusion::error::DataFusionError {
    fn from(error: ErddapError) -> Self {
        datafusion::error::DataFusionError::External(Box::new(error))
    }
}

/// A reqwest client with Beacon's timeouts and user agent.
#[derive(Clone, Debug)]
pub struct ErddapClient {
    http: reqwest::Client,
    timeout_secs: u64,
}

impl ErddapClient {
    /// Build a client. `request_timeout` caps the whole request, body included.
    pub fn new(request_timeout: Duration) -> anyhow::Result<Self> {
        Self::with_connect_timeout(request_timeout, CONNECT_TIMEOUT)
    }

    fn with_connect_timeout(
        request_timeout: Duration,
        connect_timeout: Duration,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent(concat!("Beacon/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(connect_timeout)
            .timeout(request_timeout)
            .build()?;
        Ok(Self {
            http,
            timeout_secs: request_timeout.as_secs(),
        })
    }

    /// A small response, read whole: info and axis JSON.
    pub async fn get_bytes(&self, url: &str) -> Result<Bytes, ErddapError> {
        let response = self.send(url).await?;
        let status = response.status();
        let body = response.bytes().await.map_err(|e| self.map(url, e))?;
        if !status.is_success() {
            return Err(http_error(url, status.as_u16(), &body));
        }
        Ok(body)
    }

    /// Stream a data response to a temp file. `None` when ERDDAP matched no rows.
    pub async fn download(
        &self,
        url: &str,
        suffix: &str,
    ) -> Result<Option<tempfile::NamedTempFile>, ErddapError> {
        let response = self.send(url).await?;
        let status = response.status();
        if !status.is_success() {
            let body = response.bytes().await.map_err(|e| self.map(url, e))?;
            if String::from_utf8_lossy(&body).contains(NO_RESULTS) {
                return Ok(None);
            }
            return Err(http_error(url, status.as_u16(), &body));
        }
        let file = tempfile::Builder::new()
            .prefix("beacon-erddap-")
            .suffix(suffix)
            .tempfile()?;
        let mut out = tokio::fs::File::from_std(file.reopen()?);
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            out.write_all(&chunk.map_err(|e| self.map(url, e))?).await?;
        }
        out.flush().await?;
        Ok(Some(file))
    }

    async fn send(&self, url: &str) -> Result<reqwest::Response, ErddapError> {
        tracing::debug!(url, "ERDDAP request");
        self.http
            .get(url)
            .send()
            .await
            .map_err(|e| self.map(url, e))
    }

    fn map(&self, url: &str, error: reqwest::Error) -> ErddapError {
        // A connect timeout is also a timeout, but a longer request timeout does not help.
        if error.is_connect() {
            ErddapError::Transport {
                url: url.to_string(),
                cause: format!("connection failed: {}", error_chain(&error.without_url())),
            }
        } else if error.is_timeout() {
            ErddapError::Timeout {
                url: url.to_string(),
                secs: self.timeout_secs,
            }
        } else {
            ErddapError::Transport {
                url: url.to_string(),
                cause: error_chain(&error.without_url()),
            }
        }
    }
}

/// The text of `error` followed by the text of each cause.
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut cause = error.source();
    while let Some(next) = cause {
        text.push_str(": ");
        text.push_str(&next.to_string());
        cause = next.source();
    }
    text
}

fn http_error(url: &str, status: u16, body: &[u8]) -> ErddapError {
    let text = String::from_utf8_lossy(body);
    ErddapError::Http {
        url: url.to_string(),
        status,
        message: erddap_message(&text).unwrap_or_default(),
    }
}

/// The `message="..."` of an ERDDAP `Error {...}` body, or the trimmed body.
fn erddap_message(body: &str) -> Option<String> {
    if let Some(start) = body.find("message=\"") {
        let rest = &body[start + "message=\"".len()..];
        // Scan to the closing `";`; `\"` and `\\` are escapes inside the message.
        let mut message = String::new();
        let mut chars = rest.chars().peekable();
        while let Some(c) = chars.next() {
            match c {
                '\\' if matches!(chars.peek(), Some('"' | '\\')) => {
                    message.push(chars.next().unwrap());
                }
                '"' if chars.peek() == Some(&';') => return Some(message),
                _ => message.push(c),
            }
        }
    }
    let trimmed = body.trim();
    (!trimmed.is_empty()).then(|| trimmed.chars().take(500).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{FixtureServer, Route};

    #[tokio::test]
    async fn downloads_a_response_to_a_temp_file() {
        let server = FixtureServer::start(vec![Route::file(
            "/erddap/tabledap/t.parquet",
            "tabledap.parquet",
        )])
        .await;
        let client = ErddapClient::new(Duration::from_secs(5)).unwrap();
        let file = client
            .download(
                &format!("{}/tabledap/t.parquet?a", server.erddap_url()),
                ".parquet",
            )
            .await
            .unwrap()
            .expect("a file");
        let expected = crate::fixture::test_file("tabledap.parquet");
        assert_eq!(std::fs::read(file.path()).unwrap(), expected.to_vec());
        let path = file.path().to_path_buf();
        drop(file);
        assert!(!path.exists(), "the temp file is removed on drop");
    }

    #[tokio::test]
    async fn transport_errors_keep_their_cause() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = ErddapClient::new(Duration::from_secs(5)).unwrap();
        let url = format!("http://127.0.0.1:{port}/erddap/info/t/index.json");
        let error = client.get_bytes(&url).await.unwrap_err();
        let text = error.to_string();
        let cause = text.split_once("failed: ").map(|(_, c)| c).unwrap_or("");
        assert!(!cause.trim().is_empty(), "{text}");
        let wrapped: datafusion::error::DataFusionError = error.into();
        assert!(wrapped.to_string().contains(cause), "{wrapped}");
    }

    #[tokio::test]
    async fn a_refused_connection_names_the_url_once() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let client = ErddapClient::new(Duration::from_secs(5)).unwrap();
        let url = format!("http://127.0.0.1:{port}/erddap/info/t/index.json");
        let error = client.get_bytes(&url).await.unwrap_err();
        assert!(matches!(error, ErddapError::Transport { .. }), "{error:?}");
        let text = error.to_string();
        assert!(text.contains("connection failed"), "{text}");
        assert_eq!(text.matches(&url).count(), 1, "{text}");
    }

    #[tokio::test]
    async fn a_connect_timeout_is_not_a_request_timeout() {
        let client =
            ErddapClient::with_connect_timeout(Duration::from_secs(5), Duration::from_millis(200))
                .unwrap();
        // A non-routable address: the connect times out or the network is unreachable.
        let url = "http://10.255.255.1:81/erddap/info/t/index.json";
        let error = client.get_bytes(url).await.unwrap_err();
        assert!(matches!(error, ErddapError::Transport { .. }), "{error:?}");
        let text = error.to_string();
        assert!(text.contains("connection failed"), "{text}");
        assert!(!text.contains("request_timeout_secs"), "{text}");
    }

    #[tokio::test]
    async fn no_matching_results_is_none() {
        let body = crate::fixture::test_file("no_results.txt");
        let server =
            FixtureServer::start(vec![Route::status("/erddap/tabledap/t.parquet", 404, body)])
                .await;
        let client = ErddapClient::new(Duration::from_secs(5)).unwrap();
        let url = format!("{}/tabledap/t.parquet", server.erddap_url());
        assert!(client.download(&url, ".parquet").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn other_errors_carry_status_url_and_message() {
        let body = crate::fixture::test_file("error_500.txt");
        let server =
            FixtureServer::start(vec![Route::status("/erddap/tabledap/t.parquet", 500, body)])
                .await;
        let client = ErddapClient::new(Duration::from_secs(5)).unwrap();
        let url = format!("{}/tabledap/t.parquet", server.erddap_url());
        let err = client
            .download(&url, ".parquet")
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("500"), "{err}");
        assert!(err.contains(&url), "{err}");
        assert!(!err.contains("Error {"), "the message is parsed out: {err}");
        assert!(
            err.contains("Unrecognized variable=\"no_such_variable\""),
            "the message is unescaped: {err}"
        );
    }

    #[test]
    fn parses_the_erddap_message() {
        let body = "Error {\n    code=404;\n    message=\"Not Found: Your query produced no matching results.\";\n}\n";
        assert_eq!(
            erddap_message(body).as_deref(),
            Some("Not Found: Your query produced no matching results.")
        );
        let escaped = "Error {\n    message=\"a \\\"q\\\"; b\\\\c\\\";\";\n}\n";
        assert_eq!(erddap_message(escaped).as_deref(), Some("a \"q\"; b\\c\";"));
        assert_eq!(erddap_message("plain text").as_deref(), Some("plain text"));
    }
}
