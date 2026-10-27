//! The absolute base URL of the Croissant links.

use axum::http::HeaderMap;

/// Server settings that the Croissant documents need.
#[derive(Debug, Clone)]
pub struct CroissantSettings {
    /// `BEACON_PUBLIC_URL` with no final `/`. It includes the base path.
    pub public_url: Option<String>,
    /// `BEACON_BASE_PATH`: empty, or one leading `/` and no final `/`.
    pub base_path: String,
    /// The `name` of the catalog document.
    pub catalog_title: String,
}

/// Gives the absolute URL of the server root, with no final `/`.
///
/// `public_url` wins. Else the URL is `{proto}://{host}{base_path}`. `proto` is the first
/// `X-Forwarded-Proto` value if it is `http` or `https`, else `http`. `host` is the first
/// valid `X-Forwarded-Host` value, else a valid `Host`, else `localhost`.
pub fn base_url(settings: &CroissantSettings, headers: &HeaderMap) -> String {
    if let Some(public_url) = &settings.public_url {
        return public_url.clone();
    }
    let proto = first_value(headers, "x-forwarded-proto")
        .map(str::to_ascii_lowercase)
        .filter(|proto| proto == "http" || proto == "https")
        .unwrap_or_else(|| "http".to_string());
    let host = first_value(headers, "x-forwarded-host")
        .filter(|host| valid_host(host))
        .or_else(|| first_value(headers, "host").filter(|host| valid_host(host)))
        .unwrap_or("localhost");
    format!("{proto}://{host}{}", settings.base_path)
}

/// Refuses a host that could change the path, query or user part of the URL.
fn valid_host(host: &str) -> bool {
    !host.is_empty()
        && !host
            .chars()
            .any(|c| matches!(c, '/' | '?' | '#' | '@' | '\\') || c.is_whitespace())
}

/// The first comma-separated value of a header; a proxy chain appends its own values.
fn first_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let value = headers.get(name)?.to_str().ok()?;
    let first = value.split(',').next()?.trim();
    (!first.is_empty()).then_some(first)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn settings(public_url: Option<&str>, base_path: &str) -> CroissantSettings {
        CroissantSettings {
            public_url: public_url.map(str::to_string),
            base_path: base_path.to_string(),
            catalog_title: "Beacon".to_string(),
        }
    }

    fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(*name, value.parse().unwrap());
        }
        map
    }

    #[test]
    fn base_url_prefers_the_public_url() {
        let s = settings(Some("https://b.example.org/beacon"), "/other");
        let h = headers(&[("host", "x")]);
        assert_eq!(base_url(&s, &h), "https://b.example.org/beacon");
    }

    #[test]
    fn base_url_reads_forwarded_headers() {
        let s = settings(None, "/beacon");
        let h = headers(&[
            ("x-forwarded-proto", "https, http"),
            ("x-forwarded-host", "proxy.org"),
            ("host", "internal:5001"),
        ]);
        assert_eq!(base_url(&s, &h), "https://proxy.org/beacon");
    }

    #[test]
    fn base_url_falls_back_to_host() {
        let s = settings(None, "");
        let h = headers(&[("host", "localhost:5001")]);
        assert_eq!(base_url(&s, &h), "http://localhost:5001");
    }

    #[test]
    fn base_url_refuses_a_bad_proto() {
        let s = settings(None, "");
        let h = headers(&[("x-forwarded-proto", "javascript"), ("host", "a.org")]);
        assert_eq!(base_url(&s, &h), "http://a.org");
        let h = headers(&[("x-forwarded-proto", "HTTPS"), ("host", "a.org")]);
        assert_eq!(base_url(&s, &h), "https://a.org");
    }

    #[test]
    fn base_url_refuses_a_bad_host() {
        let s = settings(None, "");
        for bad in ["evil.org/x", "a?b", "a#b", "user@evil.org", "a\\b", "a b"] {
            let h = headers(&[("x-forwarded-host", bad), ("host", "good.org")]);
            assert_eq!(base_url(&s, &h), "http://good.org", "forwarded {bad}");
            let h = headers(&[("host", bad)]);
            assert_eq!(base_url(&s, &h), "http://localhost", "host {bad}");
        }
    }
}
