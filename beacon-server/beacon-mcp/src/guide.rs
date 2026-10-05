//! The built-in guide of every Beacon instance: how Beacon works, how to write a
//! query and which client to use in a script. The `get_guide` tool returns it,
//! with the address of this server filled in.

use http::header::HOST;
use http::request::Parts;
use http::uri::Authority;

const GUIDE: &str = include_str!("guide.md");
const FLIGHT_SQL_GUIDE: &str = include_str!("guide_flight_sql.md");
/// Shown in place of the server URL when the request names no host.
const URL_PLACEHOLDER: &str = "<BEACON_URL>";
const HOST_PLACEHOLDER: &str = "<BEACON_HOST>";

/// The server settings that the guide shows to an agent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GuideConfig {
    /// The path prefix of every route, such as `/beacon`, or empty for none.
    pub base_path: String,
    /// The Flight SQL port, when Flight SQL is on and accepts anonymous sessions.
    /// Only then can a `beacondb` remote table reach this server.
    pub anonymous_flight_sql_port: Option<u16>,
}

/// The scheme and the authority that a client uses to reach this server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAddress {
    scheme: &'static str,
    authority: Authority,
}

impl ServerAddress {
    /// Read the address from the HTTP request of an MCP call.
    ///
    /// A reverse proxy sets `X-Forwarded-Proto` and `X-Forwarded-Host`, so those
    /// win over the `Host` header. The scheme is `http` unless the proxy says
    /// `https`.
    ///
    /// # Returns
    ///
    /// `None` when the request names no valid host.
    pub fn from_parts(parts: &Parts) -> Option<Self> {
        let authority = first_header_value(parts, "x-forwarded-host")
            .and_then(|host| host.parse::<Authority>().ok())
            .or_else(|| first_header_value(parts, HOST.as_str())?.parse().ok())
            .or_else(|| parts.uri.authority().cloned())?;
        let https = first_header_value(parts, "x-forwarded-proto")
            .map(|proto| proto.eq_ignore_ascii_case("https"))
            .unwrap_or_else(|| parts.uri.scheme_str() == Some("https"));
        Some(Self {
            scheme: if https { "https" } else { "http" },
            authority,
        })
    }
}

/// The first value of a header, because a chain of proxies joins values with commas.
fn first_header_value<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
    let value = parts.headers.get(name)?.to_str().ok()?;
    value
        .split(',')
        .next()
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// The base URL of the client API, such as `https://beacon.example.org/beacon`.
///
/// # Arguments
///
/// * `config` - Supplies the path prefix of the routes.
/// * `address` - The address from the request, if the request gave one.
///
/// # Returns
///
/// `<BEACON_URL>` when `address` is `None`, so the agent knows to ask for it.
pub fn beacon_url(config: &GuideConfig, address: Option<&ServerAddress>) -> String {
    match address {
        Some(address) => format!(
            "{}://{}{}",
            address.scheme, address.authority, config.base_path
        ),
        None => URL_PLACEHOLDER.to_string(),
    }
}

/// Render the full guide for one caller.
///
/// # Arguments
///
/// * `config` - The server settings that the guide shows.
/// * `address` - The address from the request, if the request gave one.
///
/// # Returns
///
/// The guide as Markdown. It has the `beacondb` section only when
/// `config.anonymous_flight_sql_port` is set.
pub fn render(config: &GuideConfig, address: Option<&ServerAddress>) -> String {
    let mut guide = GUIDE.replace("{beacon_url}", &beacon_url(config, address));
    if let Some(port) = config.anonymous_flight_sql_port {
        // `host()` drops the HTTP port; Flight SQL listens on a port of its own.
        let host = address.map_or(HOST_PLACEHOLDER, |address| address.authority.host());
        guide.push_str(
            &FLIGHT_SQL_GUIDE
                .replace("{flight_host}", host)
                .replace("{flight_port}", &port.to_string()),
        );
    }
    guide
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    fn parts(headers: &[(&str, &str)]) -> Parts {
        let mut request = Request::builder().uri("/mcp");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).expect("valid test request").into_parts().0
    }

    fn address(headers: &[(&str, &str)]) -> Option<ServerAddress> {
        ServerAddress::from_parts(&parts(headers))
    }

    #[test]
    fn the_url_comes_from_the_host_header() {
        let config = GuideConfig::default();

        let url = beacon_url(&config, address(&[("host", "localhost:5002")]).as_ref());

        assert_eq!(url, "http://localhost:5002");
    }

    #[test]
    fn forwarded_headers_win_and_the_base_path_follows() {
        let config = GuideConfig {
            base_path: "/beacon".to_string(),
            ..GuideConfig::default()
        };
        let headers = [
            ("host", "10.0.0.5:5001"),
            ("x-forwarded-host", "beacon.example.org, proxy.internal"),
            ("x-forwarded-proto", "HTTPS"),
        ];

        let url = beacon_url(&config, address(&headers).as_ref());

        assert_eq!(url, "https://beacon.example.org/beacon");
    }

    #[test]
    fn an_invalid_forwarded_host_falls_back_to_the_host_header() {
        let headers = [("host", "localhost:5001"), ("x-forwarded-host", "bad host")];

        let url = beacon_url(&GuideConfig::default(), address(&headers).as_ref());

        assert_eq!(url, "http://localhost:5001");
    }

    #[test]
    fn no_host_gives_the_placeholder() {
        let guide = render(&GuideConfig::default(), address(&[]).as_ref());

        assert!(guide.contains("POST <BEACON_URL>/api/query"), "{guide}");
        assert!(!guide.contains("{beacon_url}"));
    }

    #[test]
    fn anonymous_flight_sql_adds_the_beacondb_recipe() {
        let config = GuideConfig {
            anonymous_flight_sql_port: Some(32011),
            ..GuideConfig::default()
        };

        let guide = render(
            &config,
            address(&[("host", "beacon.example.org:5001")]).as_ref(),
        );

        assert!(
            guide.contains("beacon://beacon.example.org:32011/<table>"),
            "{guide}"
        );
        assert!(guide.contains("Client(\"http://beacon.example.org:5001\")"));
        assert!(!guide.contains("{flight_"));
    }

    #[test]
    fn the_recipe_names_a_placeholder_host_without_a_request_host() {
        let config = GuideConfig {
            anonymous_flight_sql_port: Some(32011),
            ..GuideConfig::default()
        };

        let guide = render(&config, None);

        assert!(
            guide.contains("beacon://<BEACON_HOST>:32011/<table>"),
            "{guide}"
        );
    }

    #[test]
    fn without_anonymous_flight_sql_the_guide_has_no_beacondb_recipe() {
        let guide = render(&GuideConfig::default(), None);

        assert!(!guide.contains("beacondb"));
        assert!(guide.contains("beacon-api"));
    }
}
