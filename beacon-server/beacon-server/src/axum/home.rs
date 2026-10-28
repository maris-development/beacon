//! The server home page: a static index of the surfaces a Beacon server exposes.
//!
//! The page itself is `assets/home.html`, compiled into the binary. This module
//! fills in its placeholders once at startup: the base path, admin UI presence
//! and MCP state are fixed for the life of the process. Only the Croissant
//! catalog JSON-LD changes per request, see [`HomePage`].

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use ::axum::http::header::VARY;
use ::axum::http::{HeaderMap, HeaderValue};
use ::axum::response::{Html, IntoResponse, Response};
use beacon_core::runtime::Runtime;
use beacon_croissant::{
    base_url, catalog, catalog_entries, CatalogEntry, CroissantSettings, LINK_HEADERS,
};
use tokio::sync::Mutex;
use tokio::time::{timeout_at, Instant};

/// How long the home page keeps the catalog entries. Crawlers load the page often.
const CATALOG_TTL: Duration = Duration::from_secs(60);

/// The longest time that a request waits for a catalog refresh.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(5);

/// The time after a failed refresh before the next try.
const RETRY_AFTER: Duration = Duration::from_secs(10);

/// The page template, with `{{...}}` placeholders and optional sections.
const TEMPLATE: &str = include_str!("../../assets/home.html");

/// Root of the published documentation, one directory per released version.
const DOCS_BASE: &str = "https://maris-development.github.io/beacon/docs";

/// Landing page of a documentation version. No version directory holds an
/// `index.md`, so a bare directory URL is a 404.
const DOCS_ENTRY: &str = "introduction";

/// Renders the home page served at the router root.
///
/// # Arguments
///
/// * `title` - Deployment title, from the API documentation configuration
/// * `version` - Beacon version of the running binary, shown and linked to
/// * `base_path` - Normalized path prefix, empty or `/prefix`
/// * `web_ui` - `true` when the admin web UI is mounted, which shows its card
/// * `mcp` - `true` when the MCP endpoint is enabled, which shows its address
///
/// # Returns
///
/// A complete HTML document. Text taken from the configuration is escaped. The
/// `{{jsonld}}` placeholder stays for [`with_jsonld`].
pub(crate) fn render(
    title: &str,
    version: &str,
    base_path: &str,
    web_ui: bool,
    mcp: bool,
) -> String {
    let page = section(TEMPLATE, "admin", web_ui);
    let page = section(&page, "mcp", mcp);
    page.replace("{{title}}", &escape(title))
        .replace("{{version}}", &escape(version))
        .replace("{{docs}}", &docs_url(version))
        // Links carry the base path because the root answers on both `/prefix`
        // and `/prefix/`. Relative targets resolve one level up on the first.
        .replace("{{base}}", base_path)
}

/// Fills the `{{jsonld}}` placeholder of a rendered page.
///
/// `Some` gives a `<script type="application/ld+json">` element; `None` gives nothing.
/// Each `<` in the JSON becomes the six characters `\u003c`, so no text in the catalog can
/// end the script.
pub(crate) fn with_jsonld(page: &str, catalog: Option<&serde_json::Value>) -> String {
    let script = match catalog {
        Some(catalog) => format!(
            "<script type=\"application/ld+json\">{}</script>",
            catalog.to_string().replace('<', "\\u003c")
        ),
        None => String::new(),
    };
    page.replace("{{jsonld}}", &script)
}

/// The home page handler state: the rendered page and, when Croissant is on, its catalog.
pub(crate) struct HomePage {
    page: String,
    catalog: Option<HomeSource>,
}

/// The home page source of the anonymous Croissant catalog.
struct HomeSource {
    runtime: Arc<Runtime>,
    settings: CroissantSettings,
    entries: HomeCatalog,
}

/// Loads the catalog entries of the anonymous user.
type Loader = Box<
    dyn Fn() -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<CatalogEntry>>> + Send>>
        + Send
        + Sync,
>;

/// The catalog entries that the home page keeps.
struct HomeCatalog {
    loader: Loader,
    /// Only the entries, never a page: the base URL comes from each request.
    state: std::sync::Mutex<CatalogState>,
    /// Held while a refresh runs, so at most one refresh runs.
    refresh: Mutex<()>,
}

#[derive(Default)]
struct CatalogState {
    loaded: Option<(Instant, Vec<CatalogEntry>)>,
    failed: Option<Instant>,
}

impl HomePage {
    /// A home page from a [`render`] output. `croissant` is `None` when Croissant is off.
    pub(crate) fn new(page: String, croissant: Option<(Arc<Runtime>, CroissantSettings)>) -> Self {
        let catalog = croissant.map(|(runtime, settings)| {
            let loader_runtime = runtime.clone();
            let loader: Loader = Box::new(move || {
                let runtime = loader_runtime.clone();
                Box::pin(async move {
                    let identity = runtime.authenticate_anonymous().await?;
                    catalog_entries(&runtime, &identity).await
                })
            });
            HomeSource {
                runtime,
                settings,
                entries: HomeCatalog::new(loader),
            }
        });
        Self { page, catalog }
    }

    /// The page for one request, with the anonymous catalog when Croissant and
    /// anonymous access are on.
    pub(crate) async fn serve(&self, headers: &HeaderMap) -> Response {
        let document = match &self.catalog {
            Some(source) if source.runtime.anonymous_enabled() => {
                source.entries.entries().await.map(|entries| {
                    let base = base_url(&source.settings, headers);
                    catalog(&source.settings.catalog_title, &entries, &base)
                })
            }
            _ => None,
        };
        let mut response = Html(with_jsonld(&self.page, document.as_ref())).into_response();
        if let Some(source) = &self.catalog {
            if source.settings.public_url.is_none() {
                response
                    .headers_mut()
                    .append(VARY, HeaderValue::from_static(LINK_HEADERS));
            }
        }
        response
    }
}

impl HomeCatalog {
    fn new(loader: Loader) -> Self {
        Self {
            loader,
            state: std::sync::Mutex::new(CatalogState::default()),
            refresh: Mutex::new(()),
        }
    }

    /// The catalog entries of the anonymous user, at most [`CATALOG_TTL`] old.
    ///
    /// A request waits at most [`REFRESH_TIMEOUT`]. A failed or late refresh gives the
    /// stale entries, else `None`.
    async fn entries(&self) -> Option<Vec<CatalogEntry>> {
        let deadline = Instant::now() + REFRESH_TIMEOUT;
        if let Some(entries) = self.current() {
            return entries;
        }
        let Ok(_refresh) = timeout_at(deadline, self.refresh.lock()).await else {
            return self.stale();
        };
        // An other request can have refreshed while this one waited.
        if let Some(entries) = self.current() {
            return entries;
        }
        let result = match timeout_at(deadline, (self.loader)()).await {
            Ok(result) => result,
            Err(_) => Err(anyhow::anyhow!("no answer in {REFRESH_TIMEOUT:?}")),
        };
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        match result {
            Ok(entries) => {
                state.loaded = Some((Instant::now(), entries.clone()));
                state.failed = None;
                Some(entries)
            }
            Err(error) => {
                tracing::warn!(%error, "home page served without a fresh Croissant catalog");
                // The failure time only delays the next try; the entries stay as they were.
                state.failed = Some(Instant::now());
                state.loaded.as_ref().map(|(_, entries)| entries.clone())
            }
        }
    }

    /// `Some` when no refresh is due: fresh entries, or the stale entries after a recent failure.
    fn current(&self) -> Option<Option<Vec<CatalogEntry>>> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let stale = state.loaded.as_ref().map(|(_, entries)| entries.clone());
        if let Some((loaded, _)) = &state.loaded {
            if loaded.elapsed() < CATALOG_TTL {
                return Some(stale);
            }
        }
        match state.failed {
            Some(failed) if failed.elapsed() < RETRY_AFTER => Some(stale),
            _ => None,
        }
    }

    /// The last entries that loaded, of any age.
    fn stale(&self) -> Option<Vec<CatalogEntry>> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.loaded.as_ref().map(|(_, entries)| entries.clone())
    }
}

/// Builds the documentation URL for the running version.
fn docs_url(version: &str) -> String {
    format!("{DOCS_BASE}/{}/{DOCS_ENTRY}", docs_directory(version))
}

/// Names the documentation directory of `version`.
///
/// The directory of a pre-release drops the dot of its pre-release part: version
/// `2.0.0-rc.6` publishes as `docs/2.0.0-rc6`. A release bump renames that
/// directory, so the running binary and its documentation stay in step.
fn docs_directory(version: &str) -> String {
    // Build metadata is not part of a directory name.
    let version = version.split('+').next().unwrap_or(version);
    match version.split_once('-') {
        Some((release, pre_release)) => format!("{release}-{}", pre_release.replace('.', "")),
        None => version.to_string(),
    }
}

/// Keeps or drops an optional section of the template.
///
/// A section runs from `<!--[name]-->` to `<!--[/name]-->`. `keep` removes the two
/// markers and holds the content between them; otherwise the whole block goes.
///
/// The markers exclude their line break, so the template works with either line
/// ending: Git rewrites the checked-out file on Windows.
fn section(page: &str, name: &str, keep: bool) -> String {
    let open = format!("<!--[{name}]-->");
    let close = format!("<!--[/{name}]-->");
    if keep {
        return page.replace(&open, "").replace(&close, "");
    }
    let (Some(start), Some(end)) = (page.find(&open), page.find(&close)) else {
        return page.to_string();
    };
    let mut out = String::with_capacity(page.len());
    out.push_str(&page[..start]);
    out.push_str(&page[end + close.len()..]);
    out
}

/// Escapes the characters that carry meaning in HTML text and attributes.
fn escape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            _ => out.push(character),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        docs_directory, docs_url, render, with_jsonld, HomeCatalog, Loader, CATALOG_TTL,
        DOCS_ENTRY, REFRESH_TIMEOUT, RETRY_AFTER,
    };
    use beacon_croissant::{catalog, CatalogEntry};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn optional_sections_follow_what_the_server_mounts() {
        let full = render("Beacon", "2.0.0", "", true, true);
        assert!(full.contains("href=\"/admin/\""));
        assert!(full.contains("<code>/mcp</code>"));

        let bare = render("Beacon", "2.0.0", "", false, false);
        assert!(!bare.contains("/admin/"));
        assert!(!bare.contains("/mcp"));
        assert!(
            !bare.contains("<!--["),
            "markers must not reach the browser"
        );
        // The rest of the page survives the cuts.
        assert!(bare.contains("href=\"/swagger\""));
    }

    #[test]
    fn every_link_carries_the_base_path() {
        let page = with_jsonld(&render("Beacon", "2.0.0", "/beacon", true, true), None);
        for href in [
            "/beacon/admin/",
            "/beacon/swagger",
            "/beacon/scalar/",
            "/beacon/openapi.json",
            "/beacon/api/health",
        ] {
            assert!(page.contains(&format!("href=\"{href}\"")), "missing {href}");
        }
        assert!(!page.contains("{{"), "no placeholder may survive");
    }

    #[test]
    fn the_docs_link_points_at_the_running_version() {
        let page = render("Beacon", "2.0.0-rc.6", "", false, false);
        assert!(page.contains(
            "href=\"https://maris-development.github.io/beacon/docs/2.0.0-rc6/introduction\""
        ));

        // A stable version publishes under its own number, dots intact.
        assert_eq!(
            docs_url("1.8.0"),
            "https://maris-development.github.io/beacon/docs/1.8.0/introduction"
        );
        // Build metadata names no directory.
        assert_eq!(docs_directory("2.0.0-rc.6+abc123"), "2.0.0-rc6");
    }

    /// A release bump renames the docs directory. This test holds the two
    /// together: the page must not link a directory the repository lacks.
    #[test]
    fn the_repository_carries_the_docs_of_the_compiled_version() {
        let versions = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/docs");
        if !versions.is_dir() {
            return; // Built outside a checkout of the repository.
        }

        let entry = versions
            .join(docs_directory(env!("CARGO_PKG_VERSION")))
            .join(format!("{DOCS_ENTRY}.md"));
        assert!(entry.is_file(), "the home page links {}", entry.display());
    }

    #[test]
    fn a_configured_title_cannot_inject_markup() {
        let page = render("<script>alert(1)</script>", "2.0.0", "", false, false);
        assert!(!page.contains("<script>alert"));
        assert!(page.contains("&lt;script&gt;alert(1)&lt;/script&gt;"));
    }

    #[test]
    fn script_close_is_escaped() {
        let entries = [CatalogEntry {
            name: "obs".to_string(),
            description: Some("x</script><b>".to_string()),
        }];
        let document = catalog("Beacon", &entries, "http://localhost");
        let page = with_jsonld(
            &render("Beacon", "2.0.0", "", false, false),
            Some(&document),
        );

        assert!(page.contains("<script type=\"application/ld+json\">"));
        assert_eq!(
            page.matches(r"\u003c/script>").count(),
            1,
            "the escaped text"
        );
        assert_eq!(
            page.matches("</script>").count(),
            1,
            "only the real end tag"
        );
        assert!(!page.contains("<b>"));
    }

    fn entry(name: &str) -> CatalogEntry {
        CatalogEntry {
            name: name.to_string(),
            description: None,
        }
    }

    /// A catalog whose loader gives the next item of `results` and counts its calls.
    /// `None` is a loader that never ends.
    fn catalog_of(
        results: Vec<Option<anyhow::Result<Vec<CatalogEntry>>>>,
    ) -> (HomeCatalog, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let results = Arc::new(std::sync::Mutex::new(VecDeque::from(results)));
        let counter = calls.clone();
        let loader: Loader = Box::new(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            let next = results.lock().unwrap().pop_front().expect("one more load");
            Box::pin(async move {
                match next {
                    Some(result) => result,
                    None => std::future::pending().await,
                }
            })
        });
        (HomeCatalog::new(loader), calls)
    }

    fn names(entries: Option<Vec<CatalogEntry>>) -> Option<Vec<String>> {
        entries.map(|entries| entries.into_iter().map(|e| e.name).collect())
    }

    #[tokio::test(start_paused = true)]
    async fn failed_refresh_is_not_cached() {
        let (home, calls) = catalog_of(vec![
            Some(Err(anyhow::anyhow!("provider down"))),
            Some(Ok(vec![entry("obs")])),
        ]);

        assert_eq!(names(home.entries().await), None);
        tokio::time::advance(RETRY_AFTER).await;
        assert_eq!(names(home.entries().await), Some(vec!["obs".to_string()]));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn stale_entries_survive_a_failed_refresh() {
        let (home, calls) = catalog_of(vec![
            Some(Ok(vec![entry("obs")])),
            Some(Err(anyhow::anyhow!("provider down"))),
            Some(Ok(vec![entry("obs2")])),
        ]);
        assert_eq!(names(home.entries().await), Some(vec!["obs".to_string()]));

        tokio::time::advance(CATALOG_TTL).await;
        assert_eq!(names(home.entries().await), Some(vec!["obs".to_string()]));
        // The back-off keeps the stale entries without a new load.
        assert_eq!(names(home.entries().await), Some(vec!["obs".to_string()]));
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        tokio::time::advance(RETRY_AFTER).await;
        assert_eq!(names(home.entries().await), Some(vec!["obs2".to_string()]));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_loader_gives_the_page_within_the_timeout() {
        let (home, calls) = catalog_of(vec![None]);
        let start = tokio::time::Instant::now();

        let (first, second) = tokio::join!(home.entries(), home.entries());

        assert_eq!(names(first), None);
        assert_eq!(names(second), None);
        assert_eq!(start.elapsed(), REFRESH_TIMEOUT);
        assert_eq!(calls.load(Ordering::SeqCst), 1, "one refresh at a time");
    }

    #[test]
    fn no_catalog_gives_no_script() {
        let page = with_jsonld(&render("Beacon", "2.0.0", "", false, false), None);
        assert!(!page.contains("application/ld+json"));
        assert!(!page.contains("{{jsonld}}"));
    }
}
