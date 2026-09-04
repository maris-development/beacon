//! The server home page: a static index of the surfaces a Beacon server exposes.
//!
//! The page itself is `assets/home.html`, compiled into the binary. This module
//! only fills in its placeholders, once at startup: every value the page shows
//! (base path, admin UI presence, MCP state) is fixed for the life of the process.

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
/// A complete HTML document. Text taken from the configuration is escaped.
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

/// Builds the documentation URL for the running version.
fn docs_url(version: &str) -> String {
    format!("{DOCS_BASE}/{}/{DOCS_ENTRY}", docs_directory(version))
}

/// Names the documentation directory of `version`.
///
/// The directory of a pre-release drops the dot of its pre-release part: version
/// `2.0.0-rc.5` publishes as `docs/2.0.0-rc5`. A release bump renames that
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
    use super::{docs_directory, docs_url, render, DOCS_ENTRY};

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
        let page = render("Beacon", "2.0.0", "/beacon", true, true);
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
        let page = render("Beacon", "2.0.0-rc.5", "", false, false);
        assert!(page.contains(
            "href=\"https://maris-development.github.io/beacon/docs/2.0.0-rc5/introduction\""
        ));

        // A stable version publishes under its own number, dots intact.
        assert_eq!(
            docs_url("1.8.0"),
            "https://maris-development.github.io/beacon/docs/1.8.0/introduction"
        );
        // Build metadata names no directory.
        assert_eq!(docs_directory("2.0.0-rc.5+abc123"), "2.0.0-rc5");
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
}
