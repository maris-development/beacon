// Single source of truth for the "latest" docs alias.
//
// `/docs/latest` and `/docs/latest/<any-page>` redirect to this version.
// Bump LATEST_VERSION when a new docs version ships; nothing else needs editing
// (config.mts imports this for the no-JS <meta refresh>, and the theme imports
// it for the client-side redirects).
//
// This is the newest *stable* version, not the newest folder: a pre-release
// folder never becomes the target of `/docs/latest` or the 404 fallback.
export const LATEST_VERSION = '2.0.1'

// Landing page for the version, used when someone hits `/docs/latest` with no
// sub-path. There is no `docs/<version>/index.md`, so this must be a real page.
export const LATEST_ENTRY = 'introduction'

export const latestPath = (sub = LATEST_ENTRY) => `/docs/${LATEST_VERSION}/${sub}`
