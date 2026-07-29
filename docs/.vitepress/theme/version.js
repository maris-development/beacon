// Single source of truth for the "latest" docs alias.
//
// `/docs/latest` and `/docs/latest/<any-page>` redirect to this version.
// Bump LATEST_VERSION when a new docs version ships; nothing else needs editing
// (config.mts imports this for the no-JS <meta refresh>, and the theme imports
// it for the client-side redirects).
//
// This is the newest *stable* version, not the newest folder: 2.0.0-rc1 is a
// pre-release, so `/docs/latest` and the 404 fallback deliberately resolve to
// 1.8.0. Bump to 2.0.0 when the RC goes GA.
export const LATEST_VERSION = '1.8.0'

// Landing page for the version, used when someone hits `/docs/latest` with no
// sub-path. There is no `docs/<version>/index.md`, so this must be a real page.
export const LATEST_ENTRY = 'introduction'

export const latestPath = (sub = LATEST_ENTRY) => `/docs/${LATEST_VERSION}/${sub}`
