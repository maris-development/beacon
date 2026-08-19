/**
 * The deployment prefix of the admin UI, resolved at run time.
 *
 * The server serves this UI at `{server.base_path}/admin`. `base_path` is a
 * server setting, so the build cannot know the prefix. `index.html` reads it
 * from the current URL and writes it into a `<base>` tag before the first asset
 * loads. Every path the app builds comes from that one value.
 */

/** The app root as an absolute path with a trailing slash, for example `/beacon/admin/`. */
export function appBase(): string {
  return new URL(document.baseURI).pathname;
}

/** The app root as a React Router basename: no trailing slash, or `/` at the root. */
export function routerBasename(): string {
  return appBase().replace(/\/+$/, "") || "/";
}

/**
 * The URL of the Beacon server that serves this UI: the origin plus the prefix
 * in front of `/admin`. The SDK adds `/admin/api/...` to it (the admin-gated
 * alias of the API), so a server at the root gives back only the origin.
 */
export function serverBase(): string {
  const { origin, pathname } = new URL(document.baseURI);
  const prefix = pathname.replace(/\/admin\/?$/, "").replace(/\/+$/, "");
  return origin + prefix;
}

/** The URL of a file in the `public` directory, for example the logo. */
export function assetUrl(name: string): string {
  return appBase() + name.replace(/^\/+/, "");
}
