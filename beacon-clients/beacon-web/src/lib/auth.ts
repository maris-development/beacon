/** Connection + admin-credential storage for the login-gated admin UI. */

import { ADMIN_API_PREFIX, BeaconClient } from "@beacon/client";

import { serverBase } from "./base-path";

const STORAGE_KEY = "beacon-web.connection";

/**
 * Latch that stops a proxy session from returning right after a sign-out. It
 * lives in `sessionStorage`, so it holds for this browser tab only.
 */
const PROXY_SIGNED_OUT_KEY = "beacon-web.proxy-signed-out";

/**
 * How a session authenticates to Beacon.
 *
 * `credentials`: the UI holds admin credentials and sends its own basic-auth
 * header on every request.
 *
 * `proxy`: a gateway in front of Beacon (nginx, oauth2-proxy) sets the
 * `Authorization` header on every upstream request. The UI holds no credentials
 * and sends no header of its own, so the injected header arrives untouched.
 */
export type AuthMode = "credentials" | "proxy";

/** A connection: server URL plus, in `credentials` mode, the admin credentials. */
export interface Connection {
  url: string;
  mode: AuthMode;
  /** Set in `credentials` mode only. */
  username?: string;
  password?: string;
}

export const DEFAULT_URL = "http://localhost:5001";

/**
 * Whether the UI is served by the Beacon server itself (production build) rather
 * than the standalone Vite dev server. When true, the UI talks to its own origin
 * and the login screen does not ask for a server URL.
 */
export const SAME_ORIGIN = import.meta.env.PROD;

/**
 * The API base URL when the UI is served by Beacon: the current origin plus any
 * deployment base path (the path in front of `/admin`). The SDK appends
 * `/admin/api/...` to this, so an empty base path yields just the origin.
 */
export function sameOriginUrl(): string {
  return serverBase();
}

/** The server URL to connect to: the serving origin in production, else the dev default. */
export function defaultServerUrl(): string {
  return SAME_ORIGIN ? sameOriginUrl() : DEFAULT_URL;
}

/** Loads the persisted connection from localStorage, if any. */
export function loadConnection(): Connection | null {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<Connection>;
    if (typeof parsed.url !== "string") return null;
    // Values written before proxy mode existed carry no `mode` field.
    if (parsed.mode === "proxy") return { url: parsed.url, mode: "proxy" };
    if (typeof parsed.username === "string" && typeof parsed.password === "string") {
      return {
        url: parsed.url,
        mode: "credentials",
        username: parsed.username,
        password: parsed.password,
      };
    }
  } catch {
    /* ignore malformed storage */
  }
  return null;
}

/** Persists the connection for session continuity across reloads. */
export function saveConnection(conn: Connection): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(conn));
}

/** Clears the persisted connection (logout). */
export function clearConnection(): void {
  localStorage.removeItem(STORAGE_KEY);
}

/** Whether the tab signed out of a proxy session and must not pick one up again. */
function proxySignedOut(): boolean {
  try {
    return sessionStorage.getItem(PROXY_SIGNED_OUT_KEY) === "1";
  } catch {
    return false;
  }
}

/** Blocks proxy detection for the rest of this browser tab (called on sign-out). */
export function markProxySignedOut(): void {
  try {
    sessionStorage.setItem(PROXY_SIGNED_OUT_KEY, "1");
  } catch {
    /* storage unavailable; detection stays enabled */
  }
}

/** Re-enables proxy detection (called when the user signs in by hand). */
export function clearProxySignedOut(): void {
  try {
    sessionStorage.removeItem(PROXY_SIGNED_OUT_KEY);
  } catch {
    /* storage unavailable; nothing to clear */
  }
}

/**
 * Whether the app may probe for a proxy-authenticated session.
 *
 * Only a production build probes: it is served by Beacon, so the probe goes to
 * the same origin and passes through the same gateway as the page itself. The
 * dev server has no gateway in front of it and asks for a server URL instead.
 */
export function proxyProbeEnabled(sameOrigin: boolean = SAME_ORIGIN): boolean {
  return sameOrigin && !proxySignedOut();
}

/**
 * Builds a `BeaconClient` from a connection.
 *
 * The client calls Beacon's admin-gated alias of the API, so every request the UI
 * makes sits below `/admin` — the same prefix that serves this application. A
 * deployment can then put its own security in front of `/api/*` and this panel
 * keeps working. The alias demands super-user credentials, which a session here
 * always carries: its own basic-auth header in `credentials` mode, or the header
 * the gateway injects in `proxy` mode.
 */
export function makeClient(conn: Connection): BeaconClient {
  // In proxy mode the client must send no `Authorization` header of its own.
  const credentials =
    conn.mode === "credentials" && conn.username != null && conn.password != null
      ? { username: conn.username, password: conn.password }
      : {};
  return new BeaconClient({
    url: conn.url.replace(/\/+$/, ""),
    apiPrefix: ADMIN_API_PREFIX,
    ...credentials,
  });
}

/**
 * Verifies admin credentials against `GET /admin/api/admin/check`. Throws the
 * SDK's `ApiError` (401 on bad credentials) or a `ConnectionError` if unreachable.
 */
export async function verifyAdmin(client: BeaconClient): Promise<void> {
  await client.admin.check();
}

/**
 * Detects a session that a gateway in front of Beacon already authenticates.
 *
 * Calls the admin check with no credentials. Beacon answers `401` unless the
 * request carries super-user credentials, so a `200` proves that something
 * upstream supplies them for every request. The result is a `proxy` connection;
 * anything else (401, 403, unreachable server) yields `null` and the login screen.
 *
 * The probe never leaks: it sends no credentials and reveals nothing that the
 * gateway does not already grant to the caller.
 */
export async function detectProxySession(
  options: { sameOrigin?: boolean; url?: string } = {},
): Promise<Connection | null> {
  if (!proxyProbeEnabled(options.sameOrigin ?? SAME_ORIGIN)) return null;
  const conn: Connection = {
    url: (options.url ?? defaultServerUrl()).replace(/\/+$/, ""),
    mode: "proxy",
  };
  try {
    await verifyAdmin(makeClient(conn));
    return conn;
  } catch {
    return null;
  }
}

/** The name to show for a session in the UI. */
export function identityLabel(conn: Connection | null): string {
  if (!conn) return "admin";
  return conn.mode === "proxy" ? "Proxy session" : (conn.username ?? "admin");
}
