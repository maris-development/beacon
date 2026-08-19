/** Connection + admin-credential storage for the login-gated admin UI. */

import { ADMIN_API_PREFIX, BeaconClient } from "@beacon/client";

import { serverBase } from "./base-path";

const STORAGE_KEY = "beacon-web.connection";

/** A stored connection: server URL plus admin basic-auth credentials. */
export interface Connection {
  url: string;
  username: string;
  password: string;
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
    if (
      typeof parsed.url === "string" &&
      typeof parsed.username === "string" &&
      typeof parsed.password === "string"
    ) {
      return { url: parsed.url, username: parsed.username, password: parsed.password };
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

/**
 * Builds a `BeaconClient` from a connection (credentials elevate every request).
 *
 * The client calls Beacon's admin-gated alias of the API, so every request the UI
 * makes sits below `/admin` — the same prefix that serves this application. A
 * deployment can then put its own security in front of `/api/*` and this panel
 * keeps working. The alias demands super-user credentials, which a session here
 * always carries.
 */
export function makeClient(conn: Connection): BeaconClient {
  return new BeaconClient({
    url: conn.url.replace(/\/+$/, ""),
    apiPrefix: ADMIN_API_PREFIX,
    username: conn.username,
    password: conn.password,
  });
}

/**
 * Verifies admin credentials against `GET /admin/api/admin/check`. Throws the
 * SDK's `ApiError` (401 on bad credentials) or a `ConnectionError` if unreachable.
 */
export async function verifyAdmin(client: BeaconClient): Promise<void> {
  await client.admin.check();
}
