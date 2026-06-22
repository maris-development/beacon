/** Connection + admin-credential storage for the login-gated admin UI. */

import { BeaconClient } from "@beacon/client";

const STORAGE_KEY = "beacon-web.connection";

/** A stored connection: server URL plus admin basic-auth credentials. */
export interface Connection {
  url: string;
  username: string;
  password: string;
}

export const DEFAULT_URL = "http://localhost:5001";

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

/** Builds a `BeaconClient` from a connection (credentials elevate every request). */
export function makeClient(conn: Connection): BeaconClient {
  return new BeaconClient({
    url: conn.url.replace(/\/+$/, ""),
    username: conn.username,
    password: conn.password,
  });
}

/**
 * Verifies admin credentials against `GET /api/admin/check`. Throws the SDK's
 * `ApiError` (401 on bad credentials) or a `ConnectionError` if unreachable.
 */
export async function verifyAdmin(client: BeaconClient): Promise<void> {
  await client.admin.check();
}
