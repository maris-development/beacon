/** Error helpers shared across pages. */

import { ApiError } from "@beacon/client";

/** Extracts a human-readable message from any thrown value. */
export function errorMessage(err: unknown): string {
  if (err instanceof ApiError) {
    return err.body || `Request failed (HTTP ${err.status})`;
  }
  if (err instanceof Error) return err.message;
  return String(err);
}

/** True when the error is an authentication failure (expired/invalid session). */
export function isUnauthorized(err: unknown): boolean {
  return err instanceof ApiError && err.status === 401;
}
