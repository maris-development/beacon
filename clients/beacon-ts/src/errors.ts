/** Error types thrown by the Beacon client. */

/** Base class for every error raised by this SDK. */
export class BeaconError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

/** The server could not be reached (DNS, connection refused, or the request timed out). */
export class ConnectionError extends BeaconError {
  constructor(
    readonly url: string,
    readonly cause: unknown,
  ) {
    super(`failed to reach Beacon at ${url}: ${describe(cause)}`);
  }
}

/** The request timed out before the server responded. */
export class TimeoutError extends ConnectionError {}

/**
 * The server returned a non-2xx status. `status` is the HTTP code and `body` is
 * the (best-effort decoded) response body, which Beacon returns as a JSON-encoded
 * error string for query failures.
 */
export class ApiError extends BeaconError {
  constructor(
    readonly status: number,
    readonly body: string,
    readonly url: string,
  ) {
    super(`Beacon returned ${status} for ${url}: ${body}`);
  }
}

function describe(cause: unknown): string {
  if (cause instanceof Error) return cause.message;
  return String(cause);
}
