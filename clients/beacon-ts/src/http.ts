/** Low-level isomorphic HTTP transport for the Beacon API. */

import { ApiError, ConnectionError, TimeoutError } from "./errors.js";

/** A fetch-compatible function. Defaults to the global `fetch`. */
export type FetchLike = typeof fetch;

export interface ClientOptions {
  /** Base URL of the Beacon server, e.g. `http://localhost:5001`. */
  url: string;
  /**
   * Admin basic-auth credentials. Sent on every request when provided, which
   * elevates the request to super-user (allowing DDL/DML and admin endpoints).
   */
  username?: string;
  password?: string;
  /**
   * URL prefix the server is mounted under (Beacon's `BEACON_BASE_PATH`).
   * Prepended to every API path. Defaults to "".
   */
  basePath?: string;
  /** Per-request timeout in milliseconds. Defaults to 60000. Set to 0 to disable. */
  timeoutMs?: number;
  /** Custom fetch implementation (e.g. for testing or a proxy). */
  fetch?: FetchLike;
  /** Extra headers attached to every request. */
  headers?: Record<string, string>;
}

/** Builds an HTTP Basic `Authorization` header value, isomorphically. */
export function basicAuthHeader(username: string, password: string): string {
  return `Basic ${base64Utf8(`${username}:${password}`)}`;
}

/**
 * Base64-encodes a string's UTF-8 bytes, in both the browser and Node.
 *
 * `btoa` operates on Latin-1 code points and throws on characters outside that
 * range, so we encode to UTF-8 bytes first — credentials may contain any
 * Unicode character.
 */
function base64Utf8(value: string): string {
  const bytes = new TextEncoder().encode(value);
  if (typeof Buffer !== "undefined") {
    return Buffer.from(bytes).toString("base64");
  }
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return globalThis.btoa(binary);
}

/** The resolved transport shared by the client and its sub-clients. */
export class Http {
  readonly baseUrl: string;
  private readonly fetchImpl: FetchLike;
  private readonly timeoutMs: number;
  private readonly authHeader?: string;
  private readonly extraHeaders: Record<string, string>;

  constructor(options: ClientOptions) {
    const base = options.url.replace(/\/+$/, "");
    const prefix = (options.basePath ?? "").replace(/\/+$/, "");
    this.baseUrl = `${base}${prefix}`;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    if (typeof this.fetchImpl !== "function") {
      throw new Error(
        "global fetch is unavailable; pass a `fetch` implementation in ClientOptions (Node < 18).",
      );
    }
    this.timeoutMs = options.timeoutMs ?? 60_000;
    this.extraHeaders = options.headers ?? {};
    if (options.username != null && options.password != null) {
      this.authHeader = basicAuthHeader(options.username, options.password);
    }
  }

  /** Whether admin credentials are configured. */
  get authenticated(): boolean {
    return this.authHeader != null;
  }

  /**
   * Issues a request and returns the raw `Response` (after asserting a 2xx
   * status). The caller owns the body — use this for streaming/binary results.
   */
  async fetchRaw(
    method: string,
    path: string,
    init: {
      query?: Record<string, string | number | undefined>;
      json?: unknown;
      headers?: Record<string, string>;
      signal?: AbortSignal;
    } = {},
  ): Promise<Response> {
    const url = this.buildUrl(path, init.query);
    const headers: Record<string, string> = { ...this.extraHeaders, ...init.headers };
    if (this.authHeader) headers["Authorization"] = this.authHeader;

    let body: string | undefined;
    if (init.json !== undefined) {
      headers["Content-Type"] = "application/json";
      body = JSON.stringify(init.json);
    }

    const { signal, cancel, timedOut } = this.withTimeout(init.signal);
    let res: Response;
    try {
      res = await this.fetchImpl(url, { method, headers, body, signal });
    } catch (err) {
      // Our internal controller aborted the request: classify why.
      if (signal.aborted) {
        if (timedOut()) throw new TimeoutError(url, err);
        throw err; // external cancellation — preserve the caller's AbortError/reason
      }
      throw new ConnectionError(url, err);
    } finally {
      cancel();
    }

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new ApiError(res.status, decodeErrorBody(text), url);
    }
    return res;
  }

  /** Issues a request and decodes the JSON response body. */
  async fetchJson<T>(
    method: string,
    path: string,
    init: Parameters<Http["fetchRaw"]>[2] = {},
  ): Promise<T> {
    const res = await this.fetchRaw(method, path, init);
    return (await res.json()) as T;
  }

  private buildUrl(path: string, query?: Record<string, string | number | undefined>): string {
    const url = new URL(`${this.baseUrl}${path}`);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        if (value !== undefined) url.searchParams.set(key, String(value));
      }
    }
    return url.toString();
  }

  /**
   * Composes the request's abort signal: the configured timeout plus any
   * caller-supplied signal. Returns `timedOut()` so the caller can tell a
   * timeout abort apart from an external cancellation (the abort reason is not
   * reliably propagated across runtimes, so we track it explicitly).
   */
  private withTimeout(external?: AbortSignal): {
    signal: AbortSignal;
    cancel: () => void;
    timedOut: () => boolean;
  } {
    const controller = new AbortController();
    let timedOut = false;

    // Forward an external abort to our controller, preserving its reason.
    const onExternalAbort = () => controller.abort(external?.reason);
    if (external) {
      if (external.aborted) controller.abort(external.reason);
      else external.addEventListener("abort", onExternalAbort, { once: true });
    }

    let timer: ReturnType<typeof setTimeout> | undefined;
    if (this.timeoutMs > 0) {
      timer = setTimeout(() => {
        timedOut = true;
        controller.abort(new DOMException("Request timed out", "TimeoutError"));
      }, this.timeoutMs);
    }

    const cancel = () => {
      if (timer !== undefined) clearTimeout(timer);
      external?.removeEventListener("abort", onExternalAbort);
    };
    return { signal: controller.signal, cancel, timedOut: () => timedOut };
  }
}

/**
 * Beacon returns query errors as a JSON-encoded string (`"some message"`).
 * Unwrap that to the bare message when possible; otherwise return as-is.
 */
function decodeErrorBody(text: string): string {
  try {
    const parsed: unknown = JSON.parse(text);
    if (typeof parsed === "string") return parsed;
  } catch {
    /* not JSON; fall through */
  }
  return text;
}
