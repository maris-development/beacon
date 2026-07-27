/** Authenticated administrative endpoints (`/api/admin/*`). */

import type { Http } from "./http.js";
import type { Crawler, ExternalTableSpec } from "./types.js";

/**
 * Sub-client for Beacon's admin surface. All endpoints require basic-auth
 * credentials, configured via `username`/`password` on the parent client.
 * Reachable as `client.admin`.
 */
export class AdminClient {
  constructor(private readonly http: Http) {}

  /** Verifies the configured admin credentials (`GET /api/admin/check`). */
  async check(): Promise<void> {
    await this.http.fetchRaw("GET", "/api/admin/check");
  }

  // -- crawlers ---------------------------------------------------------------

  /** Lists all defined crawlers (`GET /api/admin/crawlers`). */
  listCrawlers<T = Crawler[]>(): Promise<T> {
    return this.http.fetchJson<T>("GET", "/api/admin/crawlers");
  }

  /**
   * Creates/defines a crawler (`POST /api/admin/crawlers`). The endpoint returns
   * an empty body on success, so this resolves to `void`.
   */
  async createCrawler(definition: Crawler): Promise<void> {
    await this.http.fetchRaw("POST", "/api/admin/crawlers", { json: definition });
  }

  /** Gets a single crawler by name (`GET /api/admin/crawlers/{name}`). */
  getCrawler<T = Crawler>(name: string): Promise<T> {
    return this.http.fetchJson<T>("GET", `/api/admin/crawlers/${encodeURIComponent(name)}`);
  }

  /** Runs a crawler on demand (`POST /api/admin/crawlers/{name}/run`). */
  async runCrawler(name: string): Promise<void> {
    await this.http.fetchRaw("POST", `/api/admin/crawlers/${encodeURIComponent(name)}/run`);
  }

  /** Deletes a crawler (`DELETE /api/admin/crawlers/{name}`). */
  async dropCrawler(name: string): Promise<void> {
    await this.http.fetchRaw("DELETE", `/api/admin/crawlers/${encodeURIComponent(name)}`);
  }

  // -- external tables --------------------------------------------------------

  /**
   * Creates an external table (`POST /api/admin/external-tables`). The endpoint
   * returns an empty body on success, so this resolves to `void`.
   */
  async createExternalTable(spec: ExternalTableSpec): Promise<void> {
    await this.http.fetchRaw("POST", "/api/admin/external-tables", { json: spec });
  }

  // -- dataset files ----------------------------------------------------------

  /**
   * Uploads a file into the datasets store.
   *
   * `path` is the destination key relative to the datasets root (e.g.
   * `ctd/cruise42/a.nc`). The server validates the path, checks the extension
   * against the readable-format allowlist, and enforces `BEACON_MAX_UPLOAD_BYTES`.
   * By default an existing file is rejected with `409`; pass `overwrite` to
   * replace it.
   *
   * Files at or below `thresholdBytes` (default 50 MiB) are sent in a single
   * streaming `POST /api/admin/datasets/upload`. Larger files switch to the
   * chunked, resumable protocol (initiate → part × N → complete), slicing the
   * file into server-advertised parts; a failed part is retried (the server
   * treats a resent part as idempotent), and the session is aborted on giving up.
   * The request timeout is disabled by default so large uploads are not cut short.
   */
  uploadDataset(
    path: string,
    data: Blob | ArrayBuffer | Uint8Array,
    opts: UploadOptions = {},
  ): Promise<UploadResult> {
    const source = toUploadSource(data);
    const threshold = opts.thresholdBytes ?? DEFAULT_UPLOAD_THRESHOLD;
    if (source.size > threshold) {
      return this.uploadDatasetChunked(path, source, opts);
    }
    return this.http.fetchJson<UploadResult>("POST", "/api/admin/datasets/upload", {
      query: { path, overwrite: opts.overwrite ? "true" : undefined },
      body: data as BodyInit,
      headers: { "Content-Type": "application/octet-stream" },
      signal: opts.signal,
      timeoutMs: opts.timeoutMs ?? 0,
    });
  }

  /** Chunked/resumable upload path; see {@link uploadDataset}. */
  private async uploadDatasetChunked(
    path: string,
    source: UploadSource,
    opts: UploadOptions,
  ): Promise<UploadResult> {
    const init = await this.http.fetchJson<{ upload_id: string; part_size: number }>(
      "POST",
      "/api/admin/datasets/upload/initiate",
      { query: { path, overwrite: opts.overwrite ? "true" : undefined }, signal: opts.signal },
    );
    const partSize = init.part_size > 0 ? init.part_size : DEFAULT_UPLOAD_THRESHOLD;
    const total = source.size;

    try {
      let partNumber = 1;
      for (let offset = 0; offset < total; offset += partSize) {
        const end = Math.min(offset + partSize, total);
        await this.putPartWithRetry(init.upload_id, partNumber, source.slice(offset, end), opts);
        opts.onProgress?.({ uploaded: end, total });
        partNumber++;
      }
      return await this.http.fetchJson<UploadResult>(
        "POST",
        "/api/admin/datasets/upload/complete",
        { query: { upload_id: init.upload_id }, signal: opts.signal, timeoutMs: opts.timeoutMs ?? 0 },
      );
    } catch (err) {
      // Best-effort cleanup of the server-side session; swallow abort errors.
      await this.http
        .fetchRaw("DELETE", "/api/admin/datasets/upload", { query: { upload_id: init.upload_id } })
        .catch(() => undefined);
      throw err;
    }
  }

  /**
   * PUTs one part, retrying the same part number on transient failure. This is
   * safe because the server accepts a resent part idempotently.
   */
  private async putPartWithRetry(
    uploadId: string,
    partNumber: number,
    chunk: BodyInit,
    opts: UploadOptions,
  ): Promise<void> {
    const retries = opts.partRetries ?? 3;
    let lastErr: unknown;
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        await this.http.fetchRaw("PUT", "/api/admin/datasets/upload/part", {
          query: { upload_id: uploadId, part_number: partNumber },
          body: chunk,
          headers: { "Content-Type": "application/octet-stream" },
          signal: opts.signal,
          timeoutMs: opts.timeoutMs ?? 0,
        });
        return;
      } catch (err) {
        lastErr = err;
        if (opts.signal?.aborted) throw err;
        await new Promise((resolve) => setTimeout(resolve, 250 * (attempt + 1)));
      }
    }
    throw lastErr;
  }

  /**
   * Downloads a dataset file (`GET /api/admin/datasets/download`), resolving to a
   * `Blob`. The request timeout is disabled by default for large files.
   */
  async downloadDataset(
    path: string,
    opts: { signal?: AbortSignal; timeoutMs?: number } = {},
  ): Promise<Blob> {
    const res = await this.http.fetchRaw("GET", "/api/admin/datasets/download", {
      query: { path },
      signal: opts.signal,
      timeoutMs: opts.timeoutMs ?? 0,
    });
    return await res.blob();
  }

  /**
   * Deletes a dataset file (`DELETE /api/admin/datasets`). The server refuses with
   * `409` if a registered table references the file.
   */
  async deleteDataset(path: string): Promise<void> {
    await this.http.fetchRaw("DELETE", "/api/admin/datasets", { query: { path } });
  }

  // -- users & roles ----------------------------------------------------------

  /** Lists all users (the super-user plus stored local users) with their roles. */
  listAuthUsers(): Promise<AuthUser[]> {
    return this.http.fetchJson<AuthUser[]>("GET", "/api/admin/auth/users");
  }

  /** Lists all roles with their grant/deny rules. */
  listAuthRoles(): Promise<AuthRole[]> {
    return this.http.fetchJson<AuthRole[]>("GET", "/api/admin/auth/roles");
  }

  /**
   * Runs an auth-management statement through the SQL endpoint as the super-user.
   * Auth DDL (CREATE/DROP USER/ROLE, GRANT, DENY, REVOKE) returns no rows.
   */
  private async runSql(sql: string): Promise<void> {
    await this.http.fetchRaw("POST", "/api/query", { json: { sql } });
  }

  createRole(role: string): Promise<void> {
    return this.runSql(`CREATE ROLE ${ident(role)}`);
  }
  dropRole(role: string): Promise<void> {
    return this.runSql(`DROP ROLE ${ident(role)}`);
  }
  createUser(username: string, password: string): Promise<void> {
    return this.runSql(`CREATE USER ${ident(username)} WITH PASSWORD ${lit(password)}`);
  }
  dropUser(username: string): Promise<void> {
    return this.runSql(`DROP USER ${ident(username)}`);
  }
  grantRoleToUser(username: string, role: string): Promise<void> {
    return this.runSql(`GRANT ROLE ${ident(role)} TO USER ${ident(username)}`);
  }
  revokeRoleFromUser(username: string, role: string): Promise<void> {
    return this.runSql(`REVOKE ROLE ${ident(role)} FROM USER ${ident(username)}`);
  }

  /** Grant (or, with `deny`, deny) a privilege on a target to a role. */
  grantPrivilege(role: string, target: PrivilegeTarget, deny = false): Promise<void> {
    const verb = deny ? "DENY" : "GRANT";
    return this.runSql(`${verb} SELECT${targetClause(target)} TO ROLE ${ident(role)}`);
  }

  /** Remove a grant (default) or, with `deny`, a deny rule from a role. */
  revokePrivilege(role: string, target: PrivilegeTarget, deny = false): Promise<void> {
    const kind = deny ? "REVOKE DENY" : "REVOKE";
    return this.runSql(`${kind} SELECT${targetClause(target)} FROM ROLE ${ident(role)}`);
  }
}

/** A privilege target: all targets, a named table, or a path glob. */
export type PrivilegeTarget =
  | { type: "all" }
  | { type: "table"; value: string }
  | { type: "path"; value: string };

/** Build the `ON ...` clause for a privilege statement (empty for `all`). */
function targetClause(target: PrivilegeTarget): string {
  if (target.type === "table") return ` ON TABLE ${ident(target.value)}`;
  if (target.type === "path") return ` ON PATH ${lit(target.value)}`;
  return "";
}

/**
 * Validate a SQL identifier (role/user/table name). Restricting to a safe
 * character set is what keeps these statements injection-proof, since
 * identifiers can't be parameterized.
 */
function ident(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(name)) {
    throw new Error(
      `invalid identifier "${name}": use letters, digits, and underscores (not starting with a digit)`,
    );
  }
  return name;
}

/** Render a single-quoted SQL string literal, escaping embedded quotes. */
function lit(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/** A user account and its assigned roles. */
export interface AuthUser {
  username: string;
  roles: string[];
  is_super_user: boolean;
  /** Beacon-managed anonymous user; can't be deleted while anonymous access is on. */
  is_anonymous: boolean;
}

/** A single grant/deny rule, flattened. */
export interface AuthRule {
  privilege: string;
  target_type: "all" | "table" | "path";
  target_value: string | null;
}

/** A role with its grant and deny rules. */
export interface AuthRole {
  name: string;
  grants: AuthRule[];
  denies: AuthRule[];
}

/** Result of a successful dataset upload. */
export interface UploadResult {
  /** The normalized object key the file was written to. */
  path: string;
  /** The number of bytes written. */
  size: number;
}

/** Progress of a chunked upload. */
export interface UploadProgress {
  /** Bytes uploaded so far. */
  uploaded: number;
  /** Total bytes to upload. */
  total: number;
}

/** Options for {@link AdminClient.uploadDataset}. */
export interface UploadOptions {
  /** Replace an existing file instead of failing with `409`. */
  overwrite?: boolean;
  /** Cancels the upload (including in-flight chunked parts). */
  signal?: AbortSignal;
  /** Per-request timeout in ms; defaults to 0 (disabled) for uploads. */
  timeoutMs?: number;
  /** Files larger than this switch to the chunked protocol. Default 50 MiB. */
  thresholdBytes?: number;
  /** Retry attempts per part on transient failure (chunked only). Default 3. */
  partRetries?: number;
  /** Called after each part completes (chunked only). */
  onProgress?: (progress: UploadProgress) => void;
}

/** Default size above which {@link AdminClient.uploadDataset} chunks: 50 MiB. */
const DEFAULT_UPLOAD_THRESHOLD = 50 * 1024 * 1024;

/** A size-known, sliceable view over the upload payload. */
interface UploadSource {
  size: number;
  slice: (start: number, end: number) => BodyInit;
}

/** Normalize the accepted upload inputs into a sliceable {@link UploadSource}. */
function toUploadSource(data: Blob | ArrayBuffer | Uint8Array): UploadSource {
  if (data instanceof Blob) {
    return { size: data.size, slice: (start, end) => data.slice(start, end) };
  }
  if (data instanceof Uint8Array) {
    return { size: data.byteLength, slice: (start, end) => data.subarray(start, end) };
  }
  // ArrayBuffer
  return {
    size: data.byteLength,
    slice: (start, end) => new Uint8Array(data, start, end - start),
  };
}
