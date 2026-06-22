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

  /** Creates/defines a crawler (`POST /api/admin/crawlers`). */
  createCrawler<T = Crawler>(definition: Crawler): Promise<T> {
    return this.http.fetchJson<T>("POST", "/api/admin/crawlers", { json: definition });
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

  /** Creates an external table (`POST /api/admin/external-tables`). */
  createExternalTable<T = unknown>(spec: ExternalTableSpec): Promise<T> {
    return this.http.fetchJson<T>("POST", "/api/admin/external-tables", { json: spec });
  }
}
