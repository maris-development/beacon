/** localStorage-backed store for user-saved SQL queries. */

const STORAGE_KEY = "beacon-web.saved-queries";

export interface SavedQuery {
  id: string;
  name: string;
  sql: string;
  updatedAt: number;
}

function read(): SavedQuery[] {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return [];
    const parsed = JSON.parse(raw);
    if (Array.isArray(parsed)) return parsed as SavedQuery[];
  } catch {
    /* ignore malformed storage */
  }
  return [];
}

function write(queries: SavedQuery[]): void {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(queries));
}

function newId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) return crypto.randomUUID();
  return `q_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
}

/** Returns saved queries, most-recently-updated first. */
export function listSavedQueries(): SavedQuery[] {
  return read().sort((a, b) => b.updatedAt - a.updatedAt);
}

/**
 * Saves a query by name. If a query with the same (case-insensitive) name
 * exists, it is overwritten in place. Returns the updated list.
 */
export function saveQuery(name: string, sql: string): SavedQuery[] {
  const queries = read();
  const trimmed = name.trim();
  const existing = queries.find((q) => q.name.toLowerCase() === trimmed.toLowerCase());
  if (existing) {
    existing.sql = sql;
    existing.updatedAt = Date.now();
  } else {
    queries.push({ id: newId(), name: trimmed, sql, updatedAt: Date.now() });
  }
  write(queries);
  return listSavedQueries();
}

/** Deletes a saved query by id. Returns the updated list. */
export function deleteSavedQuery(id: string): SavedQuery[] {
  write(read().filter((q) => q.id !== id));
  return listSavedQueries();
}
