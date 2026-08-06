/**
 * The workbench's open queries.
 *
 * Tabs outlive the page: the workbench unmounts whenever you visit Tables or
 * Datasets, so the queries live in `localStorage` and are read back on mount.
 * Closing the browser keeps them too — an editor that loses your draft because
 * you clicked a menu item is worse than no editor.
 */

import * as React from "react";

const STORAGE_KEY = "beacon.query-tabs";

export interface QueryTab {
  id: string;
  /** What the tab is called. Auto-numbered on open; the SQL is the tooltip. */
  title: string;
  sql: string;
}

interface TabState {
  tabs: QueryTab[];
  activeId: string;
}

const STARTER_SQL = "SELECT 1 AS n";

function newId(): string {
  return `q${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
}

function firstState(): TabState {
  const tab = { id: newId(), title: "Query 1", sql: STARTER_SQL };
  return { tabs: [tab], activeId: tab.id };
}

/** Reads persisted tabs, repairing anything that does not look like tab state. */
function load(): TabState {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return firstState();
    const parsed = JSON.parse(raw) as Partial<TabState>;
    const tabs = (parsed.tabs ?? []).filter(
      (t): t is QueryTab =>
        !!t && typeof t.id === "string" && typeof t.sql === "string" && typeof t.title === "string",
    );
    if (tabs.length === 0) return firstState();
    const activeId = tabs.some((t) => t.id === parsed.activeId) ? parsed.activeId! : tabs[0].id;
    return { tabs, activeId };
  } catch {
    return firstState();
  }
}

function save(state: TabState) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  } catch {
    // A full or unavailable localStorage costs persistence, not the editor.
  }
}

/** The next free "Query N", so numbering doesn't collide with an open tab. */
function nextTitle(tabs: QueryTab[]): string {
  const used = new Set(
    tabs
      .map((t) => /^Query (\d+)$/.exec(t.title)?.[1])
      .filter(Boolean)
      .map(Number),
  );
  let n = 1;
  while (used.has(n)) n++;
  return `Query ${n}`;
}

export interface QueryTabsApi {
  tabs: QueryTab[];
  activeId: string;
  active: QueryTab;
  /** Switches tabs. */
  select: (id: string) => void;
  /** Replaces the active tab's SQL (what the editor calls on every keystroke). */
  setSql: (id: string, sql: string) => void;
  /** Opens a tab — with `sql` when another page sent a query over. */
  open: (sql?: string) => string;
  /** Closes a tab, keeping at least one open. */
  close: (id: string) => void;
}

/**
 * The open tabs, persisted across mounts.
 *
 * State is kept in React (so the workbench re-renders) and mirrored to storage on
 * every change; the write is small and only happens on edits a person makes.
 */
export function useQueryTabs(): QueryTabsApi {
  const [state, setState] = React.useState<TabState>(load);

  React.useEffect(() => {
    save(state);
  }, [state]);

  const select = React.useCallback((id: string) => {
    setState((prev) => (prev.tabs.some((t) => t.id === id) ? { ...prev, activeId: id } : prev));
  }, []);

  const setSql = React.useCallback((id: string, sql: string) => {
    setState((prev) => {
      const tab = prev.tabs.find((t) => t.id === id);
      if (!tab || tab.sql === sql) return prev;
      return { ...prev, tabs: prev.tabs.map((t) => (t.id === id ? { ...t, sql } : t)) };
    });
  }, []);

  const open = React.useCallback((sql?: string) => {
    const id = newId();
    setState((prev) => ({
      tabs: [...prev.tabs, { id, title: nextTitle(prev.tabs), sql: sql ?? "" }],
      activeId: id,
    }));
    return id;
  }, []);

  const close = React.useCallback((id: string) => {
    setState((prev) => {
      const index = prev.tabs.findIndex((t) => t.id === id);
      if (index < 0) return prev;
      const tabs = prev.tabs.filter((t) => t.id !== id);
      // Never leave the workbench without an editor to type in.
      if (tabs.length === 0) return firstState();
      const activeId =
        prev.activeId === id ? tabs[Math.min(index, tabs.length - 1)].id : prev.activeId;
      return { tabs, activeId };
    });
  }, []);

  const active = state.tabs.find((t) => t.id === state.activeId) ?? state.tabs[0];
  return { tabs: state.tabs, activeId: active.id, active, select, setSql, open, close };
}
