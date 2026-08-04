import * as React from "react";

import type { SqlEditorHandle } from "@/components/sql-editor";

/**
 * The SQL editor, loaded on demand.
 *
 * Monaco is by far the largest thing this app ships. Split out here, it is
 * fetched when an editor is first rendered rather than on boot, so the pages
 * that have no editor — Tables, Datasets, Crawlers, Users, Server — do not pay
 * for it.
 */
const Editor = React.lazy(() =>
  import("@/components/sql-editor").then((m) => ({ default: m.SqlEditor })),
);

/** Frees a query tab's editor model. No-op until the editor has been loaded. */
export async function disposeEditorModel(key: string) {
  const m = await import("@/components/sql-editor");
  m.disposeEditorModel(key);
}

type EditorProps = React.ComponentProps<typeof Editor>;

export const SqlEditor = React.forwardRef<SqlEditorHandle, EditorProps>((props, ref) => (
  <React.Suspense
    fallback={
      <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
        Loading editor…
      </div>
    }
  >
    <Editor {...props} ref={ref} />
  </React.Suspense>
));
SqlEditor.displayName = "SqlEditor";

export type { SqlEditorHandle };
