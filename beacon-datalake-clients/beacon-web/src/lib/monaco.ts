/**
 * The Monaco editor, assembled for one job: editing SQL.
 *
 * Imported piecewise rather than through `monaco-editor`'s main entry. That entry
 * pulls in a tokenizer for every language Monaco ships (~13 MB of source, none of
 * it SQL), and language registrations are side-effect imports a bundler cannot
 * drop. So the API comes from `editor.api`, the language from SQL's register, and
 * the editor features are named one by one — anything not listed here simply is
 * not in the bundle.
 */

import * as monaco from "monaco-editor/editor/editor.api";

// Editor features. Each is a side-effect import that registers a contribution.
import "monaco-editor/editor/contrib/bracketMatching/browser/bracketMatching.js";
import "monaco-editor/editor/contrib/clipboard/browser/clipboard.js";
import "monaco-editor/editor/contrib/comment/browser/comment.js";
import "monaco-editor/editor/contrib/contextmenu/browser/contextmenu.js";
import "monaco-editor/editor/contrib/cursorUndo/browser/cursorUndo.js";
import "monaco-editor/editor/contrib/find/browser/findController.js";
import "monaco-editor/editor/contrib/linesOperations/browser/linesOperations.js";
import "monaco-editor/editor/contrib/multicursor/browser/multicursor.js";
import "monaco-editor/editor/contrib/suggest/browser/suggestController.js";
import "monaco-editor/editor/contrib/wordOperations/browser/wordOperations.js";

// SQL: tokenizer, brackets, comment syntax.
import "monaco-editor/languages/definitions/sql/register.js";

// The one worker a SQL editor needs (diffing and basic text services). Vite
// bundles it as a real worker through the `?worker` suffix; without this Monaco
// looks for a worker URL it has no way to know.
import EditorWorker from "monaco-editor/editor/editor.worker.js?worker";

declare global {
  // eslint-disable-next-line no-var
  var MonacoEnvironment: monaco.Environment | undefined;
}

self.MonacoEnvironment = { getWorker: () => new EditorWorker() };

export const SQL_LANGUAGE_ID = "sql";

/** Editor themes that sit on the app's own surface rather than paint their own. */
export const THEMES = { light: "beacon-light", dark: "beacon-dark" } as const;

monaco.editor.defineTheme(THEMES.light, {
  base: "vs",
  inherit: true,
  rules: [],
  colors: {
    // Transparent, so the editor takes the panel's background in either theme.
    "editor.background": "#00000000",
    "editorGutter.background": "#00000000",
    "editor.lineHighlightBorder": "#00000000",
  },
});

monaco.editor.defineTheme(THEMES.dark, {
  base: "vs-dark",
  inherit: true,
  rules: [],
  colors: {
    "editor.background": "#00000000",
    "editorGutter.background": "#00000000",
    "editor.lineHighlightBorder": "#00000000",
  },
});

export { monaco };
