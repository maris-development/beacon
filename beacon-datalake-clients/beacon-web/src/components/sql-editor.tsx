import * as React from "react";

import { monaco, SQL_LANGUAGE_ID, THEMES } from "@/lib/monaco";
import { useTheme } from "@/lib/theme";
import {
  EMPTY_METADATA,
  fnDocumentation,
  fnSignature,
  SQL_KEYWORDS,
  useSqlMetadata,
  type SqlMetadata,
} from "@/lib/sql-completion";

/** What a parent can ask the editor to do. */
export interface SqlEditorHandle {
  /** Replaces the selection (or inserts at the cursor) and refocuses. */
  insert: (text: string) => void;
  focus: () => void;
}

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
  /** Enable metadata-aware autocomplete (default true). */
  autocomplete?: boolean;
  /**
   * Identity of the document being edited (a query tab's id).
   *
   * Each key gets its own Monaco model, so switching tabs restores that tab's
   * cursor, selection, and undo history instead of resetting the text of one
   * shared document. Omit for a standalone editor.
   */
  modelKey?: string;
}

/**
 * The live metadata the completion provider reads.
 *
 * Monaco registers providers per *language*, once for the page, while the
 * metadata belongs to a mounted component. The provider therefore reads through
 * this box, which the mounted editor keeps current — rather than registering a
 * new provider per mount and stacking duplicate suggestions.
 */
const metadataBox: { current: SqlMetadata; enabled: boolean } = {
  current: EMPTY_METADATA,
  enabled: true,
};

/**
 * One text model per document key, kept across mounts.
 *
 * A model holds the undo stack, so it must outlive both the editor instance and
 * the React tree: navigating away from the workbench and back should not forget
 * that you can undo.
 */
const models = new Map<string, monaco.editor.ITextModel>();

function modelFor(key: string, value: string): monaco.editor.ITextModel {
  const existing = models.get(key);
  if (existing && !existing.isDisposed()) return existing;
  const model = monaco.editor.createModel(value, SQL_LANGUAGE_ID);
  models.set(key, model);
  return model;
}

/** Forgets a document — called when its tab is closed. */
export function disposeEditorModel(key: string) {
  const model = models.get(key);
  models.delete(key);
  model?.dispose();
}

let providersRegistered = false;

function registerProviders() {
  if (providersRegistered) return;
  providersRegistered = true;

  monaco.languages.registerCompletionItemProvider(SQL_LANGUAGE_ID, {
    provideCompletionItems(model: monaco.editor.ITextModel, position: monaco.Position) {
      if (!metadataBox.enabled) return { suggestions: [] };
      const word = model.getWordUntilPosition(position);
      const range = {
        startLineNumber: position.lineNumber,
        endLineNumber: position.lineNumber,
        startColumn: word.startColumn,
        endColumn: word.endColumn,
      };
      const { tables, functions } = metadataBox.current;
      const { CompletionItemKind } = monaco.languages;

      const suggestions: monaco.languages.CompletionItem[] = [
        ...tables.map((name) => ({
          label: name,
          kind: CompletionItemKind.Struct,
          detail: "table",
          insertText: name,
          range,
        })),
        ...functions.map((fn) => ({
          label: fn.name,
          kind: CompletionItemKind.Function,
          detail: fn.returnType ?? "function",
          documentation: { value: fnDocumentation(fn) },
          // Insert the call and leave the cursor between the parentheses.
          insertText: `${fn.name}($0)`,
          insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
          range,
        })),
        ...SQL_KEYWORDS.map((keyword) => ({
          label: keyword,
          kind: CompletionItemKind.Keyword,
          insertText: keyword,
          range,
        })),
      ];
      return { suggestions };
    },
  });

  // The signature of the function being called, shown while typing its arguments.
  monaco.languages.registerSignatureHelpProvider(SQL_LANGUAGE_ID, {
    signatureHelpTriggerCharacters: ["(", ","],
    provideSignatureHelp(model: monaco.editor.ITextModel, position: monaco.Position) {
      const line = model.getValueInRange({
        startLineNumber: position.lineNumber,
        startColumn: 1,
        endLineNumber: position.lineNumber,
        endColumn: position.column,
      });
      // The nearest unclosed call to the left of the cursor.
      const call = /([A-Za-z_]\w*)\s*\([^()]*$/.exec(line);
      const fn = call && metadataBox.current.functions.find((f) => f.name === call[1]);
      if (!call || !fn) return null;
      return {
        value: {
          signatures: [
            {
              label: fnSignature(fn),
              documentation: { value: fnDocumentation(fn) },
              parameters: fn.params.map((p) => ({
                label: p.dataType ? `${p.name}: ${p.dataType}` : p.name,
                documentation: p.description,
              })),
            },
          ],
          activeSignature: 0,
          // Which argument the cursor is in: one per comma since the open paren.
          activeParameter: (line.slice(call.index).match(/,/g) ?? []).length,
        },
        dispose: () => {},
      };
    },
  });
}

/**
 * Monaco SQL editor with metadata-aware autocomplete. Cmd/Ctrl+Enter runs.
 *
 * Monaco is imperative and long-lived: the instance is created once for the
 * component's lifetime and mutated afterwards, so state changes are pushed into
 * it rather than re-rendering it. Callbacks live in refs so a parent re-render
 * never tears the editor down.
 */
export const SqlEditor = React.forwardRef<SqlEditorHandle, SqlEditorProps>(
  ({ value, onChange, onRun, autocomplete = true, modelKey }, ref) => {
    const { resolved } = useTheme();
    const metadata = useSqlMetadata(autocomplete);

    const container = React.useRef<HTMLDivElement>(null);
    const editorRef = React.useRef<monaco.editor.IStandaloneCodeEditor | null>(null);
    const onChangeRef = React.useRef(onChange);
    const onRunRef = React.useRef(onRun);
    onChangeRef.current = onChange;
    onRunRef.current = onRun;

    // Publish the metadata for the shared provider to read.
    metadataBox.current = metadata;
    metadataBox.enabled = autocomplete;

    React.useEffect(() => {
      if (!container.current) return;
      registerProviders();

      const editor = monaco.editor.create(container.current, {
        model: modelKey ? modelFor(modelKey, value) : monaco.editor.createModel(value, SQL_LANGUAGE_ID),
        theme: THEMES[resolved === "dark" ? "dark" : "light"],
        automaticLayout: true,
        minimap: { enabled: false },
        scrollBeyondLastLine: false,
        renderLineHighlight: "none",
        fontSize: 13,
        fontFamily:
          "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace",
        padding: { top: 8, bottom: 8 },
        tabSize: 2,
        scrollbar: { verticalScrollbarSize: 10, horizontalScrollbarSize: 10 },
        overviewRulerLanes: 0,
        // Suggestions come from our provider; the word-based ones would offer
        // every token in the document, which is noise next to real metadata.
        wordBasedSuggestions: "off",
        // Take input through the hidden textarea rather than the experimental
        // EditContext path. EditContext leaves that textarea `readonly`, which
        // means synthetic key events — browser automation, and some assistive
        // and IME tooling — reach a dead element and type nothing.
        editContext: false,
        quickSuggestions: autocomplete,
        suggestOnTriggerCharacters: autocomplete,
      });
      editorRef.current = editor;

      const changed = editor.onDidChangeModelContent(() => {
        onChangeRef.current(editor.getValue());
      });
      // `addCommand` rather than a keydown handler: it takes precedence over
      // Monaco's own bindings and does not fire while the suggest widget owns
      // the keystroke.
      editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
        onRunRef.current?.();
      });

      return () => {
        changed.dispose();
        // A keyed model belongs to its tab and is disposed when that tab closes;
        // an unkeyed one belongs to this editor and goes with it.
        const model = editor.getModel();
        editor.dispose();
        if (model && ![...models.values()].includes(model)) model.dispose();
        editorRef.current = null;
      };
      // Created once; `value`, theme, and options are pushed in by the effects
      // below rather than by rebuilding the editor.
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // Switching tabs swaps the model, which carries that tab's text, cursor and
    // undo stack with it.
    React.useEffect(() => {
      const editor = editorRef.current;
      if (!editor || !modelKey) return;
      const model = modelFor(modelKey, value);
      if (editor.getModel() !== model) {
        editor.setModel(model);
        editor.focus();
      }
    }, [modelKey, value]);

    // Push a value the parent changed (a saved query loaded, say) into the model,
    // but never one that merely echoes what the user just typed — that would
    // reset the cursor on every keystroke.
    React.useEffect(() => {
      const editor = editorRef.current;
      if (editor && value !== editor.getValue()) editor.setValue(value);
    }, [value]);

    React.useEffect(() => {
      monaco.editor.setTheme(THEMES[resolved === "dark" ? "dark" : "light"]);
    }, [resolved]);

    React.useEffect(() => {
      editorRef.current?.updateOptions({
        quickSuggestions: autocomplete,
        suggestOnTriggerCharacters: autocomplete,
      });
    }, [autocomplete]);

    React.useImperativeHandle(
      ref,
      () => ({
        insert(text: string) {
          const editor = editorRef.current;
          if (!editor) return;
          const selection = editor.getSelection();
          if (!selection) return;
          editor.executeEdits("insert", [{ range: selection, text, forceMoveMarkers: true }]);
          editor.focus();
        },
        focus() {
          editorRef.current?.focus();
        },
      }),
      [],
    );

    return <div ref={container} className="h-full w-full" />;
  },
);
SqlEditor.displayName = "SqlEditor";
