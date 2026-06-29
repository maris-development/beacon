import CodeMirror, { type ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { forwardRef } from "react";

import { useTheme } from "@/lib/theme";
import { useSqlCompletion } from "@/lib/sql-completion";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
}

/** CodeMirror SQL editor with metadata-aware autocomplete. Cmd/Ctrl+Enter runs. */
export const SqlEditor = forwardRef<ReactCodeMirrorRef, SqlEditorProps>(
  ({ value, onChange, onRun }, ref) => {
    const { resolved } = useTheme();
    const completion = useSqlCompletion();
    return (
      <CodeMirror
        ref={ref}
        value={value}
        height="100%"
        theme={resolved}
        extensions={completion}
        onChange={onChange}
        basicSetup={{ lineNumbers: true, foldGutter: false, highlightActiveLine: false }}
        onKeyDown={(e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
            e.preventDefault();
            onRun?.();
          }
        }}
        className="h-full"
      />
    );
  },
);
SqlEditor.displayName = "SqlEditor";
