import CodeMirror, { type ReactCodeMirrorRef } from "@uiw/react-codemirror";
import { sql } from "@codemirror/lang-sql";
import { forwardRef } from "react";

import { useTheme } from "@/lib/theme";

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onRun?: () => void;
}

/** CodeMirror SQL editor. Cmd/Ctrl+Enter triggers `onRun`. */
export const SqlEditor = forwardRef<ReactCodeMirrorRef, SqlEditorProps>(
  ({ value, onChange, onRun }, ref) => {
    const { resolved } = useTheme();
    return (
      <CodeMirror
        ref={ref}
        value={value}
        height="100%"
        theme={resolved}
        extensions={[sql()]}
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
