import * as React from "react";
import { X } from "lucide-react";

import { cn } from "@/lib/utils";

interface TokenListInputProps {
  values: string[];
  onChange: (values: string[]) => void;
  placeholder?: string;
}

/**
 * An ordered list of string tokens entered as chips. Type and press Enter (or
 * comma) to add; backspace on an empty input removes the last. Order is
 * preserved — used for partition columns.
 */
export function TokenListInput({ values, onChange, placeholder }: TokenListInputProps) {
  const [draft, setDraft] = React.useState("");

  function commit() {
    const t = draft.trim().replace(/,$/, "").trim();
    if (t && !values.includes(t)) onChange([...values, t]);
    setDraft("");
  }

  return (
    <div
      className={cn(
        "flex min-h-9 flex-wrap items-center gap-1.5 rounded-md border border-input bg-card px-2 py-1.5 text-sm shadow-sm focus-within:ring-2 focus-within:ring-ring",
      )}
    >
      {values.map((v, i) => (
        <span
          key={`${v}-${i}`}
          className="flex items-center gap-1 rounded bg-secondary px-1.5 py-0.5 font-mono text-xs"
        >
          <span className="text-muted-foreground">{i + 1}.</span>
          {v}
          <button
            type="button"
            onClick={() => onChange(values.filter((_, idx) => idx !== i))}
            className="text-muted-foreground hover:text-foreground"
            aria-label={`Remove ${v}`}
          >
            <X className="h-3 w-3" />
          </button>
        </span>
      ))}
      <input
        value={draft}
        onChange={(e) => setDraft(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === ",") {
            e.preventDefault();
            commit();
          } else if (e.key === "Backspace" && !draft && values.length) {
            onChange(values.slice(0, -1));
          }
        }}
        onBlur={commit}
        placeholder={values.length === 0 ? placeholder : ""}
        className="min-w-[8rem] flex-1 bg-transparent font-mono text-xs outline-none placeholder:font-sans placeholder:text-muted-foreground"
        spellCheck={false}
      />
    </div>
  );
}
