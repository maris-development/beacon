import * as React from "react";

import { cn } from "@/lib/utils";

interface CheckboxFieldProps {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
  hint?: string;
  id?: string;
}

/** A native checkbox with an aligned label and optional hint. */
export function CheckboxField({ checked, onChange, label, hint, id }: CheckboxFieldProps) {
  const autoId = React.useId();
  const inputId = id ?? autoId;
  return (
    <div className="flex items-start gap-2.5">
      <input
        id={inputId}
        type="checkbox"
        checked={checked}
        onChange={(e) => onChange(e.target.checked)}
        className={cn(
          "mt-0.5 h-4 w-4 shrink-0 cursor-pointer rounded border-input text-primary accent-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring",
        )}
      />
      <label htmlFor={inputId} className="cursor-pointer select-none text-sm leading-tight">
        <span className="font-medium">{label}</span>
        {hint && <span className="block text-xs font-normal text-muted-foreground">{hint}</span>}
      </label>
    </div>
  );
}
