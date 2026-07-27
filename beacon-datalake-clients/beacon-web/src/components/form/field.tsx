import type { ReactNode } from "react";

import { Label } from "@/components/ui/label";

interface FieldProps {
  label: string;
  htmlFor?: string;
  hint?: string;
  required?: boolean;
  children: ReactNode;
}

/** A labeled form field with an optional helper hint and required marker. */
export function Field({ label, htmlFor, hint, required, children }: FieldProps) {
  return (
    <div className="space-y-1.5">
      <Label htmlFor={htmlFor}>
        {label}
        {required && <span className="ml-0.5 text-destructive">*</span>}
      </Label>
      {children}
      {hint && <p className="text-xs text-muted-foreground">{hint}</p>}
    </div>
  );
}
