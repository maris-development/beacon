import type { ReactNode } from "react";
import { Info } from "lucide-react";

/**
 * A subtle informational banner explaining what a page/concept is and does.
 * Links inside `children` are styled as primary links.
 */
export function InfoBanner({ children }: { children: ReactNode }) {
  return (
    <div className="mb-4 flex items-start gap-2.5 rounded-md border border-primary/20 bg-primary/5 p-3 text-sm text-muted-foreground">
      <Info className="mt-0.5 h-4 w-4 shrink-0 text-primary" />
      <div className="[&_a:hover]:underline [&_a]:font-medium [&_a]:text-primary">{children}</div>
    </div>
  );
}
