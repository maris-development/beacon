import { cn } from "@/lib/utils";

/**
 * A small horizontal usage bar. The colour follows the load: green below 70%,
 * amber from 70%, red from 85%. Shared by the host gauges on the Server page
 * and the datasets disk space. Without a label and a detail the bar stands on
 * its own, for a row that already names the values.
 */
export function Meter({
  label,
  detail,
  pct,
  compact,
}: {
  label?: string;
  detail?: string;
  pct: number;
  compact?: boolean;
}) {
  const clamped = Math.max(0, Math.min(100, pct));
  const color = clamped >= 85 ? "bg-destructive" : clamped >= 70 ? "bg-amber-500" : "bg-primary";
  return (
    <div>
      {(label || detail) && (
        <div
          className={cn(
            "mb-1 flex items-baseline justify-between gap-2",
            compact ? "text-[11px]" : "text-xs",
          )}
        >
          <span className="truncate font-medium">{label}</span>
          {detail && <span className="shrink-0 text-muted-foreground">{detail}</span>}
        </div>
      )}
      <div className={cn("w-full overflow-hidden rounded bg-secondary", compact ? "h-1.5" : "h-2")}>
        <div
          className={cn("h-full rounded transition-all", color)}
          style={{ width: `${clamped}%` }}
        />
      </div>
    </div>
  );
}
