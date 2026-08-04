import { Plus, X } from "lucide-react";

import { cn } from "@/lib/utils";
import type { QueryTabsApi } from "@/lib/query-tabs";

/** The workbench's tab strip: one tab per open query, plus a button to add one. */
export function QueryTabs({ tabs, activeId, select, open, close }: QueryTabsApi) {
  return (
    <div className="flex items-stretch gap-px overflow-x-auto border-b bg-card">
      {tabs.map((tab) => {
        const isActive = tab.id === activeId;
        return (
          <div
            key={tab.id}
            className={cn(
              "group flex shrink-0 items-center gap-1 border-r pl-3 pr-1 text-[13px]",
              isActive
                ? "border-b-2 border-b-primary bg-background font-medium"
                : "text-muted-foreground hover:bg-secondary/60",
            )}
          >
            <button
              onClick={() => select(tab.id)}
              // The query itself is the tooltip: the label is only a number.
              title={tab.sql.trim() || "Empty query"}
              className="max-w-[12rem] truncate py-1.5"
            >
              {tab.title}
            </button>
            <button
              onClick={() => close(tab.id)}
              title={`Close ${tab.title}`}
              aria-label={`Close ${tab.title}`}
              className={cn(
                "rounded p-0.5 opacity-0 transition-opacity hover:bg-secondary group-hover:opacity-100",
                isActive && "opacity-60",
              )}
            >
              <X className="h-3.5 w-3.5" />
            </button>
          </div>
        );
      })}
      <button
        onClick={() => open()}
        title="New query"
        aria-label="New query"
        className="flex shrink-0 items-center px-2 text-muted-foreground hover:bg-secondary/60"
      >
        <Plus className="h-4 w-4" />
      </button>
    </div>
  );
}
