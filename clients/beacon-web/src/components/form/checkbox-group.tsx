import { cn } from "@/lib/utils";

export interface CheckOption {
  value: string;
  label: string;
}

interface CheckboxGroupProps {
  options: CheckOption[];
  selected: string[];
  onChange: (selected: string[]) => void;
}

/** A wrapping group of toggle chips for selecting multiple values. */
export function CheckboxGroup({ options, selected, onChange }: CheckboxGroupProps) {
  function toggle(value: string) {
    onChange(
      selected.includes(value) ? selected.filter((v) => v !== value) : [...selected, value],
    );
  }
  return (
    <div className="flex flex-wrap gap-1.5">
      {options.map((opt) => {
        const active = selected.includes(opt.value);
        return (
          <button
            key={opt.value}
            type="button"
            onClick={() => toggle(opt.value)}
            className={cn(
              "rounded-md border px-2.5 py-1 text-xs font-medium transition-colors",
              active
                ? "border-primary bg-primary text-primary-foreground"
                : "border-input bg-card text-muted-foreground hover:bg-secondary",
            )}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}
