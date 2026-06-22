/** Pretty-printed, scrollable JSON block. */
export function JsonView({ value }: { value: unknown }) {
  const text = (() => {
    try {
      return JSON.stringify(value, (_k, v) => (typeof v === "bigint" ? v.toString() : v), 2);
    } catch {
      return String(value);
    }
  })();
  return (
    <pre className="overflow-auto rounded-md border bg-secondary/40 p-3 font-mono text-xs">
      {text}
    </pre>
  );
}
