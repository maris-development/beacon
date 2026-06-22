/** Light / dark / system theme management, persisted to localStorage. */

import * as React from "react";

export type Theme = "light" | "dark" | "system";

const STORAGE_KEY = "beacon-web.theme";

interface ThemeContextValue {
  /** The user's selection (may be "system"). */
  theme: Theme;
  /** The currently applied appearance ("light" | "dark"). */
  resolved: "light" | "dark";
  setTheme: (theme: Theme) => void;
}

const ThemeContext = React.createContext<ThemeContextValue | null>(null);

function prefersDark(): boolean {
  return (
    typeof window !== "undefined" &&
    window.matchMedia?.("(prefers-color-scheme: dark)").matches
  );
}

function loadTheme(): Theme {
  const stored = typeof localStorage !== "undefined" ? localStorage.getItem(STORAGE_KEY) : null;
  return stored === "light" || stored === "dark" || stored === "system" ? stored : "system";
}

function resolve(theme: Theme): "light" | "dark" {
  return theme === "system" ? (prefersDark() ? "dark" : "light") : theme;
}

function apply(resolved: "light" | "dark"): void {
  const root = document.documentElement;
  root.classList.toggle("dark", resolved === "dark");
  root.style.colorScheme = resolved;
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  const [theme, setThemeState] = React.useState<Theme>(() => loadTheme());
  const [resolved, setResolved] = React.useState<"light" | "dark">(() => resolve(loadTheme()));

  // Apply on change and persist the selection.
  React.useEffect(() => {
    const r = resolve(theme);
    setResolved(r);
    apply(r);
    localStorage.setItem(STORAGE_KEY, theme);
  }, [theme]);

  // Follow OS changes while on "system".
  React.useEffect(() => {
    if (theme !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => {
      const r = prefersDark() ? "dark" : "light";
      setResolved(r);
      apply(r);
    };
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [theme]);

  const value = React.useMemo(
    () => ({ theme, resolved, setTheme: setThemeState }),
    [theme, resolved],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme(): ThemeContextValue {
  const ctx = React.useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within <ThemeProvider>");
  return ctx;
}
