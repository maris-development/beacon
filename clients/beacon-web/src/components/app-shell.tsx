import type { ReactNode } from "react";
import { NavLink, Outlet, useNavigate } from "react-router-dom";
import {
  BookOpen,
  ChevronDown,
  ExternalLink,
  FileStack,
  LogOut,
  Radar,
  Server,
  ShieldCheck,
  TableProperties,
  TerminalSquare,
} from "lucide-react";

/** Public Beacon documentation site. */
const DOCS_URL = "https://maris-development.github.io/beacon/";

import { cn } from "@/lib/utils";
import { useBeaconSession } from "@/lib/beacon-context";
import { ThemeToggle } from "@/components/theme-toggle";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";

const NAV = [
  { to: "/query", label: "Query editor", icon: TerminalSquare },
  { to: "/tables", label: "Tables", icon: TableProperties },
  { to: "/datasets", label: "Datasets", icon: FileStack },
  { to: "/crawlers", label: "Crawlers", icon: Radar },
  { to: "/access", label: "Users & roles", icon: ShieldCheck },
  { to: "/server", label: "Server", icon: Server },
];

export function AppShell() {
  const { connection, logout } = useBeaconSession();
  const navigate = useNavigate();

  function handleLogout() {
    logout();
    navigate("/login", { replace: true });
  }

  const host = connection ? hostLabel(connection.url) : "";

  return (
    <div className="flex h-screen flex-col bg-background">
      {/* Top navigation bar (AWS-console style). */}
      <header className="flex h-12 shrink-0 items-center gap-3 bg-topbar px-4 text-topbar-foreground">
        <div className="flex items-center gap-2">
          <img src={`${import.meta.env.BASE_URL}beacon-logo-small.png`} alt="" className="h-7 w-7" />
          <span className="beacon-gradient-text text-[16px] font-bold tracking-tight">Beacon</span>
          <span className="rounded bg-topbar-foreground/10 px-1.5 py-0.5 text-[11px] font-medium uppercase tracking-wide text-topbar-foreground/70">
            Admin
          </span>
        </div>
        <div className="ml-auto flex items-center gap-3">
          <span className="hidden items-center gap-1.5 text-xs text-topbar-foreground/70 sm:flex">
            <span className="h-2 w-2 rounded-full bg-emerald-400" />
            {host}
          </span>
          <ThemeToggle />
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <button className="flex items-center gap-1.5 rounded px-2 py-1 text-sm text-topbar-foreground/90 hover:bg-topbar-foreground/10">
                {connection?.username ?? "admin"}
                <ChevronDown className="h-4 w-4" />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel className="font-normal">
                <div className="text-sm font-medium">{connection?.username}</div>
                <div className="text-xs text-muted-foreground">{connection?.url}</div>
              </DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem onClick={handleLogout}>
                <LogOut className="h-4 w-4" />
                Sign out
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </header>
      {/* Brand accent line (matches the docs hero gradient + wordmark). */}
      <div className="h-0.5 shrink-0 bg-gradient-to-r from-[#ff512f] via-[#dd2476] to-[#24c6dc]" />

      <div className="flex min-h-0 flex-1">
        {/* Left navigation. */}
        <nav className="flex w-52 shrink-0 flex-col gap-0.5 border-r bg-card p-2">
          {NAV.map(({ to, label, icon: Icon }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                cn(
                  "flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                  isActive
                    ? "bg-primary/10 text-primary"
                    : "text-muted-foreground hover:bg-secondary/60 hover:text-foreground",
                )
              }
            >
              <Icon className="h-4 w-4" />
              {label}
            </NavLink>
          ))}
          <a
            href={DOCS_URL}
            target="_blank"
            rel="noreferrer"
            className="mt-auto flex items-center gap-2.5 rounded-md px-3 py-2 text-sm font-medium text-muted-foreground transition-colors hover:bg-secondary/60 hover:text-foreground"
          >
            <BookOpen className="h-4 w-4" />
            Documentation
            <ExternalLink className="ml-auto h-3 w-3" />
          </a>
          <div className="px-3 py-2 text-[11px] text-muted-foreground">Beacon Admin UI</div>
        </nav>

        {/* Page content. */}
        <main className="flex min-h-0 flex-1 flex-col overflow-hidden">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function hostLabel(url: string): string {
  try {
    return new URL(url).host;
  } catch {
    return url;
  }
}

/** A standard scrollable page wrapper with a title header. */
export function PageContainer({
  title,
  description,
  actions,
  children,
}: {
  title: string;
  description?: string;
  actions?: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-start justify-between gap-4 border-b bg-card px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold">{title}</h1>
          {description && <p className="text-sm text-muted-foreground">{description}</p>}
        </div>
        {actions}
      </div>
      <div className="min-h-0 flex-1 overflow-auto p-6">{children}</div>
    </div>
  );
}
