import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import {
  Activity,
  ChevronRight,
  Clock,
  Cpu,
  Gauge,
  HardDrive,
  Loader2,
  MemoryStick,
  RefreshCw,
  Search,
  Server,
  Tag,
} from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { formatBytes } from "@/lib/format";
import { PageContainer } from "@/components/app-shell";
import { DatasetStorageCard } from "@/components/dataset-storage";
import { JsonView } from "@/components/json-view";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Meter } from "@/components/ui/meter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

/** Subset of `sysinfo::System` the dashboard renders (present when host metrics are enabled). */
interface HostInfo {
  global_cpu_usage?: number;
  cpus?: Array<{ name?: string; brand?: string; frequency?: number; cpu_usage?: number }>;
  physical_core_count?: number;
  total_memory?: number;
  available_memory?: number;
  used_memory?: number;
  free_memory?: number;
  total_swap?: number;
  used_swap?: number;
  uptime?: number;
  load_average?: { one?: number; five?: number; fifteen?: number };
  long_os_version?: string;
  kernel_version?: string;
  host_name?: string;
}

interface SystemInfo {
  beacon_version?: string;
  system_info?: HostInfo | null;
}

interface FnDoc {
  name: string;
  description?: string;
}

function normalizeFns(raw: unknown): FnDoc[] {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((f) => {
      if (typeof f === "string") return { name: f };
      const o = f as Record<string, unknown>;
      const name = (o.function_name ?? o.name ?? o.function ?? o.id) as string | undefined;
      return name ? { name, description: o.description as string | undefined } : null;
    })
    .filter((f): f is FnDoc => f !== null);
}

/** Formats a duration in seconds as e.g. `3d 4h 12m`. */
function formatUptime(secs: number): string {
  const d = Math.floor(secs / 86400);
  const h = Math.floor((secs % 86400) / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const parts = [d && `${d}d`, (d || h) && `${h}h`, `${m}m`].filter(Boolean);
  return parts.join(" ");
}

export function ServerInfoPage() {
  const beacon = useBeacon();
  const [fnFilter, setFnFilter] = React.useState("");
  const [showRaw, setShowRaw] = React.useState(false);

  // Refetch host metrics periodically so usage gauges stay live.
  const infoQuery = useQuery({
    queryKey: ["info"],
    queryFn: () => beacon.info<SystemInfo>(),
    refetchInterval: 5000,
  });
  const healthQuery = useQuery({ queryKey: ["health"], queryFn: () => beacon.health() });
  const fnsQuery = useQuery({
    queryKey: ["functions"],
    queryFn: async () => normalizeFns(await beacon.functions()),
  });
  const info = infoQuery.data;
  const host = info?.system_info ?? null;

  // Scalar, aggregate, and window functions — the server lists no table
  // functions (DataFusion does not catalog them); those are in the docs.
  const allFns = (fnsQuery.data ?? []).filter((f) =>
    f.name.toLowerCase().includes(fnFilter.toLowerCase()),
  );

  return (
    <PageContainer
      title="Server"
      description="Runtime health, host resources, and available functions."
      actions={
        <div className="flex items-center gap-2">
          <Badge variant={healthQuery.data ? "success" : "destructive"}>
            {healthQuery.isLoading ? "checking…" : healthQuery.data ? "healthy" : "unreachable"}
          </Badge>
          <Button variant="outline" size="sm" onClick={() => infoQuery.refetch()}>
            <RefreshCw className={infoQuery.isFetching ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
          </Button>
        </div>
      }
    >
      {infoQuery.isLoading && <Spinner />}
      {infoQuery.isError && (
        <div className="mb-4 text-sm text-destructive">{errorMessage(infoQuery.error)}</div>
      )}

      {/* Summary tiles. */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <StatTile icon={Tag} label="Beacon version" value={info?.beacon_version ?? "—"} />
        <StatTile
          icon={Server}
          label="Host"
          value={host?.host_name ?? "—"}
          detail={host?.long_os_version}
        />
        <StatTile
          icon={Clock}
          label="Uptime"
          value={host?.uptime != null ? formatUptime(host.uptime) : "—"}
          detail={host?.kernel_version ? `kernel ${host.kernel_version}` : undefined}
        />
        <StatTile
          icon={Cpu}
          label="CPU"
          value={host?.cpus?.[0]?.brand ?? "—"}
          detail={host?.physical_core_count != null ? `${host.physical_core_count} cores` : undefined}
        />
      </div>

      {/* Datasets store disk space. The datasets store is the server's own, so
          this does not depend on BEACON_ENABLE_SYS_INFO. */}
      <div className="mt-4">
        <DatasetStorageCard />
      </div>

      {host == null && !infoQuery.isLoading && (
        <Card className="mt-4 p-4 text-sm text-muted-foreground">
          Host resource metrics are disabled. Start the server with{" "}
          <code className="rounded bg-secondary px-1 py-0.5 font-mono text-xs">
            BEACON_ENABLE_SYS_INFO=true
          </code>{" "}
          to see CPU, memory, and load.
        </Card>
      )}

      {/* Resource gauges. */}
      {host && (
        <div className="mt-4 grid gap-4 lg:grid-cols-2">
          <Card>
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2 text-base">
                <Gauge className="h-4 w-4 text-primary" /> CPU
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <Meter
                label="Overall usage"
                detail={`${(host.global_cpu_usage ?? 0).toFixed(0)}%`}
                pct={host.global_cpu_usage ?? 0}
              />
              {host.cpus && host.cpus.length > 0 && (
                <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 pt-1 sm:grid-cols-3">
                  {host.cpus.map((c, i) => (
                    <Meter
                      key={c.name ?? i}
                      label={`#${c.name ?? i + 1}`}
                      detail={`${(c.cpu_usage ?? 0).toFixed(0)}%`}
                      pct={c.cpu_usage ?? 0}
                      compact
                    />
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          <div className="grid gap-4">
            <Card>
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center gap-2 text-base">
                  <MemoryStick className="h-4 w-4 text-primary" /> Memory
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <Meter
                  label={`${formatBytes(host.used_memory ?? 0)} used`}
                  detail={`of ${formatBytes(host.total_memory ?? 0)}`}
                  pct={ratio(host.used_memory, host.total_memory)}
                />
                <div className="flex gap-4 text-xs text-muted-foreground">
                  <span>Available {formatBytes(host.available_memory ?? 0)}</span>
                  <span>Free {formatBytes(host.free_memory ?? 0)}</span>
                </div>
                {host.total_swap ? (
                  <Meter
                    label={`${formatBytes(host.used_swap ?? 0)} swap`}
                    detail={`of ${formatBytes(host.total_swap)}`}
                    pct={ratio(host.used_swap, host.total_swap)}
                  />
                ) : null}
              </CardContent>
            </Card>

            {host.load_average && (
              <Card>
                <CardHeader className="pb-2">
                  <CardTitle className="flex items-center gap-2 text-base">
                    <Activity className="h-4 w-4 text-primary" /> Load average
                  </CardTitle>
                </CardHeader>
                <CardContent className="flex gap-6">
                  <LoadStat label="1 min" value={host.load_average.one} />
                  <LoadStat label="5 min" value={host.load_average.five} />
                  <LoadStat label="15 min" value={host.load_average.fifteen} />
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      )}

      {/* Functions browser. */}
      <Card className="mt-4 flex min-h-0 flex-col">
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-base">
            <HardDrive className="h-4 w-4 text-primary" /> Functions{" "}
            <span className="text-sm font-normal text-muted-foreground">({allFns.length})</span>
          </CardTitle>
        </CardHeader>
        <CardContent className="flex min-h-0 flex-1 flex-col gap-2">
          <div className="relative">
            <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              value={fnFilter}
              onChange={(e) => setFnFilter(e.target.value)}
              placeholder="Filter functions"
              className="pl-8"
            />
          </div>
          {fnsQuery.isLoading && <Spinner />}
          <div className="grid max-h-[28rem] gap-1 overflow-auto sm:grid-cols-2">
            {allFns.map((f) => (
              <div
                key={f.name}
                className="flex items-start gap-2 rounded px-2 py-1.5 hover:bg-secondary/60"
              >
                <div className="min-w-0">
                  <div className="font-mono text-sm">{f.name}</div>
                  {f.description && (
                    <div className="truncate text-xs text-muted-foreground" title={f.description}>
                      {f.description}
                    </div>
                  )}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Raw payload, for completeness. */}
      <div className="mt-4">
        <button
          type="button"
          onClick={() => setShowRaw((s) => !s)}
          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
        >
          <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", showRaw && "rotate-90")} />
          Raw /api/info
        </button>
        {showRaw && info != null && (
          <div className="mt-2">
            <JsonView value={info} />
          </div>
        )}
      </div>
    </PageContainer>
  );
}

function ratio(used?: number, total?: number): number {
  if (!used || !total) return 0;
  return (used / total) * 100;
}

function StatTile({
  icon: Icon,
  label,
  value,
  detail,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string;
  detail?: string;
}) {
  return (
    <Card className="p-4">
      <div className="flex items-center gap-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
        <Icon className="h-4 w-4" /> {label}
      </div>
      <div className="mt-1.5 truncate text-lg font-semibold" title={value}>
        {value}
      </div>
      {detail && <div className="truncate text-xs text-muted-foreground">{detail}</div>}
    </Card>
  );
}

function LoadStat({ label, value }: { label: string; value?: number }) {
  return (
    <div>
      <div className="text-xl font-semibold tabular-nums">
        {value != null ? value.toFixed(2) : "—"}
      </div>
      <div className="text-xs text-muted-foreground">{label}</div>
    </div>
  );
}

function Spinner() {
  return (
    <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin" /> Loading…
    </div>
  );
}
