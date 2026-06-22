import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { Loader2, Search } from "lucide-react";

import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { JsonView } from "@/components/json-view";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";

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

export function ServerInfoPage() {
  const beacon = useBeacon();
  const [fnFilter, setFnFilter] = React.useState("");

  const infoQuery = useQuery({ queryKey: ["info"], queryFn: () => beacon.info() });
  const healthQuery = useQuery({ queryKey: ["health"], queryFn: () => beacon.health() });
  const fnsQuery = useQuery({
    queryKey: ["functions"],
    queryFn: async () => normalizeFns(await beacon.functions()),
  });
  const tableFnsQuery = useQuery({
    queryKey: ["table-functions"],
    queryFn: async () => normalizeFns(await beacon.tableFunctions()),
  });

  const allFns = [
    ...(fnsQuery.data ?? []).map((f) => ({ ...f, kind: "scalar" })),
    ...(tableFnsQuery.data ?? []).map((f) => ({ ...f, kind: "table" })),
  ].filter((f) => f.name.toLowerCase().includes(fnFilter.toLowerCase()));

  return (
    <PageContainer
      title="Server"
      description="Runtime information, health, and available functions."
      actions={
        <Badge variant={healthQuery.data ? "success" : "destructive"}>
          {healthQuery.isLoading ? "checking…" : healthQuery.data ? "healthy" : "unreachable"}
        </Badge>
      }
    >
      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Runtime info</CardTitle>
          </CardHeader>
          <CardContent>
            {infoQuery.isLoading && <Spinner />}
            {infoQuery.isError && <div className="text-sm text-destructive">{errorMessage(infoQuery.error)}</div>}
            {infoQuery.data != null && <JsonView value={infoQuery.data} />}
          </CardContent>
        </Card>

        <Card className="flex min-h-0 flex-col">
          <CardHeader>
            <CardTitle className="text-base">
              Functions{" "}
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
            {(fnsQuery.isLoading || tableFnsQuery.isLoading) && <Spinner />}
            <div className="max-h-[28rem] space-y-1 overflow-auto">
              {allFns.map((f) => (
                <div
                  key={`${f.kind}-${f.name}`}
                  className="flex items-start gap-2 rounded px-2 py-1.5 hover:bg-secondary/60"
                >
                  <Badge variant={f.kind === "table" ? "secondary" : "muted"} className="mt-0.5">
                    {f.kind}
                  </Badge>
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
      </div>
    </PageContainer>
  );
}

function Spinner() {
  return (
    <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
      <Loader2 className="h-4 w-4 animate-spin" /> Loading…
    </div>
  );
}
