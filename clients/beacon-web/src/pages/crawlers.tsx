import * as React from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ChevronRight, Loader2, Play, Plus, RefreshCw, Radar, Trash2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { InfoBanner } from "@/components/info-banner";
import { JsonView } from "@/components/json-view";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { CheckboxField } from "@/components/ui/checkbox-field";
import { Field } from "@/components/form/field";
import { CheckboxGroup } from "@/components/form/checkbox-group";
import { KeyValueEditor, rowsToMap, type KeyValueRow } from "@/components/form/key-value-editor";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";

/** Format identifiers accepted by a crawler's `format_filter`. */
const CRAWLER_DOCS = "https://maris-development.github.io/beacon/docs/1.7.3/data-lake/crawlers";

const FORMATS = [
  { value: "parquet", label: "Parquet" },
  { value: "nc", label: "NetCDF" },
  { value: "csv", label: "CSV" },
  { value: "zarr", label: "Zarr" },
  { value: "arrow", label: "Arrow" },
  { value: "odv", label: "ODV" },
  { value: "tiff", label: "GeoTIFF" },
  { value: "bbf", label: "BBF" },
];

function crawlerName(c: unknown): string {
  if (c && typeof c === "object" && typeof (c as Record<string, unknown>).name === "string") {
    return (c as Record<string, unknown>).name as string;
  }
  return "(unnamed)";
}

export function CrawlersPage() {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [actionError, setActionError] = React.useState<string | null>(null);

  const crawlersQuery = useQuery({
    queryKey: ["crawlers"],
    queryFn: () => beacon.admin.listCrawlers(),
  });

  const runMutation = useMutation({
    mutationFn: (name: string) => beacon.admin.runCrawler(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["crawlers"] }),
    onError: (e) => setActionError(errorMessage(e)),
  });
  const dropMutation = useMutation({
    mutationFn: (name: string) => beacon.admin.dropCrawler(name),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["crawlers"] }),
    onError: (e) => setActionError(errorMessage(e)),
  });

  const crawlers = (crawlersQuery.data as unknown[]) ?? [];

  return (
    <PageContainer
      title="Crawlers"
      description="Discover datasets and auto-register tables."
      actions={
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => crawlersQuery.refetch()}>
            <RefreshCw className={crawlersQuery.isFetching ? "h-4 w-4 animate-spin" : "h-4 w-4"} />
          </Button>
          <NewCrawlerDialog />
        </div>
      }
    >
      <InfoBanner>
        A crawler scans a prefix in the datasets store and automatically registers the files it
        finds as queryable tables — similar to an AWS Glue crawler. Run it on demand, on a
        schedule, or have it react to new files. <a href={CRAWLER_DOCS} target="_blank" rel="noreferrer">Learn more</a>.
      </InfoBanner>
      {actionError && (
        <div className="mb-4 rounded-md border border-destructive/30 bg-destructive/10 p-3 text-sm text-destructive">
          {actionError}
        </div>
      )}

      {crawlersQuery.isLoading && (
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <Loader2 className="h-4 w-4 animate-spin" /> Loading crawlers…
        </div>
      )}
      {crawlersQuery.isError && (
        <div className="text-sm text-destructive">{errorMessage(crawlersQuery.error)}</div>
      )}
      {!crawlersQuery.isLoading && crawlers.length === 0 && (
        <Card className="p-8 text-center text-sm text-muted-foreground">
          No crawlers defined yet. Create one to get started.
        </Card>
      )}

      <div className="grid gap-4">
        {crawlers.map((c, i) => {
          const name = crawlerName(c);
          return (
            <Card key={`${name}-${i}`} className="p-4">
              <div className="mb-3 flex items-center gap-2">
                <Radar className="h-4 w-4 text-primary" />
                <span className="font-semibold">{name}</span>
                <div className="ml-auto flex gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={runMutation.isPending}
                    onClick={() => {
                      setActionError(null);
                      runMutation.mutate(name);
                    }}
                  >
                    <Play className="h-4 w-4" /> Run
                  </Button>
                  <Button
                    variant="destructive"
                    size="sm"
                    disabled={dropMutation.isPending}
                    onClick={() => {
                      setActionError(null);
                      if (confirm(`Delete crawler "${name}"?`)) dropMutation.mutate(name);
                    }}
                  >
                    <Trash2 className="h-4 w-4" />
                  </Button>
                </div>
              </div>
              <JsonView value={c} />
            </Card>
          );
        })}
      </div>
    </PageContainer>
  );
}

const EMPTY_FORM = {
  name: "",
  targetPrefix: "",
  formats: [] as string[],
  tableNaming: "leaf_prefix",
  detectPartitions: true,
  schedule: "",
  eventDriven: false,
  options: [] as KeyValueRow[],
};

function NewCrawlerDialog() {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [open, setOpen] = React.useState(false);
  const [form, setForm] = React.useState(EMPTY_FORM);
  const [showPayload, setShowPayload] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  function set<K extends keyof typeof form>(key: K, value: (typeof form)[K]) {
    setForm((f) => ({ ...f, [key]: value }));
  }

  /** Builds the `CreateCrawlerRequest` payload, omitting empty optionals. */
  const payload = React.useMemo(() => {
    const p: Record<string, unknown> = {
      name: form.name.trim(),
      target_prefix: form.targetPrefix.trim(),
      table_naming: form.tableNaming,
      detect_partitions: form.detectPartitions,
      event_driven: form.eventDriven,
    };
    if (form.formats.length) p.format_filter = form.formats;
    if (form.schedule.trim()) p.schedule_secs = Number(form.schedule);
    const opts = rowsToMap(form.options);
    if (Object.keys(opts).length) p.options = opts;
    return p;
  }, [form]);

  const createMutation = useMutation({
    mutationFn: (def: Record<string, unknown>) => beacon.admin.createCrawler(def),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["crawlers"] });
      setOpen(false);
      setForm(EMPTY_FORM);
      setShowPayload(false);
    },
    onError: (e) => setError(errorMessage(e)),
  });

  function submit() {
    setError(null);
    if (!form.name.trim()) return setError("Name is required.");
    if (!form.targetPrefix.trim()) return setError("Target prefix is required.");
    if (form.schedule.trim() && !Number.isFinite(Number(form.schedule))) {
      return setError("Schedule must be a number of seconds.");
    }
    createMutation.mutate(payload);
  }

  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        setOpen(o);
        if (!o) setError(null);
      }}
    >
      <DialogTrigger asChild>
        <Button size="sm">
          <Plus className="h-4 w-4" /> New crawler
        </Button>
      </DialogTrigger>
      <DialogContent className="max-h-[88vh] max-w-xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>New crawler</DialogTitle>
          <DialogDescription>
            Crawlers scan a datasets-store prefix and auto-register the files they find as tables.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-1">
          <Field label="Name" htmlFor="cr-name" required hint="Unique identifier for this crawler.">
            <Input
              id="cr-name"
              value={form.name}
              onChange={(e) => set("name", e.target.value)}
              placeholder="argo"
            />
          </Field>

          <Field
            label="Target prefix"
            htmlFor="cr-prefix"
            required
            hint="Datasets-store path to scan, e.g. argo/."
          >
            <Input
              id="cr-prefix"
              value={form.targetPrefix}
              onChange={(e) => set("targetPrefix", e.target.value)}
              placeholder="argo/"
              className="font-mono text-sm"
            />
          </Field>

          <Field
            label="Format filter"
            hint="Restrict discovery to these formats. Leave empty to crawl every format."
          >
            <CheckboxGroup
              options={FORMATS}
              selected={form.formats}
              onChange={(v) => set("formats", v)}
            />
          </Field>

          <div className="grid grid-cols-2 gap-4">
            <Field label="Table naming" htmlFor="cr-naming" hint="How discovered tables are named.">
              <NativeSelect
                id="cr-naming"
                value={form.tableNaming}
                onChange={(e) => set("tableNaming", e.target.value)}
              >
                <option value="leaf_prefix">Leaf prefix (floats)</option>
                <option value="crawler_prefixed">Crawler-prefixed (argo_floats)</option>
              </NativeSelect>
            </Field>

            <Field
              label="Schedule (seconds)"
              htmlFor="cr-schedule"
              hint="Periodic re-crawl interval. Empty = no timer."
            >
              <Input
                id="cr-schedule"
                type="number"
                min={1}
                value={form.schedule}
                onChange={(e) => set("schedule", e.target.value)}
                placeholder="e.g. 900"
              />
            </Field>
          </div>

          <div className="space-y-2.5 rounded-md border bg-secondary/30 p-3">
            <CheckboxField
              checked={form.detectPartitions}
              onChange={(v) => set("detectPartitions", v)}
              label="Detect partitions"
              hint="Detect Hive-style key=value/ partitions in paths."
            />
            <CheckboxField
              checked={form.eventDriven}
              onChange={(v) => set("eventDriven", v)}
              label="Event-driven"
              hint="Subscribe to datasets-store events for incremental crawls."
            />
          </div>

          <Field
            label="Table options"
            hint="Extra format options forwarded into every discovered table's OPTIONS."
          >
            <KeyValueEditor
              rows={form.options}
              onChange={(v) => set("options", v)}
              keyPlaceholder="read_dimensions"
              valuePlaceholder="lat,lon"
            />
          </Field>

          <div>
            <button
              type="button"
              onClick={() => setShowPayload((s) => !s)}
              className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
            >
              <ChevronRight className={cn("h-3.5 w-3.5 transition-transform", showPayload && "rotate-90")} />
              Request payload
            </button>
            {showPayload && (
              <div className="mt-2">
                <JsonView value={payload} />
              </div>
            )}
          </div>

          {error && <p className="text-sm text-destructive">{error}</p>}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => setOpen(false)}>
            Cancel
          </Button>
          <Button onClick={submit} disabled={createMutation.isPending}>
            {createMutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            Create crawler
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
