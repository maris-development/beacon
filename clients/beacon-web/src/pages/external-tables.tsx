import * as React from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { CheckCircle2, ChevronRight, Database, Loader2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { JsonView } from "@/components/json-view";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { CheckboxField } from "@/components/ui/checkbox-field";
import { Field } from "@/components/form/field";
import { TokenListInput } from "@/components/form/token-list-input";
import { KeyValueEditor, rowsToMap, type KeyValueRow } from "@/components/form/key-value-editor";

/** Storage types accepted by `CREATE EXTERNAL TABLE ... STORED AS <type>`. */
const FILE_TYPES = ["PARQUET", "CSV", "NETCDF", "ARROW", "ZARR", "ODV", "DELTA", "ICEBERG", "REMOTE"];

/** Per-type hints to guide the location/options fields. */
const TYPE_HINTS: Record<string, string> = {
  PARQUET: "Datasets-store path or glob, e.g. obs/ or data/**/*.parquet.",
  CSV: "Datasets-store path or glob. Use options like delimiter to tune parsing.",
  NETCDF: "Datasets-store path or glob, e.g. argo/**/*.nc.",
  ARROW: "Datasets-store path or glob to Arrow IPC files.",
  ZARR: "Datasets-store path to a Zarr store.",
  ODV: "Datasets-store path or glob to ODV files.",
  DELTA: "Scheme-qualified location of the Delta table.",
  ICEBERG: "Scheme-qualified location of the Iceberg table.",
  REMOTE: "Scheme-qualified remote location.",
};

const EMPTY = {
  name: "",
  location: "",
  fileType: "PARQUET",
  partitionCols: [] as string[],
  options: [] as KeyValueRow[],
  ifNotExists: false,
};

export function ExternalTablesPage() {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [form, setForm] = React.useState(EMPTY);
  const [showPayload, setShowPayload] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);
  const [result, setResult] = React.useState<unknown>(null);

  function set<K extends keyof typeof form>(key: K, value: (typeof form)[K]) {
    setForm((f) => ({ ...f, [key]: value }));
  }

  /** Builds the `CreateExternalTableRequest` payload. */
  const payload = React.useMemo(() => {
    const p: Record<string, unknown> = {
      name: form.name.trim(),
      location: form.location.trim(),
      file_type: form.fileType,
      if_not_exists: form.ifNotExists,
    };
    if (form.partitionCols.length) p.partition_cols = form.partitionCols;
    const opts = rowsToMap(form.options);
    if (Object.keys(opts).length) p.options = opts;
    return p;
  }, [form]);

  const mutation = useMutation({
    mutationFn: (spec: Record<string, unknown>) => beacon.admin.createExternalTable(spec),
    onSuccess: () => {
      setResult({ created: true, ...payload });
      qc.invalidateQueries({ queryKey: ["tables"] });
    },
    onError: (e) => setError(errorMessage(e)),
  });

  function submit() {
    setError(null);
    setResult(null);
    if (!form.name.trim()) return setError("Name is required.");
    if (!form.location.trim()) return setError("Location is required.");
    mutation.mutate(payload);
  }

  return (
    <PageContainer
      title="External tables"
      description="Register a queryable table over files in the datasets store."
    >
      <div className="grid max-w-2xl gap-4">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Database className="h-4 w-4 text-primary" /> Create external table
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <Field label="Name" htmlFor="et-name" required hint="Logical table name.">
                <Input
                  id="et-name"
                  value={form.name}
                  onChange={(e) => set("name", e.target.value)}
                  placeholder="observations"
                />
              </Field>
              <Field label="Storage type" htmlFor="et-type" required hint="STORED AS …">
                <NativeSelect
                  id="et-type"
                  value={form.fileType}
                  onChange={(e) => set("fileType", e.target.value)}
                >
                  {FILE_TYPES.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </NativeSelect>
              </Field>
            </div>

            <Field
              label="Location"
              htmlFor="et-location"
              required
              hint={TYPE_HINTS[form.fileType] ?? "Datasets-store-relative location or glob."}
            >
              <Input
                id="et-location"
                value={form.location}
                onChange={(e) => set("location", e.target.value)}
                placeholder="obs/"
                className="font-mono text-sm"
              />
            </Field>

            <Field
              label="Partition columns"
              hint="Hive-style partition columns, in path order. Type and press Enter."
            >
              <TokenListInput
                values={form.partitionCols}
                onChange={(v) => set("partitionCols", v)}
                placeholder="year, month"
              />
            </Field>

            <Field label="Options" hint="Format-specific options forwarded to the table's OPTIONS.">
              <KeyValueEditor
                rows={form.options}
                onChange={(v) => set("options", v)}
                keyPlaceholder="delimiter"
                valuePlaceholder=";"
              />
            </Field>

            <div className="rounded-md border bg-secondary/30 p-3">
              <CheckboxField
                checked={form.ifNotExists}
                onChange={(v) => set("ifNotExists", v)}
                label="If not exists"
                hint="Skip (instead of erroring) when the table already exists."
              />
            </div>

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

            <Button onClick={submit} disabled={mutation.isPending}>
              {mutation.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
              Create table
            </Button>
          </CardContent>
        </Card>

        {result != null && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base text-emerald-600">
                <CheckCircle2 className="h-4 w-4" /> Created
              </CardTitle>
            </CardHeader>
            <CardContent>
              <JsonView value={result} />
            </CardContent>
          </Card>
        )}
      </div>
    </PageContainer>
  );
}
