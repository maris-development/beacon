import * as React from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { ChevronRight, Loader2 } from "lucide-react";

import { cn } from "@/lib/utils";
import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { JsonView } from "@/components/json-view";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { NativeSelect } from "@/components/ui/native-select";
import { CheckboxField } from "@/components/ui/checkbox-field";
import { Field } from "@/components/form/field";
import { TokenListInput } from "@/components/form/token-list-input";
import { KeyValueEditor, rowsToMap, type KeyValueRow } from "@/components/form/key-value-editor";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

/** Storage types accepted by `CREATE EXTERNAL TABLE ... STORED AS <type>`. */
const FILE_TYPES = ["PARQUET", "CSV", "NETCDF", "ARROW", "ZARR", "ODV", "DELTA", "ICEBERG", "REMOTE"];

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

const TITLES: Record<string, string> = {
  DELTA: "New Delta Lake table",
  ICEBERG: "New Iceberg table",
};

interface ExternalTableDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Storage type to preselect (e.g. "PARQUET", "DELTA", "ICEBERG"). */
  presetFormat?: string;
  /** Called with the new table name after a successful create. */
  onCreated: (name: string) => void;
}

/** Controlled dialog wrapping the guided "create external table" form. */
export function ExternalTableDialog({
  open,
  onOpenChange,
  presetFormat = "PARQUET",
  onCreated,
}: ExternalTableDialogProps) {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [name, setName] = React.useState("");
  const [location, setLocation] = React.useState("");
  const [fileType, setFileType] = React.useState(presetFormat);
  const [partitionCols, setPartitionCols] = React.useState<string[]>([]);
  const [options, setOptions] = React.useState<KeyValueRow[]>([]);
  const [ifNotExists, setIfNotExists] = React.useState(false);
  const [showPayload, setShowPayload] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // Reset to a clean form (with the requested preset) each time it opens.
  React.useEffect(() => {
    if (open) {
      setName("");
      setLocation("");
      setFileType(presetFormat);
      setPartitionCols([]);
      setOptions([]);
      setIfNotExists(false);
      setShowPayload(false);
      setError(null);
    }
  }, [open, presetFormat]);

  const payload = React.useMemo(() => {
    const p: Record<string, unknown> = {
      name: name.trim(),
      location: location.trim(),
      file_type: fileType,
      if_not_exists: ifNotExists,
    };
    if (partitionCols.length) p.partition_cols = partitionCols;
    const opts = rowsToMap(options);
    if (Object.keys(opts).length) p.options = opts;
    return p;
  }, [name, location, fileType, ifNotExists, partitionCols, options]);

  const create = useMutation({
    mutationFn: () => beacon.admin.createExternalTable(payload),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tables"] });
      onCreated(name.trim());
      onOpenChange(false);
    },
    onError: (e) => setError(errorMessage(e)),
  });

  function submit() {
    setError(null);
    if (!name.trim()) return setError("Name is required.");
    if (!location.trim()) return setError("Location is required.");
    create.mutate();
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[88vh] max-w-xl overflow-y-auto">
        <DialogHeader>
          <DialogTitle>{TITLES[presetFormat] ?? "New external table"}</DialogTitle>
          <DialogDescription>
            Registers a queryable table over data in the datasets store, without copying it.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <Field label="Name" htmlFor="et-name" required hint="Logical table name.">
              <Input
                id="et-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="observations"
              />
            </Field>
            <Field label="Storage type" htmlFor="et-type" required hint="STORED AS …">
              <NativeSelect id="et-type" value={fileType} onChange={(e) => setFileType(e.target.value)}>
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
            hint={TYPE_HINTS[fileType] ?? "Datasets-store-relative location or glob."}
          >
            <Input
              id="et-location"
              value={location}
              onChange={(e) => setLocation(e.target.value)}
              placeholder="obs/"
              className="font-mono text-sm"
            />
          </Field>

          <Field
            label="Partition columns"
            hint="Hive-style partition columns, in path order. Type and press Enter."
          >
            <TokenListInput values={partitionCols} onChange={setPartitionCols} placeholder="year, month" />
          </Field>

          <Field label="Options" hint="Format-specific options forwarded to the table's OPTIONS.">
            <KeyValueEditor
              rows={options}
              onChange={setOptions}
              keyPlaceholder="delimiter"
              valuePlaceholder=";"
            />
          </Field>

          <div className="rounded-md border bg-secondary/30 p-3">
            <CheckboxField
              checked={ifNotExists}
              onChange={setIfNotExists}
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
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={submit} disabled={create.isPending}>
            {create.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            Create table
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
