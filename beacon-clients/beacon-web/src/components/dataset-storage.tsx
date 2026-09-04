/**
 * Disk space of the datasets store, shown on the Server page and the Datasets
 * page.
 *
 * An S3 bucket has no capacity, so the total space, the free space and the used
 * percent read `n/a` there; the used space is the total size of the objects.
 */

import { useQuery } from "@tanstack/react-query";
import { HardDrive } from "lucide-react";

import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { formatBytes } from "@/lib/format";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Meter } from "@/components/ui/meter";

/** What a bucket cannot answer. */
const NOT_AVAILABLE = "n/a";

/**
 * Reads the disk space. The listing behind an S3 bucket is slow, so this polls
 * far slower than the host gauges and stays fresh for a minute.
 */
export function useDatasetStorage() {
  const beacon = useBeacon();
  return useQuery({
    queryKey: ["dataset-storage"],
    queryFn: () => beacon.admin.datasetStorage(),
    refetchInterval: 60_000,
    staleTime: 60_000,
  });
}

/** A byte count, or `n/a` when the store cannot report it. */
function bytes(value: number | null | undefined): string {
  return value == null ? NOT_AVAILABLE : formatBytes(value);
}

/** A percent with one decimal, or `n/a` when the store has no capacity. */
function percent(value: number | null | undefined): string {
  return value == null ? NOT_AVAILABLE : `${value.toFixed(1)}%`;
}

/** The full card for the Server page: a usage bar plus the four values. */
export function DatasetStorageCard() {
  const { data, isLoading, isError, error } = useDatasetStorage();

  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle className="flex items-center gap-2 text-base">
          <HardDrive className="h-4 w-4 text-primary" /> Datasets storage
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {isLoading && <div className="text-sm text-muted-foreground">Loading…</div>}
        {isError && <div className="text-sm text-destructive">{errorMessage(error)}</div>}
        {data && (
          <>
            <div className="truncate font-mono text-xs text-muted-foreground" title={data.location}>
              {data.kind === "s3" ? `s3://${data.location}` : data.location}
              {data.mount_point && ` (on ${data.mount_point})`}
            </div>
            {data.used_percent != null ? (
              <Meter
                label={`${bytes(data.used_space)} used`}
                detail={`of ${bytes(data.total_space)}`}
                pct={data.used_percent}
              />
            ) : (
              <div className="text-sm text-muted-foreground">
                An S3 bucket has no disk limit. Only the size of the objects is known
                {data.object_count != null && ` (${data.object_count} objects)`}.
              </div>
            )}
            <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
              <Value label="Total" text={bytes(data.total_space)} />
              <Value label="Used" text={bytes(data.used_space)} />
              <Value label="Free" text={bytes(data.free_space)} />
              <Value label="Used percent" text={percent(data.used_percent)} />
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
}

function Value({ label, text }: { label: string; text: string }) {
  return (
    <div>
      <div className="text-sm font-semibold tabular-nums">{text}</div>
      <div className="text-xs text-muted-foreground">{label}</div>
    </div>
  );
}

/**
 * The one-line form for the Datasets page header: the four values, and a bar
 * that turns amber then red as the disk fills.
 */
export function DatasetStorageBar() {
  const { data, isError } = useDatasetStorage();
  if (isError || !data) return null;

  return (
    <div className="mb-3 flex flex-wrap items-center gap-x-4 gap-y-1 rounded-md border px-3 py-2 text-xs">
      <span className="flex items-center gap-1.5 font-medium">
        <HardDrive className="h-3.5 w-3.5 text-primary" />
        <span className="max-w-[18rem] truncate font-mono" title={data.location}>
          {data.kind === "s3" ? `s3://${data.location}` : data.location}
        </span>
      </span>
      <span className="text-muted-foreground">
        Total <span className="tabular-nums text-foreground">{bytes(data.total_space)}</span>
      </span>
      <span className="text-muted-foreground">
        Used <span className="tabular-nums text-foreground">{bytes(data.used_space)}</span>
      </span>
      <span className="text-muted-foreground">
        Free <span className="tabular-nums text-foreground">{bytes(data.free_space)}</span>
      </span>
      <span className="text-muted-foreground">
        Used percent{" "}
        <span className="tabular-nums text-foreground">{percent(data.used_percent)}</span>
      </span>
      {data.used_percent != null && (
        <span className="ml-auto w-40">
          <Meter pct={data.used_percent} compact />
        </span>
      )}
    </div>
  );
}
