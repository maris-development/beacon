import * as React from "react";
import { useQuery } from "@tanstack/react-query";
import { FileStack, Loader2, Search } from "lucide-react";

import { useBeacon } from "@/lib/beacon-context";
import { parseSchema } from "@/lib/schema";
import { errorMessage } from "@/lib/errors";
import { PageContainer } from "@/components/app-shell";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

interface DatasetItem {
  path: string;
  format?: string;
}

function normalize(raw: unknown): DatasetItem[] {
  if (!Array.isArray(raw)) return [];
  return raw.map((item) => {
    if (typeof item === "string") return { path: item };
    const obj = item as Record<string, unknown>;
    const path = (obj.path ?? obj.file ?? obj.file_path ?? "") as string;
    const format = (obj.format ?? obj.file_format) as string | undefined;
    return { path, format };
  });
}

export function DatasetsPage() {
  const beacon = useBeacon();
  const [pattern, setPattern] = React.useState("");
  const [applied, setApplied] = React.useState("");
  const [schemaFor, setSchemaFor] = React.useState<string | null>(null);

  const totalQuery = useQuery({ queryKey: ["total-datasets"], queryFn: () => beacon.totalDatasets() });
  const datasetsQuery = useQuery({
    queryKey: ["datasets", applied],
    queryFn: async () => normalize(await beacon.datasets({ pattern: applied || undefined, limit: 200 })),
  });

  return (
    <PageContainer
      title="Datasets"
      description="Files discovered in the datasets store. Inspect a file's schema."
      actions={
        typeof totalQuery.data === "number" ? (
          <Badge variant="secondary">{totalQuery.data} total</Badge>
        ) : undefined
      }
    >
      <form
        className="mb-4 flex max-w-md items-center gap-2"
        onSubmit={(e) => {
          e.preventDefault();
          setApplied(pattern.trim());
        }}
      >
        <div className="relative flex-1">
          <Search className="absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            value={pattern}
            onChange={(e) => setPattern(e.target.value)}
            placeholder="Glob pattern, e.g. **/*.nc"
            className="pl-8"
          />
        </div>
        <Button type="submit" variant="outline">
          Filter
        </Button>
      </form>

      <Card className="overflow-hidden">
        {datasetsQuery.isLoading && (
          <div className="flex items-center gap-2 p-4 text-sm text-muted-foreground">
            <Loader2 className="h-4 w-4 animate-spin" /> Loading datasets…
          </div>
        )}
        {datasetsQuery.isError && (
          <div className="p-4 text-sm text-destructive">{errorMessage(datasetsQuery.error)}</div>
        )}
        {datasetsQuery.data && datasetsQuery.data.length === 0 && (
          <div className="p-4 text-sm text-muted-foreground">No datasets match.</div>
        )}
        {datasetsQuery.data && datasetsQuery.data.length > 0 && (
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>Path</TableHead>
                <TableHead className="w-28">Format</TableHead>
                <TableHead className="w-28" />
              </TableRow>
            </TableHeader>
            <TableBody>
              {datasetsQuery.data.map((d) => (
                <TableRow key={d.path}>
                  <TableCell className="font-mono text-xs">
                    <span className="flex items-center gap-1.5">
                      <FileStack className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                      {d.path}
                    </span>
                  </TableCell>
                  <TableCell>
                    {d.format ? <Badge variant="muted">{d.format}</Badge> : "—"}
                  </TableCell>
                  <TableCell>
                    <Button variant="ghost" size="sm" onClick={() => setSchemaFor(d.path)}>
                      Schema
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        )}
      </Card>

      <Dialog open={schemaFor !== null} onOpenChange={(o) => !o && setSchemaFor(null)}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="break-all font-mono text-sm">{schemaFor}</DialogTitle>
          </DialogHeader>
          {schemaFor && <DatasetSchema file={schemaFor} />}
        </DialogContent>
      </Dialog>
    </PageContainer>
  );
}

function DatasetSchema({ file }: { file: string }) {
  const beacon = useBeacon();
  const query = useQuery({
    queryKey: ["dataset-schema", file],
    queryFn: async () => parseSchema(await beacon.datasetSchema(file)),
  });

  if (query.isLoading)
    return (
      <div className="flex items-center gap-2 py-4 text-sm text-muted-foreground">
        <Loader2 className="h-4 w-4 animate-spin" /> Reading schema…
      </div>
    );
  if (query.isError) return <div className="py-4 text-sm text-destructive">{errorMessage(query.error)}</div>;

  return (
    <div className="max-h-[60vh] overflow-auto rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Column</TableHead>
            <TableHead>Type</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {query.data?.map((c) => (
            <TableRow key={c.name}>
              <TableCell className="font-mono">{c.name}</TableCell>
              <TableCell className="font-mono text-muted-foreground">{c.dataType}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
