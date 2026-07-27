import * as React from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { Loader2 } from "lucide-react";

import { useBeacon } from "@/lib/beacon-context";
import { errorMessage } from "@/lib/errors";
import { SqlEditor } from "@/components/sql-editor";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

/** Double-quotes a SQL identifier, escaping embedded quotes. */
function quoteIdent(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

interface CreateViewDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** True for a materialized view (stores the result), false for a plain view. */
  materialized: boolean;
  onCreated: (name: string) => void;
}

/** Controlled dialog to create a (materialized) view from a SQL query. */
export function CreateViewDialog({ open, onOpenChange, materialized, onCreated }: CreateViewDialogProps) {
  const beacon = useBeacon();
  const qc = useQueryClient();
  const [name, setName] = React.useState("");
  const [sql, setSql] = React.useState("SELECT 1 AS a");
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (open) {
      setName("");
      setSql("SELECT 1 AS a");
      setError(null);
    }
  }, [open]);

  const keyword = materialized ? "MATERIALIZED VIEW" : "VIEW";

  const create = useMutation({
    mutationFn: () => beacon.query(`CREATE ${keyword} ${quoteIdent(name.trim())} AS ${sql}`),
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
    if (!sql.trim()) return setError("Query is required.");
    create.mutate();
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle>{materialized ? "New materialized view" : "New view"}</DialogTitle>
          <DialogDescription>
            {materialized
              ? "Runs the query now and stores the result as a queryable table. Refresh it later to re-materialize."
              : "A virtual table defined by a query. It runs on each access and stores no data."}
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-3">
          <div className="space-y-1.5">
            <Label htmlFor="view-name">Name</Label>
            <Input
              id="view-name"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="my_view"
            />
          </div>
          <div className="space-y-1.5">
            <Label>Query</Label>
            <div className="h-48 overflow-hidden rounded-md border">
              <SqlEditor value={sql} onChange={setSql} />
            </div>
          </div>
          {error && <p className="text-sm text-destructive">{error}</p>}
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={submit} disabled={create.isPending}>
            {create.isPending && <Loader2 className="h-4 w-4 animate-spin" />}
            Create
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
