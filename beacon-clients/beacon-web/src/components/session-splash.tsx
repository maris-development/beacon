import { Loader2 } from "lucide-react";

/** Full-screen placeholder shown while the app resolves the session. */
export function SessionSplash() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-background">
      <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
      <span className="sr-only">Checking session</span>
    </div>
  );
}
