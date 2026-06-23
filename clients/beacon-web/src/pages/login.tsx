import * as React from "react";
import { useLocation, useNavigate } from "react-router-dom";
import { AlertCircle, Loader2, LogIn } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { HeroBackdrop } from "@/components/hero-backdrop";
import { useBeaconSession } from "@/lib/beacon-context";
import {
  DEFAULT_URL,
  SAME_ORIGIN,
  defaultServerUrl,
  makeClient,
  verifyAdmin,
  type Connection,
} from "@/lib/auth";
import { errorMessage } from "@/lib/errors";

export function LoginPage() {
  const { client, login } = useBeaconSession();
  const navigate = useNavigate();
  const location = useLocation();
  const from = (location.state as { from?: string } | null)?.from ?? "/query";

  const [url, setUrl] = React.useState(defaultServerUrl);
  const [username, setUsername] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [busy, setBusy] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // Already authenticated → skip the login screen.
  React.useEffect(() => {
    if (client) navigate(from, { replace: true });
  }, [client, from, navigate]);

  async function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError(null);
    const conn: Connection = { url: url.trim(), username, password };
    try {
      await verifyAdmin(makeClient(conn));
      login(conn);
      navigate(from, { replace: true });
    } catch (err) {
      setError(errorMessage(err));
    } finally {
      setBusy(false);
    }
  }

  return (
    <div className="relative flex min-h-screen items-center justify-center overflow-hidden bg-[#0b1020] px-4">
      <HeroBackdrop />
      <div className="relative z-10 w-full max-w-sm">
        <div className="beacon-rise mb-6 flex flex-col items-center gap-2 text-center">
          <img src="/beacon-logo-small.png" alt="" className="h-16 w-16 drop-shadow-[0_0_18px_rgba(36,198,220,0.45)]" />
          <h1 className="beacon-gradient-text text-3xl font-bold">Beacon Admin</h1>
          <p className="text-sm text-white/60">Sign in with your administrator credentials.</p>
        </div>

        <form
          onSubmit={handleSubmit}
          className="beacon-rise space-y-4 rounded-lg border border-white/10 bg-card/95 p-6 shadow-2xl backdrop-blur"
          style={{ animationDelay: "0.1s" }}
        >
          {!SAME_ORIGIN && (
            <div className="space-y-1.5">
              <Label htmlFor="url">Server URL</Label>
              <Input
                id="url"
                type="url"
                required
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder={DEFAULT_URL}
                autoComplete="url"
              />
            </div>
          )}
          <div className="space-y-1.5">
            <Label htmlFor="username">Username</Label>
            <Input
              id="username"
              required
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="beacon-admin"
              autoComplete="username"
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              autoComplete="current-password"
            />
          </div>

          {error && (
            <div className="flex items-start gap-2 rounded-md bg-destructive/10 p-2.5 text-sm text-destructive">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <span className="break-words">{error}</span>
            </div>
          )}

          <Button type="submit" className="w-full" disabled={busy}>
            {busy ? <Loader2 className="h-4 w-4 animate-spin" /> : <LogIn className="h-4 w-4" />}
            {busy ? "Signing in…" : "Sign in"}
          </Button>
        </form>

        <p className="mt-4 text-center text-xs text-white/40">
          Credentials are stored in this browser and sent to the Beacon server as HTTP Basic auth.
        </p>
      </div>
    </div>
  );
}
