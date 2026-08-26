/** React context exposing the authenticated BeaconClient and session actions. */

import * as React from "react";
import type { BeaconClient } from "@beacon/client";

import {
  type Connection,
  clearConnection,
  clearProxySignedOut,
  detectProxySession,
  loadConnection,
  makeClient,
  markProxySignedOut,
  proxyProbeEnabled,
  saveConnection,
} from "./auth";

interface BeaconContextValue {
  /** The live, authenticated client, or null when logged out. */
  client: BeaconClient | null;
  /** The current connection (server URL + username), or null when logged out. */
  connection: Connection | null;
  /** True while the app checks whether a gateway already authenticates the caller. */
  initializing: boolean;
  /** Establishes a verified session (called by the Login page after admin.check succeeds). */
  login: (conn: Connection) => void;
  /** Clears the session and persisted credentials. */
  logout: () => void;
}

const BeaconContext = React.createContext<BeaconContextValue | null>(null);

export function BeaconProvider({ children }: { children: React.ReactNode }) {
  const stored = React.useMemo(() => loadConnection(), []);
  const [connection, setConnection] = React.useState<Connection | null>(stored);
  const [client, setClient] = React.useState<BeaconClient | null>(() =>
    stored ? makeClient(stored) : null,
  );
  // A stored session starts the app at once. Without one, the app first asks the
  // server whether a gateway in front of it authenticates the caller already.
  const [initializing, setInitializing] = React.useState(
    () => stored === null && proxyProbeEnabled(),
  );

  React.useEffect(() => {
    if (!initializing) return;
    let cancelled = false;
    void detectProxySession()
      .then((conn) => {
        if (cancelled || !conn) return;
        setConnection(conn);
        setClient(makeClient(conn));
      })
      .finally(() => {
        if (!cancelled) setInitializing(false);
      });
    return () => {
      cancelled = true;
    };
  }, [initializing]);

  const login = React.useCallback((conn: Connection) => {
    // A deliberate sign-in re-arms proxy detection for the next load.
    clearProxySignedOut();
    saveConnection(conn);
    setConnection(conn);
    setClient(makeClient(conn));
  }, []);

  // Sign-out also blocks proxy detection for this tab. Without that latch a
  // proxy session would come straight back on the next load of the app.
  const logout = React.useCallback(() => {
    markProxySignedOut();
    clearConnection();
    setConnection(null);
    setClient(null);
  }, []);

  const value = React.useMemo(
    () => ({ client, connection, initializing, login, logout }),
    [client, connection, initializing, login, logout],
  );

  return <BeaconContext.Provider value={value}>{children}</BeaconContext.Provider>;
}

/** Access the session (client + login/logout). */
export function useBeaconSession(): BeaconContextValue {
  const ctx = React.useContext(BeaconContext);
  if (!ctx) throw new Error("useBeaconSession must be used within <BeaconProvider>");
  return ctx;
}

/** Access the authenticated client. Throws if used outside an authenticated route. */
export function useBeacon(): BeaconClient {
  const { client } = useBeaconSession();
  if (!client) throw new Error("useBeacon called without an authenticated session");
  return client;
}
