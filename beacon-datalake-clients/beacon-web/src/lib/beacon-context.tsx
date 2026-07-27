/** React context exposing the authenticated BeaconClient and session actions. */

import * as React from "react";
import type { BeaconClient } from "@beacon/client";

import {
  type Connection,
  clearConnection,
  loadConnection,
  makeClient,
  saveConnection,
} from "./auth";

interface BeaconContextValue {
  /** The live, authenticated client, or null when logged out. */
  client: BeaconClient | null;
  /** The current connection (server URL + username), or null when logged out. */
  connection: Connection | null;
  /** Establishes a verified session (called by the Login page after admin.check succeeds). */
  login: (conn: Connection) => void;
  /** Clears the session and persisted credentials. */
  logout: () => void;
}

const BeaconContext = React.createContext<BeaconContextValue | null>(null);

export function BeaconProvider({ children }: { children: React.ReactNode }) {
  const [connection, setConnection] = React.useState<Connection | null>(() => loadConnection());
  const [client, setClient] = React.useState<BeaconClient | null>(() => {
    const conn = loadConnection();
    return conn ? makeClient(conn) : null;
  });

  const login = React.useCallback((conn: Connection) => {
    saveConnection(conn);
    setConnection(conn);
    setClient(makeClient(conn));
  }, []);

  const logout = React.useCallback(() => {
    clearConnection();
    setConnection(null);
    setClient(null);
  }, []);

  const value = React.useMemo(
    () => ({ client, connection, login, logout }),
    [client, connection, login, logout],
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
