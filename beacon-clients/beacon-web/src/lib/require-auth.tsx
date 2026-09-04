/** Route guard: only render children when an authenticated session exists. */

import { Navigate, Outlet, useLocation } from "react-router-dom";

import { SessionSplash } from "@/components/session-splash";
import { useBeaconSession } from "./beacon-context";

export function RequireAuth() {
  const { client, initializing } = useBeaconSession();
  const location = useLocation();

  // Wait for the proxy probe; a redirect now would flash the login screen at a
  // caller that the gateway authenticates already.
  if (initializing) return <SessionSplash />;
  if (!client) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return <Outlet />;
}
