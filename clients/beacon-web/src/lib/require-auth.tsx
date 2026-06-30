/** Route guard: only render children when an authenticated session exists. */

import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useBeaconSession } from "./beacon-context";

export function RequireAuth() {
  const { client } = useBeaconSession();
  const location = useLocation();

  if (!client) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />;
  }
  return <Outlet />;
}
