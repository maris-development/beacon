/** TanStack Query provider that logs out on any 401 (session expired). */

import * as React from "react";
import { QueryCache, QueryClient, QueryClientProvider } from "@tanstack/react-query";

import { useBeaconSession } from "./beacon-context";
import { isUnauthorized } from "./errors";

export function AppQueryProvider({ children }: { children: React.ReactNode }) {
  const { logout } = useBeaconSession();

  const queryClient = React.useMemo(
    () =>
      new QueryClient({
        queryCache: new QueryCache({
          onError: (err) => {
            if (isUnauthorized(err)) logout();
          },
        }),
        defaultOptions: {
          queries: {
            retry: false,
            refetchOnWindowFocus: false,
            staleTime: 15_000,
          },
        },
      }),
    [logout],
  );

  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
}
