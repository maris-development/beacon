import { Navigate, Route, Routes } from "react-router-dom";

import { AppShell } from "./components/app-shell";
import { RequireAuth } from "./lib/require-auth";
import { LoginPage } from "./pages/login";
import { WorkbenchPage } from "./pages/workbench";
import { TablesPage } from "./pages/tables";
import { DatasetsPage } from "./pages/datasets";
import { CrawlersPage } from "./pages/crawlers";
import { ServerInfoPage } from "./pages/server-info";

export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<RequireAuth />}>
        <Route element={<AppShell />}>
          <Route index element={<Navigate to="/query" replace />} />
          <Route path="/query" element={<WorkbenchPage />} />
          <Route path="/tables" element={<TablesPage />} />
          <Route path="/datasets" element={<DatasetsPage />} />
          <Route path="/crawlers" element={<CrawlersPage />} />
          <Route path="/server" element={<ServerInfoPage />} />
        </Route>
      </Route>
      <Route path="*" element={<Navigate to="/query" replace />} />
    </Routes>
  );
}
