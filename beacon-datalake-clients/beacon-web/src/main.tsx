import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";

import App from "./App";
import { BeaconProvider } from "./lib/beacon-context";
import { AppQueryProvider } from "./lib/query-provider";
import { ThemeProvider } from "./lib/theme";
import "./index.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <ThemeProvider>
      <BrowserRouter basename={import.meta.env.BASE_URL.replace(/\/$/, "") || "/"}>
        <BeaconProvider>
          <AppQueryProvider>
            <App />
          </AppQueryProvider>
        </BeaconProvider>
      </BrowserRouter>
    </ThemeProvider>
  </React.StrictMode>,
);
