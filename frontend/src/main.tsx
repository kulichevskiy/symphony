import { QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router";

import { App } from "@/App";
import { AuthProvider } from "@/lib/auth0";
import { queryClient } from "@/lib/queryClient";

import "./index.css";

// AuthProvider gates the tree: unauthenticated users are sent to Auth0 login
// and every `/api/*` call carries the ID token (via authHeaders). It sits
// under QueryClientProvider because its allowlist probe uses React Query.
ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <QueryClientProvider client={queryClient}>
      <AuthProvider>
        <BrowserRouter basename="/ui">
          <App />
        </BrowserRouter>
      </AuthProvider>
    </QueryClientProvider>
  </React.StrictMode>,
);
