import { QueryClientProvider } from "@tanstack/react-query";
import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router";

import { App } from "@/App";
import { initAuth } from "@/lib/auth";
import { queryClient } from "@/lib/queryClient";

import "./index.css";

// Must resolve (and, if Auth0 is enabled, log in) before the app renders —
// otherwise its first `/api/*` calls hit the gate with no bearer token.
void initAuth().then(() => {
  ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
    <React.StrictMode>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename="/ui">
          <App />
        </BrowserRouter>
      </QueryClientProvider>
    </React.StrictMode>,
  );
});
