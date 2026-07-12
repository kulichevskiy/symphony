import react from "@vitejs/plugin-react";
import path from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
  base: "/ui/",
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": "http://127.0.0.1:8787",
    },
  },
  build: {
    rollupOptions: {
      output: {
        // Split rarely-changing vendor code from app code so deploys that only
        // touch app source don't invalidate the (large) vendor bundle.
        // Order matters: react-router / @auth0 / @tanstack all contain "react",
        // so match them before the react core runtime.
        manualChunks(id) {
          if (!id.includes("node_modules")) return undefined;
          if (/[\\/]react-router/.test(id)) return "vendor-router";
          if (/[\\/]@auth0[\\/]/.test(id)) return "vendor-auth";
          if (/[\\/]@tanstack[\\/]/.test(id)) return "vendor-query";
          if (/[\\/](react|react-dom|scheduler)[\\/]/.test(id))
            return "vendor-react";
          return "vendor";
        },
      },
    },
  },
});
