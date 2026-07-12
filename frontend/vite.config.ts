import react from "@vitejs/plugin-react";
import path from "node:path";
import { defineConfig, type HtmlTagDescriptor, type Plugin } from "vite";

/** Preconnect <link> hints for the build-time Auth0 domain so the browser can
 *  open the TLS connection while the bundle downloads. Data-independent: the
 *  runtime /api/auth-config may point elsewhere, in which case this is a
 *  harmless unused hint. Empty when the domain wasn't set at build time. */
export function auth0PreconnectTags(domain?: string): HtmlTagDescriptor[] {
  if (!domain) return [];
  return [
    {
      tag: "link",
      attrs: { rel: "preconnect", href: `https://${domain}`, crossorigin: true },
      injectTo: "head-prepend",
    },
  ];
}

function auth0Preconnect(): Plugin {
  let domain: string | undefined;
  return {
    name: "auth0-preconnect",
    configResolved(resolved) {
      domain = resolved.env.VITE_AUTH0_DOMAIN;
    },
    transformIndexHtml() {
      return auth0PreconnectTags(domain);
    },
  };
}

export default defineConfig({
  base: "/ui/",
  plugins: [react(), auth0Preconnect()],
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
