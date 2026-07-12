import { describe, expect, it } from "vitest";

import config, { auth0PreconnectTags } from "./vite.config.ts";

// The config may be a function or an object depending on defineConfig usage;
// our config is a plain object.
type ChunkFn = (id: string) => string | undefined;

function manualChunks(): ChunkFn {
  const output = (config as any).build?.rollupOptions?.output;
  const fn = output?.manualChunks;
  expect(typeof fn).toBe("function");
  return fn as ChunkFn;
}

const NM = "/repo/frontend/node_modules";

describe("vite build.manualChunks", () => {
  it("splits vendor deps into a dedicated chunk separate from app code", () => {
    const chunk = manualChunks();

    // App source stays in the default entry chunk (undefined = default).
    expect(chunk("/repo/frontend/src/App.tsx")).toBeUndefined();
    expect(chunk("/repo/frontend/src/pages/HomePage.tsx")).toBeUndefined();
  });

  it("groups the required vendor libraries", () => {
    const chunk = manualChunks();

    // react-router must not be miscategorised as react (it contains "react").
    expect(chunk(`${NM}/react-router/dist/index.js`)).toMatch(/vendor/);
    expect(chunk(`${NM}/react-dom/client.js`)).toMatch(/vendor/);
    expect(chunk(`${NM}/react/index.js`)).toMatch(/vendor/);
    expect(chunk(`${NM}/scheduler/index.js`)).toMatch(/vendor/);
    expect(chunk(`${NM}/@auth0/auth0-react/dist/index.js`)).toMatch(/vendor/);
    expect(chunk(`${NM}/@tanstack/react-query/build/index.js`)).toMatch(
      /vendor/,
    );

    // react and react-router land in different chunks (react-router changes
    // independently of the core react runtime).
    expect(chunk(`${NM}/react/index.js`)).not.toBe(
      chunk(`${NM}/react-router/dist/index.js`),
    );
  });
});

describe("auth0PreconnectTags", () => {
  it("emits a preconnect link for a known build-time domain", () => {
    const tags = auth0PreconnectTags("example.us.auth0.com");
    expect(tags).toHaveLength(1);
    expect(tags[0]).toMatchObject({
      tag: "link",
      attrs: { rel: "preconnect", href: "https://example.us.auth0.com" },
      injectTo: "head",
    });
  });

  it("emits nothing when the domain is unknown", () => {
    expect(auth0PreconnectTags(undefined)).toEqual([]);
    expect(auth0PreconnectTags("")).toEqual([]);
  });
});
