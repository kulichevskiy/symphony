import { describe, expect, it } from "vitest";

import { normalizeProvider, PROVIDERS } from "./providerFilter";

describe("normalizeProvider", () => {
  it("keeps the known providers", () => {
    for (const value of PROVIDERS) {
      expect(normalizeProvider(value)).toBe(value);
    }
  });

  it("falls back to 'all' for an unknown value", () => {
    expect(normalizeProvider("gemini")).toBe("all");
    expect(normalizeProvider("")).toBe("all");
  });

  it("falls back to 'all' for an absent value", () => {
    expect(normalizeProvider(null)).toBe("all");
    expect(normalizeProvider(undefined)).toBe("all");
  });

  it("offers 'all', 'codex' and 'claude' in header order", () => {
    expect(PROVIDERS).toEqual(["all", "codex", "claude"]);
  });
});
