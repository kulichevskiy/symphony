import { describe, expect, it } from "vitest";

import {
  DEFAULT_FILTERS,
  normalizeProvider,
  parseFilters,
  PROVIDERS,
  resolveInitialFilters,
  serializeFilters,
  serializePersisted,
  type Filters,
} from "./filters";

describe("normalizeProvider", () => {
  it("keeps the known providers", () => {
    for (const value of PROVIDERS) {
      expect(normalizeProvider(value)).toBe(value);
    }
  });

  it("falls back to 'all' for unknown / absent values", () => {
    expect(normalizeProvider("gemini")).toBe("all");
    expect(normalizeProvider("")).toBe("all");
    expect(normalizeProvider(null)).toBe("all");
    expect(normalizeProvider(undefined)).toBe("all");
  });
});

describe("serializeFilters (omit-at-default)", () => {
  it("emits no params when every filter is at its default", () => {
    expect(serializeFilters(DEFAULT_FILTERS).toString()).toBe("");
  });

  it("emits provider only when not 'all'", () => {
    expect(serializeFilters({ ...DEFAULT_FILTERS, provider: "codex" }).get("provider")).toBe(
      "codex",
    );
    expect(serializeFilters({ ...DEFAULT_FILTERS, provider: "all" }).has("provider")).toBe(
      false,
    );
  });

  it("emits non-empty arrays as comma lists and omits empty ones", () => {
    const params = serializeFilters({
      ...DEFAULT_FILTERS,
      teams: ["VIB", "ADJ"],
      models: [],
    });
    expect(params.get("teams")).toBe("VIB,ADJ");
    expect(params.has("models")).toBe(false);
  });

  it("emits date only when set", () => {
    expect(serializeFilters({ ...DEFAULT_FILTERS, date: "30d" }).get("date")).toBe("30d");
    expect(serializeFilters(DEFAULT_FILTERS).has("date")).toBe(false);
  });
});

describe("parseFilters", () => {
  it("round-trips a fully-populated filter set", () => {
    const filters: Filters = {
      teams: ["VIB", "ADJ"],
      provider: "codex",
      models: ["opus-4.1"],
      date: "7d",
    };
    expect(parseFilters(serializeFilters(filters))).toEqual(filters);
  });

  it("yields defaults from an empty query string", () => {
    expect(parseFilters(new URLSearchParams())).toEqual(DEFAULT_FILTERS);
  });

  it("normalizes an unknown provider back to 'all'", () => {
    expect(parseFilters(new URLSearchParams("provider=gemini")).provider).toBe("all");
  });
});

describe("serializePersisted", () => {
  it("persists teams/provider/models but never date", () => {
    const blob = JSON.parse(
      serializePersisted({
        teams: ["VIB"],
        provider: "codex",
        models: ["opus-4.1"],
        date: "7d",
      }),
    );
    expect(blob).toEqual({ teams: ["VIB"], provider: "codex", models: ["opus-4.1"] });
    expect(blob).not.toHaveProperty("date");
  });
});

describe("resolveInitialFilters (URL wins, then localStorage, then defaults)", () => {
  it("prefers the URL over stored values per field", () => {
    const resolved = resolveInitialFilters({
      params: new URLSearchParams("provider=codex"),
      stored: JSON.stringify({ provider: "claude", teams: ["VIB"] }),
    });
    expect(resolved.provider).toBe("codex"); // URL wins
    expect(resolved.teams).toEqual(["VIB"]); // falls through to stored
  });

  it("falls back to localStorage when the URL is empty", () => {
    const resolved = resolveInitialFilters({
      params: new URLSearchParams(),
      stored: JSON.stringify({ provider: "claude", models: ["opus-4.1"] }),
    });
    expect(resolved.provider).toBe("claude");
    expect(resolved.models).toEqual(["opus-4.1"]);
  });

  it("falls back to defaults with no URL and no storage", () => {
    expect(
      resolveInitialFilters({ params: new URLSearchParams(), stored: null }),
    ).toEqual(DEFAULT_FILTERS);
  });

  it("never reads date from storage — only the URL", () => {
    const fromStore = resolveInitialFilters({
      params: new URLSearchParams(),
      stored: JSON.stringify({ date: "30d" }),
    });
    expect(fromStore.date).toBeNull();
    const fromUrl = resolveInitialFilters({
      params: new URLSearchParams("date=30d"),
      stored: null,
    });
    expect(fromUrl.date).toBe("30d");
  });

  it("tolerates corrupt stored JSON", () => {
    expect(
      resolveInitialFilters({ params: new URLSearchParams(), stored: "{not json" }),
    ).toEqual(DEFAULT_FILTERS);
  });
});
