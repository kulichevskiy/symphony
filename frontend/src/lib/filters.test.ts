import { describe, expect, it } from "vitest";

import {
  DEFAULT_DATE,
  DEFAULT_FILTERS,
  dateTriggerLabel,
  dateWindowLabel,
  isDefaultDate,
  mergeFiltersIntoParams,
  normalizeProvider,
  parseDate,
  parseFilters,
  PROVIDERS,
  resolveDateWindow,
  resolveInitialFilters,
  serializeFilters,
  serializePersisted,
  teamFilterSummary,
  type DateFilter,
  type Filters,
} from "./filters";

describe("teamFilterSummary", () => {
  it("reads 'All' when nothing is selected", () => {
    expect(teamFilterSummary([])).toBe("All");
  });

  it("lists the keys when one or two are selected", () => {
    expect(teamFilterSummary(["VIB"])).toBe("VIB");
    expect(teamFilterSummary(["VIB", "ADJ"])).toBe("VIB, ADJ");
  });

  it("collapses to a count beyond two", () => {
    expect(teamFilterSummary(["VIB", "ADJ", "SYM"])).toBe("3 selected");
  });
});

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

  it("emits dates only off the 12mo default, and from/to for a custom range", () => {
    const preset = serializeFilters({
      ...DEFAULT_FILTERS,
      date: { kind: "preset", preset: "30d" },
    });
    expect(preset.get("dates")).toBe("30d");

    // 12mo is the all-time default — it emits nothing.
    expect(serializeFilters(DEFAULT_FILTERS).has("dates")).toBe(false);

    const custom = serializeFilters({
      ...DEFAULT_FILTERS,
      date: { kind: "custom", from: "2026-01-01", to: "2026-03-01" },
    });
    expect(custom.get("dates")).toBe("custom");
    expect(custom.get("from")).toBe("2026-01-01");
    expect(custom.get("to")).toBe("2026-03-01");
  });
});

describe("parseDate", () => {
  it("reads a preset, defaulting to all-time when absent or unknown", () => {
    expect(parseDate(new URLSearchParams("dates=7d"))).toEqual({
      kind: "preset",
      preset: "7d",
    });
    expect(parseDate(new URLSearchParams())).toEqual(DEFAULT_DATE);
    expect(parseDate(new URLSearchParams("dates=eternity"))).toEqual(DEFAULT_DATE);
  });

  it("reads a custom range and round-trips it", () => {
    const date: DateFilter = { kind: "custom", from: "2026-01-01", to: "2026-03-01" };
    const params = serializeFilters({ ...DEFAULT_FILTERS, date });
    expect(parseDate(params)).toEqual(date);
  });

  it("falls back to all-time on a malformed or inverted custom range", () => {
    expect(parseDate(new URLSearchParams("dates=custom&from=2026-01-01"))).toEqual(
      DEFAULT_DATE,
    );
    expect(parseDate(new URLSearchParams("dates=custom&from=nope&to=2026-03-01"))).toEqual(
      DEFAULT_DATE,
    );
    // to before from is rejected.
    expect(
      parseDate(new URLSearchParams("dates=custom&from=2026-03-01&to=2026-01-01")),
    ).toEqual(DEFAULT_DATE);
  });
});

describe("resolveDateWindow", () => {
  // 2026-05-17T12:00:00Z
  const NOW = Date.UTC(2026, 4, 17, 12, 0, 0);

  it("returns open bounds for all-time (12mo)", () => {
    expect(resolveDateWindow(DEFAULT_DATE, NOW)).toEqual({ from: null, to: null });
  });

  it("anchors relative presets to the UTC day of now", () => {
    expect(resolveDateWindow({ kind: "preset", preset: "today" }, NOW)).toEqual({
      from: "2026-05-17",
      to: "2026-05-17",
    });
    expect(resolveDateWindow({ kind: "preset", preset: "yesterday" }, NOW)).toEqual({
      from: "2026-05-16",
      to: "2026-05-16",
    });
    expect(resolveDateWindow({ kind: "preset", preset: "7d" }, NOW)).toEqual({
      from: "2026-05-11",
      to: "2026-05-17",
    });
    expect(resolveDateWindow({ kind: "preset", preset: "30d" }, NOW)).toEqual({
      from: "2026-04-18",
      to: "2026-05-17",
    });
  });

  it("passes a custom range straight through", () => {
    expect(
      resolveDateWindow({ kind: "custom", from: "2026-01-01", to: "2026-02-01" }, NOW),
    ).toEqual({ from: "2026-01-01", to: "2026-02-01" });
  });
});

describe("date labels", () => {
  it("describes the window for the stat-rail header", () => {
    expect(dateWindowLabel(DEFAULT_DATE)).toBe("all-time");
    expect(dateWindowLabel({ kind: "preset", preset: "7d" })).toBe("last 7 days");
    expect(dateWindowLabel({ kind: "custom", from: "a", to: "b" })).toBe("custom range");
  });

  it("knows when the filter is at its default", () => {
    expect(isDefaultDate(DEFAULT_DATE)).toBe(true);
    expect(isDefaultDate({ kind: "preset", preset: "7d" })).toBe(false);
    expect(isDefaultDate({ kind: "custom", from: "a", to: "b" })).toBe(false);
  });

  it("renders a short trigger label", () => {
    expect(dateTriggerLabel({ kind: "preset", preset: "7d" })).toBe("7 days");
    expect(dateTriggerLabel({ kind: "custom", from: "2026-01-01", to: "2026-02-01" })).toBe(
      "2026-01-01 → 2026-02-01",
    );
  });
});

describe("mergeFiltersIntoParams (the single URL writer)", () => {
  it("sets provider=codex, omits it on reset to 'all', and preserves unrelated params", () => {
    const base = new URLSearchParams("tab=done");

    // Non-default provider lands on the URL; the unrelated `tab` survives.
    const withCodex = mergeFiltersIntoParams(base, {
      ...DEFAULT_FILTERS,
      provider: "codex",
    });
    expect(withCodex.get("provider")).toBe("codex");
    expect(withCodex.get("tab")).toBe("done");

    // Resetting to the default drops the param again, still preserving `tab`.
    const reset = mergeFiltersIntoParams(withCodex, DEFAULT_FILTERS);
    expect(reset.has("provider")).toBe(false);
    expect(reset.get("tab")).toBe("done");
  });

  it("clears stale owned keys rather than leaving them on the URL", () => {
    const prev = new URLSearchParams("provider=claude&teams=VIB&keep=1");
    const next = mergeFiltersIntoParams(prev, DEFAULT_FILTERS);
    expect(next.has("provider")).toBe(false);
    expect(next.has("teams")).toBe(false);
    expect(next.get("keep")).toBe("1");
  });
});

describe("parseFilters", () => {
  it("round-trips a fully-populated filter set", () => {
    const filters: Filters = {
      teams: ["VIB", "ADJ"],
      provider: "codex",
      models: ["opus-4.1"],
      date: { kind: "preset", preset: "7d" },
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
        date: { kind: "preset", preset: "7d" },
      }),
    );
    expect(blob).toEqual({ teams: ["VIB"], provider: "codex", models: ["opus-4.1"] });
    expect(blob).not.toHaveProperty("date");
    expect(blob).not.toHaveProperty("dates");
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
      stored: JSON.stringify({ dates: "30d" }),
    });
    expect(fromStore.date).toEqual(DEFAULT_DATE);
    const fromUrl = resolveInitialFilters({
      params: new URLSearchParams("dates=30d"),
      stored: null,
    });
    expect(fromUrl.date).toEqual({ kind: "preset", preset: "30d" });
  });

  it("tolerates corrupt stored JSON", () => {
    expect(
      resolveInitialFilters({ params: new URLSearchParams(), stored: "{not json" }),
    ).toEqual(DEFAULT_FILTERS);
  });

  it("ignores stored fields of the wrong type (e.g. teams as a string)", () => {
    // A blob like {"teams":"VIB"} must not leak a string into `teams` — else the
    // writer's `filters.teams.join` would throw and white-screen the app.
    const resolved = resolveInitialFilters({
      params: new URLSearchParams(),
      stored: JSON.stringify({ teams: "VIB", models: 42, provider: "codex" }),
    });
    expect(resolved.teams).toEqual([]);
    expect(resolved.models).toEqual([]);
    expect(resolved.provider).toBe("codex"); // valid string still honored
  });
});
