import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import { useSearchParams } from "react-router";

import type { SegmentedOption } from "@/components/ui/segmented";

/** Selectable model providers, in display order. `"all"` means unfiltered. */
export const PROVIDERS = ["all", "codex", "claude"] as const;
export type Provider = (typeof PROVIDERS)[number];

export const PROVIDER_OPTIONS: SegmentedOption[] = [
  { value: "all", label: "All" },
  { value: "codex", label: "codex" },
  { value: "claude", label: "claude" },
];

/** Coerce any persisted/URL value to a known provider, defaulting to `"all"`. */
export function normalizeProvider(value: string | null | undefined): Provider {
  return PROVIDERS.includes(value as Provider) ? (value as Provider) : "all";
}

/** The Teams filter chip summary: "All" when empty, the keys when one or two
 *  are picked, else a "N selected" count. */
export function teamFilterSummary(selected: string[]): string {
  if (selected.length === 0) return "All";
  if (selected.length <= 2) return selected.join(", ");
  return `${selected.length} selected`;
}

/** Date-window presets, in display order. `"12mo"` is all-time (the default,
 *  emits no URL param) — the others window the spend aggregates + issue lists. */
export const DATE_PRESETS = [
  "12mo",
  "90d",
  "30d",
  "7d",
  "yesterday",
  "today",
] as const;
export type DatePreset = (typeof DATE_PRESETS)[number];

/** The resolved date filter: a named preset, or a custom UTC-day range. */
export type DateFilter =
  | { kind: "preset"; preset: DatePreset }
  | { kind: "custom"; from: string; to: string };

/** All-time — the default; contributes no URL params. */
export const DEFAULT_DATE: DateFilter = { kind: "preset", preset: "12mo" };

const DAY_RE = /^\d{4}-\d{2}-\d{2}$/;

function isDatePreset(value: string | null | undefined): value is DatePreset {
  return DATE_PRESETS.includes(value as DatePreset);
}

/** Parse the `dates`/`from`/`to` params into a {@link DateFilter}, falling back
 *  to all-time on anything unrecognized or malformed. */
export function parseDate(params: URLSearchParams): DateFilter {
  const raw = params.get("dates");
  if (raw === "custom") {
    const from = params.get("from");
    const to = params.get("to");
    if (from && to && DAY_RE.test(from) && DAY_RE.test(to) && from <= to) {
      return { kind: "custom", from, to };
    }
    return DEFAULT_DATE;
  }
  if (isDatePreset(raw)) return { kind: "preset", preset: raw };
  return DEFAULT_DATE;
}

/** Set the `dates`/`from`/`to` params for a date filter, omitting all of them
 *  at the all-time default. */
function serializeDate(date: DateFilter, params: URLSearchParams): void {
  if (date.kind === "custom") {
    params.set("dates", "custom");
    params.set("from", date.from);
    params.set("to", date.to);
    return;
  }
  if (date.preset !== "12mo") params.set("dates", date.preset);
}

function utcDayMinus(nowMs: number, days: number): string {
  const d = new Date(nowMs);
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

/** Resolve a date filter to inclusive UTC-day bounds (`YYYY-MM-DD`), or null
 *  bounds for all-time. Relative presets are anchored to `nowMs`. */
export function resolveDateWindow(
  date: DateFilter,
  nowMs: number,
): { from: string | null; to: string | null } {
  if (date.kind === "custom") return { from: date.from, to: date.to };
  const today = new Date(nowMs).toISOString().slice(0, 10);
  switch (date.preset) {
    case "12mo":
      return { from: null, to: null };
    case "90d":
      return { from: utcDayMinus(nowMs, 89), to: today };
    case "30d":
      return { from: utcDayMinus(nowMs, 29), to: today };
    case "7d":
      return { from: utcDayMinus(nowMs, 6), to: today };
    case "yesterday": {
      const y = utcDayMinus(nowMs, 1);
      return { from: y, to: y };
    }
    case "today":
      return { from: today, to: today };
  }
}

const DATE_WINDOW_LABELS: Record<DatePreset, string> = {
  "12mo": "all-time",
  "90d": "last 90 days",
  "30d": "last 30 days",
  "7d": "last 7 days",
  yesterday: "yesterday",
  today: "today",
};

/** Human label for the stat-rail header — e.g. `last 7 days`, `custom range`. */
export function dateWindowLabel(date: DateFilter): string {
  return date.kind === "custom" ? "custom range" : DATE_WINDOW_LABELS[date.preset];
}

const DATE_TRIGGER_LABELS: Record<DatePreset, string> = {
  "12mo": "12 months",
  "90d": "90 days",
  "30d": "30 days",
  "7d": "7 days",
  yesterday: "Yesterday",
  today: "Today",
};

/** Short label for the filter-chip trigger. */
export function dateTriggerLabel(date: DateFilter): string {
  return date.kind === "custom"
    ? `${date.from} → ${date.to}`
    : DATE_TRIGGER_LABELS[date.preset];
}

/** Whether the date filter is at its all-time default. */
export function isDefaultDate(date: DateFilter): boolean {
  return date.kind === "preset" && date.preset === "12mo";
}

/** Strip the `provider:` prefix off a qualified model for display. */
function modelLabel(qualified: string): string {
  const idx = qualified.indexOf(":");
  return idx === -1 ? qualified : qualified.slice(idx + 1);
}

/** The Models filter chip summary: "All" when empty, bare model names when one
 *  or two are picked, else a "N selected" count. Models are provider-qualified
 *  (`provider:model`); the chip shows just the model part. */
export function modelFilterSummary(selected: string[]): string {
  if (selected.length === 0) return "All";
  if (selected.length <= 2) return selected.map(modelLabel).join(", ");
  return `${selected.length} selected`;
}

/** Drop selected models that don't belong to the active provider. Under "all"
 *  every model is kept; otherwise only `${provider}:…` qualified models survive.
 *  This is the provider→model dependency: switching provider prunes the rest. */
export function pruneModelsForProvider(
  models: string[],
  provider: Provider,
): string[] {
  if (provider === "all") return models;
  return models.filter((m) => m.split(":")[0] === provider);
}

/** The single source of truth for every dashboard filter. */
export type Filters = {
  teams: string[];
  provider: Provider;
  models: string[];
  /** The date window; URL-only, never persisted. All-time by default. */
  date: DateFilter;
};

export const DEFAULT_FILTERS: Filters = {
  teams: [],
  provider: "all",
  models: [],
  date: DEFAULT_DATE,
};

/** localStorage blob key. Holds teams/provider/models — NOT date. */
export const FILTERS_STORAGE_KEY = "sym-filters";

/** The URL params this store owns (so a writer can clear them wholesale). */
const FILTER_PARAM_KEYS = [
  "teams",
  "provider",
  "models",
  "dates",
  "from",
  "to",
] as const;

function parseList(value: string | null): string[] {
  if (!value) return [];
  return value.split(",").map((s) => s.trim()).filter(Boolean);
}

/** Serialize filters to URL params with omit-at-default semantics: a filter at
 *  its default contributes no param. */
export function serializeFilters(filters: Filters): URLSearchParams {
  const params = new URLSearchParams();
  if (filters.teams.length) params.set("teams", filters.teams.join(","));
  if (filters.provider !== "all") params.set("provider", filters.provider);
  if (filters.models.length) params.set("models", filters.models.join(","));
  serializeDate(filters.date, params);
  return params;
}

/** The single URL writer's merge step: clear the keys this store owns, re-set
 *  only the non-default ones (via {@link serializeFilters}), and preserve any
 *  unrelated params already on the URL. */
export function mergeFiltersIntoParams(
  prev: URLSearchParams,
  filters: Filters,
): URLSearchParams {
  const next = new URLSearchParams(prev);
  for (const key of FILTER_PARAM_KEYS) next.delete(key);
  for (const [key, value] of serializeFilters(filters)) next.set(key, value);
  return next;
}

/** Parse filters from URL params, normalizing and falling back to defaults. */
export function parseFilters(params: URLSearchParams): Filters {
  return {
    teams: parseList(params.get("teams")),
    provider: normalizeProvider(params.get("provider")),
    models: parseList(params.get("models")),
    date: parseDate(params),
  };
}

/** JSON blob persisted to localStorage — teams/provider/models only. */
export function serializePersisted(filters: Filters): string {
  return JSON.stringify({
    teams: filters.teams,
    provider: filters.provider,
    models: filters.models,
  });
}

type StoredFilters = Partial<Pick<Filters, "teams" | "models">> & {
  /** Raw stored provider; normalized via `normalizeProvider` on read. */
  provider?: string;
};

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((v) => typeof v === "string");
}

function parseStored(raw: string | null): StoredFilters {
  if (!raw) return {};
  try {
    const blob = JSON.parse(raw);
    if (typeof blob !== "object" || blob === null) return {};
    // Validate per field so a corrupt value (e.g. `teams: "VIB"`) falls through
    // to defaults instead of poisoning state with a non-array.
    return {
      teams: isStringArray(blob.teams) ? blob.teams : undefined,
      provider: typeof blob.provider === "string" ? blob.provider : undefined,
      models: isStringArray(blob.models) ? blob.models : undefined,
    };
  } catch {
    return {};
  }
}

/** Resolve the initial filter set with precedence URL > localStorage > defaults,
 *  applied per field. `date` is URL-only — it never reads from storage. */
export function resolveInitialFilters({
  params,
  stored,
}: {
  params: URLSearchParams;
  stored: string | null;
}): Filters {
  const blob = parseStored(stored);
  const provider = params.has("provider")
    ? normalizeProvider(params.get("provider"))
    : normalizeProvider(blob.provider ?? DEFAULT_FILTERS.provider);
  const models = params.has("models")
    ? parseList(params.get("models"))
    : (blob.models ?? DEFAULT_FILTERS.models);
  return {
    teams: params.has("teams")
      ? parseList(params.get("teams"))
      : (blob.teams ?? DEFAULT_FILTERS.teams),
    provider,
    // Provider→model dependency on load: a URL provider crossed with stored
    // models (or vice versa) can be inconsistent; prune so resolved state and
    // the mirrored URL never carry models the provider-scoped popover omits.
    models: pruneModelsForProvider(models, provider),
    date: parseDate(params),
  };
}

type FiltersContextValue = Filters & {
  setProvider: (value: string) => void;
  setTeams: (value: string[]) => void;
  setModels: (value: string[]) => void;
  setDate: (value: DateFilter) => void;
};

const FiltersContext = createContext<FiltersContextValue | null>(null);

/**
 * Holds every dashboard filter as one source of truth. The URL wins on load
 * (so a copied link reproduces the view), then localStorage, then defaults.
 * A single writer mirrors state back to both — only non-default filters reach
 * the URL, and date is never persisted.
 */
export function FiltersProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams();

  const [filters, setFilters] = useState<Filters>(() =>
    resolveInitialFilters({
      params: searchParams,
      stored:
        typeof localStorage !== "undefined"
          ? localStorage.getItem(FILTERS_STORAGE_KEY)
          : null,
    }),
  );

  // The one and only writer: state → localStorage + URL (omit-at-default).
  useEffect(() => {
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(FILTERS_STORAGE_KEY, serializePersisted(filters));
    }
    setSearchParams((prev) => mergeFiltersIntoParams(prev, filters), {
      replace: true,
    });
  }, [filters, setSearchParams]);

  const setProvider = useCallback(
    (value: string) =>
      setFilters((f) => {
        const provider = normalizeProvider(value);
        // Provider→model dependency: prune selections that no longer belong.
        return { ...f, provider, models: pruneModelsForProvider(f.models, provider) };
      }),
    [],
  );
  const setTeams = useCallback(
    (value: string[]) => setFilters((f) => ({ ...f, teams: value })),
    [],
  );
  const setModels = useCallback(
    (value: string[]) => setFilters((f) => ({ ...f, models: value })),
    [],
  );
  const setDate = useCallback(
    (value: DateFilter) => setFilters((f) => ({ ...f, date: value })),
    [],
  );

  const value = useMemo<FiltersContextValue>(
    () => ({ ...filters, setProvider, setTeams, setModels, setDate }),
    [filters, setProvider, setTeams, setModels, setDate],
  );

  return <FiltersContext.Provider value={value}>{children}</FiltersContext.Provider>;
}

export function useFilters(): FiltersContextValue {
  const ctx = useContext(FiltersContext);
  if (!ctx) {
    throw new Error("useFilters must be used within a FiltersProvider");
  }
  return ctx;
}
