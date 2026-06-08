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

/** The single source of truth for every dashboard filter. */
export type Filters = {
  teams: string[];
  provider: Provider;
  models: string[];
  /** A completed-window token (e.g. `"7d"`); URL-only, never persisted. */
  date: string | null;
};

export const DEFAULT_FILTERS: Filters = {
  teams: [],
  provider: "all",
  models: [],
  date: null,
};

/** localStorage blob key. Holds teams/provider/models — NOT date. */
export const FILTERS_STORAGE_KEY = "sym-filters";

/** The URL params this store owns (so a writer can clear them wholesale). */
const FILTER_PARAM_KEYS = ["teams", "provider", "models", "date"] as const;

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
  if (filters.date != null) params.set("date", filters.date);
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
    date: params.get("date"),
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
  return {
    teams: params.has("teams")
      ? parseList(params.get("teams"))
      : (blob.teams ?? DEFAULT_FILTERS.teams),
    provider: params.has("provider")
      ? normalizeProvider(params.get("provider"))
      : normalizeProvider(blob.provider ?? DEFAULT_FILTERS.provider),
    models: params.has("models")
      ? parseList(params.get("models"))
      : (blob.models ?? DEFAULT_FILTERS.models),
    date: params.get("date"),
  };
}

type FiltersContextValue = Filters & {
  setProvider: (value: string) => void;
  setTeams: (value: string[]) => void;
  setModels: (value: string[]) => void;
  setDate: (value: string | null) => void;
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
      setFilters((f) => ({ ...f, provider: normalizeProvider(value) })),
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
    (value: string | null) => setFilters((f) => ({ ...f, date: value })),
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
