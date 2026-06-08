import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useState,
  type ReactNode,
} from "react";
import { useSearchParams } from "react-router";

import type { SegmentedOption } from "@/components/ui/segmented";

const STORAGE_KEY = "sym-provider";

/** Selectable model providers, in header display order. `"all"` means unfiltered. */
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

type ProviderFilterContextValue = {
  provider: Provider;
  setProvider: (value: string) => void;
};

const ProviderFilterContext = createContext<ProviderFilterContextValue | null>(null);

/**
 * Holds the global model-provider filter. The URL `?provider=` query param wins
 * on load (so a copied link reproduces the filtered view), then `localStorage`,
 * then `"all"`. Changes are mirrored back to both.
 */
export function ProviderFilterProvider({ children }: { children: ReactNode }) {
  const [searchParams, setSearchParams] = useSearchParams();

  const [provider, setProviderState] = useState<Provider>(() => {
    const fromUrl = searchParams.get("provider");
    if (fromUrl != null) {
      return normalizeProvider(fromUrl);
    }
    if (typeof localStorage !== "undefined") {
      return normalizeProvider(localStorage.getItem(STORAGE_KEY));
    }
    return "all";
  });

  useEffect(() => {
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(STORAGE_KEY, provider);
    }
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("provider", provider);
        return next;
      },
      { replace: true },
    );
  }, [provider, setSearchParams]);

  const setProvider = useCallback(
    (value: string) => setProviderState(normalizeProvider(value)),
    [],
  );

  return (
    <ProviderFilterContext.Provider value={{ provider, setProvider }}>
      {children}
    </ProviderFilterContext.Provider>
  );
}

export function useProviderFilter(): ProviderFilterContextValue {
  const ctx = useContext(ProviderFilterContext);
  if (!ctx) {
    throw new Error("useProviderFilter must be used within a ProviderFilterProvider");
  }
  return ctx;
}
