import { FilterTrigger, Popover } from "@/components/dashboard/Popover";
import type { ModelRef } from "@/lib/api";
import { modelFilterSummary, useFilters } from "@/lib/filters";
import { cn } from "@/lib/utils";

/** A provider-qualified model option key, matching the URL/state form. */
function modelKey(m: ModelRef): string {
  return `${m.provider}:${m.model}`;
}

/** Multi-select Models filter, scoped by the active provider. Options come from
 *  the always-unscoped `models` list on /spend/summary; selection lives in the
 *  URL-backed filter store and rescopes every panel server-side. Under
 *  `provider=all` options are grouped by provider; under a specific provider
 *  only that provider's models are offered. */
export function ModelFilter({ models }: { models: ModelRef[] }) {
  const { provider, models: selected, setModels } = useFilters();
  const active = selected.length > 0;

  const toggle = (key: string) => {
    setModels(
      selected.includes(key)
        ? selected.filter((k) => k !== key)
        : [...selected, key],
    );
  };

  // Scope to the active provider, then group by provider (one group under a
  // specific provider, several under "all"), preserving the server's order.
  const visible =
    provider === "all" ? models : models.filter((m) => m.provider === provider);
  const groups = new Map<string, ModelRef[]>();
  for (const m of visible) {
    const bucket = groups.get(m.provider);
    if (bucket) bucket.push(m);
    else groups.set(m.provider, [m]);
  }

  return (
    <Popover
      trigger={({ open, toggle: toggleOpen }) => (
        <FilterTrigger
          label="Models"
          value={modelFilterSummary(selected)}
          active={active}
          open={open}
          onClick={toggleOpen}
        />
      )}
    >
      <div className="flex flex-col">
        {active ? (
          <button
            type="button"
            onClick={() => setModels([])}
            className="mb-1 px-2 py-1 text-left text-xs text-muted-foreground hover:text-foreground"
          >
            Clear
          </button>
        ) : null}
        {visible.length === 0 ? (
          <span className="px-2 py-1 text-xs text-muted-foreground">No models</span>
        ) : (
          [...groups.entries()].map(([groupProvider, groupModels]) => (
            <div key={groupProvider} className="flex flex-col">
              {provider === "all" ? (
                <span className="px-2 pb-0.5 pt-1.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
                  {groupProvider}
                </span>
              ) : null}
              {groupModels.map((m) => {
                const key = modelKey(m);
                const checked = selected.includes(key);
                return (
                  <label
                    key={key}
                    className={cn(
                      "flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-sm hover:bg-secondary/60",
                      checked ? "text-foreground" : "text-muted-foreground",
                    )}
                  >
                    <input
                      type="checkbox"
                      checked={checked}
                      onChange={() => toggle(key)}
                      className="h-3.5 w-3.5"
                    />
                    {m.model}
                  </label>
                );
              })}
            </div>
          ))
        )}
      </div>
    </Popover>
  );
}
