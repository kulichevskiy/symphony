import { useQuery } from "@tanstack/react-query";

import { ModelFilter } from "@/components/dashboard/ModelFilter";
import { TeamFilter } from "@/components/dashboard/TeamFilter";
import { Segmented } from "@/components/ui/segmented";
import { fetchSpendSummary } from "@/lib/api";
import { PROVIDER_OPTIONS, useFilters } from "@/lib/filters";

import { DateFilter } from "./DateFilter";

/** The global filter bar: the Teams and Models multi-selects, the Model provider
 *  control, and the date window. The Teams/Models options come from the
 *  always-unscoped `teams`/`models` lists on /spend/summary — fetched without
 *  filters so they never narrow themselves. */
export function FilterBar() {
  const { provider, setProvider } = useFilters();
  const optionsQuery = useQuery({
    queryKey: ["filter-options"],
    queryFn: () => fetchSpendSummary(),
    staleTime: 5 * 60_000,
  });
  return (
    <div className="border-b border-border bg-background">
      <div className="mx-auto flex w-full max-w-[1200px] flex-wrap items-center gap-2 px-4 py-2 sm:px-6 lg:px-8">
        <TeamFilter teams={optionsQuery.data?.teams ?? []} />
        <ModelFilter models={optionsQuery.data?.models ?? []} />
        <span className="flex items-center gap-1.5">
          <span className="hidden text-xs font-medium text-muted-foreground sm:inline">
            Model
          </span>
          <Segmented
            ariaLabel="Model provider"
            options={PROVIDER_OPTIONS}
            value={provider}
            onChange={setProvider}
          />
        </span>
        <DateFilter />
      </div>
    </div>
  );
}
