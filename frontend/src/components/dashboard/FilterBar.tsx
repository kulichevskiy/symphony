import { useQuery } from "@tanstack/react-query";

import { TeamFilter } from "@/components/dashboard/TeamFilter";
import { Segmented } from "@/components/ui/segmented";
import { fetchSpendSummary } from "@/lib/api";
import { PROVIDER_OPTIONS, useFilters } from "@/lib/filters";

/** The global filter bar: the Teams multi-select and the Model provider
 *  control. The Teams options come from the always-unscoped `teams` list on
 *  /spend/summary — fetched without filters so it never narrows itself. */
export function FilterBar() {
  const { provider, setProvider } = useFilters();
  const teamsQuery = useQuery({
    queryKey: ["filter-teams"],
    queryFn: () => fetchSpendSummary(),
    staleTime: 5 * 60_000,
    select: (s) => s.teams,
  });
  return (
    <div className="border-b border-border bg-background">
      <div className="mx-auto flex w-full max-w-[1200px] flex-wrap items-center gap-2 px-4 py-2 sm:px-6 lg:px-8">
        <TeamFilter teams={teamsQuery.data ?? []} />
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
      </div>
    </div>
  );
}
