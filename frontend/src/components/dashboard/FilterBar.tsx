import { Segmented } from "@/components/ui/segmented";
import { PROVIDER_OPTIONS, useFilters } from "@/lib/filters";

/** The global filter bar. Today it carries only the provider control (the
 *  tracer); later slices drop teams/models/date filters in beside it using the
 *  shared `Popover` + `FilterTrigger` primitives. */
export function FilterBar() {
  const { provider, setProvider } = useFilters();
  return (
    <div className="border-b border-border bg-background">
      <div className="mx-auto flex w-full max-w-[1200px] flex-wrap items-center gap-2 px-4 py-2 sm:px-6 lg:px-8">
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
