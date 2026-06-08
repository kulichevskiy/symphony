import { FilterTrigger, Popover } from "@/components/dashboard/Popover";
import { teamFilterSummary, useFilters } from "@/lib/filters";
import { cn } from "@/lib/utils";

/** Multi-select Teams filter. Options come from the always-unscoped `teams`
 *  list (config bindings); selection lives in the URL-backed filter store and
 *  rescopes every panel server-side. */
export function TeamFilter({ teams }: { teams: string[] }) {
  const { teams: selected, setTeams } = useFilters();
  const active = selected.length > 0;

  const toggle = (key: string) => {
    setTeams(
      selected.includes(key)
        ? selected.filter((k) => k !== key)
        : [...selected, key],
    );
  };

  return (
    <Popover
      trigger={({ open, toggle: toggleOpen }) => (
        <FilterTrigger
          label="Teams"
          value={teamFilterSummary(selected)}
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
            onClick={() => setTeams([])}
            className="mb-1 px-2 py-1 text-left text-xs text-muted-foreground hover:text-foreground"
          >
            Clear
          </button>
        ) : null}
        {teams.length === 0 ? (
          <span className="px-2 py-1 text-xs text-muted-foreground">
            No teams
          </span>
        ) : (
          teams.map((key) => {
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
                {key}
              </label>
            );
          })
        )}
      </div>
    </Popover>
  );
}
