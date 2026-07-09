import { useQuery } from "@tanstack/react-query";

import { Card } from "@/components/ui/card";
import { type BindingView, type ConfigView, fetchConfigView } from "@/lib/api";

// Pipeline roles in dispatch order; the config view keys its `roles` map by
// these names.
const ROLE_ORDER = [
  "implement",
  "review_find",
  "review_verify",
  "fix",
  "accept",
] as const;

/** One binding's card: identity + concurrency cap + the resolved role matrix. */
function BindingCard({ binding }: { binding: BindingView }) {
  return (
    <Card className="p-5">
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="rounded bg-secondary px-1.5 py-0.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            {binding.provider}
          </span>
          <span className="font-mono text-sm font-semibold">
            {binding.project_key}
          </span>
          <span className="text-muted-foreground">→</span>
          <span className="font-mono text-sm">{binding.github_repo}</span>
        </div>
        <span className="font-mono text-xs text-muted-foreground">
          max concurrent · {binding.max_concurrent}
        </span>
      </div>
      <div className="overflow-x-auto rounded-md border border-border">
        <table className="w-full caption-bottom text-sm">
          <thead>
            <tr className="border-b border-border bg-secondary/40 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
              <th className="px-3 py-1.5 text-left font-medium">Role</th>
              <th className="px-3 py-1.5 text-left font-medium">Agent</th>
              <th className="px-3 py-1.5 text-left font-medium">Model</th>
              <th className="px-3 py-1.5 text-left font-medium">Effort</th>
            </tr>
          </thead>
          <tbody>
            {ROLE_ORDER.map((role) => {
              const r = binding.roles[role];
              if (!r) return null;
              return (
                <tr
                  key={role}
                  className="border-b border-border/70 last:border-0"
                >
                  <td className="px-3 py-2 font-mono text-xs">{role}</td>
                  <td className="px-3 py-2 text-xs">{r.agent}</td>
                  <td className="px-3 py-2 font-mono text-xs text-muted-foreground">
                    {r.model ?? "—"}
                  </td>
                  <td className="px-3 py-2 font-mono text-xs text-muted-foreground">
                    {r.effort ?? "—"}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Card>
  );
}

/** Pure presentation of the loaded config — no fetching. */
export function ConfigDetails({ config }: { config: ConfigView }) {
  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center gap-x-6 gap-y-1 font-mono text-xs text-muted-foreground">
        <span>global max concurrent · {config.global_max_concurrent}</span>
        <span>poll interval · {config.poll_interval_secs}s</span>
      </div>
      {config.bindings.length ? (
        config.bindings.map((b) => (
          <BindingCard key={`${b.project_key}/${b.github_repo}`} binding={b} />
        ))
      ) : (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          No bindings configured
        </div>
      )}
    </div>
  );
}

export function ConfigPage() {
  const { data, error, isLoading } = useQuery({
    queryKey: ["config"],
    queryFn: fetchConfigView,
    staleTime: Infinity,
  });

  return (
    <main className="mx-auto w-full max-w-[1200px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-5">
        <div className="flex flex-wrap items-center gap-2">
          <h1 className="text-xl font-semibold tracking-tight">Configuration</h1>
          <span className="rounded-md border border-border px-2 py-0.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
            Read-only
          </span>
        </div>
        <p className="mt-0.5 text-sm text-muted-foreground">
          Effective config as loaded at daemon startup. Secrets are omitted.
          Editing is not supported here.
        </p>
      </div>

      {isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : error ? (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          Failed to load config
        </div>
      ) : data ? (
        <ConfigDetails config={data} />
      ) : null}
    </main>
  );
}
