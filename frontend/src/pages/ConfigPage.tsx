import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useMemo, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import {
  type BindingRecord,
  type BindingWrite,
  ConfigWriteError,
  type ConfigOptions,
  type ConfigView,
  createBinding,
  deleteBinding,
  fetchBindings,
  fetchConfigOptions,
  fetchConfigView,
  type FieldError,
  updateBinding,
} from "@/lib/api";

// Pipeline roles in dispatch order; the config view keys its `roles` map by
// these names.
const ROLE_ORDER = [
  "implement",
  "review_find",
  "review_verify",
  "fix",
  "accept",
] as const;

/** One binding's resolved role matrix (read-only projection). */
function BindingCard({ binding }: { binding: ConfigView["bindings"][number] }) {
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

/** Pure presentation of the resolved config — no fetching. */
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

// --- Editable binding CRUD ---------------------------------------------------

/** First validation message anchored at `key` (loc[0]); undefined if none. */
export function bindingErrorFor(
  errors: FieldError[],
  key: string,
): string | undefined {
  const hit = errors.find((e) => e.loc[0] === key);
  return hit?.msg;
}

/** Errors not anchored on a curated field (e.g. `roles`, cross-binding) — the
 *  advanced-JSON section renders these with their path. */
function advancedErrors(errors: FieldError[], curated: string[]): FieldError[] {
  return errors.filter((e) => !curated.includes(String(e.loc[0])));
}

const CURATED_KEYS = [
  "provider",
  "project_key",
  "github_repo",
  "issue_label",
  "states",
  "max_concurrent",
  "local_review",
  "remote_review",
  "merge_strategy",
  "auto_merge",
  "allow_auto_merge",
  "verify_cmd",
];

function get(payload: Record<string, unknown>, key: string): unknown {
  return payload[key];
}

function str(v: unknown): string {
  return v === undefined || v === null ? "" : String(v);
}

/** The drawer form for one binding (create when `binding` is null). */
export function BindingForm({
  binding,
  options,
  onSaved,
  onCancel,
}: {
  binding: BindingRecord | null;
  options: ConfigOptions;
  onSaved: () => void;
  onCancel: () => void;
}) {
  const initial = useMemo<Record<string, unknown>>(() => {
    if (binding) return { ...binding.payload };
    return { provider: "linear", states: { ready: "" } };
  }, [binding]);

  const [payload, setPayload] = useState<Record<string, unknown>>(initial);
  const [enabled, setEnabled] = useState(binding ? binding.enabled : true);
  const [priority, setPriority] = useState(binding ? binding.priority : 0);
  const [raw, setRaw] = useState(() => JSON.stringify(initial, null, 2));
  const [rawError, setRawError] = useState<string | null>(null);
  const [fieldErrors, setFieldErrors] = useState<FieldError[]>([]);
  const [conflict, setConflict] = useState<number | null | false>(false);
  const [warnings, setWarnings] = useState<string[]>([]);
  const [saving, setSaving] = useState(false);

  function patch(next: Record<string, unknown>) {
    setPayload(next);
    setRaw(JSON.stringify(next, null, 2));
    setRawError(null);
  }

  function setKey(key: string, value: unknown) {
    const next = { ...payload };
    if (value === "" || value === undefined) delete next[key];
    else next[key] = value;
    patch(next);
  }

  function setReady(value: string) {
    const states = { ...(payload.states as Record<string, unknown> | undefined) };
    states.ready = value;
    patch({ ...payload, states });
  }

  function onRawChange(text: string) {
    setRaw(text);
    try {
      const parsed = JSON.parse(text);
      setPayload(parsed);
      setRawError(null);
    } catch (e) {
      setRawError(e instanceof Error ? e.message : "invalid JSON");
    }
  }

  async function submit() {
    if (rawError) return;
    setSaving(true);
    setFieldErrors([]);
    setConflict(false);
    setWarnings([]);
    const body: BindingWrite = {
      payload,
      enabled,
      priority,
      version: binding ? binding.version : undefined,
    };
    try {
      const saved = binding
        ? await updateBinding(binding.id, body)
        : await createBinding(body);
      if (saved.warnings?.length) {
        setWarnings(saved.warnings);
      }
      onSaved();
    } catch (e) {
      if (e instanceof ConfigWriteError) {
        if (e.status === 422) setFieldErrors(e.fieldErrors);
        else if (e.status === 409) setConflict(e.conflictVersion);
        else setFieldErrors([{ loc: ["_"], msg: e.message }]);
      } else {
        setFieldErrors([{ loc: ["_"], msg: "Unexpected error" }]);
      }
    } finally {
      setSaving(false);
    }
  }

  const states = (payload.states as Record<string, unknown> | undefined) ?? {};
  const advanced = advancedErrors(fieldErrors, CURATED_KEYS);
  const topError = bindingErrorFor(fieldErrors, "_");

  function field(label: string, key: string, node: ReactNode) {
    const err = bindingErrorFor(fieldErrors, key);
    return (
      <label className="block space-y-1">
        <span className="text-xs font-medium text-muted-foreground">{label}</span>
        {node}
        {err ? <span className="block text-xs text-destructive" role="alert">{err}</span> : null}
      </label>
    );
  }

  return (
    <div
      className="fixed inset-y-0 right-0 z-50 flex w-full max-w-md flex-col overflow-y-auto border-l border-border bg-background p-6 shadow-xl"
      role="dialog"
      aria-label={binding ? "Edit binding" : "Create binding"}
    >
      <div className="mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold">
          {binding ? `Edit ${binding.project_key} → ${binding.github_repo}` : "New binding"}
        </h2>
        <Button variant="ghost" onClick={onCancel} type="button">
          Close
        </Button>
      </div>

      {conflict !== false ? (
        <Alert className="mb-4 border-destructive/50" role="alert">
          <AlertTitle>Edit conflict</AlertTitle>
          <AlertDescription>
            This binding changed since you loaded it
            {conflict != null ? ` (now version ${conflict})` : ""}. Reload and
            reapply your edit.
          </AlertDescription>
        </Alert>
      ) : null}

      {warnings.length ? (
        <Alert className="mb-4" role="status">
          <AlertTitle>Saved with warnings</AlertTitle>
          <AlertDescription>
            {warnings.map((w) => (
              <div key={w}>{w}</div>
            ))}
          </AlertDescription>
        </Alert>
      ) : null}

      {topError ? (
        <Alert className="mb-4 border-destructive/50" role="alert">
          <AlertDescription>{topError}</AlertDescription>
        </Alert>
      ) : null}

      <div className="space-y-4">
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
            aria-label="enabled"
          />
          Enabled
        </label>

        {field(
          "Provider",
          "provider",
          <Select
            value={str(get(payload, "provider")) || "linear"}
            onChange={(e) => setKey("provider", e.target.value)}
            aria-label="provider"
          >
            <option value="linear">linear</option>
            <option value="jira">jira</option>
          </Select>,
        )}
        {field(
          "Project key",
          "project_key",
          <Input
            value={str(get(payload, "project_key"))}
            onChange={(e) => setKey("project_key", e.target.value)}
            aria-label="project_key"
          />,
        )}
        {field(
          "GitHub repo",
          "github_repo",
          <Input
            value={str(get(payload, "github_repo"))}
            onChange={(e) => setKey("github_repo", e.target.value)}
            aria-label="github_repo"
          />,
        )}
        {field(
          "Issue label",
          "issue_label",
          <Input
            value={str(get(payload, "issue_label"))}
            onChange={(e) => setKey("issue_label", e.target.value)}
            aria-label="issue_label"
          />,
        )}
        {field(
          "Ready state",
          "states",
          <Input
            value={str(states.ready)}
            onChange={(e) => setReady(e.target.value)}
            aria-label="ready_state"
          />,
        )}
        {field(
          "Max concurrent",
          "max_concurrent",
          <Input
            type="number"
            value={str(get(payload, "max_concurrent"))}
            onChange={(e) =>
              setKey(
                "max_concurrent",
                e.target.value === "" ? "" : Number(e.target.value),
              )
            }
            aria-label="max_concurrent"
          />,
        )}
        <label className="block space-y-1">
          <span className="text-xs font-medium text-muted-foreground">Priority</span>
          <Input
            type="number"
            value={String(priority)}
            onChange={(e) => setPriority(Number(e.target.value))}
            aria-label="priority"
          />
        </label>
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={Boolean(get(payload, "local_review"))}
            onChange={(e) => setKey("local_review", e.target.checked)}
            aria-label="local_review"
          />
          Local review
        </label>
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={get(payload, "remote_review") !== false}
            onChange={(e) => setKey("remote_review", e.target.checked)}
            aria-label="remote_review"
          />
          Remote review
        </label>
        {field(
          "Merge strategy",
          "merge_strategy",
          <Select
            value={str(get(payload, "merge_strategy")) || "squash"}
            onChange={(e) => setKey("merge_strategy", e.target.value)}
            aria-label="merge_strategy"
          >
            {options.merge_strategies.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
          </Select>,
        )}
        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={get(payload, "auto_merge") !== false}
            onChange={(e) => setKey("auto_merge", e.target.checked)}
            aria-label="auto_merge"
          />
          Auto merge
        </label>
        {field(
          "Verify command",
          "verify_cmd",
          <Input
            value={str(get(payload, "verify_cmd"))}
            onChange={(e) => setKey("verify_cmd", e.target.value)}
            aria-label="verify_cmd"
          />,
        )}

        <details className="rounded-md border border-border p-3">
          <summary className="cursor-pointer text-sm font-medium">
            Advanced (raw JSON)
          </summary>
          <textarea
            className="mt-2 h-48 w-full rounded-md border border-input bg-background p-2 font-mono text-xs"
            value={raw}
            onChange={(e) => onRawChange(e.target.value)}
            aria-label="raw_payload"
          />
          {rawError ? (
            <span className="block text-xs text-destructive" role="alert">
              {rawError}
            </span>
          ) : null}
          {advanced.map((e) => (
            <span
              key={e.loc.join(".")}
              className="block text-xs text-destructive"
              role="alert"
            >
              {e.loc.join(".")}: {e.msg}
            </span>
          ))}
        </details>
      </div>

      <div className="mt-6 flex gap-2">
        <Button onClick={submit} disabled={saving || Boolean(rawError)} type="button">
          {saving ? "Saving…" : "Save"}
        </Button>
        <Button variant="secondary" onClick={onCancel} type="button">
          Cancel
        </Button>
      </div>
    </div>
  );
}

/** One editable binding row with enable/edit/delete/reorder controls. */
function EditableBindingCard({
  binding,
  onEdit,
  onDelete,
  onReorder,
  isFirst,
  isLast,
}: {
  binding: BindingRecord;
  onEdit: () => void;
  onDelete: () => void;
  onReorder: (dir: -1 | 1) => void;
  isFirst: boolean;
  isLast: boolean;
}) {
  return (
    <Card className="flex flex-wrap items-center justify-between gap-3 p-4">
      <div className="flex items-center gap-2">
        <span className="rounded bg-secondary px-1.5 py-0.5 text-[11px] font-medium uppercase tracking-wide text-muted-foreground">
          {binding.tracker_provider || "linear"}
        </span>
        <span className="font-mono text-sm font-semibold">
          {binding.project_key}
        </span>
        <span className="text-muted-foreground">→</span>
        <span className="font-mono text-sm">{binding.github_repo}</span>
        {binding.issue_label ? (
          <span className="rounded bg-secondary px-1.5 py-0.5 text-xs">
            {binding.issue_label}
          </span>
        ) : null}
        {!binding.enabled ? (
          <span className="text-xs text-muted-foreground">(disabled)</span>
        ) : null}
      </div>
      <div className="flex items-center gap-1">
        <span className="mr-2 font-mono text-xs text-muted-foreground">
          priority {binding.priority}
        </span>
        <Button
          variant="ghost"
          type="button"
          aria-label={`move up ${binding.id}`}
          disabled={isFirst}
          onClick={() => onReorder(-1)}
        >
          ↑
        </Button>
        <Button
          variant="ghost"
          type="button"
          aria-label={`move down ${binding.id}`}
          disabled={isLast}
          onClick={() => onReorder(1)}
        >
          ↓
        </Button>
        <Button variant="secondary" type="button" onClick={onEdit}>
          Edit
        </Button>
        <Button variant="ghost" type="button" onClick={onDelete}>
          Delete
        </Button>
      </div>
    </Card>
  );
}

/** The editable bindings list + create button + drawer form. */
export function BindingsPanel({
  bindings,
  options,
  onChanged,
}: {
  bindings: BindingRecord[];
  options: ConfigOptions;
  onChanged: () => void;
}) {
  const [editing, setEditing] = useState<BindingRecord | null>(null);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const ordered = [...bindings].sort(
    (a, b) => a.priority - b.priority || a.id - b.id,
  );

  async function remove(b: BindingRecord) {
    if (!window.confirm(`Delete binding ${b.project_key} → ${b.github_repo}?`)) {
      return;
    }
    setError(null);
    try {
      await deleteBinding(b.id, b.version);
      onChanged();
    } catch (e) {
      setError(
        e instanceof ConfigWriteError && e.status === 409
          ? "Binding changed since load — reload and retry the delete."
          : "Failed to delete binding.",
      );
    }
  }

  async function reorder(index: number, dir: -1 | 1) {
    const a = ordered[index];
    const b = ordered[index + dir];
    if (!a || !b) return;
    setError(null);
    try {
      // Swap the two rows' priorities (each a versioned write).
      await updateBinding(a.id, {
        payload: a.payload,
        enabled: a.enabled,
        priority: b.priority,
        version: a.version,
      });
      await updateBinding(b.id, {
        payload: b.payload,
        enabled: b.enabled,
        priority: a.priority,
        version: b.version,
      });
      onChanged();
    } catch {
      setError("Failed to reorder — reload and retry.");
    }
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Bindings</h2>
        <Button type="button" onClick={() => setCreating(true)}>
          New binding
        </Button>
      </div>

      {error ? (
        <Alert className="border-destructive/50" role="alert">
          <AlertDescription>{error}</AlertDescription>
        </Alert>
      ) : null}

      {ordered.length ? (
        ordered.map((b, i) => (
          <EditableBindingCard
            key={b.id}
            binding={b}
            isFirst={i === 0}
            isLast={i === ordered.length - 1}
            onEdit={() => setEditing(b)}
            onDelete={() => remove(b)}
            onReorder={(dir) => reorder(i, dir)}
          />
        ))
      ) : (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          No bindings yet — create one to start dispatching.
        </div>
      )}

      {creating ? (
        <BindingForm
          binding={null}
          options={options}
          onSaved={() => {
            setCreating(false);
            onChanged();
          }}
          onCancel={() => setCreating(false)}
        />
      ) : null}
      {editing ? (
        <BindingForm
          binding={editing}
          options={options}
          onSaved={() => {
            setEditing(null);
            onChanged();
          }}
          onCancel={() => setEditing(null)}
        />
      ) : null}
    </div>
  );
}

export function ConfigPage() {
  const view = useQuery({
    queryKey: ["config"],
    queryFn: fetchConfigView,
    staleTime: Infinity,
  });
  const bindings = useQuery({
    queryKey: ["config", "bindings"],
    queryFn: fetchBindings,
    staleTime: Infinity,
  });
  const options = useQuery({
    queryKey: ["config", "options"],
    queryFn: fetchConfigOptions,
    staleTime: Infinity,
  });

  function refetchAll() {
    void bindings.refetch();
    void view.refetch();
  }

  return (
    <main className="mx-auto w-full max-w-[1200px] px-4 py-6 sm:px-6 lg:px-8">
      <div className="mb-5">
        <h1 className="text-xl font-semibold tracking-tight">Configuration</h1>
        <p className="mt-0.5 text-sm text-muted-foreground">
          Bindings live in the database and are picked up by the daemon on the
          next tick. Secrets are never shown.
        </p>
      </div>

      {bindings.data && options.data ? (
        <div className="mb-8">
          <BindingsPanel
            bindings={bindings.data}
            options={options.data}
            onChanged={refetchAll}
          />
        </div>
      ) : bindings.isLoading || options.isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : null}

      <div className="mb-2 mt-6">
        <h2 className="text-lg font-semibold">Resolved role matrix</h2>
        <p className="mt-0.5 text-sm text-muted-foreground">
          Effective per-stage agent/model/effort as the daemon would dispatch.
        </p>
      </div>
      {view.data ? (
        <ConfigDetails config={view.data} />
      ) : view.isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          Failed to load config
        </div>
      )}
    </main>
  );
}
