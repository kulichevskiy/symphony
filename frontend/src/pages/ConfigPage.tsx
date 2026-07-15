import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useMemo, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select } from "@/components/ui/select";
import {
  ApiError,
  type BindingRecord,
  type BindingWrite,
  ConfigWriteError,
  type ConfigOptions,
  type ConfigView,
  createBinding,
  deleteBinding,
  type DrainBlockers,
  fetchBindings,
  fetchConfigOptions,
  fetchConfigView,
  fetchRoles,
  type FieldError,
  type RoleCell,
  type RolesMatrix,
  updateBinding,
  updateRoles,
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

// Which cells of a role are actually threaded into a dispatched command — the
// rest validate/display but never affect a runtime dispatch, so exposing them
// as editable would offer a knob that silently does nothing:
//  * `effort` only ever reaches a subprocess flag for `implement`
//    (`build_runner_command`); `review_find`/`fix`'s command builders take no
//    `effort` param, and `review_verify`/`accept` are never resolved on any
//    dispatch path.
//  * `review_verify.agent` never picks the verifier's own CLI — the verifier
//    pass always reuses the legacy `binding.agent` (`_lifecycle.py` passes it
//    through as `implementer_agent`). It must still be editable: `resolved_
//    role("review_verify", ...).model` only reaches the verifier's `--model`
//    when the *resolved* agent is `claude`
//    (`effective_config._synthesize_legacy_role_fields`), and that agent
//    defaults to the implementer-opposite family
//    (`resolved_reviewer_agent()`) — so for the common claude implementer, a
//    `review_verify.model` override silently drops unless `agent` is also
//    pinned to `claude`. When the row's own `agent` cell is explicitly
//    `codex`, the model cell is hidden outright (see `modelWired` below): the
//    verifier's codex model always comes from the legacy `binding.codex_model`
//    (`implementer_codex_model`), never from `resolved_role("review_verify",
//    ...).model`, so a codex-resolved override is *always* a no-op, not just
//    the claude-default case above.
//  * `fix.agent` is NOT wired: `_run_fix_agent` picks its CLI from the legacy
//    `binding.agent`, never from `resolved_role("fix", ...).agent`
//    (`orchestrator/poll/_base.py`/`_helpers.build_fix_runner_command`). Its
//    hidden cell is kept in lockstep with `implement.agent` (see `cellChange`)
//    so `_synthesize_legacy_role_fields` can still sync `binding.agent` for
//    the legacy readers (completion parsing, activity, cost) when an operator
//    picks codex for `implement`.
//    `fix.model` IS live *only for a claude-resolved fix role*:
//    `_fix_claude_model` reads `resolved_role("fix", ...).model` and threads
//    it into the fixer's `--model`. `_run_fix_agent` always passes
//    `codex_model=binding.codex_model` — never `resolved_role("fix",
//    ...).model` — so a codex-resolved fix role's model cell (see
//    `modelWired`) is hidden the same way as `review_verify`'s.
//  * `accept.agent`/`accept.model` are NOT wired: `build_acceptance_command`
//    hardcodes the `claude` CLI and takes no model param at all
//    (`agent/runners/acceptance.py`).
const ROLE_FIELDS: Record<string, { agent: boolean; model: boolean; effort: boolean }> = {
  implement: { agent: true, model: true, effort: true },
  review_find: { agent: true, model: true, effort: false },
  review_verify: { agent: true, model: true, effort: false },
  fix: { agent: false, model: true, effort: false },
  accept: { agent: false, model: false, effort: false },
};

/** Muted placeholder for a cell whose field the runtime never reads. */
function UnusedCell() {
  return <span className="text-xs text-muted-foreground">not used</span>;
}

// --- Role matrix editing (SYM-191) -------------------------------------------

/** Models offered for an (agent) pick. An inherited (empty) agent leaves the
 *  family unknown client-side, so the model cell offers the union of both
 *  families (mirroring `effortsFor`'s inherited-agent fallback) — an operator
 *  can still override just the model without first pinning an agent; the
 *  server family-checks the resolved pair at save. */
function modelsFor(options: ConfigOptions, agent: string): string[] {
  if (agent === "codex") return options.codex_models;
  if (agent === "claude") return options.claude_aliases;
  return [...new Set([...options.claude_aliases, ...options.codex_models])].sort();
}

/** Best-effort resolved agent family for `role`, used only to decide whether
 *  a dead model cell should be hidden — NOT a full inheritance resolution.
 *  An explicit binding cell wins, then the global matrix's cell for the same
 *  role (so a binding that leaves `implement.agent` inherited still resolves
 *  to a global `codex` default instead of silently falling through to the
 *  hardcoded `claude` guess below); otherwise `fix` mirrors `implement`
 *  (kept in lockstep by `cellChange`).
 *
 *  `review_verify` does NOT mirror `implement` here: server-side,
 *  `resolved_role`'s fallback for a non-builder role is
 *  `resolved_reviewer_agent()` (`config.py`), which reads only the binding's
 *  legacy top-level `agent`/`reviewer_agent` fields — never `implement`'s
 *  resolved matrix value. Every DB-managed binding has those legacy fields at
 *  their pydantic defaults (the CRUD API rejects them outright, and the
 *  importer strips them into explicit matrix cells instead), so an inherited
 *  `review_verify` always resolves to the fixed opposite of the default
 *  `"claude"`, i.e. `"codex"` — regardless of what `implement.agent` is
 *  pinned to. Deriving it from `implement` here would disagree with the
 *  server whenever `implement.agent` is pinned via the matrix (binding or
 *  global) but `review_verify.agent` is left inherited (SYM-191 review). */
function effectiveAgent(
  role: string,
  roles: RolesMatrix,
  globalRoles: RolesMatrix,
): string {
  const own = String(roles[role]?.agent ?? globalRoles[role]?.agent ?? "");
  if (own) return own;
  if (role === "review_verify") return "codex";
  if (role === "fix") return effectiveAgent("implement", roles, globalRoles);
  return "claude";
}

/** Efforts offered for an (agent, model) pick. Claude efforts are per model
 *  (the live capability set); an inherited agent offers the union so an effort
 *  override over an inherited model is still selectable — the server
 *  family-checks it against the resolved role. */
function effortsFor(options: ConfigOptions, agent: string, model: string): string[] {
  if (agent === "codex") return options.codex_efforts;
  if (agent === "claude") {
    return options.claude_efforts_by_model[model] ?? options.claude_efforts;
  }
  return [...new Set([...options.claude_efforts, ...options.codex_efforts])].sort();
}

/** The 5-role × (agent, model, effort) matrix editor. Every cell offers an
 *  explicit "inherit" (empty value); a set value is an override. Used for both
 *  the per-binding matrix and the global card, distinguished by `scope` (which
 *  also namespaces the aria-labels so both can render on one page). */
export function RoleMatrixEditor({
  scope,
  roles,
  globalRoles = {},
  options,
  onChange,
}: {
  scope: string;
  roles: RolesMatrix;
  globalRoles?: RolesMatrix;
  options: ConfigOptions;
  onChange: (next: RolesMatrix) => void;
}) {
  /** Set a single role's single field within `next`, in place. Factored out
   *  of `cellChange` so a builder-agent change can apply the same
   *  clear-on-family-switch logic to `fix`/`accept` as it does to the row the
   *  operator actually touched. */
  function setCell(next: RolesMatrix, role: string, field: keyof RoleCell, value: string) {
    const cell: RoleCell = { ...(next[role] ?? {}) };
    if (value === "") delete cell[field];
    else cell[field] = value;
    // Switching the agent can strand an out-of-family model/effort — clear
    // them so the row never carries a pair the new agent would reject.
    if (field === "agent") {
      delete cell.model;
      delete cell.effort;
    }
    // Switching the model can strand an effort the new model doesn't offer
    // (e.g. sonnet has no "high") — drop it so the Select never holds a
    // stored value with no matching <option>.
    if (field === "model" && cell.effort) {
      const agentForEfforts = String(cell.agent ?? "");
      if (!effortsFor(options, agentForEfforts, value).includes(cell.effort)) {
        delete cell.effort;
      }
    }
    if (Object.keys(cell).length === 0) delete next[role];
    else next[role] = cell;
  }

  function cellChange(role: string, field: keyof RoleCell, value: string) {
    const next: RolesMatrix = { ...roles };
    setCell(next, role, field, value);
    // `implement`/`fix`/`accept` share dispatch's builder-agent identity:
    // `_synthesize_legacy_role_fields` only bridges the legacy `binding.agent`
    // field (read by completion parsing, activity, and cost attribution) back
    // to the daemon's other builder-role readers when all three resolve to
    // the same family. `fix`/`accept`'s agent cell is hidden (never wired to
    // their own dispatch — see `ROLE_FIELDS`), so keep it locked to
    // `implement`'s here rather than letting it silently diverge and leave
    // those legacy readers on a stale family (SYM-191 review).
    if (role === "implement" && field === "agent") {
      setCell(next, "fix", "agent", value);
      setCell(next, "accept", "agent", value);
    }
    // `_synthesize_legacy_role_fields` only derives the legacy `codex_model`
    // (what a codex-resolved `fix`/`accept` actually dispatch with — their
    // own model cell is a no-op for codex, see `modelWired`) when
    // `impl.model == fix.model == acc.model`. Keep those two cells mirroring
    // `implement`'s model whenever the (possibly just-changed) family is
    // codex, in whichever order agent/model are set, or a picked non-default
    // model silently never reaches `fix`/`accept` (SYM-191 review).
    if (role === "implement" && (field === "agent" || field === "model")) {
      if (effectiveAgent("implement", next, globalRoles) === "codex") {
        const implModel = String(next.implement?.model ?? "");
        setCell(next, "fix", "model", implModel);
        setCell(next, "accept", "model", implModel);
      }
    }
    onChange(next);
  }

  return (
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
            const fields = ROLE_FIELDS[role];
            const cell = roles[role] ?? {};
            const agent = String(cell.agent ?? "");
            const model = String(cell.model ?? "");
            const effort = String(cell.effort ?? "");
            // `review_verify`/`fix` resolve their codex model from the legacy
            // `binding.codex_model`, never from this cell — once the row's
            // agent (explicit, propagated, or the resolved-default family —
            // see `effectiveAgent`) is codex, editing it here is always a
            // no-op, so hide it rather than offer a dead knob. This also
            // covers the common case of a default Claude implementer, whose
            // `review_verify` inherits Codex with an empty (not `"codex"`)
            // agent cell.
            const modelWired =
              fields.model &&
              !(
                (role === "review_verify" || role === "fix") &&
                effectiveAgent(role, roles, globalRoles) === "codex"
              );
            // Include a stored effort not in the current option list (e.g.
            // loaded before a model change tightened the set) so the Select
            // never renders a value with no matching <option>.
            const effortOptions = effortsFor(options, agent, model);
            const effortChoices =
              effort && !effortOptions.includes(effort)
                ? [...effortOptions, effort]
                : effortOptions;
            // Same fallback for the model cell: a stored model may be absent
            // from `modelsFor` either because the agent is inherited (family
            // unknown client-side) or because it's a full `claude-*` ID not in
            // the alias list — surface it as a selected option either way
            // instead of silently rendering "inherit".
            const modelOptions = modelsFor(options, agent);
            const modelChoices =
              model && !modelOptions.includes(model)
                ? [...modelOptions, model]
                : modelOptions;
            return (
              <tr key={role} className="border-b border-border/70 last:border-0">
                <td className="px-3 py-2 font-mono text-xs">{role}</td>
                <td className="px-3 py-2">
                  {fields.agent ? (
                    <Select
                      value={agent}
                      onChange={(e) => cellChange(role, "agent", e.target.value)}
                      aria-label={`${scope} ${role} agent`}
                    >
                      <option value="">inherit</option>
                      {options.agent_families.map((a) => (
                        <option key={a} value={a}>
                          {a}
                        </option>
                      ))}
                    </Select>
                  ) : (
                    <UnusedCell />
                  )}
                </td>
                <td className="px-3 py-2">
                  {modelWired ? (
                    <Select
                      value={model}
                      onChange={(e) => cellChange(role, "model", e.target.value)}
                      aria-label={`${scope} ${role} model`}
                    >
                      <option value="">inherit</option>
                      {modelChoices.map((m) => (
                        <option key={m} value={m}>
                          {m}
                        </option>
                      ))}
                    </Select>
                  ) : (
                    <UnusedCell />
                  )}
                </td>
                <td className="px-3 py-2">
                  {fields.effort ? (
                    <Select
                      value={effort}
                      onChange={(e) => cellChange(role, "effort", e.target.value)}
                      aria-label={`${scope} ${role} effort`}
                    >
                      <option value="">inherit</option>
                      {effortChoices.map((ef) => (
                        <option key={ef} value={ef}>
                          {ef}
                        </option>
                      ))}
                    </Select>
                  ) : (
                    <UnusedCell />
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

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
  return errors.filter(
    (e) => e.loc[0] !== "_" && !curated.includes(String(e.loc[0])),
  );
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
  "verify_cmd",
  "webhook_enabled",
  "webhook_secret",
  "roles",
];

function get(payload: Record<string, unknown>, key: string): unknown {
  return payload[key];
}

function str(v: unknown): string {
  return v === undefined || v === null ? "" : String(v);
}

/** Imported bindings intentionally keep YAML aliases (`linear_team_key`,
 * `linear_states`) in their payload — `RepoBinding` accepts either name, but
 * the curated fields below read only the canonical ones. Canonicalize before
 * the form initializes from it so an edit of an imported binding doesn't
 * appear to have lost its project key / states. */
function canonicalizePayload(
  payload: Record<string, unknown>,
): Record<string, unknown> {
  const next = { ...payload };
  if (next.project_key === undefined && next.linear_team_key !== undefined) {
    next.project_key = next.linear_team_key;
    delete next.linear_team_key;
  }
  if (next.states === undefined && next.linear_states !== undefined) {
    next.states = next.linear_states;
    delete next.linear_states;
  }
  return next;
}

/** The drawer form for one binding (create when `binding` is null). */
export function BindingForm({
  binding,
  options,
  globalRoles = {},
  onSaved,
  onCancel,
}: {
  binding: BindingRecord | null;
  options: ConfigOptions;
  globalRoles?: RolesMatrix;
  onSaved: (warnings?: string[]) => void;
  onCancel: () => void;
}) {
  const initial = useMemo<Record<string, unknown>>(() => {
    if (binding) return canonicalizePayload(binding.payload);
    return {
      provider: "linear",
      states: { ready: "" },
      // Without a global `GITHUB_WEBHOOK_SECRET`, the field's own default of
      // `true` would make the write path reject the create until the
      // operator also sets a per-binding secret — default it off instead so
      // a first-time create just works; the checkbox lets them turn it on.
      webhook_enabled: options.github_webhook_secret_configured,
    };
  }, [binding, options.github_webhook_secret_configured]);

  const [payload, setPayload] = useState<Record<string, unknown>>(initial);
  const [priority, setPriority] = useState(binding ? binding.priority : 0);
  const [raw, setRaw] = useState(() => JSON.stringify(initial, null, 2));
  const [rawError, setRawError] = useState<string | null>(null);
  const [fieldErrors, setFieldErrors] = useState<FieldError[]>([]);
  const [conflict, setConflict] = useState<number | null | false>(false);
  const [blockers, setBlockers] = useState<DrainBlockers | null>(null);
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
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        setRawError("must be a JSON object");
        return;
      }
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
    setBlockers(null);
    const body: BindingWrite = {
      payload,
      // Preserve the current enabled state on edit (the card's toggle owns
      // enable/disable, SYM-193); a new binding starts enabled.
      enabled: binding ? binding.enabled : true,
      priority,
      version: binding ? binding.version : undefined,
    };
    try {
      const saved = binding
        ? await updateBinding(binding.id, body)
        : await createBinding(body);
      onSaved(saved.warnings);
    } catch (e) {
      if (e instanceof ConfigWriteError) {
        if (e.status === 422) setFieldErrors(e.fieldErrors);
        else if (e.blockers) setBlockers(e.blockers);
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

  /** Checkbox variant of `field` — checked-state input plus the same
   *  curated-key error rendering (these keys are excluded from the advanced
   *  list, so without this the error would render nowhere). */
  function checkboxField(label: string, key: string, node: ReactNode) {
    const err = bindingErrorFor(fieldErrors, key);
    return (
      <div className="space-y-1">
        <label className="flex items-center gap-2 text-sm">
          {node}
          {label}
        </label>
        {err ? <span className="block text-xs text-destructive" role="alert">{err}</span> : null}
      </div>
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

      {blockers ? (
        <Alert className="mb-4 border-destructive/50" role="alert">
          <AlertTitle>Cannot apply — active work must drain first</AlertTitle>
          <AlertDescription>{formatBlockers(blockers)}</AlertDescription>
        </Alert>
      ) : conflict !== false ? (
        <Alert className="mb-4 border-destructive/50" role="alert">
          <AlertTitle>Edit conflict</AlertTitle>
          <AlertDescription>
            This binding changed since you loaded it
            {conflict != null ? ` (now version ${conflict})` : ""}. Reload and
            reapply your edit.
          </AlertDescription>
        </Alert>
      ) : null}

      {topError ? (
        <Alert className="mb-4 border-destructive/50" role="alert">
          <AlertDescription>{topError}</AlertDescription>
        </Alert>
      ) : null}

      <div className="space-y-4">
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
        {checkboxField(
          "Local review",
          "local_review",
          <input
            type="checkbox"
            checked={Boolean(get(payload, "local_review"))}
            onChange={(e) => setKey("local_review", e.target.checked)}
            aria-label="local_review"
          />,
        )}
        {checkboxField(
          "Remote review",
          "remote_review",
          <input
            type="checkbox"
            checked={get(payload, "remote_review") !== false}
            onChange={(e) => setKey("remote_review", e.target.checked)}
            aria-label="remote_review"
          />,
        )}
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
        {checkboxField(
          "Auto merge",
          "auto_merge",
          <input
            type="checkbox"
            checked={get(payload, "auto_merge") !== false}
            onChange={(e) => setKey("auto_merge", e.target.checked)}
            aria-label="auto_merge"
          />,
        )}
        {field(
          "Verify command",
          "verify_cmd",
          <Input
            value={str(get(payload, "verify_cmd"))}
            onChange={(e) => setKey("verify_cmd", e.target.value)}
            aria-label="verify_cmd"
          />,
        )}
        {checkboxField(
          "Webhook enabled",
          "webhook_enabled",
          <input
            type="checkbox"
            checked={get(payload, "webhook_enabled") !== false}
            onChange={(e) => setKey("webhook_enabled", e.target.checked)}
            aria-label="webhook_enabled"
          />,
        )}
        {field(
          "Webhook secret",
          "webhook_secret",
          <>
            <Input
              type="password"
              value={str(get(payload, "webhook_secret"))}
              onChange={(e) => setKey("webhook_secret", e.target.value)}
              aria-label="webhook_secret"
              placeholder={binding?.webhook_secret_set ? "set — leave blank to keep" : ""}
            />
            {!options.github_webhook_secret_configured ? (
              <span className="block text-xs text-muted-foreground">
                No global GITHUB_WEBHOOK_SECRET configured — required here when
                webhook enabled is on.
              </span>
            ) : null}
          </>,
        )}

        <div className="space-y-1">
          <span className="text-xs font-medium text-muted-foreground">
            Roles (per-binding overrides — inherit falls back to the global
            matrix)
          </span>
          <RoleMatrixEditor
            scope="binding"
            roles={(payload.roles as RolesMatrix | undefined) ?? {}}
            globalRoles={globalRoles}
            options={options}
            onChange={(next) => {
              const cleaned = { ...payload };
              if (Object.keys(next).length === 0) delete cleaned.roles;
              else cleaned.roles = next;
              patch(cleaned);
            }}
          />
          {bindingErrorFor(fieldErrors, "roles") ? (
            <span className="block text-xs text-destructive" role="alert">
              {bindingErrorFor(fieldErrors, "roles")}
            </span>
          ) : null}
        </div>

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

/** Render a drain-guard blocker map as a compact human-readable summary. */
function formatBlockers(b: DrainBlockers): string {
  const parts: string[] = [];
  if (b.running_runs.length) parts.push(`running: ${b.running_runs.join(", ")}`);
  if (b.open_prs.length) parts.push(`open PRs: ${b.open_prs.join(", ")}`);
  if (b.operator_waits.length)
    parts.push(`parked: ${b.operator_waits.join(", ")}`);
  if (b.scheduled_slots) parts.push(`scheduled: ${b.scheduled_slots}`);
  return parts.join("; ");
}

/** One editable binding row with enable/edit/delete/reorder controls. */
function EditableBindingCard({
  binding,
  onEdit,
  onDelete,
  onReorder,
  onToggleEnabled,
  isFirst,
  isLast,
}: {
  binding: BindingRecord;
  onEdit: () => void;
  onDelete: () => void;
  onReorder: (dir: -1 | 1) => void;
  onToggleEnabled: () => void;
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
        {binding.active_work ? (
          <span
            className="rounded bg-amber-500/15 px-1.5 py-0.5 text-xs text-amber-600"
            title="This binding has active work — delete/rename is blocked until it drains."
          >
            active work
          </span>
        ) : null}
      </div>
      <div className="flex items-center gap-1">
        <label className="mr-2 flex items-center gap-1 text-xs text-muted-foreground">
          <input
            type="checkbox"
            checked={binding.enabled}
            onChange={onToggleEnabled}
            aria-label={`enabled ${binding.id}`}
          />
          enabled
        </label>
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
  globalRoles = {},
  onChanged,
}: {
  bindings: BindingRecord[];
  options: ConfigOptions;
  globalRoles?: RolesMatrix;
  onChanged: () => void;
}) {
  const [editing, setEditing] = useState<BindingRecord | null>(null);
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [savedWarnings, setSavedWarnings] = useState<string[] | null>(null);

  // Mirror the backend dispatch order (`db/config_bindings.list_all`): equal
  // priorities break ties by the natural key, not `id` — otherwise this panel
  // shows a different order than the daemon actually dispatches in.
  const ordered = [...bindings].sort(
    (a, b) =>
      a.priority - b.priority ||
      a.project_key.localeCompare(b.project_key) ||
      a.github_repo.localeCompare(b.github_repo) ||
      a.issue_label.localeCompare(b.issue_label) ||
      a.tracker_provider.localeCompare(b.tracker_provider) ||
      a.tracker_site.localeCompare(b.tracker_site),
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
      if (e instanceof ConfigWriteError && e.blockers) {
        setError(`Cannot delete — active work must drain first. ${formatBlockers(e.blockers)}`);
      } else {
        setError(
          e instanceof ConfigWriteError && e.status === 409
            ? "Binding changed since load — reload and retry the delete."
            : "Failed to delete binding.",
        );
      }
    }
  }

  async function toggleEnabled(b: BindingRecord) {
    setError(null);
    try {
      await updateBinding(b.id, {
        payload: b.payload,
        enabled: !b.enabled,
        priority: b.priority,
        version: b.version,
      });
      onChanged();
    } catch (e) {
      setError(
        e instanceof ConfigWriteError && e.status === 409
          ? "Binding changed since load — reload and retry."
          : "Failed to toggle the binding.",
      );
    }
  }

  async function reorder(index: number, dir: -1 | 1) {
    const other = index + dir;
    if (!ordered[index] || !ordered[other]) return;
    setError(null);

    const moved = [...ordered];
    [moved[index], moved[other]] = [moved[other], moved[index]];

    // Renumber against the new positions so the swap is never a no-op (e.g.
    // every new binding defaults to priority 0) — a plain value swap between
    // two equal priorities writes nothing and the sort order never changes.
    // Disabled rows are excluded: the backend rejects any write carrying
    // `enabled: false` (the lifecycle guard ships in SYM-193), so renumbering
    // one would 422 the whole reorder even though its priority display value
    // never actually changes anything today.
    const writes = ordered
      .map((b) => ({ b, priority: moved.findIndex((m) => m.id === b.id) }))
      .filter(({ b, priority }) => b.enabled && b.priority !== priority);

    try {
      for (const { b, priority } of writes) {
        await updateBinding(b.id, {
          payload: b.payload,
          enabled: b.enabled,
          priority,
          version: b.version,
        });
      }
      onChanged();
    } catch {
      setError("Failed to reorder — reload and retry.");
      // A partial renumber may have landed — refetch so the UI reflects the
      // actual DB state instead of showing the stale pre-reorder order.
      onChanged();
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

      {savedWarnings?.length ? (
        <Alert role="status">
          <AlertTitle>Saved with warnings</AlertTitle>
          {savedWarnings.map((w) => (
            <AlertDescription key={w}>{w}</AlertDescription>
          ))}
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
            onToggleEnabled={() => toggleEnabled(b)}
          />
        ))
      ) : (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          No bindings yet — create one to start dispatching.
        </div>
      )}

      {creating ? (
        <BindingForm
          key="new"
          binding={null}
          options={options}
          globalRoles={globalRoles}
          onSaved={(warnings) => {
            setCreating(false);
            setSavedWarnings(warnings?.length ? warnings : null);
            onChanged();
          }}
          onCancel={() => setCreating(false)}
        />
      ) : null}
      {editing ? (
        <BindingForm
          key={editing.id}
          binding={editing}
          options={options}
          globalRoles={globalRoles}
          onSaved={(warnings) => {
            setEditing(null);
            setSavedWarnings(warnings?.length ? warnings : null);
            onChanged();
          }}
          onCancel={() => setEditing(null)}
        />
      ) : null}
    </div>
  );
}

/** Editor for the fleet-wide global roles matrix (its own version + conflict
 *  handling). Non-blocking diversity warnings render as a banner; the save
 *  still succeeds. */
export function GlobalRolesCard({
  initialRoles,
  initialVersion,
  options,
  onSaved,
}: {
  initialRoles: RolesMatrix;
  initialVersion: number;
  options: ConfigOptions;
  onSaved?: () => void;
}) {
  const [roles, setRoles] = useState<RolesMatrix>(initialRoles);
  const [version, setVersion] = useState(initialVersion);
  const [fieldErrors, setFieldErrors] = useState<FieldError[]>([]);
  const [conflict, setConflict] = useState<number | null | false>(false);
  const [warnings, setWarnings] = useState<string[] | null>(null);
  const [saving, setSaving] = useState(false);

  async function save() {
    setSaving(true);
    setFieldErrors([]);
    setConflict(false);
    setWarnings(null);
    try {
      const saved = await updateRoles({ roles, version });
      setVersion(saved.version);
      setRoles(saved.roles);
      setWarnings(saved.warnings?.length ? saved.warnings : null);
      onSaved?.();
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

  const rolesErr =
    bindingErrorFor(fieldErrors, "roles") ?? bindingErrorFor(fieldErrors, "_");

  return (
    <Card className="space-y-4 p-5">
      <div>
        <h2 className="text-lg font-semibold">Global roles matrix</h2>
        <p className="mt-0.5 text-sm text-muted-foreground">
          Fleet-wide default agent/model/effort per role. Per-binding cells set
          to inherit fall back here.
        </p>
      </div>

      {conflict !== false ? (
        <Alert className="border-destructive/50" role="alert">
          <AlertTitle>Edit conflict</AlertTitle>
          <AlertDescription>
            The global matrix changed since you loaded it
            {conflict != null ? ` (now version ${conflict})` : ""}. Reload and
            reapply your edit.
          </AlertDescription>
        </Alert>
      ) : null}

      {rolesErr ? (
        <Alert className="border-destructive/50" role="alert">
          <AlertDescription>{rolesErr}</AlertDescription>
        </Alert>
      ) : null}

      {warnings?.length ? (
        <Alert role="status">
          <AlertTitle>Saved with warnings</AlertTitle>
          {warnings.map((w) => (
            <AlertDescription key={w}>{w}</AlertDescription>
          ))}
        </Alert>
      ) : null}

      <RoleMatrixEditor
        scope="global"
        roles={roles}
        options={options}
        onChange={setRoles}
      />

      <Button onClick={save} disabled={saving} type="button">
        {saving ? "Saving…" : "Save global matrix"}
      </Button>
    </Card>
  );
}

export function ConfigPage() {
  const view = useQuery({
    queryKey: ["config"],
    queryFn: fetchConfigView,
    staleTime: Infinity,
  });
  // A 404 means the CRUD router isn't mounted (legacy YAML topology owns
  // bindings) — retrying can't change that, so resolve immediately instead
  // of running the default 3 retries before the read-only notice can render.
  const retryUnlessNotFound = (failureCount: number, error: unknown) =>
    !(error instanceof ApiError && error.status === 404) && failureCount < 3;
  const bindings = useQuery({
    queryKey: ["config", "bindings"],
    queryFn: fetchBindings,
    staleTime: Infinity,
    retry: retryUnlessNotFound,
  });
  const options = useQuery({
    queryKey: ["config", "options"],
    queryFn: fetchConfigOptions,
    staleTime: Infinity,
    retry: retryUnlessNotFound,
  });
  const roles = useQuery({
    queryKey: ["config", "roles"],
    queryFn: fetchRoles,
    staleTime: Infinity,
    retry: retryUnlessNotFound,
  });

  function refetchAll() {
    void bindings.refetch();
    void roles.refetch();
    void view.refetch();
  }

  // The backend intentionally doesn't mount `/api/config/{options,bindings}`
  // when a legacy YAML topology still owns bindings — DB writes here would
  // round-trip looking successful while the daemon silently never applies
  // them (`ui_db_owns_topology=False`). A 404 on either query means that, not
  // a real failure — show the resolved (read-only) config below instead of
  // an error banner.
  const isReadOnlyConfig =
    (bindings.error instanceof ApiError && bindings.error.status === 404) ||
    (options.error instanceof ApiError && options.error.status === 404);

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
        <div className="mb-8 space-y-8">
          <BindingsPanel
            bindings={bindings.data}
            options={options.data}
            globalRoles={roles.data?.roles}
            onChanged={refetchAll}
          />
          {roles.data ? (
            // The card tracks its own version/roles across saves (updating
            // from each PUT response), so it isn't keyed on the fetched
            // version — remounting would wipe the just-shown warning banner.
            // `onSaved` still refetches the `roles` query itself (not just the
            // resolved matrix below) so a later remount re-seeds from the
            // bumped version instead of the stale `initialVersion` — otherwise
            // the next save 409s against a version the server left behind.
            <GlobalRolesCard
              initialRoles={roles.data.roles}
              initialVersion={roles.data.version}
              options={options.data}
              onSaved={() => {
                void roles.refetch();
                void view.refetch();
              }}
            />
          ) : null}
        </div>
      ) : isReadOnlyConfig ? (
        <div className="mb-8 rounded-md border border-border p-6 text-sm text-muted-foreground">
          Bindings are still configured via the legacy YAML file, not the
          database — editing here isn't available yet. The resolved config
          below reflects what the daemon actually runs.
        </div>
      ) : bindings.isLoading || options.isLoading ? (
        <p className="text-sm text-muted-foreground">Loading…</p>
      ) : bindings.isError || options.isError ? (
        <div className="rounded-md border border-border p-6 text-sm text-muted-foreground">
          Failed to load bindings
        </div>
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
