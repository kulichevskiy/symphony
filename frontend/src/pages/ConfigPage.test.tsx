// @vitest-environment jsdom
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderToStaticMarkup } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";

import type {
  BindingRecord,
  ConfigOptions,
  ConfigView,
  RolesMatrix,
} from "@/lib/api";
import { registerTokenProvider } from "@/lib/auth";

import {
  BindingForm,
  BindingsPanel,
  ConfigDetails,
  ConfigPage,
  GlobalRolesCard,
  RoleMatrixEditor,
} from "./ConfigPage";

const config: ConfigView = {
  read_only: true,
  global_max_concurrent: 7,
  poll_interval_secs: 42,
  bindings: [
    {
      provider: "linear",
      project_key: "SYM",
      github_repo: "org/symphony",
      max_concurrent: 3,
      roles: {
        implement: { agent: "codex", model: null, effort: null },
        review_find: { agent: "claude", model: "opus", effort: "high" },
      },
    },
  ],
};

const OPTIONS: ConfigOptions = {
  agent_families: ["claude", "codex"],
  codex_models: ["gpt-5.1-codex"],
  claude_aliases: ["haiku", "opus", "sonnet"],
  codex_efforts: ["high", "low", "medium", "minimal"],
  claude_efforts: ["high", "low", "max", "medium", "xhigh"],
  claude_efforts_by_model: {
    opus: ["low", "medium", "high"],
    sonnet: ["low", "medium"],
    haiku: ["low"],
  },
  merge_strategies: ["squash", "merge", "rebase"],
  github_webhook_secret_configured: true,
};

function record(overrides: Partial<BindingRecord> = {}): BindingRecord {
  return {
    id: 1,
    version: 4,
    enabled: true,
    priority: 0,
    updated_at: "2026-07-13T00:00:00Z",
    updated_by: "alice@example.com",
    project_key: "ENG",
    github_repo: "org/repo",
    issue_label: "",
    tracker_provider: "linear",
    tracker_site: "default",
    webhook_secret_set: false,
    payload: { project_key: "ENG", github_repo: "org/repo", states: { ready: "Todo" } },
    ...overrides,
  };
}

function mockFetch(status: number, body: unknown) {
  const fn = vi.fn(
    async (_input: RequestInfo | URL, _init?: RequestInit) =>
      new Response(body === undefined ? null : JSON.stringify(body), { status }),
  );
  vi.stubGlobal("fetch", fn);
  return fn;
}

afterEach(() => {
  cleanup();
  registerTokenProvider(null);
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("ConfigDetails", () => {
  it("renders bindings, roles and concurrency caps", () => {
    const html = renderToStaticMarkup(<ConfigDetails config={config} />);
    expect(html).toContain("SYM");
    expect(html).toContain("org/symphony");
    expect(html).toContain("global max concurrent · 7");
    expect(html).toContain("max concurrent · 3");
    expect(html).toContain("implement");
    expect(html).toContain("opus");
    expect(html).toContain("high");
  });

  it("shows an empty state when no bindings are configured", () => {
    const html = renderToStaticMarkup(
      <ConfigDetails config={{ ...config, bindings: [] }} />,
    );
    expect(html).toContain("No bindings configured");
  });
});

describe("BindingForm", () => {
  it("renders from a fetched record with options-driven dropdowns", () => {
    render(
      <BindingForm
        binding={record({ payload: { project_key: "ENG", github_repo: "org/repo", merge_strategy: "rebase", states: { ready: "Backlog" } } })}
        options={OPTIONS}
        onSaved={() => {}}
        onCancel={() => {}}
      />,
    );
    expect((screen.getByLabelText("project_key") as HTMLInputElement).value).toBe("ENG");
    expect((screen.getByLabelText("ready_state") as HTMLInputElement).value).toBe("Backlog");
    // Merge-strategy dropdown offers exactly the options served by the backend.
    const merge = screen.getByLabelText("merge_strategy") as HTMLSelectElement;
    expect([...merge.options].map((o) => o.value)).toEqual([
      "squash",
      "merge",
      "rebase",
    ]);
    expect(merge.value).toBe("rebase");
  });

  it("canonicalizes imported YAML aliases before rendering the form", () => {
    render(
      <BindingForm
        binding={record({
          payload: {
            linear_team_key: "ENG",
            github_repo: "org/repo",
            linear_states: { ready: "Backlog" },
          },
        })}
        options={OPTIONS}
        onSaved={() => {}}
        onCancel={() => {}}
      />,
    );
    expect((screen.getByLabelText("project_key") as HTMLInputElement).value).toBe("ENG");
    expect((screen.getByLabelText("ready_state") as HTMLInputElement).value).toBe("Backlog");
  });

  it("posts a create with the edited payload", async () => {
    const fetchMock = mockFetch(201, { ...record(), id: 9 });
    const onSaved = vi.fn();
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={onSaved} onCancel={() => {}} />,
    );
    fireEvent.change(screen.getByLabelText("project_key"), { target: { value: "ENG" } });
    fireEvent.change(screen.getByLabelText("github_repo"), { target: { value: "org/repo" } });
    fireEvent.change(screen.getByLabelText("ready_state"), { target: { value: "Todo" } });
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/config/bindings");
    expect(init?.method).toBe("POST");
    const sent = JSON.parse(init?.body as string);
    expect(sent.payload.project_key).toBe("ENG");
    expect(sent.payload.states.ready).toBe("Todo");
    expect(sent.version).toBeUndefined();
  });

  it("puts an edit carrying the loaded version (optimistic lock)", async () => {
    const fetchMock = mockFetch(200, record({ version: 5 }));
    const onSaved = vi.fn();
    render(
      <BindingForm binding={record()} options={OPTIONS} onSaved={onSaved} onCancel={() => {}} />,
    );
    fireEvent.change(screen.getByLabelText("max_concurrent"), { target: { value: "6" } });
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/config/bindings/1");
    expect(init?.method).toBe("PUT");
    const sent = JSON.parse(init?.body as string);
    expect(sent.version).toBe(4);
    expect(sent.payload.max_concurrent).toBe(6);
  });

  it("has no enabled toggle in the drawer and preserves the binding's state", async () => {
    const fetchMock = mockFetch(200, record({ enabled: false, version: 5 }));
    const onSaved = vi.fn();
    render(
      <BindingForm
        binding={record({ enabled: false })}
        options={OPTIONS}
        onSaved={onSaved}
        onCancel={() => {}}
      />,
    );
    // The card owns the enable/disable toggle (SYM-193); the drawer has none.
    expect(screen.queryByLabelText("enabled")).toBeNull();
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    // The edit preserves the disabled state rather than silently re-enabling.
    expect(sent.enabled).toBe(false);
  });

  it("defaults webhook_enabled off when no global secret is configured", async () => {
    const fetchMock = mockFetch(201, { ...record(), id: 9 });
    const onSaved = vi.fn();
    render(
      <BindingForm
        binding={null}
        options={{ ...OPTIONS, github_webhook_secret_configured: false }}
        onSaved={onSaved}
        onCancel={() => {}}
      />,
    );
    expect(
      (screen.getByLabelText("webhook_enabled") as HTMLInputElement).checked,
    ).toBe(false);
    fireEvent.change(screen.getByLabelText("project_key"), { target: { value: "ENG" } });
    fireEvent.change(screen.getByLabelText("github_repo"), { target: { value: "org/repo" } });
    fireEvent.change(screen.getByLabelText("ready_state"), { target: { value: "Todo" } });
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.payload.webhook_enabled).toBe(false);
  });

  it("renders a 422 validation error on the exact field", async () => {
    mockFetch(422, { detail: [{ loc: ["project_key"], msg: "field required" }] });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(screen.getByText("field required")).toBeTruthy());
  });

  it("renders a 422 error on a checkbox-only field (auto_merge)", async () => {
    mockFetch(422, { detail: [{ loc: ["auto_merge"], msg: "not allowed with this merge strategy" }] });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() =>
      expect(screen.getByText("not allowed with this merge strategy")).toBeTruthy(),
    );
  });

  it("renders a 422 webhook_secret error on the curated field, not hidden in advanced", async () => {
    mockFetch(422, {
      detail: [
        {
          loc: ["webhook_secret"],
          msg: "webhook_enabled requires a webhook_secret when no global GITHUB_WEBHOOK_SECRET is configured",
        },
      ],
    });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() =>
      expect(
        screen.getByText(
          "webhook_enabled requires a webhook_secret when no global GITHUB_WEBHOOK_SECRET is configured",
        ),
      ).toBeTruthy(),
    );
  });

  it("rejects non-object raw JSON (e.g. null) instead of storing it", () => {
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.change(screen.getByLabelText("raw_payload"), { target: { value: "null" } });
    expect(screen.getByText("must be a JSON object")).toBeTruthy();
    expect((screen.getByText("Save") as HTMLButtonElement).disabled).toBe(true);
  });

  it("renders a 422 roles error at the roles matrix, not hidden in advanced", async () => {
    mockFetch(422, { detail: [{ loc: ["roles"], msg: "unknown Codex model 'x'" }] });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    // Curated now — rendered as the raw message at the matrix, not prefixed
    // with its `roles.` path under the advanced JSON section.
    await waitFor(() =>
      expect(screen.getByText("unknown Codex model 'x'")).toBeTruthy(),
    );
    expect(screen.queryByText("roles: unknown Codex model 'x'")).toBeNull();
  });

  it("renders a 422 allow_auto_merge error under the advanced section, not silently", async () => {
    mockFetch(422, {
      detail: [{ loc: ["allow_auto_merge"], msg: "input should be a valid boolean" }],
    });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() =>
      expect(
        screen.getByText("allow_auto_merge: input should be a valid boolean"),
      ).toBeTruthy(),
    );
  });

  it("includes a per-binding role override in the saved payload", async () => {
    const fetchMock = mockFetch(200, record({ version: 5 }));
    const onSaved = vi.fn();
    render(
      <BindingForm binding={record()} options={OPTIONS} onSaved={onSaved} onCancel={() => {}} />,
    );
    fireEvent.change(screen.getByLabelText("binding review_find agent"), {
      target: { value: "codex" },
    });
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.payload.roles.review_find).toEqual({ agent: "codex" });
  });

  it("renders an existing per-binding role override as a set cell", () => {
    render(
      <BindingForm
        binding={record({
          payload: {
            project_key: "ENG",
            github_repo: "org/repo",
            states: { ready: "Todo" },
            roles: { implement: { agent: "codex", model: "gpt-5.1-codex" } },
          },
        })}
        options={OPTIONS}
        onSaved={() => {}}
        onCancel={() => {}}
      />,
    );
    expect(
      (screen.getByLabelText("binding implement agent") as HTMLSelectElement).value,
    ).toBe("codex");
    expect(
      (screen.getByLabelText("binding implement model") as HTMLSelectElement).value,
    ).toBe("gpt-5.1-codex");
    // A role left unset stays at inherit.
    expect(
      (screen.getByLabelText("binding review_find agent") as HTMLSelectElement).value,
    ).toBe("");
  });

  it("shows a conflict banner on a 409", async () => {
    mockFetch(409, { detail: { current_version: 8, msg: "conflict" } });
    render(
      <BindingForm binding={record()} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(screen.getByText(/Edit conflict/)).toBeTruthy());
    expect(screen.getByText(/now version 8/)).toBeTruthy();
  });
});

describe("BindingsPanel", () => {
  it("deletes a binding after confirmation, carrying its version", async () => {
    const fetchMock = mockFetch(204, undefined);
    vi.spyOn(window, "confirm").mockReturnValue(true);
    const onChanged = vi.fn();
    render(
      <BindingsPanel bindings={[record({ id: 3, version: 7 })]} options={OPTIONS} onChanged={onChanged} />,
    );
    fireEvent.click(screen.getByText("Delete"));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/config/bindings/3?version=7");
    expect(init?.method).toBe("DELETE");
  });

  it("toggles a binding's enabled state from the card", async () => {
    const fetchMock = mockFetch(200, record({ id: 3, version: 7, enabled: false }));
    const onChanged = vi.fn();
    render(
      <BindingsPanel
        bindings={[record({ id: 3, version: 7, enabled: true })]}
        options={OPTIONS}
        onChanged={onChanged}
      />,
    );
    fireEvent.click(screen.getByLabelText("enabled 3"));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/config/bindings/3");
    expect(init?.method).toBe("PUT");
    const body = JSON.parse(init?.body as string);
    expect(body.enabled).toBe(false);
  });

  it("shows an active-work indicator on the card", () => {
    render(
      <BindingsPanel
        bindings={[record({ active_work: true })]}
        options={OPTIONS}
        onChanged={() => {}}
      />,
    );
    expect(screen.getByText("active work")).toBeTruthy();
  });

  it("renders the drain blocker list when a delete is rejected", async () => {
    mockFetch(409, {
      detail: {
        msg: "cannot delete a binding with active work",
        blockers: {
          running_runs: ["ENG-1"],
          open_prs: ["ENG-2"],
          operator_waits: [],
          scheduled_slots: 0,
        },
      },
    });
    vi.spyOn(window, "confirm").mockReturnValue(true);
    render(
      <BindingsPanel bindings={[record({ id: 3, version: 7 })]} options={OPTIONS} onChanged={() => {}} />,
    );
    fireEvent.click(screen.getByText("Delete"));
    await waitFor(() =>
      expect(screen.getByText(/active work must drain first/)).toBeTruthy(),
    );
    expect(screen.getByText(/running: ENG-1/)).toBeTruthy();
    expect(screen.getByText(/open PRs: ENG-2/)).toBeTruthy();
  });

  it("does not delete when the confirm is dismissed", () => {
    const fetchMock = mockFetch(204, undefined);
    vi.spyOn(window, "confirm").mockReturnValue(false);
    render(
      <BindingsPanel bindings={[record()]} options={OPTIONS} onChanged={() => {}} />,
    );
    fireEvent.click(screen.getByText("Delete"));
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("opens the create drawer from the New binding button", () => {
    render(<BindingsPanel bindings={[]} options={OPTIONS} onChanged={() => {}} />);
    fireEvent.click(screen.getByText("New binding"));
    expect(screen.getByRole("dialog", { name: "Create binding" })).toBeTruthy();
  });

  it("reorders by swapping adjacent priorities", async () => {
    const fetchMock = mockFetch(200, record());
    const onChanged = vi.fn();
    render(
      <BindingsPanel
        bindings={[
          record({ id: 1, priority: 0, version: 2 }),
          record({ id: 2, priority: 1, version: 3, github_repo: "org/other" }),
        ]}
        options={OPTIONS}
        onChanged={onChanged}
      />,
    );
    fireEvent.click(screen.getByLabelText("move down 1"));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());
    // First write bumps binding 1 to the neighbour's priority.
    const firstBody = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/config/bindings/1");
    expect(firstBody.priority).toBe(1);
  });

  it("still flips the order when both rows share the default priority", async () => {
    const fetchMock = mockFetch(200, record());
    const onChanged = vi.fn();
    render(
      <BindingsPanel
        bindings={[
          // Equal priority: natural-key tiebreak (matching the daemon's
          // dispatch order), not `id`, decides which row is "first" — pick
          // repos that alphabetize the same way the ids are given.
          record({ id: 1, priority: 0, version: 2, github_repo: "org/aaa" }),
          record({ id: 2, priority: 0, version: 3, github_repo: "org/other" }),
        ]}
        options={OPTIONS}
        onChanged={onChanged}
      />,
    );
    fireEvent.click(screen.getByLabelText("move down 1"));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());
    // A swap of equal priority values would be a no-op; the reorder must
    // instead renumber so binding 1 sorts after binding 2.
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/config/bindings/1");
    const body = JSON.parse(init?.body as string);
    expect(body.priority).toBe(1);
  });

  it("excludes disabled rows from the reorder write set", async () => {
    const fetchMock = mockFetch(200, record());
    const onChanged = vi.fn();
    render(
      <BindingsPanel
        bindings={[
          record({ id: 1, priority: 0, version: 2, github_repo: "org/aaa" }),
          record({
            id: 2,
            priority: 0,
            version: 3,
            enabled: false,
            github_repo: "org/other",
          }),
        ]}
        options={OPTIONS}
        onChanged={onChanged}
      />,
    );
    fireEvent.click(screen.getByLabelText("move down 1"));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());
    // Both rows renumber, but the disabled one must not be written — the
    // backend 422s any write carrying `enabled: false`.
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/config/bindings/1");
  });

  it("threads globalRoles down into the binding's role matrix editor", () => {
    // implement's agent cell is left inherited on the binding; only the
    // global matrix pins it to codex. The binding form must still see that
    // to hide fix's dead model cell (SYM-191 review).
    render(
      <BindingsPanel
        bindings={[]}
        options={OPTIONS}
        globalRoles={{ implement: { agent: "codex" } }}
        onChanged={() => {}}
      />,
    );
    fireEvent.click(screen.getByText("New binding"));
    expect(screen.queryByLabelText("binding fix model")).toBeNull();
  });
});

describe("RoleMatrixEditor", () => {
  it("offers an explicit inherit option in every cell", () => {
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    const agent = screen.getByLabelText("binding implement agent") as HTMLSelectElement;
    expect([...agent.options].map((o) => o.value)).toEqual(["", "claude", "codex"]);
    expect(agent.value).toBe("");
  });

  it("varies Claude effort options by the selected model", () => {
    // opus supports low/medium/high; sonnet only low/medium (per OPTIONS).
    let roles: RolesMatrix = {
      implement: { agent: "claude", model: "opus" },
    };
    const { rerender } = render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    const effortOpts = () =>
      [...(screen.getByLabelText("binding implement effort") as HTMLSelectElement).options].map(
        (o) => o.value,
      );
    expect(effortOpts()).toEqual(["", "low", "medium", "high"]);

    rerender(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "claude", model: "sonnet" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(effortOpts()).toEqual(["", "low", "medium"]);
  });

  it("clears model/effort when the agent changes families", () => {
    let roles: RolesMatrix = {
      implement: { agent: "claude", model: "opus", effort: "high" },
    };
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText("binding implement agent"), {
      target: { value: "codex" },
    });
    expect(roles.implement).toEqual({ agent: "codex" });
  });

  it("keeps fix/accept's hidden agent cell locked to implement's (builder-agent sync)", () => {
    // `_synthesize_legacy_role_fields` only bridges the legacy
    // `binding.agent` field (still read by completion parsing, activity, and
    // cost attribution) back onto the daemon's other builder readers when
    // implement/fix/accept all resolve to the same family — so switching
    // implement's agent here must carry fix/accept along, even though their
    // own agent cell is hidden.
    let roles: RolesMatrix = {};
    const { rerender } = render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText("binding implement agent"), {
      target: { value: "codex" },
    });
    expect(roles.fix).toEqual({ agent: "codex" });
    expect(roles.accept).toEqual({ agent: "codex" });

    rerender(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText("binding implement agent"), {
      target: { value: "" },
    });
    expect(roles.fix).toBeUndefined();
    expect(roles.accept).toBeUndefined();
  });

  it("mirrors implement's codex model onto fix/accept's hidden model cell", () => {
    // `_synthesize_legacy_role_fields` only derives the legacy `codex_model`
    // that a codex-resolved fix/accept actually dispatch with when
    // impl.model == fix.model == acc.model; picking a non-default codex
    // model on implement must carry it onto fix/accept too, or their
    // dispatch silently keeps the stale/default model (SYM-191 review).
    let roles: RolesMatrix = {};
    const { rerender } = render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    // Step 1: pick Codex as the implementer (mirrors fix/accept's agent, per
    // the existing builder-agent sync).
    fireEvent.change(screen.getByLabelText("binding implement agent"), {
      target: { value: "codex" },
    });
    rerender(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    // Step 2: pick a non-default Codex model.
    fireEvent.change(screen.getByLabelText("binding implement model"), {
      target: { value: "gpt-5.1-codex" },
    });
    expect(roles.implement).toEqual({ agent: "codex", model: "gpt-5.1-codex" });
    expect(roles.fix).toEqual({ agent: "codex", model: "gpt-5.1-codex" });
    expect(roles.accept).toEqual({ agent: "codex", model: "gpt-5.1-codex" });

    // A stale fix/accept model (e.g. loaded from before this sync existed)
    // is corrected, not just filled in when absent.
    roles = {
      implement: { agent: "codex", model: "gpt-5.1-codex" },
      fix: { agent: "codex", model: "stale-model" },
      accept: { agent: "codex", model: "stale-model" },
    };
    rerender(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText("binding implement model"), {
      target: { value: "gpt-5.1-codex" },
    });
    expect(roles.fix).toEqual({ agent: "codex", model: "gpt-5.1-codex" });
    expect(roles.accept).toEqual({ agent: "codex", model: "gpt-5.1-codex" });
  });

  it("hides fix's model cell once its (propagated) agent resolves to codex", () => {
    let roles: RolesMatrix = {
      implement: { agent: "claude" },
      fix: { agent: "claude", model: "sonnet" },
    };
    const { rerender } = render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    expect(screen.getByLabelText("binding fix model")).toBeTruthy();

    fireEvent.change(screen.getByLabelText("binding implement agent"), {
      target: { value: "codex" },
    });
    // Switching families also strands fix's stale claude model.
    expect(roles.fix).toEqual({ agent: "codex" });
    rerender(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    expect(screen.queryByLabelText("binding fix model")).toBeNull();
  });

  it("hides fix's model cell when the binding leaves agent inherited and the global matrix resolves it to codex", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ fix: { model: "sonnet" } }}
        globalRoles={{ implement: { agent: "codex" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(screen.queryByLabelText("binding fix model")).toBeNull();
  });

  it("hides review_verify's model cell when its own agent is inherited, regardless of the global implementer", () => {
    // review_verify's inherited default does NOT mirror implement (server's
    // `resolved_reviewer_agent()` fallback reads only the binding's legacy
    // `agent`/`reviewer_agent` fields, always defaults for a DB-managed
    // binding) — it's always the fixed "codex", so the model cell stays
    // hidden here even though the global matrix pins `implement` to `claude`
    // (SYM-191 review).
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ review_verify: { model: "opus" } }}
        globalRoles={{ implement: { agent: "claude" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(screen.queryByLabelText("binding review_verify model")).toBeNull();
  });

  it("does not treat review_verify's inherited default as implement-opposite when the global matrix pins only implement", () => {
    // An imported or API-created global matrix pinning only
    // `implement.agent: codex` and leaving `review_verify.agent` inherited
    // must NOT flip review_verify's effective family to "claude" (the
    // implement-opposite) — the server's fallback for an inherited
    // review_verify ignores the matrix's `implement` cell entirely, so it
    // stays "codex" regardless (SYM-191 review).
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{}}
        globalRoles={{ implement: { agent: "codex" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(screen.queryByLabelText("binding review_verify model")).toBeNull();
  });

  it("shows review_verify's model cell for a claude (or inherited) agent", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ review_verify: { agent: "claude", model: "opus" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(screen.getByLabelText("binding review_verify model")).toBeTruthy();
  });

  it("hides review_verify's model cell once its own agent is set to codex", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ review_verify: { agent: "codex" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(screen.queryByLabelText("binding review_verify model")).toBeNull();
  });

  it("drops an effort the new model doesn't support when the model changes", () => {
    // opus supports high; sonnet (per OPTIONS) only offers low/medium.
    let roles: RolesMatrix = {
      implement: { agent: "claude", model: "opus", effort: "high" },
    };
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText("binding implement model"), {
      target: { value: "sonnet" },
    });
    expect(roles.implement).toEqual({ agent: "claude", model: "sonnet" });
  });

  it("keeps a still-supported effort when the model changes", () => {
    let roles: RolesMatrix = {
      implement: { agent: "claude", model: "opus", effort: "medium" },
    };
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={roles}
        options={OPTIONS}
        onChange={(next) => {
          roles = next;
        }}
      />,
    );
    fireEvent.change(screen.getByLabelText("binding implement model"), {
      target: { value: "sonnet" },
    });
    expect(roles.implement).toEqual({ agent: "claude", model: "sonnet", effort: "medium" });
  });

  it("renders a stored effort not in the current option list instead of blanking the select", () => {
    // sonnet only offers low/medium (per OPTIONS); a stale "high" cell must
    // still show up as a selectable, selected option rather than going blank.
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "claude", model: "sonnet", effort: "high" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const effort = screen.getByLabelText("binding implement effort") as HTMLSelectElement;
    expect(effort.value).toBe("high");
    expect([...effort.options].map((o) => o.value)).toEqual(["", "low", "medium", "high"]);
  });

  it("surfaces a model-only cell (inherited agent) as selected and editable, not blank/disabled", () => {
    // Legacy shape from the SYM-188 importer: `{model: "opus"}` with no
    // `agent` when the op's agent matched the baseline.
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { model: "opus" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const model = screen.getByLabelText("binding implement model") as HTMLSelectElement;
    expect(model.value).toBe("opus");
    expect([...model.options].map((o) => o.value)).toContain("opus");
    expect(model.disabled).toBe(false);
  });

  it("offers the union of both families' models when the agent is inherited", () => {
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    const model = screen.getByLabelText("binding implement model") as HTMLSelectElement;
    expect(model.value).toBe("");
    expect(model.disabled).toBe(false);
    expect([...model.options].map((o) => o.value)).toEqual([
      "",
      ...[...OPTIONS.claude_aliases, ...OPTIONS.codex_models].sort(),
    ]);
  });

  it("renders a stored model not in the current family list instead of blanking the select", () => {
    // A full `claude-*` model ID (accepted by `_role_model_in_family` and
    // preserved by the importer) has no entry in the alias list.
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "claude", model: "claude-opus-4-20250514" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const model = screen.getByLabelText("binding implement model") as HTMLSelectElement;
    expect(model.value).toBe("claude-opus-4-20250514");
    expect([...model.options].map((o) => o.value)).toContain("claude-opus-4-20250514");
  });

  it("hides the effort control for roles whose effort the runtime never reads", () => {
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    expect(screen.getByLabelText("binding implement effort")).toBeTruthy();
    for (const role of ["review_find", "review_verify", "fix", "accept"]) {
      expect(screen.queryByLabelText(`binding ${role} effort`)).toBeNull();
    }
  });

  it("hides review_verify's model cell when its agent inherits Codex from a default Claude implementer", () => {
    // With everything inherited, review_verify's fixed fallback (Codex —
    // see `effectiveAgent`) applies regardless of what `implement` resolves
    // to — a Codex-resolved verifier reads its model from the legacy
    // `binding.codex_model`, never this cell, so it must stay hidden even
    // though the cell's own `agent` is `""` rather than `"codex"`
    // (SYM-191 review).
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    expect(screen.getByLabelText("binding review_verify agent")).toBeTruthy();
    expect(screen.queryByLabelText("binding review_verify model")).toBeNull();
    expect(screen.getByLabelText("binding review_find agent")).toBeTruthy();
    expect(screen.getByLabelText("binding review_find model")).toBeTruthy();
  });

  it("shows review_verify's model cell once its own agent is pinned to claude", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ review_verify: { agent: "claude" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(screen.getByLabelText("binding review_verify model")).toBeTruthy();
  });

  it("shows fix's live model cell but hides its agent (dispatch CLI stays the legacy binding.agent)", () => {
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    expect(screen.queryByLabelText("binding fix agent")).toBeNull();
    expect(screen.getByLabelText("binding fix model")).toBeTruthy();
  });

  it("hides accept's agent+model controls (dispatch never resolves them)", () => {
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    expect(screen.queryByLabelText("binding accept agent")).toBeNull();
    expect(screen.queryByLabelText("binding accept model")).toBeNull();
  });
});

const rolesResponse = (over: Partial<{ roles: RolesMatrix; version: number; warnings: string[] }> = {}) => ({
  roles: {},
  version: 2,
  ...over,
});

describe("GlobalRolesCard", () => {
  it("saves the edited matrix carrying its version", async () => {
    const fetchMock = mockFetch(200, rolesResponse({ version: 3 }));
    const onSaved = vi.fn();
    render(
      <GlobalRolesCard
        initialRoles={{}}
        initialVersion={2}
        options={OPTIONS}
        onSaved={onSaved}
      />,
    );
    fireEvent.change(screen.getByLabelText("global implement agent"), {
      target: { value: "codex" },
    });
    fireEvent.click(screen.getByText("Save global matrix"));
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/config/roles");
    expect(init?.method).toBe("PUT");
    const sent = JSON.parse(init?.body as string);
    expect(sent.version).toBe(2);
    expect(sent.roles.implement).toEqual({ agent: "codex" });
  });

  it("shows a non-blocking warning banner and still succeeds", async () => {
    mockFetch(200, rolesResponse({ warnings: ["cross-family review diversity is lost"] }));
    render(
      <GlobalRolesCard initialRoles={{}} initialVersion={0} options={OPTIONS} />,
    );
    fireEvent.click(screen.getByText("Save global matrix"));
    await waitFor(() =>
      expect(screen.getByText("cross-family review diversity is lost")).toBeTruthy(),
    );
    expect(screen.getByText("Saved with warnings")).toBeTruthy();
  });

  it("renders a conflict banner on a 409", async () => {
    mockFetch(409, { detail: { current_version: 9, msg: "conflict" } });
    render(
      <GlobalRolesCard initialRoles={{}} initialVersion={2} options={OPTIONS} />,
    );
    fireEvent.click(screen.getByText("Save global matrix"));
    await waitFor(() => expect(screen.getByText(/Edit conflict/)).toBeTruthy());
    expect(screen.getByText(/now version 9/)).toBeTruthy();
  });

  it("renders a 422 validation error", async () => {
    mockFetch(422, { detail: [{ loc: ["roles"], msg: "unknown Claude effort 'turbo'" }] });
    render(
      <GlobalRolesCard initialRoles={{}} initialVersion={2} options={OPTIONS} />,
    );
    fireEvent.click(screen.getByText("Save global matrix"));
    await waitFor(() =>
      expect(screen.getByText("unknown Claude effort 'turbo'")).toBeTruthy(),
    );
  });
});

describe("ConfigPage", () => {
  it("refetches the roles query (not just the resolved view) after a global matrix save", async () => {
    let rolesGetCalls = 0;
    const fn = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      if (url === "/api/config" && method === "GET") {
        return new Response(JSON.stringify(config), { status: 200 });
      }
      if (url === "/api/config/bindings" && method === "GET") {
        return new Response(JSON.stringify([]), { status: 200 });
      }
      if (url === "/api/config/options" && method === "GET") {
        return new Response(JSON.stringify(OPTIONS), { status: 200 });
      }
      if (url === "/api/config/roles" && method === "GET") {
        rolesGetCalls += 1;
        return new Response(
          JSON.stringify({ roles: {}, version: rolesGetCalls === 1 ? 0 : 1 }),
          { status: 200 },
        );
      }
      if (url === "/api/config/roles" && method === "PUT") {
        return new Response(JSON.stringify({ roles: {}, version: 1, warnings: [] }), {
          status: 200,
        });
      }
      throw new Error(`unexpected request ${method} ${url}`);
    });
    vi.stubGlobal("fetch", fn);

    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={client}>
        <ConfigPage />
      </QueryClientProvider>,
    );

    await screen.findByText("Global roles matrix");
    expect(rolesGetCalls).toBe(1);

    fireEvent.click(screen.getByText("Save global matrix"));
    // With `staleTime: Infinity`, a remount after a save would otherwise re-seed
    // from the pre-save version and spuriously 409 the next save — asserting
    // the GET fires again (not just the resolved-view GET) is the regression
    // check for that.
    await waitFor(() => expect(rolesGetCalls).toBe(2));
  });
});
