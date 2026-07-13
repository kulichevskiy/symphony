// @vitest-environment jsdom
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
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

  it("has no toggle for the unsupported disabled state and always saves enabled", async () => {
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
    expect(screen.queryByLabelText("enabled")).toBeNull();
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.enabled).toBe(true);
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
      (screen.getByLabelText("binding fix agent") as HTMLSelectElement).value,
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
