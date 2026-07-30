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
  Connection,
  RolesMatrix,
} from "@/lib/api";
import { registerTokenProvider } from "@/lib/auth";

import {
  BindingForm,
  BindingsPanel,
  ConfigDetails,
  ConfigPage,
  ConnectionsPanel,
  GlobalRolesCard,
  RoleMatrixEditor,
} from "./ConfigPage";

const CONNECTIONS: Connection[] = [
  { provider: "github", label: "GitHub", status: "not_connected", expires_at: null },
  { provider: "linear", label: "Linear", status: "not_connected", expires_at: null },
  { provider: "claude", label: "Claude", status: "not_connected", expires_at: null },
  { provider: "codex", label: "Codex", status: "not_connected", expires_at: null },
];

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
  codex_models: ["gpt-5.5", "gpt-5.6-sol"],
  claude_aliases: ["haiku", "opus", "sonnet"],
  codex_efforts: ["high", "low", "max", "medium", "ultra", "xhigh"],
  codex_efforts_by_model: {
    "gpt-5.5": ["low", "medium", "high", "xhigh"],
    "gpt-5.6-sol": ["low", "medium", "high", "xhigh", "max", "ultra"],
  },
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
    webhook_secret_version: 0,
    webhook_secret_updated_at: "",
    webhook_secret_updated_by: "",
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

describe("ConnectionsPanel", () => {
  it("renders one card per provider with its status", () => {
    const html = renderToStaticMarkup(<ConnectionsPanel connections={CONNECTIONS} />);
    for (const label of ["GitHub", "Linear", "Claude", "Codex"]) {
      expect(html).toContain(label);
    }
    expect(html.match(/Not connected/g)).toHaveLength(4);
    // Connect/Disconnect/Test are present but inert.
    expect(html).toContain("Connect");
    expect(html).toContain("disabled");
  });

  it("shows status and expiry for a connected provider", () => {
    const html = renderToStaticMarkup(
      <ConnectionsPanel
        connections={[
          {
            provider: "github",
            label: "GitHub",
            status: "connected",
            expires_at: "2026-08-01T00:00:00Z",
          },
        ]}
      />,
    );
    expect(html).toContain("Connected");
    expect(html).toContain("2026-08-01T00:00:00Z");
  });

  it("navigates to the GitHub authorize URL when Connect is clicked", async () => {
    const fetchMock = mockFetch(200, {
      authorize_url: "https://github.com/login/oauth/authorize?state=xyz",
    });
    const assign = vi.fn();
    const originalLocation = window.location;
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { ...originalLocation, assign },
    });
    render(
      <ConnectionsPanel
        connections={[
          { provider: "github", label: "GitHub", status: "not_connected", expires_at: null },
        ]}
        onChanged={() => {}}
      />,
    );
    fireEvent.click(screen.getByText("Connect"));
    await waitFor(() =>
      expect(assign).toHaveBeenCalledWith(
        "https://github.com/login/oauth/authorize?state=xyz",
      ),
    );
    expect(fetchMock.mock.calls[0][0]).toBe("/api/oauth/github/start");
    Object.defineProperty(window, "location", {
      configurable: true,
      value: originalLocation,
    });
  });

  it("disconnects and refetches when Disconnect is clicked", async () => {
    const fetchMock = mockFetch(200, { status: "not_connected" });
    const onChanged = vi.fn();
    render(
      <ConnectionsPanel
        connections={[
          { provider: "github", label: "GitHub", status: "connected", expires_at: null },
        ]}
        onChanged={onChanged}
      />,
    );
    fireEvent.click(screen.getByText("Disconnect"));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("/api/oauth/github/disconnect");
    expect(init?.method).toBe("POST");
  });

  it("reports the Test result", async () => {
    mockFetch(200, { status: "live" });
    render(
      <ConnectionsPanel
        connections={[
          { provider: "github", label: "GitHub", status: "connected", expires_at: null },
        ]}
        onChanged={() => {}}
      />,
    );
    fireEvent.click(screen.getByText("Test"));
    await waitFor(() => expect(screen.getByText(/live/i)).toBeTruthy());
  });

  it("navigates to the Linear authorize URL when Connect is clicked", async () => {
    const fetchMock = mockFetch(200, {
      authorize_url: "https://linear.app/oauth/authorize?state=xyz",
    });
    const assign = vi.fn();
    const originalLocation = window.location;
    Object.defineProperty(window, "location", {
      configurable: true,
      value: { ...originalLocation, assign },
    });
    render(
      <ConnectionsPanel
        connections={[
          { provider: "linear", label: "Linear", status: "not_connected", expires_at: null },
        ]}
        onChanged={() => {}}
      />,
    );
    fireEvent.click(screen.getByText("Connect"));
    await waitFor(() =>
      expect(assign).toHaveBeenCalledWith("https://linear.app/oauth/authorize?state=xyz"),
    );
    expect(fetchMock.mock.calls[0][0]).toBe("/api/oauth/linear/start");
    Object.defineProperty(window, "location", {
      configurable: true,
      value: originalLocation,
    });
  });

  it("leaves unwired providers' buttons disabled", () => {
    render(
      <ConnectionsPanel
        connections={[
          { provider: "vertex", label: "Vertex", status: "not_connected", expires_at: null },
        ]}
        onChanged={() => {}}
      />,
    );
    expect((screen.getByText("Connect") as HTMLButtonElement).disabled).toBe(true);
  });

  it("opens the Codex device-auth flow and polls to connected", async () => {
    // Stays pending until the operator "finishes" on the provider site, so the
    // panel (URL + code) is observable before the poll loop clears it.
    let finished = false;
    const fn = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
      if (String(input).endsWith("/start")) {
        return new Response(
          JSON.stringify({
            verification_uri: "https://auth.openai.com/device",
            user_code: "WDJB-MJHT",
            login_session: "sess-c",
          }),
          { status: 200 },
        );
      }
      return new Response(
        JSON.stringify({ status: finished ? "connected" : "pending", expires_at: null }),
        { status: 200 },
      );
    });
    vi.stubGlobal("fetch", fn);
    const onChanged = vi.fn();
    render(
      <ConnectionsPanel
        connections={[
          { provider: "codex", label: "Codex", status: "not_connected", expires_at: null },
        ]}
        onChanged={onChanged}
      />,
    );
    // Codex is wired via device-auth, not a redirect.
    expect((screen.getByText("Connect") as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(screen.getByText("Connect"));
    // Surfaces the verification URL + user code for the operator, and holds
    // while the login is pending.
    await waitFor(() => {
      expect(screen.getByText(/auth\.openai\.com\/device/)).toBeTruthy();
      expect(screen.getByText("WDJB-MJHT")).toBeTruthy();
    });
    // Operator completes it; the next poll reports connected.
    finished = true;
    await waitFor(() => expect(onChanged).toHaveBeenCalled(), { timeout: 4000 });

    expect(fn.mock.calls[0][0]).toBe("/api/oauth/codex/start");
    const poll = fn.mock.calls.find((c) => String(c[0]).endsWith("/api/oauth/codex/poll"));
    expect(poll).toBeTruthy();
    const body = JSON.parse(String((poll?.[1] as RequestInit).body));
    expect(body.login_session).toBe("sess-c");
  });

  it("opens the Claude code-paste flow and submits the pasted code", async () => {
    const fn = vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
      if (String(input).endsWith("/start")) {
        return new Response(
          JSON.stringify({
            authorize_url: "https://claude.ai/oauth/authorize?code=1",
            login_session: "sess-1",
          }),
          { status: 200 },
        );
      }
      return new Response(JSON.stringify({ status: "connected", expires_at: null }), {
        status: 200,
      });
    });
    vi.stubGlobal("fetch", fn);
    const onChanged = vi.fn();
    render(
      <ConnectionsPanel
        connections={[
          { provider: "claude", label: "Claude", status: "not_connected", expires_at: null },
        ]}
        onChanged={onChanged}
      />,
    );
    // Claude is wired via code-paste, not a redirect.
    expect((screen.getByText("Connect") as HTMLButtonElement).disabled).toBe(false);
    fireEvent.click(screen.getByText("Connect"));
    await waitFor(() => expect(screen.getByText(/claude\.ai\/oauth/)).toBeTruthy());

    const input = screen.getByLabelText(/authorization code/i);
    fireEvent.change(input, { target: { value: "pasted-code" } });
    fireEvent.click(screen.getByText(/Submit/i));
    await waitFor(() => expect(onChanged).toHaveBeenCalled());

    expect(fn.mock.calls[0][0]).toBe("/api/oauth/claude/start");
    const submit = fn.mock.calls.find((c) =>
      String(c[0]).endsWith("/api/oauth/claude/submit-code"),
    );
    expect(submit).toBeTruthy();
    const body = JSON.parse(String((submit?.[1] as RequestInit).body));
    expect(body.code).toBe("pasted-code");
    expect(body.login_session).toBe("sess-1");
  });
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

  it("offers a clear-secret checkbox only when a secret is already set, and sends the loaded version", async () => {
    const fetchMock = mockFetch(200, record({ version: 5, webhook_secret_set: true }));
    const onSaved = vi.fn();
    render(
      <BindingForm
        binding={record({ webhook_secret_set: true, webhook_secret_version: 3 })}
        options={OPTIONS}
        onSaved={onSaved}
        onCancel={() => {}}
      />,
    );
    fireEvent.click(screen.getByLabelText("webhook_secret_clear"));
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.webhook_secret_clear).toBe(true);
    expect(sent.webhook_secret_version).toBe(3);
    expect(sent.payload.webhook_secret).toBeUndefined();
  });

  it("has no clear-secret checkbox when no secret is set yet", () => {
    render(
      <BindingForm
        binding={record({ webhook_secret_set: false })}
        options={OPTIONS}
        onSaved={() => {}}
        onCancel={() => {}}
      />,
    );
    expect(screen.queryByLabelText("webhook_secret_clear")).toBeNull();
  });

  it("hides the clear-secret checkbox and drops a pending clear once github_repo is edited", async () => {
    const fetchMock = mockFetch(200, record({ version: 5 }));
    const onSaved = vi.fn();
    render(
      <BindingForm
        binding={record({ webhook_secret_set: true, webhook_secret_version: 3 })}
        options={OPTIONS}
        onSaved={onSaved}
        onCancel={() => {}}
      />,
    );
    fireEvent.click(screen.getByLabelText("webhook_secret_clear"));
    fireEvent.change(screen.getByLabelText("github_repo"), {
      target: { value: "org/other-repo" },
    });
    expect(screen.queryByLabelText("webhook_secret_clear")).toBeNull();

    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.webhook_secret_clear).toBe(false);
  });

  it("sends the loaded repo-secret version when replacing an existing secret", async () => {
    const fetchMock = mockFetch(200, record({ version: 5, webhook_secret_set: true }));
    const onSaved = vi.fn();
    render(
      <BindingForm
        binding={record({ webhook_secret_set: true, webhook_secret_version: 3 })}
        options={OPTIONS}
        onSaved={onSaved}
        onCancel={() => {}}
      />,
    );
    fireEvent.change(screen.getByLabelText("webhook_secret"), {
      target: { value: "new-secret" },
    });
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.payload.webhook_secret).toBe("new-secret");
    expect(sent.webhook_secret_version).toBe(3);
  });

  it("does not send a repo-secret version on an edit that doesn't touch the secret", async () => {
    const fetchMock = mockFetch(200, record({ version: 5, webhook_secret_set: true }));
    const onSaved = vi.fn();
    render(
      <BindingForm
        binding={record({ webhook_secret_set: true, webhook_secret_version: 3 })}
        options={OPTIONS}
        onSaved={onSaved}
        onCancel={() => {}}
      />,
    );
    fireEvent.change(screen.getByLabelText("max_concurrent"), { target: { value: "6" } });
    fireEvent.click(screen.getByText("Save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalled());
    const sent = JSON.parse(fetchMock.mock.calls[0][1]?.body as string);
    expect(sent.webhook_secret_version).toBeUndefined();
    expect(sent.webhook_secret_clear).toBe(false);
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
            roles: { implement: { agent: "codex", model: "gpt-5.6-sol" } },
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
    ).toBe("gpt-5.6-sol");
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

  it("renders the blocker list, not the conflict banner, on a drain-guard 409 from a rename", async () => {
    mockFetch(409, {
      detail: {
        msg: "cannot rename a binding with active work",
        blockers: {
          running_runs: ["ENG-1"],
          open_prs: [],
          operator_waits: [],
          scheduled_slots: 0,
        },
      },
    });
    render(
      <BindingForm binding={record()} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.change(screen.getByLabelText("project_key"), { target: { value: "OTHER" } });
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() =>
      expect(screen.getByText(/active work must drain first/)).toBeTruthy(),
    );
    expect(screen.getByText(/running: ENG-1/)).toBeTruthy();
    expect(screen.queryByText(/Edit conflict/)).toBeNull();
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

  it("includes disabled rows in the reorder write set", async () => {
    const fetchMock = mockFetch(200, record());
    const onChanged = vi.fn();
    render(
      <BindingsPanel
        bindings={[
          record({ id: 1, priority: 0, version: 2, github_repo: "org/aaa" }),
          record({
            id: 2,
            priority: 1,
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
    // Disabled rows are valid config now (SYM-193) and the drain guard
    // exempts priority-only edits, so both rows renumber and both writes go
    // out — otherwise the disabled row's stale priority snaps back on refetch.
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/config/bindings/1");
    expect(fetchMock.mock.calls[1][0]).toBe("/api/config/bindings/2");
  });

  it("threads globalRoles down into the binding's role matrix editor", () => {
    // implement's agent cell is left inherited on the binding; only the
    // global matrix pins it to codex. The binding form must still see that,
    // or its inherited rows offer the wrong family's models.
    render(
      <BindingsPanel
        bindings={[]}
        options={OPTIONS}
        globalRoles={{ implement: { agent: "codex" } }}
        onChanged={() => {}}
      />,
    );
    fireEvent.click(screen.getByText("New binding"));
    const model = screen.getByLabelText("binding implement model") as HTMLSelectElement;
    expect([...model.options].map((o) => o.value)).toEqual(["", ...OPTIONS.codex_models]);
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

  it("varies Codex effort options by the selected model", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "codex", model: "gpt-5.6-sol" } }}
        options={OPTIONS}
        onChange={() => undefined}
      />,
    );

    const effort = screen.getByLabelText("binding implement effort") as HTMLSelectElement;
    expect([...effort.options].map((option) => option.value)).toEqual([
      "",
      "low",
      "medium",
      "high",
      "xhigh",
      "max",
      "ultra",
    ]);
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

  it("edits only the touched row — no cell is mirrored onto another role", () => {
    // Every role's agent/model/effort now reaches its own dispatched command,
    // so nothing needs bridging through a sibling row: picking codex for
    // `implement` must leave `fix`/`accept` inherited rather than silently
    // pinning them (the pre-SYM-191-cleanup behavior).
    let roles: RolesMatrix = {};
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
    expect(roles).toEqual({ implement: { agent: "codex" } });
  });

  it("offers the resolved family's models for an inherited agent cell", () => {
    // `review_find` defaults to the opposite of the resolved `implement`
    // family (`resolved_reviewer_agent`), `review_verify` to implement's own
    // — so with implement pinned to codex, the two review rows offer
    // different families even though both agent cells are inherited.
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "codex" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const modelOpts = (role: string) =>
      [
        ...(screen.getByLabelText(`binding ${role} model`) as HTMLSelectElement).options,
      ].map((o) => o.value);
    expect(modelOpts("review_find")).toEqual(["", ...OPTIONS.claude_aliases]);
    expect(modelOpts("review_verify")).toEqual(["", ...OPTIONS.codex_models]);
    // Builder roles fall back to the binding's legacy `agent`, always the
    // "claude" default for a DB-managed binding.
    expect(modelOpts("fix")).toEqual(["", ...OPTIONS.claude_aliases]);
    expect(modelOpts("accept")).toEqual(["", ...OPTIONS.claude_aliases]);
  });

  it("names the family an inherited agent cell resolves to", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "codex" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const inheritLabel = (role: string) =>
      (screen.getByLabelText(`binding ${role} agent`) as HTMLSelectElement).options[0]
        .textContent;
    expect(inheritLabel("review_verify")).toBe("inherit (codex)");
    expect(inheritLabel("review_find")).toBe("inherit (claude)");
    // A pinned cell still describes what picking inherit would land on, not
    // the pin itself.
    expect(inheritLabel("implement")).toBe("inherit (claude)");
  });

  it("names the value a model/effort cell inherits from the global matrix", () => {
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{}}
        globalRoles={{ implement: { model: "opus", effort: "high" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const first = (label: string) =>
      (screen.getByLabelText(label) as HTMLSelectElement).options[0].textContent;
    expect(first("binding implement model")).toBe("inherit (opus)");
    expect(first("binding implement effort")).toBe("inherit (high)");
  });

  it("does not advertise an inherited value from the other family", () => {
    // The server drops a global cell's model/effort when the binding pins a
    // different agent (`resolved_role`'s family boundary), so hinting the
    // Claude model here would lie about what the row resolves to.
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "codex" } }}
        globalRoles={{ implement: { agent: "claude", model: "opus" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    expect(
      (screen.getByLabelText("binding implement model") as HTMLSelectElement).options[0]
        .textContent,
    ).toBe("inherit");
  });

  it("offers a role's effort options for the model it inherits", () => {
    // The binding pins no model; the global matrix's gpt-5.5 (per OPTIONS,
    // no max/ultra) is what the row runs with, so its efforts are what the
    // cell may offer.
    render(
      <RoleMatrixEditor
        scope="binding"
        roles={{ implement: { agent: "codex" } }}
        globalRoles={{ implement: { agent: "codex", model: "gpt-5.5" } }}
        options={OPTIONS}
        onChange={() => {}}
      />,
    );
    const effort = screen.getByLabelText("binding implement effort") as HTMLSelectElement;
    expect([...effort.options].map((o) => o.value)).toEqual([
      "",
      "low",
      "medium",
      "high",
      "xhigh",
    ]);
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

  it("offers the resolved family's models when the agent is inherited", () => {
    // An inherited `implement` resolves to the binding's legacy `agent`
    // default (claude), so offering codex models here would let the operator
    // save a pair the server's family check rejects.
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    const model = screen.getByLabelText("binding implement model") as HTMLSelectElement;
    expect(model.value).toBe("");
    expect(model.disabled).toBe(false);
    expect([...model.options].map((o) => o.value)).toEqual(["", ...OPTIONS.claude_aliases]);
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

  it("exposes agent, model and effort for every role", () => {
    // All 15 cells reach their role's dispatched command (see the
    // `RoleMatrixEditor` header comment), so none is hidden or read-only.
    render(
      <RoleMatrixEditor scope="binding" roles={{}} options={OPTIONS} onChange={() => {}} />,
    );
    for (const role of ["implement", "review_find", "review_verify", "fix", "accept"]) {
      for (const field of ["agent", "model", "effort"]) {
        const cell = screen.getByLabelText(`binding ${role} ${field}`) as HTMLSelectElement;
        expect(cell.disabled).toBe(false);
      }
    }
    expect(screen.queryByText("not used")).toBeNull();
  });

  it("edits any role's effort independently", () => {
    let roles: RolesMatrix = {};
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
    fireEvent.change(screen.getByLabelText("binding accept effort"), {
      target: { value: "low" },
    });
    expect(roles).toEqual({ accept: { effort: "low" } });
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
      if (url === "/api/connections" && method === "GET") {
        return new Response(JSON.stringify(CONNECTIONS), { status: 200 });
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
    // The Connections section renders its four provider cards from the API.
    await screen.findByText("GitHub");
    for (const label of ["Linear", "Claude", "Codex"]) {
      expect(screen.getByText(label)).toBeTruthy();
    }

    fireEvent.click(screen.getByText("Save global matrix"));
    // With `staleTime: Infinity`, a remount after a save would otherwise re-seed
    // from the pre-save version and spuriously 409 the next save — asserting
    // the GET fires again (not just the resolved-view GET) is the regression
    // check for that.
    await waitFor(() => expect(rolesGetCalls).toBe(2));
  });
});
