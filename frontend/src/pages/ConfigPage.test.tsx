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

import type { BindingRecord, ConfigOptions, ConfigView } from "@/lib/api";
import { registerTokenProvider } from "@/lib/auth";

import { BindingForm, BindingsPanel, ConfigDetails } from "./ConfigPage";

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
  merge_strategies: ["squash", "merge", "rebase"],
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

  it("renders a 422 validation error on the exact field", async () => {
    mockFetch(422, { detail: [{ loc: ["project_key"], msg: "field required" }] });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() => expect(screen.getByText("field required")).toBeTruthy());
  });

  it("renders a 422 roles error under the advanced section with its path", async () => {
    mockFetch(422, { detail: [{ loc: ["roles"], msg: "unknown Codex model 'x'" }] });
    render(
      <BindingForm binding={null} options={OPTIONS} onSaved={() => {}} onCancel={() => {}} />,
    );
    fireEvent.click(screen.getByText("Save"));
    await waitFor(() =>
      expect(screen.getByText("roles: unknown Codex model 'x'")).toBeTruthy(),
    );
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
          record({ id: 1, priority: 0, version: 2 }),
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
});
