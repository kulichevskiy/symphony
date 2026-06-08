import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import { FiltersProvider, isDefaultDate, useFilters } from "./filters";

function Probe() {
  const { provider, teams, models, date } = useFilters();
  const dateStr = isDefaultDate(date)
    ? "-"
    : date.kind === "preset"
      ? date.preset
      : "custom";
  return (
    <span data-testid="probe">
      {`${provider}|${teams.join(",")}|${models.join(",")}|${dateStr}`}
    </span>
  );
}

function render(initialEntry: string): string {
  return renderToStaticMarkup(
    <MemoryRouter initialEntries={[initialEntry]}>
      <FiltersProvider>
        <Probe />
      </FiltersProvider>
    </MemoryRouter>,
  );
}

describe("FiltersProvider / useFilters", () => {
  it("reads the provider from the URL on load", () => {
    expect(render("/?provider=codex")).toContain("codex|||-");
  });

  it("defaults to 'all' with no params", () => {
    expect(render("/")).toContain("all|||-");
  });

  it("parses teams and date from the URL", () => {
    expect(render("/?teams=VIB,ADJ&dates=7d")).toContain("all|VIB,ADJ||7d");
  });

  it("parses provider-qualified models from the URL", () => {
    expect(render("/?models=claude:opus-4.1,codex:gpt-5-codex")).toContain(
      "all||claude:opus-4.1,codex:gpt-5-codex|-",
    );
  });
});
