import { renderToStaticMarkup } from "react-dom/server";
import { MemoryRouter } from "react-router";
import { describe, expect, it } from "vitest";

import { FiltersProvider } from "@/lib/filters";

import { buildMonthGrid, DateFilter } from "./DateFilter";

function render(initialEntry: string): string {
  return renderToStaticMarkup(
    <MemoryRouter initialEntries={[initialEntry]}>
      <FiltersProvider>
        <DateFilter />
      </FiltersProvider>
    </MemoryRouter>,
  );
}

describe("DateFilter trigger", () => {
  it("reads the default (all-time) as an inactive chip", () => {
    const markup = render("/");
    expect(markup).toContain("Date");
    expect(markup).toContain("12 months");
    // Default → inactive styling (muted, not the raised active background).
    expect(markup).toContain("bg-secondary/60");
  });

  it("reflects a preset from the URL as an active chip", () => {
    const markup = render("/?dates=7d");
    expect(markup).toContain("7 days");
    expect(markup).toContain("bg-background");
  });

  it("reflects a custom range from the URL", () => {
    const markup = render("/?dates=custom&from=2026-01-01&to=2026-03-01");
    expect(markup).toContain("2026-01-01 → 2026-03-01");
  });
});

describe("buildMonthGrid", () => {
  it("pads to whole Sun..Sat weeks and lays out every day of the month", () => {
    // May 2026: May 1 is a Friday; 31 days.
    const weeks = buildMonthGrid(2026, 4);
    expect(weeks.every((w) => w.length === 7)).toBe(true);
    // Leading pad: Sun..Thu are null before Fri May 1.
    expect(weeks[0].slice(0, 5)).toEqual([null, null, null, null, null]);
    expect(weeks[0][5]).toBe("2026-05-01");
    const days = weeks.flat().filter(Boolean);
    expect(days[0]).toBe("2026-05-01");
    expect(days[days.length - 1]).toBe("2026-05-31");
    expect(days).toHaveLength(31);
  });
});
