import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { FilterTrigger, Popover } from "./Popover";

describe("FilterTrigger", () => {
  it("renders its label and an active value summary", () => {
    const markup = renderToStaticMarkup(
      <FilterTrigger label="Team" value="VIB +1" active />,
    );
    expect(markup).toContain("Team");
    expect(markup).toContain("VIB +1");
    expect(markup).toContain('aria-expanded="false"');
  });
});

describe("Popover", () => {
  it("renders the trigger but keeps the panel closed by default", () => {
    const markup = renderToStaticMarkup(
      <Popover trigger={({ toggle }) => <FilterTrigger label="Team" onClick={toggle} />}>
        <div>panel-body</div>
      </Popover>,
    );
    expect(markup).toContain("Team");
    expect(markup).not.toContain("panel-body");
  });
});
