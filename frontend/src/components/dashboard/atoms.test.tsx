import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { TokenSplit } from "@/lib/api";

import { MixBar } from "./atoms";

describe("MixBar", () => {
  it("sizes segments by the row's own raw-token proportions", () => {
    const split: TokenSplit = {
      input_tokens: 25,
      output_tokens: 25,
      cache_write_tokens: 25,
      cache_read_tokens: 25,
    };
    const markup = renderToStaticMarkup(<MixBar split={split} />);
    // Four equal segments → 25% each, no segment encodes a hidden total length.
    expect(markup.match(/width:25%/g)).toHaveLength(4);
  });

  it("encodes proportions, not magnitude (small and large rows look identical)", () => {
    const small: TokenSplit = {
      input_tokens: 1,
      output_tokens: 3,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
    };
    const large: TokenSplit = {
      input_tokens: 1_000_000,
      output_tokens: 3_000_000,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
    };
    const widths = (markup: string) => markup.match(/width:[\d.]+%/g);
    const smallBar = renderToStaticMarkup(<MixBar split={small} />);
    const largeBar = renderToStaticMarkup(<MixBar split={large} />);
    // Same proportions → identical segment geometry regardless of magnitude.
    expect(widths(smallBar)).toEqual(widths(largeBar));
    expect(widths(smallBar)).toEqual(["width:25%", "width:75%"]);
  });

  it("omits zero segments and renders an empty bar at zero total", () => {
    const markup = renderToStaticMarkup(
      <MixBar
        split={{
          input_tokens: 0,
          output_tokens: 0,
          cache_write_tokens: 0,
          cache_read_tokens: 0,
        }}
      />,
    );
    expect(markup).not.toContain("width:");
  });
});
