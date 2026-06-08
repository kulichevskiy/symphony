import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import type { TokenSplit } from "@/lib/api";

import { MixBar, PROVIDER_TINT } from "./atoms";

describe("token palette", () => {
  it("colors the four mix segments with the v2 token palette", () => {
    const split: TokenSplit = {
      input_tokens: 1,
      output_tokens: 1,
      cache_write_tokens: 1,
      cache_read_tokens: 1,
    };
    const markup = renderToStaticMarkup(<MixBar split={split} />);
    // input=blue, output=violet, cache-write=cyan, cache-read=slate.
    expect(markup).toContain("bg-blue-500");
    expect(markup).toContain("bg-violet-500");
    expect(markup).toContain("bg-cyan-500");
    expect(markup).toContain("bg-slate-300");
    // No leftovers from the old in=sky / out=emerald / cache-write=amber palette.
    expect(markup).not.toContain("bg-sky-500");
    expect(markup).not.toContain("bg-emerald-500");
    expect(markup).not.toContain("bg-amber-500");
  });

  it("maps provider dots to the v2 provider palette", () => {
    // codex=blue, claude=violet (replacing codex=sky / claude=orange).
    expect(PROVIDER_TINT.codex).toBe("bg-blue-500");
    expect(PROVIDER_TINT.claude).toBe("bg-violet-500");
  });
});

describe("MixBar", () => {
  it("sizes segments by the row's own raw-token proportions", () => {
    const split: TokenSplit = {
      input_tokens: 25,
      output_tokens: 25,
      cache_write_tokens: 25,
      cache_read_tokens: 25,
    };
    const markup = renderToStaticMarkup(<MixBar split={split} />);
    // Four equal segments → 25% each.
    expect(markup.match(/width:25%/g)).toHaveLength(4);
  });

  it("defaults to composition: small and large rows fill the full track", () => {
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
    // Same proportions → identical geometry: full-width fill + 25/75 segments.
    expect(widths(smallBar)).toEqual(widths(largeBar));
    expect(widths(smallBar)).toEqual(["width:100%", "width:25%", "width:75%"]);
  });

  it("magnitude mode scales the bar's total length by tokens vs maxTotal", () => {
    const half: TokenSplit = {
      input_tokens: 25,
      output_tokens: 25,
      cache_write_tokens: 0,
      cache_read_tokens: 0,
    };
    const markup = renderToStaticMarkup(
      <MixBar split={half} mode="magnitude" maxTotal={100} />,
    );
    // 50 of 100 total → 50% length, with the 50/50 segment split inside.
    expect(markup).toContain("width:50%");
    expect(markup.match(/width:50%/g)).toHaveLength(3);
  });

  it("omits zero segments at zero total (no colored fill)", () => {
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
    expect(markup).not.toContain("bg-blue-500");
    expect(markup).not.toContain("bg-violet-500");
    expect(markup).not.toContain("bg-cyan-500");
  });
});
