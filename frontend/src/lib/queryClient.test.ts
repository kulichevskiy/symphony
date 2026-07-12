import { describe, expect, it } from "vitest";

import { queryClient } from "./queryClient";

describe("queryClient defaults", () => {
  it("sets a small default staleTime so a tab refocus within it refires nothing", () => {
    // ~6 dashboard queries poll on their own intervals; without a staleTime a
    // refocus marks them all stale and refetches everything just-polled.
    expect(queryClient.getDefaultOptions().queries?.staleTime).toBe(5_000);
  });
});
