import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { App } from "./App";

describe("Support Queue seed", () => {
  it("renders the application heading", () => {
    render(<App />);
    expect(screen.getByRole("heading", { name: "Support Queue" })).toBeInTheDocument();
  });
});
