import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { AccessDenied } from "./auth0";

describe("AccessDenied", () => {
  it("shows an access-denied message with a sign-out action", () => {
    const markup = renderToStaticMarkup(<AccessDenied onSignOut={() => {}} />);
    expect(markup).toContain("Access denied");
    expect(markup).toContain("allowlist");
    expect(markup).toContain("Sign out");
  });
});
