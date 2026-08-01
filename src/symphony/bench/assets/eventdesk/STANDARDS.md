# Engineering standards

- Preserve public API behavior unless a ticket explicitly changes it.
- Put business invariants in backend transactions, not only in UI validation.
- Store timestamps as timezone-aware UTC ISO-8601 strings.
- Validate inputs at the HTTP boundary and use explicit status codes.
- Keep SQLite access parameterized and schema initialization backward-compatible.
- Add focused tests for success, failure, persistence, and races required by the ticket.
- Type-check Python and TypeScript. Do not use `Any`, disabled checks, or swallowed exceptions to
  make CI pass.
- Never commit credentials, generated databases, build output, or dependency directories.
