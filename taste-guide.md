# Symphony Taste Guide

## Principles

Acceptance should protect the user's requested product experience, not just the
presence of code paths. Prefer shipped behavior that is explicit, polished, and
native to the repo's existing UI language. When a ticket asks for a visual or
interaction detail, treat degraded stand-ins as failed work even when they are
technically functional.

## Hard rules (acceptance must reject if violated)

- Do not accept placeholder UI, raw component names, debug labels, or fallback
  text where a user-facing visual asset, icon, image, or control was requested.
- Do not accept work that removes or weakens behavior explicitly requested by
  the Linear ticket in order to make the patch smaller.
- Do not accept hidden or unreachable implementations of user-facing features.

## Known past mistakes

- VIB icon ticket: the implementation rendered inline text where an `<Icon/>`
  component was intended, so the UI showed the icon name/fallback text instead
  of an actual icon.
