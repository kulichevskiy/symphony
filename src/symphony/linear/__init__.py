"""Linear API client and inbound poll loop.

Linear is the user-facing control plane in the Python port (see
docs/python-port-research.md §2). This package owns:

- Outbound: every status comment + state transition Symphony emits.
- Inbound: comment-driven slash commands and state-driven dispatch.
"""
