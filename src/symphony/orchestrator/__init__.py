"""Top-level orchestrator: poll loop, scan, scheduler, review.

`poll.py` is the one always-running task. Everything else is invoked from
the poll cycle. See docs/python-port-research.md §8 for the asyncio shape
and §13.2 for the eight concerns inside review.py.
"""
