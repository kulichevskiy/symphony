"""GitHub I/O — thin wrapper over the local `gh` CLI.

All GitHub access in Symphony routes through `GitHub` so that argv
changes between `gh` versions get caught in one place and tests can
fake the wrapper rather than mocking subprocesses.
"""

from .client import CheckRun, GitHub, GitHubError, PRChecks

__all__ = ["CheckRun", "GitHub", "GitHubError", "PRChecks"]
