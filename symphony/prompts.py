"""Jinja2 rendering helpers. Real templates land in #3 (round1) and #4 (review)."""

from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined


def make_env(prompts_dir: Path) -> Environment:
    return Environment(
        loader=FileSystemLoader(str(prompts_dir)),
        undefined=StrictUndefined,
        keep_trailing_newline=True,
    )


def render(env: Environment, template_name: str, context: dict) -> str:
    return env.get_template(template_name).render(**context)
