"""CLI entrypoints and argument parsing."""

from xgd_app.core import parse_args
from xgd_app.runtime import main

__all__ = ["main", "parse_args"]
