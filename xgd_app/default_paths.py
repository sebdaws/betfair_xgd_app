"""Load optional external-project paths from app_data/default_paths.json."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any


APP_DIR = Path(__file__).resolve().parents[1]
DEFAULT_PATHS_FILE = APP_DIR / "app_data" / "default_paths.json"


def _to_path(value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    path = Path(text).expanduser()
    if not path.is_absolute():
        path = (APP_DIR / path).resolve()
    else:
        path = path.resolve()
    return path


@lru_cache(maxsize=1)
def load_external_paths(path_file: Path = DEFAULT_PATHS_FILE) -> dict[str, Path]:
    if not path_file.exists():
        return {}
    try:
        payload = json.loads(path_file.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    external = payload.get("external_paths")
    if not isinstance(external, dict):
        return {}

    out: dict[str, Path] = {}
    for key, raw_value in external.items():
        path = _to_path(raw_value)
        if path is None:
            continue
        out[str(key)] = path
    return out


def get_external_path(key: str) -> Path | None:
    return load_external_paths().get(str(key))

