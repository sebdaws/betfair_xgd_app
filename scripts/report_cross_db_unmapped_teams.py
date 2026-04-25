#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

# Allow running this script from inside ./scripts while importing project modules.
PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

from xgd_app.core import DEFAULT_MANUAL_TEAM_MAPPINGS, source_specific_app_data_path
from xgd_app.data.sofascore_loader import _normalize_team_key


@dataclass(frozen=True)
class TeamRecord:
    source_name: str
    source_norm: str
    mapped_raw_name: str
    canonical_raw_norm: str
    via_manual_mapping: bool


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Report teams that do not map across two databases "
            "(for example FotMob primary vs SofaScore events DB)."
        )
    )
    parser.add_argument(
        "--primary-db",
        required=True,
        help="Primary DB path (typically the app --db-path, e.g. fotmob_local.db).",
    )
    parser.add_argument(
        "--events-db",
        required=True,
        help="Events DB path (typically the app --match-events-db-path, e.g. sofascore_local.db).",
    )
    parser.add_argument(
        "--primary-team-mappings",
        default="",
        help=(
            "Optional team mapping JSON for primary DB. "
            "Defaults to app_data/manual_team_mappings.<primary_db_stem>.json"
        ),
    )
    parser.add_argument(
        "--events-team-mappings",
        default="",
        help=(
            "Optional team mapping JSON for events DB. "
            "Defaults to app_data/manual_team_mappings.<events_db_stem>.json"
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Max rows shown per side in text mode (default: 200).",
    )
    parser.add_argument(
        "--min-similarity",
        type=float,
        default=0.65,
        help="Minimum similarity for suggested counterpart names (default: 0.65).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON report instead of text.",
    )
    parser.add_argument(
        "--name-contains",
        default="",
        help="Optional substring filter for team names (for example: mainz).",
    )
    return parser.parse_args()


def resolve_db_path(path_value: str) -> Path:
    raw = Path(str(path_value or "").strip()).expanduser()
    path = raw.resolve() if raw.is_absolute() else (Path.cwd() / raw).resolve()
    if not path.exists():
        raise FileNotFoundError(f"DB file not found: {path}")
    return path


def resolve_mapping_path(explicit_path: str, db_path: Path) -> Path:
    if str(explicit_path or "").strip():
        raw = Path(explicit_path).expanduser()
        path = raw.resolve() if raw.is_absolute() else (Path.cwd() / raw).resolve()
        return path
    return source_specific_app_data_path(DEFAULT_MANUAL_TEAM_MAPPINGS, db_path)


def load_source_to_raw_team_lookup(mapping_path: Path) -> dict[str, str]:
    if not mapping_path.exists():
        return {}
    try:
        payload = json.loads(mapping_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}

    out: dict[str, str] = {}
    for raw_name, source_name in payload.items():
        raw_text = str(raw_name or "").strip()
        source_text = str(source_name or "").strip()
        source_norm = _normalize_team_key(source_text)
        if not raw_text or not source_norm:
            continue
        out.setdefault(source_norm, raw_text)
    return out


def load_match_team_names(db_path: Path) -> list[str]:
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            """
            SELECT DISTINCT t.name
            FROM teams t
            JOIN matches m
              ON m.home_team_id = t.id OR m.away_team_id = t.id
            WHERE t.name IS NOT NULL AND TRIM(t.name) <> ''
            ORDER BY t.name
            """
        ).fetchall()
    finally:
        conn.close()
    return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]


def build_team_records(team_names: list[str], source_to_raw_lookup: dict[str, str]) -> list[TeamRecord]:
    records: list[TeamRecord] = []
    seen: set[tuple[str, str]] = set()
    for source_name in team_names:
        source_text = str(source_name or "").strip()
        source_norm = _normalize_team_key(source_text)
        if not source_text or not source_norm:
            continue
        mapped_raw = str(source_to_raw_lookup.get(source_norm, source_text)).strip()
        canonical_raw_norm = _normalize_team_key(mapped_raw)
        if not canonical_raw_norm:
            continue
        key = (source_text.casefold(), canonical_raw_norm)
        if key in seen:
            continue
        seen.add(key)
        records.append(
            TeamRecord(
                source_name=source_text,
                source_norm=source_norm,
                mapped_raw_name=mapped_raw,
                canonical_raw_norm=canonical_raw_norm,
                via_manual_mapping=bool(mapped_raw.casefold() != source_text.casefold()),
            )
        )
    return records


def score_name_similarity(left_norm: str, right_norm: str) -> float:
    seq = SequenceMatcher(None, left_norm, right_norm).ratio()
    left_tokens = {token for token in left_norm.split() if token}
    right_tokens = {token for token in right_norm.split() if token}
    overlap = 0.0
    if left_tokens and right_tokens:
        overlap = len(left_tokens & right_tokens) / len(left_tokens | right_tokens)
    return (0.7 * seq) + (0.3 * overlap)


def best_suggestions(
    source_norm: str,
    candidates: list[TeamRecord],
    min_similarity: float,
    limit: int = 3,
) -> list[dict[str, Any]]:
    scored: list[tuple[float, TeamRecord]] = []
    for row in candidates:
        score = score_name_similarity(source_norm, row.source_norm)
        if score < float(min_similarity):
            continue
        scored.append((score, row))
    scored.sort(key=lambda item: (-item[0], item[1].source_name.lower()))
    out: list[dict[str, Any]] = []
    for score, row in scored[: max(1, int(limit))]:
        out.append(
            {
                "name": row.source_name,
                "mapped_raw": row.mapped_raw_name,
                "score": round(float(score), 3),
            }
        )
    return out


def build_unmapped_report(
    source_records: list[TeamRecord],
    target_records: list[TeamRecord],
    min_similarity: float,
    name_contains: str = "",
) -> list[dict[str, Any]]:
    name_filter = str(name_contains or "").strip().casefold()
    target_canonical_keys = {row.canonical_raw_norm for row in target_records}
    unmapped: list[dict[str, Any]] = []
    for row in source_records:
        if row.canonical_raw_norm in target_canonical_keys:
            continue
        if name_filter and (name_filter not in row.source_name.casefold()) and (name_filter not in row.mapped_raw_name.casefold()):
            continue
        unmapped.append(
            {
                "source_name": row.source_name,
                "mapped_raw": row.mapped_raw_name,
                "canonical_raw_norm": row.canonical_raw_norm,
                "via_manual_mapping": row.via_manual_mapping,
                "suggestions": best_suggestions(
                    source_norm=row.source_norm,
                    candidates=target_records,
                    min_similarity=min_similarity,
                    limit=3,
                ),
            }
        )
    unmapped.sort(key=lambda item: str(item.get("source_name", "")).lower())
    return unmapped


def render_text_section(title: str, rows: list[dict[str, Any]], limit: int) -> str:
    lines: list[str] = [title]
    if not rows:
        lines.append("  (none)")
        return "\n".join(lines)

    shown = rows[: max(1, int(limit))]
    for row in shown:
        source_name = str(row.get("source_name", "")).strip()
        mapped_raw = str(row.get("mapped_raw", "")).strip()
        via_manual = bool(row.get("via_manual_mapping"))
        marker = "manual" if via_manual else "auto/self"
        lines.append(f"  - {source_name} -> {mapped_raw} ({marker})")
        suggestions = row.get("suggestions", [])
        if isinstance(suggestions, list) and suggestions:
            sug_parts = [
                f"{str(s.get('name', '')).strip()} ({s.get('score', 0):.3f})"
                for s in suggestions
                if isinstance(s, dict) and str(s.get("name", "")).strip()
            ]
            if sug_parts:
                lines.append(f"    suggestions: {', '.join(sug_parts)}")
    if len(rows) > len(shown):
        lines.append(f"  ... {len(rows) - len(shown)} more not shown (raise --limit)")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    primary_db = resolve_db_path(args.primary_db)
    events_db = resolve_db_path(args.events_db)
    primary_mapping_path = resolve_mapping_path(args.primary_team_mappings, primary_db)
    events_mapping_path = resolve_mapping_path(args.events_team_mappings, events_db)

    primary_lookup = load_source_to_raw_team_lookup(primary_mapping_path)
    events_lookup = load_source_to_raw_team_lookup(events_mapping_path)
    primary_teams = load_match_team_names(primary_db)
    events_teams = load_match_team_names(events_db)
    primary_records = build_team_records(primary_teams, primary_lookup)
    events_records = build_team_records(events_teams, events_lookup)

    primary_unmapped = build_unmapped_report(
        source_records=primary_records,
        target_records=events_records,
        min_similarity=float(args.min_similarity),
        name_contains=args.name_contains,
    )
    events_unmapped = build_unmapped_report(
        source_records=events_records,
        target_records=primary_records,
        min_similarity=float(args.min_similarity),
        name_contains=args.name_contains,
    )

    payload = {
        "primary_db": str(primary_db),
        "events_db": str(events_db),
        "primary_team_mappings_path": str(primary_mapping_path),
        "events_team_mappings_path": str(events_mapping_path),
        "primary_team_count": len(primary_records),
        "events_team_count": len(events_records),
        "unmapped_from_primary_count": len(primary_unmapped),
        "unmapped_from_events_count": len(events_unmapped),
        "unmapped_from_primary": primary_unmapped,
        "unmapped_from_events": events_unmapped,
    }

    if args.json:
        print(json.dumps(payload, indent=2, ensure_ascii=True))
        return 0

    print(f"Primary DB: {primary_db}")
    print(f"Events DB:  {events_db}")
    print(f"Primary mappings: {primary_mapping_path}")
    print(f"Events mappings:  {events_mapping_path}")
    print()
    print(render_text_section("Unmapped from primary -> events", primary_unmapped, int(args.limit)))
    print()
    print(render_text_section("Unmapped from events -> primary", events_unmapped, int(args.limit)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
