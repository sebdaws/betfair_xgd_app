#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import re
import sqlite3
import sys
import threading
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd


APP_DIR = Path(__file__).resolve().parent
WORKSPACE_DIR = APP_DIR.parent
WEBAPP_DIR = APP_DIR / "webapp"

DEFAULT_SOFASCORE_DB = APP_DIR / "sofascore_local.db"
_fallback_db = WORKSPACE_DIR / "Sofascore_scraper" / "sofascore_local.db"
if (not DEFAULT_SOFASCORE_DB.exists()) and _fallback_db.exists():
    DEFAULT_SOFASCORE_DB = _fallback_db

DEFAULT_SELECTED_LEAGUES = APP_DIR / "selected_leagues.txt"
DEFAULT_ALL_LEAGUES = APP_DIR / "all_leagues.txt"

FINISHED_STATUSES = {
    "ended",
    "after penalties",
    "after extra time",
    "aet",
    "penalties",
    "ft",
}

REQUIRED_SOFASCORE_TABLES = {
    "matches",
    "teams",
    "competitions",
    "seasons",
    "match_stats",
    "match_shots",
    "stat_definitions",
}

TOKEN_REPLACEMENTS = {
    "utd": "united",
    "munchen": "munich",
    "st": "saint",
}

STOP_WORDS = {
    "fc",
    "cf",
    "ac",
    "sc",
    "afc",
    "club",
    "de",
    "the",
}

TOP5_LEAGUE_TARGETS = [
    {
        "label": "Premier League",
        "aliases": ["english premier league", "premier league"],
    },
    {
        "label": "La Liga",
        "aliases": ["spanish la liga", "la liga"],
    },
    {
        "label": "Serie A",
        "aliases": ["italian serie a", "serie a"],
    },
    {
        "label": "Bundesliga",
        "aliases": ["german bundesliga", "bundesliga"],
    },
    {
        "label": "Ligue 1",
        "aliases": ["french ligue 1", "ligue 1"],
    },
]

ASIAN_GOAL_MARKET_TYPES = [
    "ALT_TOTAL_GOALS",
]

GOAL_MARKET_NAME_RE = re.compile(
    r"(?:over\s*/\s*under|goal\s*line|total\s*goals?)\s*([0-9]+(?:\.[0-9]+)?)",
    flags=re.IGNORECASE,
)
GOAL_MARKET_TYPE_RE = re.compile(r"OVER_UNDER_(\d+)$", flags=re.IGNORECASE)
GOAL_RUNNER_RE = re.compile(r"\b(?:over|under)\s*([0-9]+(?:\.[0-9]+)?)\b", flags=re.IGNORECASE)
NON_GOAL_MARKET_TERMS = {
    "corner",
    "booking",
    "bookings",
    "card",
    "cards",
    "offside",
    "offsides",
    "throw",
    "shots",
    "shot",
    "foul",
    "penalty",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Betfair + SofaScore xGD web app")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8090)
    p.add_argument("--config-module", default="betfair_credentials")
    p.add_argument("--username")
    p.add_argument("--password")
    p.add_argument("--app-key")
    p.add_argument("--session-token")
    p.add_argument("--cert-file")
    p.add_argument("--key-file")
    p.add_argument("--max-markets", type=int, default=1000)
    p.add_argument("--horizon-days", type=int, default=0)
    p.add_argument("--market-types", default="ASIAN_HANDICAP")
    p.add_argument("--refresh-seconds", type=int, default=45)
    p.add_argument("--db-path", default=str(DEFAULT_SOFASCORE_DB))
    p.add_argument("--periods", default="Season,5,3")
    p.add_argument("--min-games", type=int, default=3)
    return p.parse_args()


def load_module_from_path(path: Path, module_name: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load module from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        sys.modules.pop(module_name, None)
        raise
    return module


def parse_list_csv(value: str) -> list[str]:
    return [item.strip() for item in str(value).split(",") if item.strip()]


def parse_periods(raw: str) -> tuple[Any, ...]:
    out: list[Any] = []
    for part in str(raw).split(","):
        value = part.strip()
        if not value:
            continue
        if value.lower() == "season":
            out.append("Season")
            continue
        out.append(int(value))
    if not out:
        return ("Season", 5, 3)
    return tuple(out)


def parse_iso_utc(value: Any) -> pd.Timestamp | pd.NaT:
    if value is None:
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    try:
        return pd.to_datetime(text, utc=True)
    except Exception:
        return pd.NaT


def split_event_teams(event_name: str | None) -> tuple[str | None, str | None]:
    if not event_name:
        return None, None
    separators = [" v ", " vs ", " @ "]
    lowered = event_name.lower()
    for sep in separators:
        idx = lowered.find(sep)
        if idx == -1:
            continue
        left = event_name[:idx].strip()
        right = event_name[idx + len(sep) :].strip()
        if left and right:
            return left, right
    return None, None


def normalize_runner_name(value: str | None) -> str:
    return normalize_team_name(value)


def runner_name_matches(candidate: str | None, target: str | None) -> bool:
    c = normalize_runner_name(candidate)
    t = normalize_runner_name(target)
    if not c or not t:
        return False
    return c == t or c in t or t in c


def parse_handicap_value(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def get_best_offer_price(ex_data: dict[str, Any], side: str) -> float | None:
    offers = ex_data.get(side, [])
    if not offers:
        return None
    prices = [offer.get("price") for offer in offers if isinstance(offer.get("price"), (int, float))]
    if not prices:
        return None
    return max(prices) if side == "availableToBack" else min(prices)


def format_handicap_value(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    if float(value).is_integer():
        return f"{int(value):+d}"
    return f"{float(value):+g}"


def format_price_value(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{float(value):.2f}"


def format_goal_line_value(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    number = float(value)
    if number.is_integer():
        return f"{int(number)}.0"
    return f"{number:g}"


def parse_goal_line_from_catalogue(catalogue: dict[str, Any]) -> float | None:
    market_name = str(catalogue.get("marketName", "")).strip()
    match = GOAL_MARKET_NAME_RE.search(market_name)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass

    market_type_raw = str(
        catalogue.get("marketType")
        or (catalogue.get("description", {}) or {}).get("marketType")
        or ""
    ).strip()
    match = GOAL_MARKET_TYPE_RE.search(market_type_raw)
    if match:
        try:
            return float(match.group(1)) / 10.0
        except ValueError:
            pass

    found_values: set[float] = set()
    for runner in catalogue.get("runners", []):
        runner_name = str(runner.get("runnerName", "")).strip()
        runner_match = GOAL_RUNNER_RE.search(runner_name)
        if not runner_match:
            continue
        try:
            found_values.add(float(runner_match.group(1)))
        except ValueError:
            continue
    if len(found_values) == 1:
        return next(iter(found_values))
    return None


def extract_goal_runner_handicaps(catalogue: dict[str, Any]) -> set[float]:
    values: set[float] = set()
    for runner in catalogue.get("runners", []):
        handicap = parse_handicap_value(runner.get("handicap"))
        if handicap is not None:
            values.add(float(handicap))
    return values


def is_goal_line_market(catalogue: dict[str, Any]) -> bool:
    market_name = str(catalogue.get("marketName", "")).strip().lower()
    if not market_name:
        return False
    has_goal_line_label = (
        ("over/under" in market_name)
        or ("over under" in market_name)
        or ("goal line" in market_name)
        or ("total goals" in market_name)
    )
    if not has_goal_line_label:
        return False
    if any(term in market_name for term in NON_GOAL_MARKET_TERMS):
        return False

    market_type = str(
        (catalogue.get("description", {}) or {}).get("marketType")
        or catalogue.get("marketType")
        or ""
    ).strip().upper()
    runner_lines = extract_goal_runner_handicaps(catalogue)
    if market_type == "ALT_TOTAL_GOALS":
        if not runner_lines:
            return False
        return any(abs((line * 4) - round(line * 4)) < 1e-6 for line in runner_lines)

    goal_line = parse_goal_line_from_catalogue(catalogue)
    if goal_line is None and not runner_lines:
        return False

    candidate_lines = set(runner_lines)
    if goal_line is not None:
        candidate_lines.add(float(goal_line))
    return any(abs((line * 4) - round(line * 4)) < 1e-6 for line in candidate_lines)


def goal_market_snapshot(catalogue: dict[str, Any], market_book: dict[str, Any] | None) -> dict[str, Any]:
    market_level_line = parse_goal_line_from_catalogue(catalogue)
    if not market_book:
        return {
            "goal_line": market_level_line,
            "score": float("inf"),
            "under_mid": None,
            "over_mid": None,
        }

    runner_info: dict[int, str] = {}
    for runner in catalogue.get("runners", []):
        selection_id = runner.get("selectionId")
        if isinstance(selection_id, int):
            runner_info[selection_id] = str(runner.get("runnerName", "")).strip()

    over_rows_by_line: dict[float, dict[str, float | None]] = {}
    under_rows_by_line: dict[float, dict[str, float | None]] = {}

    def store_best(bucket: dict[float, dict[str, float | None]], line: float, row: dict[str, float | None]) -> None:
        key = round(float(line), 6)
        existing = bucket.get(key)
        if existing is None:
            bucket[key] = row
            return
        row_spread = row.get("spread")
        existing_spread = existing.get("spread")
        if row_spread is None and existing_spread is None:
            return
        if existing_spread is None:
            bucket[key] = row
            return
        if row_spread is None:
            return
        if float(row_spread) < float(existing_spread):
            bucket[key] = row

    for runner in market_book.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue

        name = runner_info.get(selection_id, "").strip().lower()
        if not name:
            continue

        line_value = parse_handicap_value(runner.get("handicap"))
        if line_value is None:
            line_value = market_level_line
        if line_value is None:
            continue

        ex = runner.get("ex", {})
        back = get_best_offer_price(ex, "availableToBack")
        lay = get_best_offer_price(ex, "availableToLay")
        row = {
            "mid": ((float(back) + float(lay)) / 2.0) if (back is not None and lay is not None) else None,
            "spread": (float(lay) - float(back)) if (back is not None and lay is not None) else None,
        }
        if "over" in name:
            store_best(over_rows_by_line, float(line_value), row)
        elif "under" in name:
            store_best(under_rows_by_line, float(line_value), row)

    candidate_lines = sorted(set(over_rows_by_line.keys()) & set(under_rows_by_line.keys()))
    if not candidate_lines:
        return {
            "goal_line": market_level_line,
            "score": float("inf"),
            "under_mid": None,
            "over_mid": None,
        }

    lines: list[dict[str, float | None]] = []
    for line in candidate_lines:
        over_row = over_rows_by_line.get(line, {})
        under_row = under_rows_by_line.get(line, {})
        over_mid = over_row.get("mid")
        under_mid = under_row.get("mid")
        score = (
            abs(float(over_mid) - 2.0) + abs(float(under_mid) - 2.0)
            if (over_mid is not None and under_mid is not None)
            else float("inf")
        )
        lines.append(
            {
                "goal_line": float(line),
                "score": float(score),
                "under_mid": (float(under_mid) if under_mid is not None else None),
                "over_mid": (float(over_mid) if over_mid is not None else None),
            }
        )

    priced_lines = [line for line in lines if line.get("under_mid") is not None and line.get("over_mid") is not None]
    if priced_lines:
        best = min(priced_lines, key=lambda row: (float(row["score"]), abs(float(row["goal_line"]) - 2.5)))
    else:
        best = min(lines, key=lambda row: abs(float(row["goal_line"]) - 2.5))
    return {
        "goal_line": best["goal_line"],
        "score": best["score"],
        "under_mid": best.get("under_mid"),
        "over_mid": best.get("over_mid"),
    }


def event_goal_mainline_snapshot(
    goal_catalogues: list[dict[str, Any]],
    goal_books_by_market_id: dict[str, dict[str, Any]],
) -> dict[str, str]:
    snapshots: list[dict[str, Any]] = []
    for cat in goal_catalogues:
        market_id = str(cat.get("marketId", "")).strip()
        snapshot = goal_market_snapshot(cat, goal_books_by_market_id.get(market_id))
        if snapshot.get("goal_line") is None:
            continue
        snapshots.append(snapshot)

    if not snapshots:
        return {"goal_mainline": "-", "goal_under_price": "-", "goal_over_price": "-"}

    priced = [row for row in snapshots if row.get("score", float("inf")) != float("inf")]
    if priced:
        best = min(priced, key=lambda row: (float(row["score"]), abs(float(row["goal_line"]) - 2.5)))
    else:
        best = min(snapshots, key=lambda row: abs(float(row["goal_line"]) - 2.5))
    return {
        "goal_mainline": format_goal_line_value(best["goal_line"]),
        "goal_under_price": format_price_value(best.get("under_mid")),
        "goal_over_price": format_price_value(best.get("over_mid")),
    }


def market_mainline_snapshot(catalogue: dict[str, Any], market_book: dict[str, Any] | None) -> dict[str, str]:
    default = {"mainline": "-", "home_price": "-", "away_price": "-"}
    if not market_book:
        return default

    event_name = str(catalogue.get("event", {}).get("name", "")).strip()
    home_team, away_team = split_event_teams(event_name)
    if not home_team or not away_team:
        return default

    runner_info: dict[int, dict[str, Any]] = {}
    for runner in catalogue.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue
        runner_info[selection_id] = {
            "name": str(runner.get("runnerName", "")).strip(),
            "handicap": runner.get("handicap"),
        }

    home_candidates: list[dict[str, Any]] = []
    away_candidates: list[dict[str, Any]] = []
    for runner in market_book.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue
        info = runner_info.get(selection_id, {})
        runner_name = str(info.get("name", "")).strip() or f"Selection {selection_id}"
        handicap = parse_handicap_value(runner.get("handicap", info.get("handicap")))
        if handicap is None:
            continue

        ex = runner.get("ex", {})
        back = get_best_offer_price(ex, "availableToBack")
        lay = get_best_offer_price(ex, "availableToLay")
        row = {
            "handicap": float(handicap),
            "back": float(back) if back is not None else None,
            "lay": float(lay) if lay is not None else None,
            "spread": (float(lay) - float(back)) if (back is not None and lay is not None) else None,
        }
        if runner_name_matches(runner_name, home_team):
            home_candidates.append(row)
        elif runner_name_matches(runner_name, away_team):
            away_candidates.append(row)

    def best_by_handicap(rows: list[dict[str, Any]]) -> dict[float, dict[str, Any]]:
        out: dict[float, dict[str, Any]] = {}
        for row in rows:
            key = round(float(row["handicap"]), 6)
            existing = out.get(key)
            if existing is None:
                out[key] = row
                continue

            row_spread = row.get("spread")
            existing_spread = existing.get("spread")
            if row_spread is None and existing_spread is None:
                continue
            if existing_spread is None:
                out[key] = row
                continue
            if row_spread is None:
                continue
            if float(row_spread) < float(existing_spread):
                out[key] = row
        return out

    home_by_hcp = best_by_handicap(home_candidates)
    away_by_hcp = best_by_handicap(away_candidates)
    if not home_by_hcp or not away_by_hcp:
        return default

    lines: list[dict[str, Any]] = []
    for home_hcp_key, home_row in home_by_hcp.items():
        away_hcp_key = round(-home_hcp_key, 6)
        away_row = away_by_hcp.get(away_hcp_key)
        if away_row is None:
            continue

        home_back = home_row.get("back")
        home_lay = home_row.get("lay")
        away_back = away_row.get("back")
        away_lay = away_row.get("lay")
        home_mid = (
            (float(home_back) + float(home_lay)) / 2.0
            if (home_back is not None and home_lay is not None)
            else None
        )
        away_mid = (
            (float(away_back) + float(away_lay)) / 2.0
            if (away_back is not None and away_lay is not None)
            else None
        )
        score = (
            abs(home_mid - 2.0) + abs(away_mid - 2.0)
            if (home_mid is not None and away_mid is not None)
            else float("inf")
        )
        lines.append(
            {
                "home_handicap": float(home_row["handicap"]),
                "home_mid": home_mid,
                "away_mid": away_mid,
                "score": score,
            }
        )

    if not lines:
        return default

    priced_lines = [line for line in lines if line.get("home_mid") is not None and line.get("away_mid") is not None]
    if priced_lines:
        best_line = min(priced_lines, key=lambda line: (line["score"], abs(line["home_handicap"])))
    else:
        # Fallback: still show a handicap line even if prices are currently unavailable.
        best_line = min(lines, key=lambda line: abs(line["home_handicap"]))

    return {
        "mainline": format_handicap_value(best_line["home_handicap"]),
        "home_price": format_price_value(best_line["home_mid"]),
        "away_price": format_price_value(best_line["away_mid"]),
    }


def normalize_team_name(value: str | None) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = []
    for token in text.split():
        token = TOKEN_REPLACEMENTS.get(token, token)
        if token in STOP_WORDS:
            continue
        tokens.append(token)
    return " ".join(tokens)


def token_set(value: str) -> set[str]:
    return {part for part in value.split() if part}


def normalize_competition_name(value: str | None) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zA-Z0-9]+", " ", text.lower()).strip()
    return " ".join(text.split())


def select_top5_competition_ids(competitions: list[Any]) -> tuple[list[str], list[str]]:
    comp_rows: list[dict[str, str]] = []
    for comp in competitions:
        comp_id = str(getattr(comp, "comp_id", "")).strip()
        name = str(getattr(comp, "name", "")).strip()
        if not comp_id or not name:
            continue
        comp_rows.append(
            {
                "competition_id": comp_id,
                "competition_name": name,
                "competition_name_norm": normalize_competition_name(name),
            }
        )

    selected_ids: list[str] = []
    missing_targets: list[str] = []
    used_ids: set[str] = set()

    for target in TOP5_LEAGUE_TARGETS:
        aliases_norm = [normalize_competition_name(alias) for alias in target["aliases"]]
        best: dict[str, str] | None = None
        best_score = -1
        best_name_len = 10_000

        for row in comp_rows:
            if row["competition_id"] in used_ids:
                continue
            comp_norm = row["competition_name_norm"]
            score = -1
            if comp_norm in aliases_norm:
                score = 3
            elif any(alias in comp_norm for alias in aliases_norm):
                score = 2
            elif any(set(alias.split()).issubset(set(comp_norm.split())) for alias in aliases_norm):
                score = 1

            if score < 0:
                continue
            name_len = len(row["competition_name"])
            if score > best_score or (score == best_score and name_len < best_name_len):
                best = row
                best_score = score
                best_name_len = name_len

        if best is None:
            missing_targets.append(target["label"])
            continue

        used_ids.add(best["competition_id"])
        selected_ids.append(best["competition_id"])

    return selected_ids, missing_targets


def parse_season_date(value: Any, is_end: bool) -> pd.Timestamp | pd.NaT:
    if value is None:
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    ts = parse_iso_utc(text)
    if pd.isna(ts):
        return pd.NaT
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) and is_end:
        return ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return ts


def _sqlite_table_names(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows if row and row[0]}


def resolve_sofascore_db_path(db_path: str) -> Path:
    requested = Path(db_path).expanduser().resolve()
    fallback = (WORKSPACE_DIR / "Sofascore_scraper" / "sofascore_local.db").resolve()

    candidates: list[Path] = []
    for candidate in (requested, fallback):
        if candidate not in candidates:
            candidates.append(candidate)

    checked: list[tuple[Path, set[str] | None]] = []
    for candidate in candidates:
        if not candidate.exists():
            checked.append((candidate, None))
            continue
        try:
            tables = _sqlite_table_names(candidate)
        except Exception:
            checked.append((candidate, None))
            continue
        checked.append((candidate, tables))
        if REQUIRED_SOFASCORE_TABLES.issubset(tables):
            return candidate

    detail_lines: list[str] = []
    for candidate, tables in checked:
        if tables is None:
            if candidate.exists():
                detail_lines.append(f"- {candidate}: unreadable or invalid sqlite file")
            else:
                detail_lines.append(f"- {candidate}: not found")
            continue
        missing = sorted(REQUIRED_SOFASCORE_TABLES - tables)
        detail_lines.append(f"- {candidate}: missing tables: {', '.join(missing)}")

    details = "\n".join(detail_lines)
    raise RuntimeError(
        "SofaScore DB is missing required schema (expected tables like 'matches'). "
        "Set --db-path to a valid sofascore_local.db.\n"
        f"Checked:\n{details}"
    )


def load_sofascore_inputs(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    path = resolve_sofascore_db_path(db_path)

    finished_sql = """
    WITH xg_stats AS (
        SELECT
            ms.match_id,
            MAX(COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))) AS home_xg_stat,
            MAX(COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))) AS away_xg_stat
        FROM match_stats ms
        JOIN stat_definitions sd
            ON sd.id = ms.stat_definition_id
        WHERE LOWER(COALESCE(sd.metric, '')) = 'expected goals'
          AND UPPER(COALESCE(ms.period, '')) = 'ALL'
        GROUP BY ms.match_id
    ),
    shot_aggs AS (
        SELECT
            match_id,
            SUM(CASE WHEN is_home = 1 THEN COALESCE(CAST(NULLIF(xg, '') AS REAL), 0) ELSE 0 END) AS home_xg_shots,
            SUM(CASE WHEN is_home = 0 THEN COALESCE(CAST(NULLIF(xg, '') AS REAL), 0) ELSE 0 END) AS away_xg_shots,
            SUM(CASE WHEN is_home = 1 THEN COALESCE(CAST(NULLIF(xgot, '') AS REAL), 0) ELSE 0 END) AS home_xgot_shots,
            SUM(CASE WHEN is_home = 0 THEN COALESCE(CAST(NULLIF(xgot, '') AS REAL), 0) ELSE 0 END) AS away_xgot_shots
        FROM match_shots
        GROUP BY match_id
    ),
    keeper_aggs AS (
        SELECT
            ms.match_id,
            MAX(COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))) AS home_goalkeeper_goals_prevented,
            MAX(COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))) AS away_goalkeeper_goals_prevented
        FROM match_stats ms
        JOIN stat_definitions sd
            ON sd.id = ms.stat_definition_id
        WHERE LOWER(COALESCE(sd.metric, '')) = 'goals prevented'
          AND UPPER(COALESCE(ms.period, '')) = 'ALL'
        GROUP BY ms.match_id
    )
    SELECT
        m.id AS match_id,
        m.season_id,
        m.kickoff_ts AS date_time,
        m.status AS match_status,
        c.name AS competition_name,
        c.country AS area_name,
        s.name AS season_name,
        s.start_date AS season_start_date,
        s.end_date AS season_end_date,
        ht.name AS home_team_name,
        at.name AS away_team_name,
        COALESCE(m.home_ft_score, m.home_score_final) AS home_goals,
        COALESCE(m.away_ft_score, m.away_score_final) AS away_goals,
        xg.home_xg_stat,
        xg.away_xg_stat,
        sa.home_xg_shots,
        sa.away_xg_shots,
        sa.home_xgot_shots,
        sa.away_xgot_shots,
        ka.home_goalkeeper_goals_prevented,
        ka.away_goalkeeper_goals_prevented
    FROM matches m
    JOIN teams ht ON ht.id = m.home_team_id
    JOIN teams at ON at.id = m.away_team_id
    LEFT JOIN competitions c ON c.id = m.competition_id
    LEFT JOIN seasons s ON s.id = m.season_id
    LEFT JOIN xg_stats xg ON xg.match_id = m.id
    LEFT JOIN shot_aggs sa ON sa.match_id = m.id
    LEFT JOIN keeper_aggs ka ON ka.match_id = m.id
    WHERE m.kickoff_ts IS NOT NULL
      AND COALESCE(m.home_ft_score, m.home_score_final) IS NOT NULL
      AND COALESCE(m.away_ft_score, m.away_score_final) IS NOT NULL
    """

    fixtures_sql = """
    SELECT
        m.id AS match_id,
        m.season_id,
        m.kickoff_ts AS date_time,
        c.name AS competition_name,
        c.country AS area_name,
        ht.name AS home_team_name,
        at.name AS away_team_name
    FROM matches m
    JOIN teams ht ON ht.id = m.home_team_id
    JOIN teams at ON at.id = m.away_team_id
    LEFT JOIN competitions c ON c.id = m.competition_id
    WHERE LOWER(TRIM(COALESCE(m.status, ''))) = 'not started'
      AND m.kickoff_ts IS NOT NULL
    """

    conn = sqlite3.connect(str(path))
    try:
        raw_finished = pd.read_sql_query(finished_sql, conn)
        fixtures_df = pd.read_sql_query(fixtures_sql, conn)
        teams_df = pd.read_sql_query("SELECT name FROM teams ORDER BY name", conn)
    finally:
        conn.close()

    raw_finished["status_norm"] = raw_finished["match_status"].fillna("").astype(str).str.strip().str.lower()
    raw_finished = raw_finished[raw_finished["status_norm"].isin(FINISHED_STATUSES)].copy()
    raw_finished["date_time"] = pd.to_datetime(raw_finished["date_time"], errors="coerce", utc=True)
    raw_finished["season_start_date"] = raw_finished["season_start_date"].apply(lambda v: parse_season_date(v, False))
    raw_finished["season_end_date"] = raw_finished["season_end_date"].apply(lambda v: parse_season_date(v, True))

    numeric_cols = [
        "home_goals",
        "away_goals",
        "home_xg_stat",
        "away_xg_stat",
        "home_xg_shots",
        "away_xg_shots",
        "home_xgot_shots",
        "away_xgot_shots",
        "home_goalkeeper_goals_prevented",
        "away_goalkeeper_goals_prevented",
    ]
    for col in numeric_cols:
        if col in raw_finished.columns:
            raw_finished[col] = pd.to_numeric(raw_finished[col], errors="coerce")

    raw_finished["home_xg"] = raw_finished["home_xg_stat"].where(raw_finished["home_xg_stat"].notna(), raw_finished["home_xg_shots"])
    raw_finished["away_xg"] = raw_finished["away_xg_stat"].where(raw_finished["away_xg_stat"].notna(), raw_finished["away_xg_shots"])
    raw_finished["home_xgot_keeper"] = raw_finished["home_goals"] + raw_finished["away_goalkeeper_goals_prevented"]
    raw_finished["away_xgot_keeper"] = raw_finished["away_goals"] + raw_finished["home_goalkeeper_goals_prevented"]

    # Prefer xGoT derived from goals + opponent goalkeeper goals prevented.
    raw_finished["home_xgot"] = raw_finished["home_xgot_keeper"]
    raw_finished.loc[raw_finished["home_xgot"].isna(), "home_xgot"] = raw_finished["home_xgot_shots"]
    raw_finished.loc[raw_finished["home_xgot"].isna(), "home_xgot"] = raw_finished["home_xg"]

    raw_finished["away_xgot"] = raw_finished["away_xgot_keeper"]
    raw_finished.loc[raw_finished["away_xgot"].isna(), "away_xgot"] = raw_finished["away_xgot_shots"]
    raw_finished.loc[raw_finished["away_xgot"].isna(), "away_xgot"] = raw_finished["away_xg"]

    finished = raw_finished[
        [
            "match_id",
            "season_id",
            "competition_name",
            "area_name",
            "season_name",
            "season_start_date",
            "season_end_date",
            "date_time",
            "home_team_name",
            "away_team_name",
            "home_goals",
            "away_goals",
            "home_xg",
            "away_xg",
            "home_xgot",
            "away_xgot",
        ]
    ].copy()
    finished = finished.dropna(subset=["date_time", "home_team_name", "away_team_name", "home_goals", "away_goals", "home_xg", "away_xg"])

    home_rows = pd.DataFrame(
        {
            "match_id": finished["match_id"],
            "team": finished["home_team_name"],
            "opponent": finished["away_team_name"],
            "venue": "Home",
            "date_time": finished["date_time"],
            "GF": finished["home_goals"],
            "GA": finished["away_goals"],
            "xG": finished["home_xg"],
            "xGA": finished["away_xg"],
            "xGoT": finished["home_xgot"].where(finished["home_xgot"].notna(), finished["home_xg"]),
            "xGoTA": finished["away_xgot"].where(finished["away_xgot"].notna(), finished["away_xg"]),
            "season_id": finished["season_id"],
            "competition_name": finished["competition_name"],
            "area_name": finished["area_name"],
            "season_name": finished["season_name"],
            "season_start_date": finished["season_start_date"],
            "season_end_date": finished["season_end_date"],
        }
    )
    away_rows = pd.DataFrame(
        {
            "match_id": finished["match_id"],
            "team": finished["away_team_name"],
            "opponent": finished["home_team_name"],
            "venue": "Away",
            "date_time": finished["date_time"],
            "GF": finished["away_goals"],
            "GA": finished["home_goals"],
            "xG": finished["away_xg"],
            "xGA": finished["home_xg"],
            "xGoT": finished["away_xgot"].where(finished["away_xgot"].notna(), finished["away_xg"]),
            "xGoTA": finished["home_xgot"].where(finished["home_xgot"].notna(), finished["home_xg"]),
            "season_id": finished["season_id"],
            "competition_name": finished["competition_name"],
            "area_name": finished["area_name"],
            "season_name": finished["season_name"],
            "season_start_date": finished["season_start_date"],
            "season_end_date": finished["season_end_date"],
        }
    )

    form_df = pd.concat([home_rows, away_rows], ignore_index=True)
    form_df = form_df.sort_values(["date_time", "match_id", "venue"], kind="mergesort").reset_index(drop=True)

    fixtures_df["date_time"] = pd.to_datetime(fixtures_df["date_time"], errors="coerce", utc=True)
    fixtures_df = fixtures_df.dropna(subset=["date_time", "home_team_name", "away_team_name"]).reset_index(drop=True)

    teams = teams_df["name"].dropna().astype(str).str.strip()
    team_list = sorted({name for name in teams if name})
    return form_df, fixtures_df, team_list


@dataclass
class MatchMapping:
    home_raw: str | None
    away_raw: str | None
    home_sofa: str | None
    away_sofa: str | None
    home_score: float | None
    away_score: float | None
    home_method: str
    away_method: str


class TeamMatcher:
    def __init__(self, team_names: list[str]) -> None:
        unique = sorted({str(name).strip() for name in team_names if str(name).strip()})
        self.teams = unique
        self.norm_by_team = {team: normalize_team_name(team) for team in unique}
        self.tokens_by_team = {team: token_set(norm) for team, norm in self.norm_by_team.items()}
        self.by_norm: dict[str, list[str]] = {}
        for team, norm in self.norm_by_team.items():
            self.by_norm.setdefault(norm, []).append(team)

    def match(self, raw_name: str | None, disallow: set[str] | None = None) -> tuple[str | None, float | None, str]:
        if raw_name is None or not str(raw_name).strip():
            return None, None, "missing"
        disallow = disallow or set()
        raw_norm = normalize_team_name(raw_name)
        if not raw_norm:
            return None, None, "missing"

        exact = [name for name in self.by_norm.get(raw_norm, []) if name not in disallow]
        if exact:
            return exact[0], 1.0, "exact"

        target_tokens = token_set(raw_norm)
        best_name = None
        best_score = -1.0
        for candidate in self.teams:
            if candidate in disallow:
                continue
            cand_norm = self.norm_by_team[candidate]
            cand_tokens = self.tokens_by_team[candidate]
            if not cand_norm:
                continue

            seq = SequenceMatcher(None, raw_norm, cand_norm).ratio()
            overlap = 0.0
            if target_tokens and cand_tokens:
                overlap = len(target_tokens & cand_tokens) / len(target_tokens | cand_tokens)
            substring_bonus = 0.0
            raw_flat = raw_norm.replace(" ", "")
            cand_flat = cand_norm.replace(" ", "")
            if raw_flat in cand_flat or cand_flat in raw_flat:
                substring_bonus = 0.08

            score = (0.7 * seq) + (0.3 * overlap) + substring_bonus
            if score > best_score:
                best_score = score
                best_name = candidate

        if best_name is None or best_score < 0.72:
            return None, best_score if best_score >= 0 else None, "unmatched"
        return best_name, float(best_score), "fuzzy"


def pick_best_fixture(
    fixtures: pd.DataFrame,
    home_team: str,
    away_team: str,
    kickoff_time: pd.Timestamp,
) -> dict[str, Any] | None:
    if fixtures.empty:
        return None
    same_pair = fixtures[(fixtures["home_team_name"] == home_team) & (fixtures["away_team_name"] == away_team)].copy()
    if same_pair.empty:
        return None
    same_pair["delta_minutes"] = ((same_pair["date_time"] - kickoff_time).abs().dt.total_seconds() / 60.0)
    best = same_pair.sort_values(["delta_minutes", "date_time"]).iloc[0].to_dict()
    if float(best.get("delta_minutes", 1e9)) > (24 * 60):
        return None
    return best


def build_predictions(
    betfair_games_df: pd.DataFrame,
    form_df: pd.DataFrame,
    fixtures_df: pd.DataFrame,
    team_matcher: TeamMatcher,
    calc_wyscout_form_tables: Any,
    periods: tuple[Any, ...],
    min_games: int,
) -> pd.DataFrame:
    if betfair_games_df.empty:
        return pd.DataFrame()

    mapped_games: list[dict[str, Any]] = []
    model_games: list[dict[str, Any]] = []

    for row in betfair_games_df.to_dict(orient="records"):
        mapping = MatchMapping(
            home_raw=row.get("home_raw"),
            away_raw=row.get("away_raw"),
            home_sofa=None,
            away_sofa=None,
            home_score=None,
            away_score=None,
            home_method="missing",
            away_method="missing",
        )

        home_team, home_score, home_method = team_matcher.match(row.get("home_raw"))
        away_team, away_score, away_method = team_matcher.match(row.get("away_raw"), disallow={home_team} if home_team else None)

        mapping.home_sofa = home_team
        mapping.away_sofa = away_team
        mapping.home_score = home_score
        mapping.away_score = away_score
        mapping.home_method = home_method
        mapping.away_method = away_method

        fixture = None
        if home_team and away_team and not pd.isna(row.get("kickoff_time")):
            fixture = pick_best_fixture(fixtures_df, home_team, away_team, row["kickoff_time"])

        mapped_row = {
            **row,
            "home_sofa": mapping.home_sofa,
            "away_sofa": mapping.away_sofa,
            "home_match_score": mapping.home_score,
            "away_match_score": mapping.away_score,
            "home_match_method": mapping.home_method,
            "away_match_method": mapping.away_method,
            "fixture_found": fixture is not None,
            "fixture_match_id": fixture.get("match_id") if fixture else None,
            "fixture_competition": fixture.get("competition_name") if fixture else None,
            "fixture_area": fixture.get("area_name") if fixture else None,
            "fixture_season_id": fixture.get("season_id") if fixture else None,
        }
        mapped_games.append(mapped_row)

        if not home_team or not away_team:
            continue

        model_games.append(
            {
                "home": home_team,
                "away": away_team,
                "match_date": row["kickoff_time"],
                "season_id": fixture.get("season_id") if fixture else None,
                "competition_name": fixture.get("competition_name") if fixture else None,
                "area_name": fixture.get("area_name") if fixture else None,
            }
        )

    if not model_games:
        return pd.DataFrame(mapped_games)

    model_games_df = pd.DataFrame(model_games)
    game_tables, source_games = calc_wyscout_form_tables(
        model_games_df,
        form_df,
        periods=periods,
        return_source_games=True,
        min_games=int(min_games),
    )

    pred_rows: list[dict[str, Any]] = []
    model_idx = 0
    for mapped in mapped_games:
        if not mapped.get("home_sofa") or not mapped.get("away_sofa"):
            pred_rows.append({**mapped, "period": None})
            continue

        _, _, _, _, reduced = game_tables[model_idx]
        sources = source_games[model_idx]
        home_used = len(sources.get("home_source_games", []))
        away_used = len(sources.get("away_source_games", []))
        warning = sources.get("warning")

        for _, period_row in reduced.iterrows():
            pred_rows.append(
                {
                    **mapped,
                    "period": period_row.get("Period"),
                    "home_xg": float(period_row.get("Team Home Real xG", 0)),
                    "away_xg": float(period_row.get("Team Away Real xG", 0)),
                    "total_xg": float(period_row.get("Total Team Real xG", 0)),
                    "xgd": float(period_row.get("Team Real xGD", 0)),
                    "total_min_xg": float(period_row.get("Total Min Real xG", 0)),
                    "total_max_xg": float(period_row.get("Total Max Real xG", 0)),
                    "home_games_used": int(home_used),
                    "away_games_used": int(away_used),
                    "model_warning": warning,
                }
            )
        model_idx += 1

    out = pd.DataFrame(pred_rows)
    if "period" in out.columns:
        out["period"] = out["period"].map(lambda v: "" if pd.isna(v) else str(v))
    sort_cols = ["kickoff_time", "competition", "event_name", "period"]
    existing = [c for c in sort_cols if c in out.columns]
    if existing:
        out = out.sort_values(existing).reset_index(drop=True)
    return out


def to_native(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, bool, int, float)):
        return value
    if pd.isna(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def clamp_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(min_value, min(max_value, out))


def build_recent_form_rows(source_df: Any, recent_n: int) -> list[dict[str, Any]]:
    if not isinstance(source_df, pd.DataFrame) or source_df.empty:
        return []

    preferred_cols = [
        "date_time",
        "competition_name",
        "team",
        "opponent",
        "venue",
        "GF",
        "GA",
        "xG",
        "xGA",
        "xGoT",
        "xGoTA",
    ]
    cols = [c for c in preferred_cols if c in source_df.columns]
    if not cols:
        return []

    recent_df = source_df.copy()
    if "date_time" in recent_df.columns:
        recent_df["date_time"] = pd.to_datetime(recent_df["date_time"], errors="coerce", utc=True)
        recent_df = recent_df.sort_values("date_time")
    recent_df = recent_df.tail(int(recent_n))
    if "date_time" in recent_df.columns:
        recent_df = recent_df.sort_values("date_time", ascending=False)

    rows: list[dict[str, Any]] = []
    for row in recent_df[cols].to_dict(orient="records"):
        out: dict[str, Any] = {}
        for key, value in row.items():
            if key == "date_time":
                ts = pd.to_datetime(value, errors="coerce", utc=True)
                out[key] = ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(ts) else to_native(value)
            else:
                out[key] = to_native(value)
        rows.append(out)
    return rows


def build_team_venue_recent_rows(
    form_df: pd.DataFrame,
    team_name: str | None,
    kickoff_time: Any,
    recent_n: int,
    season_id: Any = None,
    competition_name: str | None = None,
    area_name: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    if not team_name or form_df.empty:
        return {"home": [], "away": []}

    df = form_df[form_df["team"] == team_name].copy()
    if df.empty:
        return {"home": [], "away": []}

    kickoff_ts = pd.to_datetime(kickoff_time, errors="coerce", utc=True)
    if not pd.isna(kickoff_ts) and "date_time" in df.columns:
        df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce", utc=True)
        df = df[df["date_time"] < kickoff_ts]

    if season_id is not None and not pd.isna(season_id) and "season_id" in df.columns:
        df = df[df["season_id"] == season_id]

    if competition_name and "competition_name" in df.columns:
        df = df[df["competition_name"] == competition_name]
    if area_name and "area_name" in df.columns:
        df = df[df["area_name"] == area_name]

    home_df = df[df["venue"].astype(str).str.lower() == "home"].copy() if "venue" in df.columns else df.iloc[0:0].copy()
    away_df = df[df["venue"].astype(str).str.lower() == "away"].copy() if "venue" in df.columns else df.iloc[0:0].copy()

    return {
        "home": build_recent_form_rows(home_df, recent_n),
        "away": build_recent_form_rows(away_df, recent_n),
    }


def format_day_label(day_iso: str) -> str:
    try:
        dt_obj = dt.datetime.strptime(day_iso, "%Y-%m-%d")
        return dt_obj.strftime("%a %Y-%m-%d")
    except Exception:
        return day_iso


class AppState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.lock = threading.Lock()

        betfair_scraper_path = APP_DIR / "betfair_scraper.py"
        if not betfair_scraper_path.exists():
            betfair_scraper_path = WORKSPACE_DIR / "Bot Finder" / "betfair_scraper.py"
        wyscout_data_path = APP_DIR / "wyscout_data.py"
        if not wyscout_data_path.exists():
            wyscout_data_path = WORKSPACE_DIR / "XGD Model" / "wyscout_data.py"

        self.betfair_module = load_module_from_path(betfair_scraper_path, "xgd_betfair_scraper")
        self.xgd_module = load_module_from_path(wyscout_data_path, "xgd_wyscout_data")

        self.credentials = self._resolve_credentials(args)
        self.market_types = parse_list_csv(args.market_types)
        if not self.market_types:
            raise RuntimeError("At least one market type is required.")

        self.max_markets = max(1, min(int(args.max_markets), 1000))
        self.horizon_days = int(args.horizon_days)
        self.refresh_seconds = max(10, int(args.refresh_seconds))
        self.periods = parse_periods(args.periods)
        self.min_games = int(args.min_games)

        self.form_df, self.fixtures_df, teams = load_sofascore_inputs(args.db_path)
        self.team_matcher = TeamMatcher(teams)

        self.games_df = pd.DataFrame()
        self.last_refresh: dt.datetime | None = None

    @staticmethod
    def _load_module_settings(module_path: Path) -> dict[str, str | None]:
        if not module_path.exists():
            return {}
        spec = importlib.util.spec_from_file_location(module_path.stem, str(module_path))
        if spec is None or spec.loader is None:
            return {}
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return {
            "username": getattr(module, "BETFAIR_USERNAME", None),
            "password": getattr(module, "BETFAIR_PASSWORD", None),
            "app_key": getattr(module, "BETFAIR_APP_KEY", None),
            "session_token": getattr(module, "BETFAIR_SESSION_TOKEN", None),
            "cert_file": getattr(module, "BETFAIR_CERT_FILE", None),
            "key_file": getattr(module, "BETFAIR_KEY_FILE", None),
        }

    def _resolve_credentials(self, args: argparse.Namespace) -> dict[str, str | None]:
        cfg_filename = f"{args.config_module}.py"
        local_cfg = APP_DIR / cfg_filename
        workspace_cfg = WORKSPACE_DIR / "Bot Finder" / cfg_filename
        if local_cfg.exists():
            module_path = local_cfg
        elif workspace_cfg.exists():
            module_path = workspace_cfg
        else:
            module_path = local_cfg
        settings = self._load_module_settings(module_path)

        def pick(cli_value: str | None, env_name: str, file_key: str) -> str | None:
            return cli_value or os.getenv(env_name) or settings.get(file_key)

        creds = {
            "username": pick(args.username, "BETFAIR_USERNAME", "username"),
            "password": pick(args.password, "BETFAIR_PASSWORD", "password"),
            "app_key": pick(args.app_key, "BETFAIR_APP_KEY", "app_key"),
            "session_token": pick(args.session_token, "BETFAIR_SESSION_TOKEN", "session_token"),
            "cert_file": pick(args.cert_file, "BETFAIR_CERT_FILE", "cert_file"),
            "key_file": pick(args.key_file, "BETFAIR_KEY_FILE", "key_file"),
        }

        if not creds["app_key"]:
            raise RuntimeError("Missing Betfair app key.")

        has_session = bool(str(creds["session_token"] or "").strip())
        has_cert_bundle = bool(
            str(creds["username"] or "").strip()
            and str(creds["password"] or "").strip()
            and str(creds["cert_file"] or "").strip()
            and str(creds["key_file"] or "").strip()
        )
        if not has_session and not has_cert_bundle:
            raise RuntimeError(
                "Provide BETFAIR_SESSION_TOKEN, or username/password plus cert/key credentials."
            )
        return creds

    def _build_client(self, use_session_token: bool):
        cfg = self.betfair_module.Config(
            username=str(self.credentials.get("username") or "") or None,
            password=str(self.credentials.get("password") or "") or None,
            app_key=str(self.credentials["app_key"]),
            cert_file=str(self.credentials.get("cert_file") or "") or None,
            key_file=str(self.credentials.get("key_file") or "") or None,
            session_token=(str(self.credentials.get("session_token") or "") or None) if use_session_token else None,
            poll_interval=3.0,
            max_markets=self.max_markets,
            pre_match_only=True,
            market_types=self.market_types,
            selected_leagues_file=str(DEFAULT_SELECTED_LEAGUES),
            all_leagues_file=str(DEFAULT_ALL_LEAGUES),
            export_leagues_only=False,
        )
        return self.betfair_module.BetfairClient(cfg)

    def _login_client(self):
        has_session = bool(str(self.credentials.get("session_token") or "").strip())
        has_cert_bundle = bool(
            str(self.credentials.get("username") or "").strip()
            and str(self.credentials.get("password") or "").strip()
            and str(self.credentials.get("cert_file") or "").strip()
            and str(self.credentials.get("key_file") or "").strip()
        )

        client = self._build_client(use_session_token=has_session)
        try:
            client.login()
            return client
        except RuntimeError as exc:
            msg = str(exc)
            invalid_session = (
                "INVALID_SESSION_INFORMATION" in msg
                or "ANGX-0003" in msg
                or "invalid session" in msg.lower()
            )
            if has_session and invalid_session and has_cert_bundle:
                client = self._build_client(use_session_token=False)
                client.login()
                return client
            raise

    @staticmethod
    def _is_invalid_session_error(exc: Exception) -> bool:
        msg = str(exc)
        return (
            "INVALID_SESSION_INFORMATION" in msg
            or "ANGX-0003" in msg
            or "invalid session" in msg.lower()
        )

    def refresh_games(self, force: bool = False) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        with self.lock:
            if not force and self.last_refresh is not None:
                delta = (now - self.last_refresh).total_seconds()
                if delta < self.refresh_seconds:
                    return

        client = self._login_client()
        try:
            competitions = client.list_competitions()
        except RuntimeError as exc:
            has_cert_bundle = bool(
                str(self.credentials.get("username") or "").strip()
                and str(self.credentials.get("password") or "").strip()
                and str(self.credentials.get("cert_file") or "").strip()
                and str(self.credentials.get("key_file") or "").strip()
            )
            if has_cert_bundle and self._is_invalid_session_error(exc):
                client = self._build_client(use_session_token=False)
                client.login()
                competitions = client.list_competitions()
            else:
                raise
        competition_ids, missing = select_top5_competition_ids(competitions)
        if missing:
            raise RuntimeError("Missing top-5 leagues in Betfair API: " + ", ".join(missing))
        if not competition_ids:
            raise RuntimeError("Could not resolve top-5 competition IDs from Betfair API.")

        catalogues: list[dict[str, Any]] = []
        for comp_id in competition_ids:
            catalogues.extend(client.list_handicap_markets([comp_id]) or [])

        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for cat in catalogues:
            market_id = str(cat.get("marketId", "")).strip()
            if not market_id or market_id in seen:
                continue
            seen.add(market_id)
            deduped.append(cat)

        now_ts = pd.Timestamp.now(tz="UTC")
        horizon = None if self.horizon_days <= 0 else now_ts + pd.Timedelta(days=self.horizon_days)

        candidate_markets: list[dict[str, Any]] = []
        for cat in deduped:
            market_id = str(cat.get("marketId", "")).strip()
            kickoff_raw = str(cat.get("marketStartTime", "")).strip()
            kickoff_ts = parse_iso_utc(kickoff_raw)
            if not market_id or pd.isna(kickoff_ts):
                continue
            if kickoff_ts <= now_ts:
                continue
            if horizon is not None and kickoff_ts > horizon:
                continue

            event_name = str(cat.get("event", {}).get("name", "")).strip()
            home_raw, away_raw = split_event_teams(event_name)
            event_id = str(cat.get("event", {}).get("id", "")).strip()
            candidate_markets.append(
                {
                    "catalogue": cat,
                    "market_id": market_id,
                    "event_id": event_id,
                    "kickoff_raw": kickoff_raw,
                    "kickoff_time": kickoff_ts,
                    "event_name": event_name,
                    "home_raw": home_raw,
                    "away_raw": away_raw,
                }
            )

        market_ids = [row["market_id"] for row in candidate_markets]
        books = client.list_market_books(market_ids) if market_ids else []
        book_by_market_id: dict[str, dict[str, Any]] = {}
        for book in books:
            market_id = str(book.get("marketId", "")).strip()
            if market_id:
                book_by_market_id[market_id] = book

        goal_catalogues = client.list_markets(competition_ids, ASIAN_GOAL_MARKET_TYPES)
        candidate_event_ids = {
            str(row.get("event_id", "")).strip()
            for row in candidate_markets
            if str(row.get("event_id", "")).strip()
        }
        candidate_goal_catalogues: list[dict[str, Any]] = []
        for cat in goal_catalogues:
            market_id = str(cat.get("marketId", "")).strip()
            kickoff_raw = str(cat.get("marketStartTime", "")).strip()
            kickoff_ts = parse_iso_utc(kickoff_raw)
            event_id = str(cat.get("event", {}).get("id", "")).strip()
            if not market_id or pd.isna(kickoff_ts) or not event_id:
                continue
            if kickoff_ts <= now_ts:
                continue
            if horizon is not None and kickoff_ts > horizon:
                continue
            if candidate_event_ids and event_id not in candidate_event_ids:
                continue
            if not is_goal_line_market(cat):
                continue
            candidate_goal_catalogues.append(cat)

        goal_market_ids = [str(cat.get("marketId", "")).strip() for cat in candidate_goal_catalogues]
        goal_books = client.list_market_books(goal_market_ids) if goal_market_ids else []
        goal_books_by_market_id: dict[str, dict[str, Any]] = {}
        for book in goal_books:
            market_id = str(book.get("marketId", "")).strip()
            if market_id:
                goal_books_by_market_id[market_id] = book

        goal_catalogues_by_event: dict[str, list[dict[str, Any]]] = {}
        for cat in candidate_goal_catalogues:
            event_id = str(cat.get("event", {}).get("id", "")).strip()
            if not event_id:
                continue
            goal_catalogues_by_event.setdefault(event_id, []).append(cat)

        rows: list[dict[str, Any]] = []
        for market in candidate_markets:
            cat = market["catalogue"]
            market_id = market["market_id"]
            mainline_snapshot = market_mainline_snapshot(cat, book_by_market_id.get(market_id))
            goal_snapshot = event_goal_mainline_snapshot(
                goal_catalogues_by_event.get(str(market.get("event_id", "")).strip(), []),
                goal_books_by_market_id,
            )
            rows.append(
                {
                    "market_id": market_id,
                    "event_name": market["event_name"],
                    "competition": str(cat.get("competition", {}).get("name", "")).strip(),
                    "market_name": str(cat.get("marketName", "")).strip(),
                    "kickoff_time": market["kickoff_time"],
                    "kickoff_raw": market["kickoff_raw"],
                    "home_raw": market["home_raw"],
                    "away_raw": market["away_raw"],
                    "mainline": mainline_snapshot["mainline"],
                    "home_price": mainline_snapshot["home_price"],
                    "away_price": mainline_snapshot["away_price"],
                    "goal_mainline": goal_snapshot["goal_mainline"],
                    "goal_under_price": goal_snapshot["goal_under_price"],
                    "goal_over_price": goal_snapshot["goal_over_price"],
                }
            )

        games_df = pd.DataFrame(rows)
        if not games_df.empty:
            games_df = games_df.sort_values(["kickoff_time", "competition", "event_name"]).reset_index(drop=True)

        with self.lock:
            self.games_df = games_df
            self.last_refresh = dt.datetime.now(dt.timezone.utc)

    def list_games_grouped_by_day(self) -> dict[str, Any]:
        self.refresh_games(force=False)
        with self.lock:
            games_df = self.games_df.copy()
            last_refresh = self.last_refresh

        if games_df.empty:
            return {"days": [], "total_games": 0, "last_refresh_utc": last_refresh.isoformat() if last_refresh else None}

        games_df["day"] = games_df["kickoff_time"].dt.strftime("%Y-%m-%d")
        days_out: list[dict[str, Any]] = []
        for day in sorted(games_df["day"].dropna().unique().tolist()):
            day_df = games_df[games_df["day"] == day].sort_values(["kickoff_time", "competition", "event_name"])
            games: list[dict[str, Any]] = []
            for row in day_df.to_dict(orient="records"):
                kickoff_ts = row.get("kickoff_time")
                kickoff_utc = pd.to_datetime(kickoff_ts, utc=True).strftime("%H:%M") if not pd.isna(kickoff_ts) else "-"
                games.append(
                    {
                        "market_id": str(row.get("market_id", "")),
                        "event_name": str(row.get("event_name", "")),
                        "competition": str(row.get("competition", "")),
                        "market_name": str(row.get("market_name", "")),
                        "kickoff_raw": str(row.get("kickoff_raw", "")),
                        "kickoff_utc": kickoff_utc,
                        "mainline": str(row.get("mainline", "-") or "-"),
                        "home_price": str(row.get("home_price", "-") or "-"),
                        "away_price": str(row.get("away_price", "-") or "-"),
                        "goal_mainline": str(row.get("goal_mainline", "-") or "-"),
                        "goal_under_price": str(row.get("goal_under_price", "-") or "-"),
                        "goal_over_price": str(row.get("goal_over_price", "-") or "-"),
                    }
                )
            days_out.append({"date": day, "date_label": format_day_label(day), "games": games})

        return {
            "days": days_out,
            "total_games": int(len(games_df)),
            "last_refresh_utc": last_refresh.isoformat() if last_refresh else None,
        }

    def get_game_xgd(self, market_id: str, recent_n: int = 5, venue_recent_n: int = 5) -> dict[str, Any]:
        self.refresh_games(force=False)
        recent_n = clamp_int(recent_n, default=5, min_value=1, max_value=20)
        venue_recent_n = clamp_int(venue_recent_n, default=5, min_value=1, max_value=20)
        with self.lock:
            games_df = self.games_df.copy()

        if games_df.empty:
            raise KeyError("No games available")

        game_df = games_df[games_df["market_id"].astype(str) == str(market_id)].copy()
        if game_df.empty:
            raise KeyError("Market not found")

        game_row = game_df.iloc[0].to_dict()
        prediction_df = build_predictions(
            betfair_games_df=game_df,
            form_df=self.form_df,
            fixtures_df=self.fixtures_df,
            team_matcher=self.team_matcher,
            calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
            periods=self.periods,
            min_games=self.min_games,
        )

        if prediction_df.empty:
            return {
                "market_id": market_id,
                "event_name": str(game_row.get("event_name", "")),
                "competition": str(game_row.get("competition", "")),
                "kickoff_raw": str(game_row.get("kickoff_raw", "")),
                "mainline": str(game_row.get("mainline", "-")),
                "home_price": str(game_row.get("home_price", "-")),
                "away_price": str(game_row.get("away_price", "-")),
                "goal_mainline": str(game_row.get("goal_mainline", "-")),
                "goal_under_price": str(game_row.get("goal_under_price", "-")),
                "goal_over_price": str(game_row.get("goal_over_price", "-")),
                "summary": None,
                "period_rows": [],
                "mapping_rows": [],
                "recent_n": recent_n,
                "home_recent_rows": [],
                "away_recent_rows": [],
                "venue_recent_n": venue_recent_n,
                "home_team_venue_rows": {"home": [], "away": []},
                "away_team_venue_rows": {"home": [], "away": []},
                "warning": "No xGD output produced for this game.",
            }

        season_slice = prediction_df[prediction_df["period"].astype(str).str.lower() == "season"].copy()
        summary_row = season_slice.iloc[0] if not season_slice.empty else prediction_df.iloc[0]
        summary = {
            "home_xg": to_native(summary_row.get("home_xg")),
            "away_xg": to_native(summary_row.get("away_xg")),
            "total_xg": to_native(summary_row.get("total_xg")),
            "xgd": to_native(summary_row.get("xgd")),
        }

        period_cols = [
            "period",
            "home_xg",
            "away_xg",
            "total_xg",
            "xgd",
            "total_min_xg",
            "total_max_xg",
            "home_games_used",
            "away_games_used",
            "model_warning",
        ]
        period_cols = [c for c in period_cols if c in prediction_df.columns]
        period_rows: list[dict[str, Any]] = []
        for row in prediction_df[period_cols].to_dict(orient="records"):
            period_rows.append({k: to_native(v) for k, v in row.items()})

        mapping_cols = [
            "home_raw",
            "away_raw",
            "home_sofa",
            "away_sofa",
            "home_match_method",
            "away_match_method",
            "home_match_score",
            "away_match_score",
            "fixture_found",
            "fixture_competition",
            "fixture_area",
            "fixture_season_id",
        ]
        mapping_cols = [c for c in mapping_cols if c in prediction_df.columns]
        mapping_df = prediction_df[mapping_cols].drop_duplicates().reset_index(drop=True)
        mapping_rows = [{k: to_native(v) for k, v in row.items()} for row in mapping_df.to_dict(orient="records")]

        home_recent_rows: list[dict[str, Any]] = []
        away_recent_rows: list[dict[str, Any]] = []
        home_team_venue_rows: dict[str, list[dict[str, Any]]] = {"home": [], "away": []}
        away_team_venue_rows: dict[str, list[dict[str, Any]]] = {"home": [], "away": []}
        if not mapping_df.empty:
            mapping_row = mapping_df.iloc[0].to_dict()
            home_sofa = mapping_row.get("home_sofa")
            away_sofa = mapping_row.get("away_sofa")
            kickoff_time = game_row.get("kickoff_time")
            if home_sofa and away_sofa and not pd.isna(kickoff_time):
                model_games_df = pd.DataFrame(
                    [
                        {
                            "home": home_sofa,
                            "away": away_sofa,
                            "match_date": kickoff_time,
                            "season_id": mapping_row.get("fixture_season_id"),
                            "competition_name": mapping_row.get("fixture_competition"),
                            "area_name": mapping_row.get("fixture_area"),
                        }
                    ]
                )
                try:
                    _, source_games = self.xgd_module.calc_wyscout_form_tables(
                        model_games_df,
                        self.form_df,
                        periods=self.periods,
                        return_source_games=True,
                        min_games=self.min_games,
                    )
                    if source_games:
                        source = source_games[0]
                        home_recent_rows = build_recent_form_rows(source.get("home_source_games"), recent_n)
                        away_recent_rows = build_recent_form_rows(source.get("away_source_games"), recent_n)
                except Exception:
                    home_recent_rows = []
                    away_recent_rows = []

                home_team_venue_rows = build_team_venue_recent_rows(
                    form_df=self.form_df,
                    team_name=home_sofa,
                    kickoff_time=kickoff_time,
                    recent_n=venue_recent_n,
                    season_id=mapping_row.get("fixture_season_id"),
                    competition_name=mapping_row.get("fixture_competition"),
                    area_name=mapping_row.get("fixture_area"),
                )
                away_team_venue_rows = build_team_venue_recent_rows(
                    form_df=self.form_df,
                    team_name=away_sofa,
                    kickoff_time=kickoff_time,
                    recent_n=venue_recent_n,
                    season_id=mapping_row.get("fixture_season_id"),
                    competition_name=mapping_row.get("fixture_competition"),
                    area_name=mapping_row.get("fixture_area"),
                )

        return {
            "market_id": market_id,
            "event_name": str(game_row.get("event_name", "")),
            "competition": str(game_row.get("competition", "")),
            "kickoff_raw": str(game_row.get("kickoff_raw", "")),
            "mainline": str(game_row.get("mainline", "-")),
            "home_price": str(game_row.get("home_price", "-")),
            "away_price": str(game_row.get("away_price", "-")),
            "goal_mainline": str(game_row.get("goal_mainline", "-")),
            "goal_under_price": str(game_row.get("goal_under_price", "-")),
            "goal_over_price": str(game_row.get("goal_over_price", "-")),
            "summary": summary,
            "period_rows": period_rows,
            "mapping_rows": mapping_rows,
            "recent_n": recent_n,
            "home_recent_rows": home_recent_rows,
            "away_recent_rows": away_recent_rows,
            "venue_recent_n": venue_recent_n,
            "home_team_venue_rows": home_team_venue_rows,
            "away_team_venue_rows": away_team_venue_rows,
            "warning": None,
        }


class AppHandler(BaseHTTPRequestHandler):
    state: AppState

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            return self._serve_static("index.html", "text/html; charset=utf-8")
        if path == "/app.js":
            return self._serve_static("app.js", "application/javascript; charset=utf-8")
        if path == "/styles.css":
            return self._serve_static("styles.css", "text/css; charset=utf-8")
        if path == "/api/games":
            return self._serve_games()
        if path == "/api/game-xgd":
            query = parse_qs(parsed.query)
            market_id = (query.get("market_id") or [""])[0]
            recent_n_raw = (query.get("recent_n") or ["5"])[0]
            recent_n = clamp_int(recent_n_raw, default=5, min_value=1, max_value=20)
            venue_recent_n_raw = (query.get("venue_recent_n") or ["5"])[0]
            venue_recent_n = clamp_int(venue_recent_n_raw, default=5, min_value=1, max_value=20)
            return self._serve_game_xgd(market_id, recent_n, venue_recent_n)

        self._json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _serve_games(self) -> None:
        try:
            payload = self.state.list_games_grouped_by_day()
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_game_xgd(self, market_id: str, recent_n: int, venue_recent_n: int) -> None:
        if not market_id:
            self._json({"error": "market_id is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = self.state.get_game_xgd(market_id, recent_n=recent_n, venue_recent_n=venue_recent_n)
            self._json(payload)
        except KeyError:
            self._json({"error": "Market not found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_static(self, relative_path: str, content_type: str) -> None:
        path = WEBAPP_DIR / relative_path
        if not path.exists():
            self._json({"error": f"Missing asset: {path}"}, status=HTTPStatus.NOT_FOUND)
            return
        data = path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def main() -> None:
    args = parse_args()
    state = AppState(args)
    state.refresh_games(force=True)

    AppHandler.state = state
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    print(f"xGD web app running on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
