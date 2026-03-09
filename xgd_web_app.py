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
from collections import Counter
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
DEFAULT_MANUAL_TEAM_MAPPINGS = APP_DIR / "manual_team_mappings.json"
DEFAULT_MANUAL_COMPETITION_MAPPINGS = APP_DIR / "manual_competition_mappings.json"
DEFAULT_LEAGUE_TIER = "Unassigned"

PERIOD_METRIC_COLUMNS = (
    "season_xgd",
    "last5_xgd",
    "last3_xgd",
    "season_strength",
    "last5_strength",
    "last3_strength",
    "season_min_xg",
    "last5_min_xg",
    "last3_min_xg",
    "season_max_xg",
    "last5_max_xg",
    "last3_max_xg",
    "xgd_competition_mismatch",
)

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


def ensure_selected_leagues_file(path: Path) -> None:
    if path.exists():
        return
    with path.open("w", encoding="utf-8") as f:
        f.write("# Format: competition_id|competition_name|tier\n")
        f.write("# Tier is optional, but recommended for UI filtering.\n")
        f.write("# Example:\n")
        f.write("# 10932509|English Premier League|Tier 1\n")


def load_selected_league_entries(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        return [], []

    selected: list[dict[str, str]] = []
    invalid_lines: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            value = raw_line.strip()
            if not value or value.startswith("#"):
                continue
            parts = [part.strip() for part in value.split("|")]
            if len(parts) not in (2, 3):
                invalid_lines.append(value)
                continue
            competition_id, competition_name = parts[0], parts[1]
            tier = parts[2] if len(parts) == 3 else DEFAULT_LEAGUE_TIER
            if not competition_id or not competition_name:
                invalid_lines.append(value)
                continue
            selected.append(
                {
                    "competition_id": competition_id,
                    "competition_name": competition_name,
                    "tier": tier or DEFAULT_LEAGUE_TIER,
                }
            )
    return selected, invalid_lines


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
    market_name = str(catalogue.get("marketName", "")).strip().lower()
    is_match_odds_market = market_name == "match odds"
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
    home_match_odds_candidates: list[dict[str, Any]] = []
    away_match_odds_candidates: list[dict[str, Any]] = []
    for runner in market_book.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue
        info = runner_info.get(selection_id, {})
        runner_name = str(info.get("name", "")).strip() or f"Selection {selection_id}"

        ex = runner.get("ex", {})
        back = get_best_offer_price(ex, "availableToBack")
        lay = get_best_offer_price(ex, "availableToLay")
        mid_price = (
            (float(back) + float(lay)) / 2.0
            if (back is not None and lay is not None)
            else (float(back) if back is not None else (float(lay) if lay is not None else None))
        )
        spread = (float(lay) - float(back)) if (back is not None and lay is not None) else None

        handicap = parse_handicap_value(runner.get("handicap", info.get("handicap")))
        if is_match_odds_market:
            handicap = None
        if handicap is None:
            if runner_name_matches(runner_name, home_team):
                home_match_odds_candidates.append(
                    {
                        "mid": mid_price,
                        "spread": spread,
                    }
                )
            elif runner_name_matches(runner_name, away_team):
                away_match_odds_candidates.append(
                    {
                        "mid": mid_price,
                        "spread": spread,
                    }
                )
            continue

        row = {
            "handicap": float(handicap),
            "back": float(back) if back is not None else None,
            "lay": float(lay) if lay is not None else None,
            "spread": spread,
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

    lines: list[dict[str, Any]] = []
    if home_by_hcp and away_by_hcp:
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

    def select_match_odds_mid(rows: list[dict[str, Any]]) -> float | None:
        if not rows:
            return None
        valid = [row for row in rows if row.get("mid") is not None]
        if not valid:
            return None

        def sort_key(row: dict[str, Any]) -> tuple[float, float]:
            spread_value = row.get("spread")
            spread_rank = float(spread_value) if spread_value is not None else float("inf")
            mid = float(row.get("mid"))
            return (spread_rank, abs(mid - 2.0))

        best = min(valid, key=sort_key)
        return float(best.get("mid"))

    if not lines:
        home_match_odds_mid = select_match_odds_mid(home_match_odds_candidates)
        away_match_odds_mid = select_match_odds_mid(away_match_odds_candidates)
        if home_match_odds_mid is not None and away_match_odds_mid is not None:
            return {
                "mainline": "-",
                "home_price": format_price_value(home_match_odds_mid),
                "away_price": format_price_value(away_match_odds_mid),
            }
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
        self.team_set = set(unique)
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


def apply_manual_or_match(
    raw_name: str | None,
    team_matcher: TeamMatcher,
    manual_mapping_lookup: dict[str, str] | None = None,
    disallow: set[str] | None = None,
) -> tuple[str | None, float | None, str]:
    if raw_name is None or not str(raw_name).strip():
        return None, None, "missing"

    disallow = disallow or set()
    if manual_mapping_lookup:
        raw_norm = normalize_team_name(raw_name)
        manual_target = manual_mapping_lookup.get(raw_norm)
        if manual_target and manual_target in team_matcher.team_set and manual_target not in disallow:
            return manual_target, 1.0, "manual"

    return team_matcher.match(raw_name, disallow=disallow)


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


def map_betfair_games(
    betfair_games_df: pd.DataFrame,
    fixtures_df: pd.DataFrame,
    team_matcher: TeamMatcher,
    manual_mapping_lookup: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
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

        home_team, home_score, home_method = apply_manual_or_match(
            row.get("home_raw"),
            team_matcher=team_matcher,
            manual_mapping_lookup=manual_mapping_lookup,
        )
        away_team, away_score, away_method = apply_manual_or_match(
            row.get("away_raw"),
            team_matcher=team_matcher,
            manual_mapping_lookup=manual_mapping_lookup,
            disallow={home_team} if home_team else None,
        )

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

    return mapped_games, model_games


def build_predictions(
    betfair_games_df: pd.DataFrame,
    form_df: pd.DataFrame,
    fixtures_df: pd.DataFrame,
    team_matcher: TeamMatcher,
    calc_wyscout_form_tables: Any,
    periods: tuple[Any, ...],
    min_games: int,
    manual_mapping_lookup: dict[str, str] | None = None,
    manual_competition_mapping_lookup: dict[str, str] | None = None,
) -> pd.DataFrame:
    if betfair_games_df.empty:
        return pd.DataFrame()

    mapped_games, model_games = map_betfair_games(
        betfair_games_df=betfair_games_df,
        fixtures_df=fixtures_df,
        team_matcher=team_matcher,
        manual_mapping_lookup=manual_mapping_lookup,
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
            pred_rows.append({**mapped, "period": None, "xgd_competition_mismatch": False})
            continue

        _, _, _, _, reduced = game_tables[model_idx]
        sources = source_games[model_idx]
        xgd_competition_mismatch = source_competitions_differ_from_betfair_competition(
            betfair_competition=mapped.get("competition"),
            home_source_games=sources.get("home_source_games"),
            away_source_games=sources.get("away_source_games"),
            manual_competition_mapping_lookup=manual_competition_mapping_lookup,
        )
        home_used = len(sources.get("home_source_games", []))
        away_used = len(sources.get("away_source_games", []))
        warning = sources.get("warning")
        no_common_season = "no common active season found" in str(warning or "").lower()

        for _, period_row in reduced.iterrows():
            home_xg = first_numeric_value(period_row, ("Team Home Real xG", "Avg Home Real xG"), default=0.0)
            away_xg = first_numeric_value(period_row, ("Team Away Real xG", "Avg Away Real xG"), default=0.0)
            total_xg = first_numeric_value(period_row, ("Total Team Real xG", "Total Avg Real xG"), default=0.0)
            xgd = first_numeric_value(period_row, ("Team Real xGD", "Avg Real xGD"), default=0.0)
            strength = first_numeric_value(period_row, ("Strength",), default=0.0)
            total_min_xg = first_numeric_value(period_row, ("Total Min Real xG",), default=0.0)
            total_max_xg = first_numeric_value(period_row, ("Total Max Real xG",), default=0.0)
            pred_rows.append(
                {
                    **mapped,
                    "period": period_row.get("Period"),
                    "home_xg": home_xg,
                    "away_xg": away_xg,
                    "total_xg": total_xg,
                    "xgd": (None if no_common_season else xgd),
                    "strength": (None if no_common_season else strength),
                    "total_min_xg": (None if no_common_season else total_min_xg),
                    "total_max_xg": (None if no_common_season else total_max_xg),
                    "home_games_used": int(home_used),
                    "away_games_used": int(away_used),
                    "model_warning": warning,
                    "xgd_competition_mismatch": bool(xgd_competition_mismatch),
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


def simplify_model_warning(messages: list[str]) -> str | None:
    cleaned = [str(msg).strip() for msg in messages if str(msg).strip()]
    if not cleaned:
        return None
    lowered = [msg.lower() for msg in cleaned]
    if any("no common active season found" in msg for msg in lowered):
        return "Season warning: no common active season found. xGD may be less reliable."
    if any("season boundaries unavailable" in msg for msg in lowered):
        return "Season warning: season boundaries unavailable. xGD may be less reliable."
    return "Model warning for this fixture. xGD may be less reliable."


def first_numeric_value(
    row: pd.Series | dict[str, Any],
    keys: tuple[str, ...],
    default: float | None = 0.0,
) -> float | None:
    for key in keys:
        value = row.get(key)
        if pd.isna(value):
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return default


def period_rows_from_reduced_table(
    reduced_df: pd.DataFrame,
    warning: str | None = None,
    home_games_used: int = 0,
    away_games_used: int = 0,
) -> list[dict[str, Any]]:
    if not isinstance(reduced_df, pd.DataFrame) or reduced_df.empty:
        return []

    no_common_season = "no common active season found" in str(warning or "").lower()
    period_rows: list[dict[str, Any]] = []
    for _, period_row in reduced_df.iterrows():
        home_xg = first_numeric_value(period_row, ("Team Home Real xG", "Avg Home Real xG"), default=0.0)
        away_xg = first_numeric_value(period_row, ("Team Away Real xG", "Avg Away Real xG"), default=0.0)
        total_xg = first_numeric_value(period_row, ("Total Team Real xG", "Total Avg Real xG"), default=0.0)
        xgd = first_numeric_value(period_row, ("Team Real xGD", "Avg Real xGD"), default=0.0)
        strength = first_numeric_value(period_row, ("Strength",), default=0.0)
        total_min_xg = first_numeric_value(period_row, ("Total Min Real xG",), default=0.0)
        total_max_xg = first_numeric_value(period_row, ("Total Max Real xG",), default=0.0)
        period_rows.append(
            {
                "period": period_row.get("Period"),
                "home_xg": home_xg,
                "away_xg": away_xg,
                "total_xg": total_xg,
                "xgd": (None if no_common_season else xgd),
                "strength": (None if no_common_season else strength),
                "total_min_xg": (None if no_common_season else total_min_xg),
                "total_max_xg": (None if no_common_season else total_max_xg),
                "home_games_used": int(home_games_used),
                "away_games_used": int(away_games_used),
                "model_warning": warning,
            }
        )
    return [{k: to_native(v) for k, v in row.items()} for row in period_rows]


def summary_from_period_rows(period_rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not period_rows:
        return None
    season_row = next((row for row in period_rows if normalize_period_key(row.get("period")) == "season"), None)
    chosen = season_row or period_rows[0]
    return {
        "home_xg": to_native(chosen.get("home_xg")),
        "away_xg": to_native(chosen.get("away_xg")),
        "total_xg": to_native(chosen.get("total_xg")),
        "xgd": to_native(chosen.get("xgd")),
        "strength": to_native(chosen.get("strength")),
    }


def is_european_competition_name(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    key = normalize_competition_key(text)
    lowered = text.casefold()
    if "uefa" in lowered:
        return True
    return any(
        token in key
        for token in (
            "championsleague",
            "europaleague",
            "conferenceleague",
            "supercup",
        )
    )


def infer_area_for_competition(
    form_df: pd.DataFrame,
    competition_name: str | None,
    fallback_area: str | None = None,
) -> str | None:
    comp = str(competition_name or "").strip()
    if not comp:
        return str(fallback_area or "").strip() or None
    if form_df.empty or "competition_name" not in form_df.columns or "area_name" not in form_df.columns:
        return str(fallback_area or "").strip() or None

    comp_rows = form_df[form_df["competition_name"].astype(str) == comp]
    if comp_rows.empty:
        return str(fallback_area or "").strip() or None

    area_counts: Counter[str] = Counter(
        str(value).strip()
        for value in comp_rows["area_name"].dropna().tolist()
        if str(value).strip()
    )
    if not area_counts:
        return str(fallback_area or "").strip() or None
    return area_counts.most_common(1)[0][0]


def infer_shared_domestic_competition(
    form_df: pd.DataFrame,
    home_team: str | None,
    away_team: str | None,
    kickoff_time: Any,
) -> tuple[str | None, str | None]:
    home_name = str(home_team or "").strip()
    away_name = str(away_team or "").strip()
    if not home_name or not away_name or form_df.empty:
        return None, None
    if "team" not in form_df.columns or "competition_name" not in form_df.columns:
        return None, None

    cutoff = pd.to_datetime(kickoff_time, errors="coerce", utc=True)

    def collect_counts(team_name: str) -> Counter[tuple[str, str]]:
        team_rows = form_df[form_df["team"].astype(str) == team_name].copy()
        if team_rows.empty:
            return Counter()

        if not pd.isna(cutoff) and "date_time" in team_rows.columns:
            team_rows["date_time"] = pd.to_datetime(team_rows["date_time"], errors="coerce", utc=True)
            team_rows = team_rows[team_rows["date_time"] < cutoff]

        if (
            not pd.isna(cutoff)
            and "season_start_date" in team_rows.columns
            and "season_end_date" in team_rows.columns
        ):
            active_rows = team_rows[
                (pd.to_datetime(team_rows["season_start_date"], errors="coerce") <= cutoff)
                & (pd.to_datetime(team_rows["season_end_date"], errors="coerce") >= cutoff)
            ].copy()
            if not active_rows.empty:
                team_rows = active_rows

        counts: Counter[tuple[str, str]] = Counter()
        for row in team_rows.to_dict(orient="records"):
            competition_name = str(row.get("competition_name", "")).strip()
            if not competition_name:
                continue
            area_name = str(row.get("area_name", "")).strip()
            if area_name.casefold() == "europe":
                continue
            if is_european_competition_name(competition_name):
                continue
            counts[(competition_name, area_name)] += 1
        return counts

    home_counts = collect_counts(home_name)
    away_counts = collect_counts(away_name)
    common = set(home_counts.keys()).intersection(set(away_counts.keys()))
    if not common:
        return None, None

    best_competition = max(
        common,
        key=lambda key: (
            home_counts[key] + away_counts[key],
            min(home_counts[key], away_counts[key]),
            home_counts[key],
            away_counts[key],
            key[0].casefold(),
        ),
    )
    return best_competition[0], best_competition[1]


def infer_team_domestic_competition(
    form_df: pd.DataFrame,
    team_name: str | None,
    kickoff_time: Any,
) -> tuple[str | None, str | None]:
    team = str(team_name or "").strip()
    if not team or form_df.empty:
        return None, None
    if "team" not in form_df.columns or "competition_name" not in form_df.columns:
        return None, None

    team_rows = form_df[form_df["team"].astype(str) == team].copy()
    if team_rows.empty:
        return None, None

    cutoff = pd.to_datetime(kickoff_time, errors="coerce", utc=True)
    if not pd.isna(cutoff) and "date_time" in team_rows.columns:
        team_rows["date_time"] = pd.to_datetime(team_rows["date_time"], errors="coerce", utc=True)
        team_rows = team_rows[team_rows["date_time"] < cutoff]

    if (
        not pd.isna(cutoff)
        and "season_start_date" in team_rows.columns
        and "season_end_date" in team_rows.columns
    ):
        active_rows = team_rows[
            (pd.to_datetime(team_rows["season_start_date"], errors="coerce") <= cutoff)
            & (pd.to_datetime(team_rows["season_end_date"], errors="coerce") >= cutoff)
        ].copy()
        if not active_rows.empty:
            team_rows = active_rows

    if team_rows.empty:
        return None, None

    counts: Counter[tuple[str, str]] = Counter()
    for row in team_rows.to_dict(orient="records"):
        competition_name = str(row.get("competition_name", "")).strip()
        if not competition_name:
            continue
        area_name = str(row.get("area_name", "")).strip()
        if area_name.casefold() == "europe":
            continue
        if is_european_competition_name(competition_name):
            continue
        counts[(competition_name, area_name)] += 1

    if not counts:
        return None, None
    best = max(counts.items(), key=lambda item: (item[1], item[0][0].casefold()))[0]
    return best[0], best[1]


def select_team_competition_rows(
    form_df: pd.DataFrame,
    team_name: str | None,
    competition_name: str | None,
    area_name: str | None,
    kickoff_time: Any,
) -> tuple[pd.DataFrame, Any]:
    team = str(team_name or "").strip()
    competition = str(competition_name or "").strip()
    area = str(area_name or "").strip()
    if not team or not competition or form_df.empty:
        return form_df.iloc[0:0].copy(), None
    if "team" not in form_df.columns or "competition_name" not in form_df.columns:
        return form_df.iloc[0:0].copy(), None

    rows = form_df[
        (form_df["team"].astype(str) == team)
        & (form_df["competition_name"].astype(str) == competition)
    ].copy()
    if area and "area_name" in rows.columns:
        rows = rows[rows["area_name"].astype(str) == area].copy()

    cutoff = pd.to_datetime(kickoff_time, errors="coerce", utc=True)
    if not pd.isna(cutoff) and "date_time" in rows.columns:
        rows["date_time"] = pd.to_datetime(rows["date_time"], errors="coerce", utc=True)
        rows = rows[rows["date_time"] < cutoff]
    if rows.empty:
        return rows, None

    season_id_used = None
    if (
        not pd.isna(cutoff)
        and "season_start_date" in rows.columns
        and "season_end_date" in rows.columns
        and "season_id" in rows.columns
    ):
        active = rows[
            (pd.to_datetime(rows["season_start_date"], errors="coerce") <= cutoff)
            & (pd.to_datetime(rows["season_end_date"], errors="coerce") >= cutoff)
        ].copy()
        if not active.empty:
            counts = active["season_id"].dropna().value_counts()
            if not counts.empty:
                season_id_used = counts.index[0]
                rows = active[active["season_id"] == season_id_used].copy()
            else:
                rows = active

    if season_id_used is None and "season_id" in rows.columns:
        season_values = rows["season_id"].dropna()
        if not season_values.empty:
            if "date_time" in rows.columns:
                rows = rows.sort_values("date_time")
            season_id_used = rows["season_id"].dropna().iloc[-1]
            rows = rows[rows["season_id"] == season_id_used].copy()

    return rows, season_id_used


def build_xgd_view_from_form_df(
    *,
    view_id: str,
    label: str,
    home_sofa: str,
    away_sofa: str,
    kickoff_time: Any,
    calc_form_df: pd.DataFrame,
    full_form_df: pd.DataFrame,
    calc_wyscout_form_tables: Any,
    periods: tuple[Any, ...],
    min_games: int,
    home_venue_filter: dict[str, Any] | None = None,
    away_venue_filter: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if calc_form_df.empty:
        return None

    model_games_df = pd.DataFrame([{"home": home_sofa, "away": away_sofa, "match_date": kickoff_time}])
    try:
        game_tables, source_games = calc_wyscout_form_tables(
            model_games_df,
            calc_form_df,
            periods=periods,
            return_source_games=True,
            min_games=int(min_games),
        )
    except Exception:
        return None

    if not game_tables:
        return None

    _, _, _, _, reduced = game_tables[0]
    source = source_games[0] if source_games else {}
    home_source = source.get("home_source_games")
    away_source = source.get("away_source_games")
    home_used = int(len(home_source)) if isinstance(home_source, pd.DataFrame) else int(len(home_source or []))
    away_used = int(len(away_source)) if isinstance(away_source, pd.DataFrame) else int(len(away_source or []))
    warning_raw = str(source.get("warning") or "").strip() or None

    period_rows = period_rows_from_reduced_table(
        reduced_df=reduced,
        warning=warning_raw,
        home_games_used=home_used,
        away_games_used=away_used,
    )
    if not period_rows:
        return None

    home_recent_rows = build_recent_form_rows(home_source, recent_n=None)
    away_recent_rows = build_recent_form_rows(away_source, recent_n=None)

    home_filter = home_venue_filter or {}
    away_filter = away_venue_filter or {}
    home_team_venue_rows = build_team_venue_recent_rows(
        form_df=full_form_df,
        team_name=home_sofa,
        kickoff_time=kickoff_time,
        recent_n=None,
        season_id=home_filter.get("season_id"),
        competition_name=home_filter.get("competition_name"),
        area_name=home_filter.get("area_name"),
    )
    away_team_venue_rows = build_team_venue_recent_rows(
        form_df=full_form_df,
        team_name=away_sofa,
        kickoff_time=kickoff_time,
        recent_n=None,
        season_id=away_filter.get("season_id"),
        competition_name=away_filter.get("competition_name"),
        area_name=away_filter.get("area_name"),
    )

    return {
        "id": str(view_id or "view"),
        "label": str(label or "View"),
        "summary": summary_from_period_rows(period_rows),
        "period_rows": period_rows,
        "warning": simplify_model_warning([warning_raw]) if warning_raw else None,
        "home_recent_rows": home_recent_rows,
        "away_recent_rows": away_recent_rows,
        "home_team_venue_rows": home_team_venue_rows,
        "away_team_venue_rows": away_team_venue_rows,
    }


def resolve_sofa_competition_name(
    raw_competition: str | None,
    manual_competition_mapping_lookup: dict[str, str] | None,
    sofa_competitions: list[str],
    sofa_competition_by_norm: dict[str, str],
    sofa_competition_set: set[str],
) -> str | None:
    raw_text = str(raw_competition or "").strip()
    if not raw_text:
        return None

    raw_key = normalize_competition_key(raw_text)
    if raw_key and manual_competition_mapping_lookup:
        manual_target = manual_competition_mapping_lookup.get(raw_key)
        if manual_target and manual_target in sofa_competition_set:
            return manual_target

    matched_name, _, _ = match_competition_name(
        raw_text,
        competition_names=sofa_competitions,
        competition_by_norm=sofa_competition_by_norm,
    )
    return matched_name


def build_competition_specific_xgd_section(
    home_sofa: str,
    away_sofa: str,
    kickoff_time: Any,
    competition_name: str,
    area_name: str | None,
    label: str,
    calc_wyscout_form_tables: Any,
    form_df: pd.DataFrame,
    periods: tuple[Any, ...],
    min_games: int,
) -> dict[str, Any] | None:
    if not home_sofa or not away_sofa:
        return None
    comp_name = str(competition_name or "").strip()
    area = str(area_name or "").strip()
    if not comp_name:
        return None

    model_row: dict[str, Any] = {
        "home": home_sofa,
        "away": away_sofa,
        "match_date": kickoff_time,
        "competition_name": comp_name,
    }
    if area:
        model_row["area_name"] = area

    try:
        game_tables, source_games = calc_wyscout_form_tables(
            pd.DataFrame([model_row]),
            form_df,
            periods=periods,
            return_source_games=True,
            min_games=int(min_games),
        )
    except Exception:
        return None

    if not game_tables:
        return None

    _, _, _, _, reduced = game_tables[0]
    source = source_games[0] if source_games else {}
    home_source = source.get("home_source_games")
    away_source = source.get("away_source_games")
    home_used = int(len(home_source)) if isinstance(home_source, pd.DataFrame) else int(len(home_source or []))
    away_used = int(len(away_source)) if isinstance(away_source, pd.DataFrame) else int(len(away_source or []))
    warning = str(source.get("warning") or "").strip() or None

    period_rows = period_rows_from_reduced_table(
        reduced_df=reduced,
        warning=warning,
        home_games_used=home_used,
        away_games_used=away_used,
    )
    if not period_rows:
        return None

    return {
        "label": str(label or comp_name),
        "competition_name": comp_name,
        "area_name": (area or None),
        "summary": summary_from_period_rows(period_rows),
        "period_rows": period_rows,
        "warning": simplify_model_warning([warning]) if warning else None,
    }


def clamp_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(min_value, min(max_value, out))


def format_float_value(value: Any, decimals: int = 2) -> str:
    try:
        if value is None or pd.isna(value):
            return "-"
        return f"{float(value):.{int(decimals)}f}"
    except Exception:
        return "-"


def normalize_period_key(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    if text == "season":
        return "season"
    try:
        as_int = int(float(text))
    except Exception:
        return None
    if as_int == 5:
        return "last5"
    if as_int == 3:
        return "last3"
    return None


def normalize_competition_key(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip().casefold()
    if not text:
        return ""
    return re.sub(r"[^a-z0-9]+", "", text)


def competition_keys_match(left: str, right: str) -> bool:
    left_key = str(left or "").strip()
    right_key = str(right or "").strip()
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    if left_key in right_key or right_key in left_key:
        return True
    return False


def competition_token_set(value: Any) -> set[str]:
    text = str(value or "").strip().casefold()
    if not text:
        return set()
    return set(re.findall(r"[a-z0-9]+", text))


def match_competition_name(
    raw_name: str | None,
    competition_names: list[str],
    competition_by_norm: dict[str, str] | None = None,
) -> tuple[str | None, float | None, str]:
    if raw_name is None or not str(raw_name).strip():
        return None, None, "missing"

    raw_text = str(raw_name).strip()
    raw_norm = normalize_competition_key(raw_text)
    if not raw_norm:
        return None, None, "missing"

    by_norm = competition_by_norm or {}
    exact = by_norm.get(raw_norm)
    if exact:
        return exact, 1.0, "exact"

    raw_tokens = competition_token_set(raw_text)
    best_name: str | None = None
    best_score = -1.0
    for candidate in competition_names:
        cand_name = str(candidate).strip()
        if not cand_name:
            continue
        cand_norm = normalize_competition_key(cand_name)
        if not cand_norm:
            continue

        seq = SequenceMatcher(None, raw_norm, cand_norm).ratio()
        cand_tokens = competition_token_set(cand_name)
        overlap = 0.0
        if raw_tokens and cand_tokens:
            overlap = len(raw_tokens & cand_tokens) / len(raw_tokens | cand_tokens)
        substring_bonus = 0.0
        if raw_norm in cand_norm or cand_norm in raw_norm:
            substring_bonus = 0.08
        score = (0.7 * seq) + (0.3 * overlap) + substring_bonus
        if score > best_score:
            best_score = score
            best_name = cand_name

    if best_name is None or best_score < 0.8:
        return None, best_score if best_score >= 0 else None, "unmatched"
    return best_name, float(best_score), "fuzzy"


def source_competitions_differ_from_betfair_competition(
    betfair_competition: Any,
    home_source_games: Any,
    away_source_games: Any,
    manual_competition_mapping_lookup: dict[str, str] | None = None,
) -> bool:
    betfair_key = normalize_competition_key(betfair_competition)
    if not betfair_key:
        return False

    expected_keys: set[str] = {betfair_key}
    if manual_competition_mapping_lookup:
        manual_target = manual_competition_mapping_lookup.get(betfair_key)
        mapped_key = normalize_competition_key(manual_target) if manual_target else ""
        if mapped_key:
            expected_keys.add(mapped_key)

    source_keys: set[str] = set()
    for source_df in (home_source_games, away_source_games):
        if not isinstance(source_df, pd.DataFrame) or source_df.empty or "competition_name" not in source_df.columns:
            continue
        for value in source_df["competition_name"].dropna().astype(str).tolist():
            key = normalize_competition_key(value)
            if key:
                source_keys.add(key)

    if not source_keys:
        return False
    return any(
        all(not competition_keys_match(expected_key, source_key) for expected_key in expected_keys)
        for source_key in source_keys
    )


def coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        try:
            if pd.isna(value):
                return False
        except Exception:
            pass
        return bool(value)
    text = str(value).strip().lower()
    if not text:
        return False
    return text in {"1", "true", "yes", "y", "t"}


def extract_period_metrics(prediction_df: pd.DataFrame) -> pd.DataFrame:
    out_cols = [
        "market_id",
        "season_xgd",
        "last5_xgd",
        "last3_xgd",
        "season_strength",
        "last5_strength",
        "last3_strength",
        "season_min_xg",
        "last5_min_xg",
        "last3_min_xg",
        "season_max_xg",
        "last5_max_xg",
        "last3_max_xg",
        "xgd_competition_mismatch",
    ]
    if prediction_df.empty or "market_id" not in prediction_df.columns:
        return pd.DataFrame(columns=out_cols)

    base = prediction_df.copy()
    base = base.dropna(subset=["market_id"]).drop_duplicates(subset=["market_id"], keep="first")
    out = base[["market_id"]].copy()
    if "xgd_competition_mismatch" in base.columns:
        out["xgd_competition_mismatch"] = base["xgd_competition_mismatch"].map(coerce_bool)
    else:
        out["xgd_competition_mismatch"] = False

    metric_specs = [
        ("xgd", "xgd"),
        ("strength", "strength"),
        ("total_min_xg", "min_xg"),
        ("total_max_xg", "max_xg"),
    ]

    if "period" not in prediction_df.columns:
        for source_col, target_suffix in metric_specs:
            season_col = f"season_{target_suffix}"
            last5_col = f"last5_{target_suffix}"
            last3_col = f"last3_{target_suffix}"
            if source_col in base.columns:
                out[season_col] = base[source_col]
            else:
                out[season_col] = None
            out[last5_col] = None
            out[last3_col] = None
        return out[out_cols]

    with_period = prediction_df.copy()
    with_period["_period_key"] = with_period["period"].map(normalize_period_key)
    with_period = with_period[with_period["_period_key"].notna()].copy()

    for source_col, target_suffix in metric_specs:
        season_col = f"season_{target_suffix}"
        last5_col = f"last5_{target_suffix}"
        last3_col = f"last3_{target_suffix}"
        if source_col not in with_period.columns:
            out[season_col] = None
            out[last5_col] = None
            out[last3_col] = None
            continue

        pivot = with_period.pivot_table(
            index="market_id",
            columns="_period_key",
            values=source_col,
            aggfunc="first",
        )
        out = out.merge(
            pivot.rename(
                columns={
                    "season": season_col,
                    "last5": last5_col,
                    "last3": last3_col,
                }
            ),
            left_on="market_id",
            right_index=True,
            how="left",
        )
        if season_col not in out.columns:
            out[season_col] = None
        if last5_col not in out.columns:
            out[last5_col] = None
        if last3_col not in out.columns:
            out[last3_col] = None

    return out[out_cols]


def build_recent_form_rows(source_df: Any, recent_n: int | None = None) -> list[dict[str, Any]]:
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
    if recent_n is not None:
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
    recent_n: int | None = None,
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
        "home": build_recent_form_rows(home_df, recent_n=recent_n),
        "away": build_recent_form_rows(away_df, recent_n=recent_n),
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
        self.manual_mappings_path = DEFAULT_MANUAL_TEAM_MAPPINGS
        self.manual_team_mappings = self._load_manual_team_mappings()
        self.manual_mapping_lookup = self._build_manual_mapping_lookup(self.manual_team_mappings)
        sofa_competitions = []
        if "competition_name" in self.fixtures_df.columns:
            sofa_competitions = sorted(
                {
                    str(value).strip()
                    for value in self.fixtures_df["competition_name"].dropna().tolist()
                    if str(value).strip()
                }
            )
        self.sofa_competitions = sofa_competitions
        self.sofa_competition_set = set(self.sofa_competitions)
        self.sofa_competition_by_norm: dict[str, str] = {}
        for competition_name in self.sofa_competitions:
            norm = normalize_competition_key(competition_name)
            if norm and norm not in self.sofa_competition_by_norm:
                self.sofa_competition_by_norm[norm] = competition_name
        self.manual_competition_mappings_path = DEFAULT_MANUAL_COMPETITION_MAPPINGS
        self.manual_competition_mappings = self._load_manual_competition_mappings()
        self.manual_competition_mapping_lookup = self._build_manual_competition_mapping_lookup(
            self.manual_competition_mappings
        )

        self.games_df = pd.DataFrame()
        self.last_refresh: dt.datetime | None = None

    def _load_manual_team_mappings(self) -> dict[str, str]:
        path = self.manual_mappings_path
        if not path.exists():
            return {}
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

        if isinstance(raw_data, dict):
            items = raw_data.items()
        elif isinstance(raw_data, list):
            pairs: list[tuple[str, str]] = []
            for row in raw_data:
                if not isinstance(row, dict):
                    continue
                raw_name = str(row.get("raw_name", "")).strip()
                sofa_name = str(row.get("sofa_name", "")).strip()
                if raw_name and sofa_name:
                    pairs.append((raw_name, sofa_name))
            items = pairs
        else:
            return {}

        out: dict[str, str] = {}
        for raw_name, sofa_name in items:
            raw_team = str(raw_name).strip()
            sofa_team = str(sofa_name).strip()
            if not raw_team or not sofa_team:
                continue
            if sofa_team not in self.team_matcher.team_set:
                continue
            out[raw_team] = sofa_team
        return out

    @staticmethod
    def _build_manual_mapping_lookup(mappings: dict[str, str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for raw_name, sofa_name in mappings.items():
            norm = normalize_team_name(raw_name)
            if norm:
                out[norm] = sofa_name
        return out

    def _load_manual_competition_mappings(self) -> dict[str, str]:
        path = self.manual_competition_mappings_path
        if not path.exists():
            return {}
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

        if isinstance(raw_data, dict):
            items = raw_data.items()
        elif isinstance(raw_data, list):
            pairs: list[tuple[str, str]] = []
            for row in raw_data:
                if not isinstance(row, dict):
                    continue
                raw_name = str(row.get("raw_name", "")).strip()
                sofa_name = str(row.get("sofa_name", "")).strip()
                if raw_name and sofa_name:
                    pairs.append((raw_name, sofa_name))
            items = pairs
        else:
            return {}

        out: dict[str, str] = {}
        for raw_name, sofa_name in items:
            raw_comp = str(raw_name).strip()
            sofa_comp = str(sofa_name).strip()
            if not raw_comp or not sofa_comp:
                continue
            if sofa_comp not in self.sofa_competition_set:
                continue
            out[raw_comp] = sofa_comp
        return out

    @staticmethod
    def _build_manual_competition_mapping_lookup(mappings: dict[str, str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for raw_name, sofa_name in mappings.items():
            norm = normalize_competition_key(raw_name)
            if norm:
                out[norm] = sofa_name
        return out

    def _save_manual_team_mappings(self) -> None:
        path = self.manual_mappings_path
        ordered = {key: self.manual_team_mappings[key] for key in sorted(self.manual_team_mappings, key=str.lower)}
        body = json.dumps(ordered, indent=2, ensure_ascii=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(body + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def _save_manual_competition_mappings(self) -> None:
        path = self.manual_competition_mappings_path
        ordered = {
            key: self.manual_competition_mappings[key]
            for key in sorted(self.manual_competition_mappings, key=str.lower)
        }
        body = json.dumps(ordered, indent=2, ensure_ascii=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(body + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def get_manual_mapping_lookup_snapshot(self) -> dict[str, str]:
        with self.lock:
            return dict(self.manual_mapping_lookup)

    def get_manual_competition_mapping_lookup_snapshot(self) -> dict[str, str]:
        with self.lock:
            return dict(self.manual_competition_mapping_lookup)

    def _recompute_cached_model_metrics(self) -> None:
        with self.lock:
            games_df = self.games_df.copy()
            manual_mapping_lookup = dict(self.manual_mapping_lookup)
            manual_competition_mapping_lookup = dict(self.manual_competition_mapping_lookup)
        if games_df.empty:
            return
        try:
            prediction_df = build_predictions(
                betfair_games_df=games_df,
                form_df=self.form_df,
                fixtures_df=self.fixtures_df,
                team_matcher=self.team_matcher,
                calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
                periods=self.periods,
                min_games=self.min_games,
                manual_mapping_lookup=manual_mapping_lookup,
                manual_competition_mapping_lookup=manual_competition_mapping_lookup,
            )
        except Exception:
            prediction_df = pd.DataFrame()

        metrics_df = extract_period_metrics(prediction_df)
        for col in PERIOD_METRIC_COLUMNS:
            if col in games_df.columns:
                games_df = games_df.drop(columns=[col])
        if not metrics_df.empty:
            games_df = games_df.merge(metrics_df, on="market_id", how="left")
        for col in PERIOD_METRIC_COLUMNS:
            if col not in games_df.columns:
                games_df[col] = None

        with self.lock:
            self.games_df = games_df

    def list_manual_team_mappings(self) -> dict[str, Any]:
        self.refresh_games(force=False)
        with self.lock:
            mappings = dict(self.manual_team_mappings)
            games_df = self.games_df.copy()
            manual_mapping_lookup = dict(self.manual_mapping_lookup)
            sofa_teams = list(self.team_matcher.teams)
            competition_mappings = dict(self.manual_competition_mappings)
            competition_mapping_lookup = dict(self.manual_competition_mapping_lookup)
            sofa_competitions = list(self.sofa_competitions)
            sofa_competition_by_norm = dict(self.sofa_competition_by_norm)

        mapped_raw_norms: set[str] = set()
        unmatched_rows: list[dict[str, Any]] = []
        seen_unmatched: set[tuple[str, str, str]] = set()
        auto_mapping_candidates: dict[str, dict[str, Any]] = {}
        manual_raw_norms = {
            normalize_team_name(raw_name)
            for raw_name in mappings.keys()
            if normalize_team_name(raw_name)
        }

        def method_rank(method: str) -> int:
            method_norm = str(method or "").strip().lower()
            if method_norm == "exact":
                return 0
            if method_norm == "fuzzy":
                return 1
            return 2

        if not games_df.empty:
            try:
                mapped_rows, _ = map_betfair_games(
                    betfair_games_df=games_df,
                    fixtures_df=self.fixtures_df,
                    team_matcher=self.team_matcher,
                    manual_mapping_lookup=manual_mapping_lookup,
                )
            except Exception:
                mapped_rows = []

            if mapped_rows:
                prediction_df = pd.DataFrame(mapped_rows)
                cols = [
                    "market_id",
                    "event_name",
                    "competition",
                    "kickoff_raw",
                    "home_raw",
                    "away_raw",
                    "home_sofa",
                    "away_sofa",
                    "home_match_score",
                    "away_match_score",
                    "home_match_method",
                    "away_match_method",
                ]
                cols = [col for col in cols if col in prediction_df.columns]
                row_df = prediction_df[cols].drop_duplicates(subset=["market_id"], keep="first")
                for row in row_df.to_dict(orient="records"):
                    for side in ("home", "away"):
                        raw_name = str(row.get(f"{side}_raw", "")).strip()
                        raw_norm = normalize_team_name(raw_name)
                        sofa_name = str(row.get(f"{side}_sofa", "")).strip()
                        method = str(row.get(f"{side}_match_method", "")).strip().lower()
                        score_raw = row.get(f"{side}_match_score")
                        try:
                            score = float(score_raw)
                        except Exception:
                            score = -1.0
                        if not raw_name:
                            continue
                        if sofa_name and method not in {"missing", "unmatched"}:
                            if raw_norm:
                                mapped_raw_norms.add(raw_norm)
                            if (
                                method != "manual"
                                and raw_norm
                                and raw_norm not in manual_raw_norms
                            ):
                                candidate = {
                                    "raw_name": raw_name,
                                    "sofa_name": sofa_name,
                                    "is_manual": False,
                                    "match_method": method or "auto",
                                    "_rank": (method_rank(method), -score, str(raw_name).lower()),
                                }
                                existing = auto_mapping_candidates.get(raw_norm)
                                if existing is None or candidate["_rank"] < existing["_rank"]:
                                    auto_mapping_candidates[raw_norm] = candidate
                            continue
                        if raw_norm and raw_norm in manual_raw_norms:
                            continue
                        unique_key = (
                            str(row.get("market_id", "")).strip(),
                            side,
                            raw_name,
                        )
                        if unique_key in seen_unmatched:
                            continue
                        seen_unmatched.add(unique_key)
                        unmatched_rows.append(
                            {
                                "raw_name": raw_name,
                                "side": "Home" if side == "home" else "Away",
                                "event_name": str(row.get("event_name", "")).strip(),
                                "competition": str(row.get("competition", "")).strip(),
                                "kickoff_raw": str(row.get("kickoff_raw", "")).strip(),
                            }
                        )

        mapping_rows = [
            {
                "raw_name": raw_name,
                "sofa_name": sofa_name,
                "is_manual": True,
                "match_method": "manual",
            }
            for raw_name, sofa_name in sorted(mappings.items(), key=lambda kv: kv[0].lower())
        ]
        auto_mapping_rows = sorted(
            [
                {
                    "raw_name": row.get("raw_name", ""),
                    "sofa_name": row.get("sofa_name", ""),
                    "is_manual": False,
                    "match_method": row.get("match_method", "auto"),
                }
                for row in auto_mapping_candidates.values()
            ],
            key=lambda row: str(row.get("raw_name", "")).lower(),
        )
        mapping_rows.extend(auto_mapping_rows)
        unmatched_rows = [
            row
            for row in unmatched_rows
            if (
                normalize_team_name(str(row.get("raw_name", "")).strip()) not in mapped_raw_norms
                and normalize_team_name(str(row.get("raw_name", "")).strip()) not in manual_raw_norms
            )
        ]
        unmatched_rows = sorted(
            unmatched_rows,
            key=lambda row: (
                str(row.get("kickoff_raw", "")).strip(),
                str(row.get("event_name", "")).strip().lower(),
                str(row.get("side", "")).strip(),
                str(row.get("raw_name", "")).strip().lower(),
            ),
        )

        competition_manual_norms = {
            normalize_competition_key(raw_name)
            for raw_name in competition_mappings.keys()
            if normalize_competition_key(raw_name)
        }
        mapped_competition_norms: set[str] = set()
        unmatched_competitions: list[dict[str, Any]] = []
        auto_competition_candidates: dict[str, dict[str, Any]] = {}

        def competition_method_rank(method: str) -> int:
            method_norm = str(method or "").strip().lower()
            if method_norm == "exact":
                return 0
            if method_norm == "fuzzy":
                return 1
            return 2

        competition_stats: dict[str, dict[str, Any]] = {}
        for row in games_df.to_dict(orient="records"):
            competition_name = str(row.get("competition", "")).strip()
            if not competition_name:
                continue
            comp_norm = normalize_competition_key(competition_name)
            if not comp_norm:
                continue
            entry = competition_stats.setdefault(
                competition_name,
                {
                    "raw_name": competition_name,
                    "raw_norm": comp_norm,
                    "games_count": 0,
                    "next_kickoff": str(row.get("kickoff_raw", "")).strip(),
                },
            )
            entry["games_count"] = int(entry.get("games_count", 0)) + 1
            kickoff_raw = str(row.get("kickoff_raw", "")).strip()
            if kickoff_raw:
                current_next = str(entry.get("next_kickoff", "")).strip()
                if not current_next or kickoff_raw < current_next:
                    entry["next_kickoff"] = kickoff_raw

        for entry in competition_stats.values():
            raw_name = str(entry.get("raw_name", "")).strip()
            raw_norm = str(entry.get("raw_norm", "")).strip()
            if not raw_name or not raw_norm:
                continue

            manual_target = competition_mapping_lookup.get(raw_norm)
            if manual_target and manual_target in sofa_competitions:
                mapped_competition_norms.add(raw_norm)
                continue

            matched_name, matched_score, match_method = match_competition_name(
                raw_name=raw_name,
                competition_names=sofa_competitions,
                competition_by_norm=sofa_competition_by_norm,
            )
            if matched_name and match_method not in {"missing", "unmatched"}:
                mapped_competition_norms.add(raw_norm)
                if raw_norm not in competition_manual_norms:
                    candidate = {
                        "raw_name": raw_name,
                        "sofa_name": matched_name,
                        "is_manual": False,
                        "match_method": match_method or "auto",
                        "_rank": (
                            competition_method_rank(match_method),
                            -(float(matched_score) if matched_score is not None else 0.0),
                            str(raw_name).lower(),
                        ),
                    }
                    existing = auto_competition_candidates.get(raw_norm)
                    if existing is None or candidate["_rank"] < existing["_rank"]:
                        auto_competition_candidates[raw_norm] = candidate
                continue

            if raw_norm in competition_manual_norms:
                continue
            unmatched_competitions.append(
                {
                    "raw_name": raw_name,
                    "games_count": int(entry.get("games_count", 0)),
                    "next_kickoff": str(entry.get("next_kickoff", "")).strip(),
                }
            )

        competition_mapping_rows = [
            {
                "raw_name": raw_name,
                "sofa_name": sofa_name,
                "is_manual": True,
                "match_method": "manual",
            }
            for raw_name, sofa_name in sorted(competition_mappings.items(), key=lambda kv: kv[0].lower())
        ]
        auto_competition_rows = sorted(
            [
                {
                    "raw_name": row.get("raw_name", ""),
                    "sofa_name": row.get("sofa_name", ""),
                    "is_manual": False,
                    "match_method": row.get("match_method", "auto"),
                }
                for row in auto_competition_candidates.values()
            ],
            key=lambda row: str(row.get("raw_name", "")).lower(),
        )
        competition_mapping_rows.extend(auto_competition_rows)
        unmatched_competitions = [
            row
            for row in unmatched_competitions
            if normalize_competition_key(str(row.get("raw_name", "")).strip()) not in mapped_competition_norms
            and normalize_competition_key(str(row.get("raw_name", "")).strip()) not in competition_manual_norms
        ]
        unmatched_competitions = sorted(
            unmatched_competitions,
            key=lambda row: (
                str(row.get("next_kickoff", "")).strip(),
                str(row.get("raw_name", "")).strip().lower(),
            ),
        )

        return {
            "mappings": mapping_rows,
            "unmatched": unmatched_rows,
            "sofa_teams": sofa_teams,
            "manual_count": len(mappings),
            "auto_count": len(auto_mapping_rows),
            "competition_mappings": competition_mapping_rows,
            "unmatched_competitions": unmatched_competitions,
            "sofa_competitions": sofa_competitions,
            "manual_competition_count": len(competition_mappings),
            "auto_competition_count": len(auto_competition_rows),
        }

    def upsert_manual_team_mapping(self, raw_name: str, sofa_name: str) -> None:
        raw_team = str(raw_name).strip()
        sofa_team = str(sofa_name).strip()
        if not raw_team:
            raise ValueError("raw_name is required")
        if not sofa_team:
            raise ValueError("sofa_name is required")
        if sofa_team not in self.team_matcher.team_set:
            raise ValueError(f"Unknown SofaScore team: {sofa_team}")

        with self.lock:
            self.manual_team_mappings[raw_team] = sofa_team
            self.manual_mapping_lookup = self._build_manual_mapping_lookup(self.manual_team_mappings)
            self._save_manual_team_mappings()

    def delete_manual_team_mapping(self, raw_name: str) -> bool:
        raw_team = str(raw_name).strip()
        if not raw_team:
            raise ValueError("raw_name is required")
        deleted = False
        with self.lock:
            if raw_team in self.manual_team_mappings:
                self.manual_team_mappings.pop(raw_team, None)
                deleted = True
            self.manual_mapping_lookup = self._build_manual_mapping_lookup(self.manual_team_mappings)
            self._save_manual_team_mappings()
        return deleted

    def upsert_manual_competition_mapping(self, raw_name: str, sofa_name: str) -> None:
        raw_comp = str(raw_name).strip()
        sofa_comp = str(sofa_name).strip()
        if not raw_comp:
            raise ValueError("raw_name is required")
        if not sofa_comp:
            raise ValueError("sofa_name is required")
        if sofa_comp not in self.sofa_competition_set:
            raise ValueError(f"Unknown SofaScore competition: {sofa_comp}")

        with self.lock:
            self.manual_competition_mappings[raw_comp] = sofa_comp
            self.manual_competition_mapping_lookup = self._build_manual_competition_mapping_lookup(
                self.manual_competition_mappings
            )
            self._save_manual_competition_mappings()

    def delete_manual_competition_mapping(self, raw_name: str) -> bool:
        raw_comp = str(raw_name).strip()
        if not raw_comp:
            raise ValueError("raw_name is required")
        deleted = False
        with self.lock:
            if raw_comp in self.manual_competition_mappings:
                self.manual_competition_mappings.pop(raw_comp, None)
                deleted = True
            self.manual_competition_mapping_lookup = self._build_manual_competition_mapping_lookup(
                self.manual_competition_mappings
            )
            self._save_manual_competition_mappings()
        return deleted

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
            cached_games_df = self.games_df.copy()

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
        self.betfair_module.write_all_leagues_file(str(DEFAULT_ALL_LEAGUES), competitions)
        ensure_selected_leagues_file(DEFAULT_SELECTED_LEAGUES)
        selected_entries, invalid_lines = load_selected_league_entries(DEFAULT_SELECTED_LEAGUES)
        if invalid_lines:
            print(
                "Warning: invalid lines in selected leagues file "
                "(expected competition_id|competition_name|tier): "
                + ", ".join(invalid_lines),
                file=sys.stderr,
            )
        if not selected_entries:
            raise RuntimeError(
                f"No leagues selected in {DEFAULT_SELECTED_LEAGUES}. "
                "Add competition_id|competition_name|tier lines and refresh."
            )

        competition_names_by_id: dict[str, str] = {}
        for comp in competitions:
            comp_id = str(getattr(comp, "comp_id", "")).strip()
            comp_name = str(getattr(comp, "name", "")).strip()
            if comp_id:
                competition_names_by_id[comp_id] = comp_name

        competition_ids: list[str] = []
        tier_by_competition_id: dict[str, str] = {}
        seen_competition_ids: set[str] = set()
        for selected in selected_entries:
            competition_id = str(selected.get("competition_id", "")).strip()
            if not competition_id:
                continue
            if competition_id not in seen_competition_ids:
                seen_competition_ids.add(competition_id)
                competition_ids.append(competition_id)
            tier_by_competition_id[competition_id] = selected.get("tier") or DEFAULT_LEAGUE_TIER
        if not competition_ids:
            raise RuntimeError(
                f"No valid selected leagues matched Betfair competitions. "
                f"Check {DEFAULT_SELECTED_LEAGUES} and {DEFAULT_ALL_LEAGUES}."
            )

        catalogues = client.list_handicap_markets(competition_ids) or []

        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for cat in catalogues:
            market_id = str(cat.get("marketId", "")).strip()
            if not market_id or market_id in seen:
                continue
            seen.add(market_id)
            deduped.append(cat)

        requested_market_types = {
            str(market_type).strip().upper()
            for market_type in self.market_types
            if str(market_type).strip()
        }
        if "MATCH_ODDS" not in requested_market_types:
            covered_competition_ids = {
                str(cat.get("competition", {}).get("id", "")).strip()
                for cat in deduped
                if str(cat.get("competition", {}).get("id", "")).strip()
            }
            fallback_competition_ids = [
                comp_id
                for comp_id in competition_ids
                if comp_id and comp_id not in covered_competition_ids
            ]
            if fallback_competition_ids:
                fallback_catalogues = client.list_markets(fallback_competition_ids, ["MATCH_ODDS"]) or []
                for cat in fallback_catalogues:
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
            market_type_code = str(cat.get("description", {}).get("marketType", "")).strip().upper()
            is_match_odds_market = market_type_code == "MATCH_ODDS"
            mainline_snapshot = market_mainline_snapshot(cat, book_by_market_id.get(market_id))
            if is_match_odds_market:
                goal_snapshot = {"goal_mainline": "-", "goal_under_price": "-", "goal_over_price": "-"}
            else:
                goal_snapshot = event_goal_mainline_snapshot(
                    goal_catalogues_by_event.get(str(market.get("event_id", "")).strip(), []),
                    goal_books_by_market_id,
                )
            rows.append(
                {
                    "market_id": market_id,
                    "event_name": market["event_name"],
                    "competition": str(cat.get("competition", {}).get("name", "")).strip(),
                    "tier": tier_by_competition_id.get(
                        str(cat.get("competition", {}).get("id", "")).strip(), DEFAULT_LEAGUE_TIER
                    ),
                    "market_name": str(cat.get("marketName", "")).strip(),
                    "kickoff_time": market["kickoff_time"],
                    "kickoff_raw": market["kickoff_raw"],
                    "home_raw": market["home_raw"],
                    "away_raw": market["away_raw"],
                    "mainline": ("-" if is_match_odds_market else mainline_snapshot["mainline"]),
                    "home_price": mainline_snapshot["home_price"],
                    "away_price": mainline_snapshot["away_price"],
                    "goal_mainline": goal_snapshot["goal_mainline"],
                    "goal_under_price": goal_snapshot["goal_under_price"],
                    "goal_over_price": goal_snapshot["goal_over_price"],
                }
            )

        games_df = pd.DataFrame(rows)
        if not games_df.empty:
            cached_metric_cols = [col for col in PERIOD_METRIC_COLUMNS if col in cached_games_df.columns]
            cached_metrics_df = pd.DataFrame()
            if (not cached_games_df.empty) and ("market_id" in cached_games_df.columns) and cached_metric_cols:
                cached_metrics_df = (
                    cached_games_df[["market_id", *cached_metric_cols]]
                    .dropna(subset=["market_id"])
                    .drop_duplicates(subset=["market_id"], keep="first")
                )

            # Recompute only when no cached xGD exists yet (typically first boot).
            if cached_metrics_df.empty:
                manual_mapping_lookup = self.get_manual_mapping_lookup_snapshot()
                manual_competition_mapping_lookup = self.get_manual_competition_mapping_lookup_snapshot()
                try:
                    prediction_df = build_predictions(
                        betfair_games_df=games_df,
                        form_df=self.form_df,
                        fixtures_df=self.fixtures_df,
                        team_matcher=self.team_matcher,
                        calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
                        periods=self.periods,
                        min_games=self.min_games,
                        manual_mapping_lookup=manual_mapping_lookup,
                        manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                    )
                except Exception:
                    prediction_df = pd.DataFrame()

                period_metrics_df = extract_period_metrics(prediction_df)
                if not period_metrics_df.empty:
                    games_df = games_df.merge(period_metrics_df, on="market_id", how="left")
            else:
                games_df = games_df.merge(cached_metrics_df, on="market_id", how="left")

            for col in PERIOD_METRIC_COLUMNS:
                if col not in games_df.columns:
                    games_df[col] = None

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
            return {
                "days": [],
                "tiers": [],
                "total_games": 0,
                "last_refresh_utc": last_refresh.isoformat() if last_refresh else None,
            }

        if "tier" not in games_df.columns:
            games_df["tier"] = DEFAULT_LEAGUE_TIER
        games_df["tier"] = games_df["tier"].fillna(DEFAULT_LEAGUE_TIER).astype(str)
        available_tiers = sorted({tier.strip() for tier in games_df["tier"].tolist() if tier.strip()})

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
                        "tier": str(row.get("tier", DEFAULT_LEAGUE_TIER) or DEFAULT_LEAGUE_TIER),
                        "market_name": str(row.get("market_name", "")),
                        "kickoff_raw": str(row.get("kickoff_raw", "")),
                        "kickoff_utc": kickoff_utc,
                        "mainline": str(row.get("mainline", "-") or "-"),
                        "home_price": str(row.get("home_price", "-") or "-"),
                        "away_price": str(row.get("away_price", "-") or "-"),
                        "goal_mainline": str(row.get("goal_mainline", "-") or "-"),
                        "goal_under_price": str(row.get("goal_under_price", "-") or "-"),
                        "goal_over_price": str(row.get("goal_over_price", "-") or "-"),
                        "season_xgd": format_float_value(row.get("season_xgd"), decimals=2),
                        "last5_xgd": format_float_value(row.get("last5_xgd"), decimals=2),
                        "last3_xgd": format_float_value(row.get("last3_xgd"), decimals=2),
                        "season_strength": format_float_value(row.get("season_strength"), decimals=2),
                        "last5_strength": format_float_value(row.get("last5_strength"), decimals=2),
                        "last3_strength": format_float_value(row.get("last3_strength"), decimals=2),
                        "xgd_competition_mismatch": (
                            bool(row.get("xgd_competition_mismatch"))
                            if not pd.isna(row.get("xgd_competition_mismatch"))
                            else False
                        ),
                        "season_min_xg": format_float_value(row.get("season_min_xg"), decimals=2),
                        "last5_min_xg": format_float_value(row.get("last5_min_xg"), decimals=2),
                        "last3_min_xg": format_float_value(row.get("last3_min_xg"), decimals=2),
                        "season_max_xg": format_float_value(row.get("season_max_xg"), decimals=2),
                        "last5_max_xg": format_float_value(row.get("last5_max_xg"), decimals=2),
                        "last3_max_xg": format_float_value(row.get("last3_max_xg"), decimals=2),
                    }
                )
            days_out.append({"date": day, "date_label": format_day_label(day), "games": games})

        return {
            "days": days_out,
            "tiers": available_tiers,
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
        manual_mapping_lookup = self.get_manual_mapping_lookup_snapshot()
        manual_competition_mapping_lookup = self.get_manual_competition_mapping_lookup_snapshot()
        prediction_df = build_predictions(
            betfair_games_df=game_df,
            form_df=self.form_df,
            fixtures_df=self.fixtures_df,
            team_matcher=self.team_matcher,
            calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
            periods=self.periods,
            min_games=self.min_games,
            manual_mapping_lookup=manual_mapping_lookup,
            manual_competition_mapping_lookup=manual_competition_mapping_lookup,
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
                "alternate_xgd_sections": [],
                "xgd_views": [],
                "warning": "No xGD output produced for this game.",
            }

        season_slice = prediction_df[prediction_df["period"].astype(str).str.lower() == "season"].copy()
        summary_row = season_slice.iloc[0] if not season_slice.empty else prediction_df.iloc[0]
        summary = {
            "home_xg": to_native(summary_row.get("home_xg")),
            "away_xg": to_native(summary_row.get("away_xg")),
            "total_xg": to_native(summary_row.get("total_xg")),
            "xgd": to_native(summary_row.get("xgd")),
            "strength": to_native(summary_row.get("strength")),
        }

        period_cols = [
            "period",
            "home_xg",
            "away_xg",
            "total_xg",
            "xgd",
            "strength",
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
        model_warning_values: list[str] = []
        if "model_warning" in prediction_df.columns:
            warning_series = prediction_df["model_warning"].dropna().astype(str).map(str.strip)
            model_warning_values = [msg for msg in warning_series.tolist() if msg]
        warning_message = simplify_model_warning(model_warning_values)

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
        alternate_xgd_sections: list[dict[str, Any]] = []
        primary_mapping_row: dict[str, Any] | None = None
        primary_home_sofa: str | None = None
        primary_away_sofa: str | None = None
        kickoff_time = game_row.get("kickoff_time")
        if not mapping_df.empty:
            mapping_row = mapping_df.iloc[0].to_dict()
            primary_mapping_row = mapping_row
            home_sofa = mapping_row.get("home_sofa")
            away_sofa = mapping_row.get("away_sofa")
            primary_home_sofa = str(home_sofa) if home_sofa else None
            primary_away_sofa = str(away_sofa) if away_sofa else None
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
                        home_recent_rows = build_recent_form_rows(source.get("home_source_games"), recent_n=None)
                        away_recent_rows = build_recent_form_rows(source.get("away_source_games"), recent_n=None)
                except Exception:
                    home_recent_rows = []
                    away_recent_rows = []

                home_team_venue_rows = build_team_venue_recent_rows(
                    form_df=self.form_df,
                    team_name=home_sofa,
                    kickoff_time=kickoff_time,
                    recent_n=None,
                    season_id=mapping_row.get("fixture_season_id"),
                    competition_name=mapping_row.get("fixture_competition"),
                    area_name=mapping_row.get("fixture_area"),
                )
                away_team_venue_rows = build_team_venue_recent_rows(
                    form_df=self.form_df,
                    team_name=away_sofa,
                    kickoff_time=kickoff_time,
                    recent_n=None,
                    season_id=mapping_row.get("fixture_season_id"),
                    competition_name=mapping_row.get("fixture_competition"),
                    area_name=mapping_row.get("fixture_area"),
                )

                betfair_competition = str(game_row.get("competition", "")).strip()
                fixture_competition = str(mapping_row.get("fixture_competition", "")).strip()
                fixture_area = str(mapping_row.get("fixture_area", "")).strip()
                is_european_fixture = (
                    is_european_competition_name(betfair_competition)
                    or is_european_competition_name(fixture_competition)
                    or fixture_area.casefold() == "europe"
                )
                if is_european_fixture:
                    with self.lock:
                        sofa_competitions = list(self.sofa_competitions)
                        sofa_competition_by_norm = dict(self.sofa_competition_by_norm)
                        sofa_competition_set = set(self.sofa_competition_set)

                    seen_alt_keys: set[tuple[str, str]] = set()
                    european_competition = resolve_sofa_competition_name(
                        raw_competition=betfair_competition,
                        manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                        sofa_competitions=sofa_competitions,
                        sofa_competition_by_norm=sofa_competition_by_norm,
                        sofa_competition_set=sofa_competition_set,
                    )
                    if (not european_competition) and fixture_competition and (
                        is_european_competition_name(fixture_competition) or fixture_area.casefold() == "europe"
                    ):
                        european_competition = fixture_competition
                    european_area = infer_area_for_competition(
                        form_df=self.form_df,
                        competition_name=european_competition,
                        fallback_area=(fixture_area or "Europe"),
                    )
                    if european_competition and (
                        is_european_competition_name(european_competition)
                        or str(european_area or "").strip().casefold() == "europe"
                    ):
                        european_section = build_competition_specific_xgd_section(
                            home_sofa=str(home_sofa),
                            away_sofa=str(away_sofa),
                            kickoff_time=kickoff_time,
                            competition_name=european_competition,
                            area_name=european_area,
                            label=f"{european_competition} games",
                            calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
                            form_df=self.form_df,
                            periods=self.periods,
                            min_games=self.min_games,
                        )
                        if european_section:
                            european_key = (
                                normalize_competition_key(european_section.get("competition_name")),
                                normalize_competition_key(european_section.get("area_name")),
                            )
                            seen_alt_keys.add(european_key)
                            alternate_xgd_sections.append(european_section)

                    domestic_competition, domestic_area = infer_shared_domestic_competition(
                        form_df=self.form_df,
                        home_team=str(home_sofa),
                        away_team=str(away_sofa),
                        kickoff_time=kickoff_time,
                    )
                    if domestic_competition:
                        domestic_key = (
                            normalize_competition_key(domestic_competition),
                            normalize_competition_key(domestic_area),
                        )
                        if domestic_key not in seen_alt_keys:
                            domestic_section = build_competition_specific_xgd_section(
                                home_sofa=str(home_sofa),
                                away_sofa=str(away_sofa),
                                kickoff_time=kickoff_time,
                                competition_name=domestic_competition,
                                area_name=domestic_area,
                                label=f"Shared domestic league games ({domestic_competition})",
                                calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
                                form_df=self.form_df,
                                periods=self.periods,
                                min_games=self.min_games,
                            )
                            if domestic_section:
                                seen_alt_keys.add(domestic_key)
                                alternate_xgd_sections.append(domestic_section)

        base_view = {
            "id": "fixture",
            "label": "Fixture",
            "summary": summary,
            "period_rows": period_rows,
            "warning": warning_message,
            "home_recent_rows": home_recent_rows,
            "away_recent_rows": away_recent_rows,
            "home_team_venue_rows": home_team_venue_rows,
            "away_team_venue_rows": away_team_venue_rows,
        }
        xgd_views: list[dict[str, Any]] = [base_view]

        tier_text = str(game_row.get("tier", "")).strip().casefold()
        is_tier_zero = tier_text.startswith("tier 0") or tier_text == "tier0"
        if (
            is_tier_zero
            and primary_mapping_row
            and primary_home_sofa
            and primary_away_sofa
            and not pd.isna(kickoff_time)
        ):
            with self.lock:
                sofa_competitions = list(self.sofa_competitions)
                sofa_competition_by_norm = dict(self.sofa_competition_by_norm)
                sofa_competition_set = set(self.sofa_competition_set)

            betfair_competition = str(game_row.get("competition", "")).strip()
            fixture_competition = str(primary_mapping_row.get("fixture_competition", "")).strip()
            fixture_area = str(primary_mapping_row.get("fixture_area", "")).strip()

            cup_competition = resolve_sofa_competition_name(
                raw_competition=betfair_competition,
                manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                sofa_competitions=sofa_competitions,
                sofa_competition_by_norm=sofa_competition_by_norm,
                sofa_competition_set=sofa_competition_set,
            )
            if not cup_competition and fixture_competition:
                cup_competition = fixture_competition
            cup_area = infer_area_for_competition(
                form_df=self.form_df,
                competition_name=cup_competition,
                fallback_area=fixture_area,
            )

            cup_view = None
            if cup_competition:
                cup_form_df = self.form_df.copy()
                if "competition_name" in cup_form_df.columns:
                    cup_form_df = cup_form_df[cup_form_df["competition_name"].astype(str) == str(cup_competition)].copy()
                if cup_area and "area_name" in cup_form_df.columns:
                    cup_form_df = cup_form_df[cup_form_df["area_name"].astype(str) == str(cup_area)].copy()
                cup_view = build_xgd_view_from_form_df(
                    view_id="cup",
                    label=f"Cup: {cup_competition}",
                    home_sofa=primary_home_sofa,
                    away_sofa=primary_away_sofa,
                    kickoff_time=kickoff_time,
                    calc_form_df=cup_form_df,
                    full_form_df=self.form_df,
                    calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
                    periods=self.periods,
                    min_games=self.min_games,
                    home_venue_filter={
                        "season_id": None,
                        "competition_name": cup_competition,
                        "area_name": cup_area,
                    },
                    away_venue_filter={
                        "season_id": None,
                        "competition_name": cup_competition,
                        "area_name": cup_area,
                    },
                )

            home_league_comp, home_league_area = infer_team_domestic_competition(
                form_df=self.form_df,
                team_name=primary_home_sofa,
                kickoff_time=kickoff_time,
            )
            away_league_comp, away_league_area = infer_team_domestic_competition(
                form_df=self.form_df,
                team_name=primary_away_sofa,
                kickoff_time=kickoff_time,
            )
            league_view = None
            if home_league_comp and away_league_comp:
                home_league_rows, home_league_season = select_team_competition_rows(
                    form_df=self.form_df,
                    team_name=primary_home_sofa,
                    competition_name=home_league_comp,
                    area_name=home_league_area,
                    kickoff_time=kickoff_time,
                )
                away_league_rows, away_league_season = select_team_competition_rows(
                    form_df=self.form_df,
                    team_name=primary_away_sofa,
                    competition_name=away_league_comp,
                    area_name=away_league_area,
                    kickoff_time=kickoff_time,
                )
                if not home_league_rows.empty and not away_league_rows.empty:
                    league_form_df = pd.concat([home_league_rows, away_league_rows], ignore_index=True)
                    same_domestic_competition = (
                        normalize_competition_key(home_league_comp) == normalize_competition_key(away_league_comp)
                        and normalize_competition_key(home_league_area) == normalize_competition_key(away_league_area)
                    )
                    if not same_domestic_competition:
                        if "season_id" in league_form_df.columns:
                            league_form_df["season_id"] = "__tier0_domestic__"
                        if "season_start_date" in league_form_df.columns:
                            league_form_df["season_start_date"] = pd.Timestamp("1900-01-01", tz="UTC")
                        if "season_end_date" in league_form_df.columns:
                            league_form_df["season_end_date"] = pd.Timestamp("2100-12-31 23:59:59", tz="UTC")

                    if same_domestic_competition:
                        league_label = f"League: {home_league_comp}"
                    else:
                        league_label = f"Leagues: {home_league_comp} / {away_league_comp}"

                    league_view = build_xgd_view_from_form_df(
                        view_id="league",
                        label=league_label,
                        home_sofa=primary_home_sofa,
                        away_sofa=primary_away_sofa,
                        kickoff_time=kickoff_time,
                        calc_form_df=league_form_df,
                        full_form_df=self.form_df,
                        calc_wyscout_form_tables=self.xgd_module.calc_wyscout_form_tables,
                        periods=self.periods,
                        min_games=self.min_games,
                        home_venue_filter={
                            "season_id": home_league_season,
                            "competition_name": home_league_comp,
                            "area_name": home_league_area,
                        },
                        away_venue_filter={
                            "season_id": away_league_season,
                            "competition_name": away_league_comp,
                            "area_name": away_league_area,
                        },
                    )

            tier_zero_views = [view for view in (cup_view, league_view) if view]
            if tier_zero_views:
                xgd_views = tier_zero_views

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
            "alternate_xgd_sections": alternate_xgd_sections,
            "xgd_views": xgd_views,
            "warning": warning_message,
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
        if path == "/api/manual-mappings":
            return self._serve_manual_mappings()

        self._json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/manual-mappings":
            return self._upsert_manual_mapping()
        if path == "/api/manual-mappings/delete":
            return self._delete_manual_mapping()
        if path == "/api/manual-competition-mappings":
            return self._upsert_manual_competition_mapping()
        if path == "/api/manual-competition-mappings/delete":
            return self._delete_manual_competition_mapping()

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

    def _serve_manual_mappings(self) -> None:
        try:
            payload = self.state.list_manual_team_mappings()
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _upsert_manual_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        sofa_name = str(payload.get("sofa_name", "")).strip()
        try:
            self.state.upsert_manual_team_mapping(raw_name=raw_name, sofa_name=sofa_name)
            self._json({"ok": True})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _delete_manual_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        try:
            deleted = self.state.delete_manual_team_mapping(raw_name=raw_name)
            self._json({"ok": True, "deleted": bool(deleted)})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _upsert_manual_competition_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        sofa_name = str(payload.get("sofa_name", "")).strip()
        try:
            self.state.upsert_manual_competition_mapping(raw_name=raw_name, sofa_name=sofa_name)
            self._json({"ok": True})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _delete_manual_competition_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        try:
            deleted = self.state.delete_manual_competition_mapping(raw_name=raw_name)
            self._json({"ok": True, "deleted": bool(deleted)})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _read_json_body(self) -> dict[str, Any]:
        length_raw = self.headers.get("Content-Length", "0")
        try:
            length = max(0, int(length_raw))
        except Exception:
            length = 0
        if length <= 0:
            return {}
        body = self.rfile.read(length)
        if not body:
            return {}
        try:
            parsed = json.loads(body.decode("utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

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
