#!/usr/bin/env python3
from __future__ import annotations

import argparse
import bz2
import datetime as dt
import importlib.util
import json
import re
import sys
import threading
import unicodedata
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any

import pandas as pd

from xgd_app.markets.goals import (
    format_goal_line_value,
    parse_goal_line_from_catalogue,
)
from xgd_app.markets.handicap import (
    format_handicap_value,
    format_price_value,
    parse_handicap_value,
    runner_name_matches,
)
from xgd_app.data.sofascore_loader import (
    gamestate_for_perspective,
    load_sofascore_inputs,
)
from xgd_app.default_paths import get_external_path
from xgd_app.services.games import GamesService
from xgd_app.services.historical import HistoricalService
from xgd_app.services.mappings import MappingService
from xgd_app.web.handler import AppHandler


APP_DIR = Path(__file__).resolve().parents[1]
WEBAPP_DIR = APP_DIR / "webapp"
APP_DATA_DIR = APP_DIR / "app_data"

DEFAULT_SOFASCORE_DB = APP_DIR / "sofascore_local.db"
_configured_sofascore_db = get_external_path("sofascore_db")
if (not DEFAULT_SOFASCORE_DB.exists()) and _configured_sofascore_db is not None:
    DEFAULT_SOFASCORE_DB = _configured_sofascore_db

DEFAULT_SELECTED_LEAGUES = APP_DATA_DIR / "selected_leagues.txt"
DEFAULT_ALL_LEAGUES = APP_DATA_DIR / "all_leagues.txt"
DEFAULT_MANUAL_TEAM_MAPPINGS = APP_DATA_DIR / "manual_team_mappings.json"
DEFAULT_MANUAL_COMPETITION_MAPPINGS = APP_DATA_DIR / "manual_competition_mappings.json"
DEFAULT_SAVED_GAMES = APP_DATA_DIR / "saved_games.json"
DEFAULT_LEAGUE_TIER = "Unassigned"
DEFAULT_BETFAIR_HISTORICAL_DIR = APP_DIR / "data" / "BASIC"
_fallback_historical_dir = APP_DIR / "historical_data" / "BASIC"
if (not DEFAULT_BETFAIR_HISTORICAL_DIR.exists()) and _fallback_historical_dir.exists():
    DEFAULT_BETFAIR_HISTORICAL_DIR = _fallback_historical_dir

MONTH_ABBR_TO_NUM = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

PERIOD_METRIC_COLUMNS = (
    "season_home_xg",
    "last5_home_xg",
    "last3_home_xg",
    "season_away_xg",
    "last5_away_xg",
    "last3_away_xg",
    "season_total_xg",
    "last5_total_xg",
    "last3_total_xg",
    "season_xgd",
    "last5_xgd",
    "last3_xgd",
    "season_xgd_perf",
    "last5_xgd_perf",
    "last3_xgd_perf",
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
    "xgd_domestic_fallback",
)

GAMESTATES = ("drawing", "winning", "losing")
GAMESTATE_EVENT_METRIC_KEYS = tuple(
    f"{metric}_{direction}_{state}"
    for metric in ("corners", "cards")
    for direction in ("for", "against")
    for state in GAMESTATES
)
GAMESTATE_TIME_KEYS = tuple(f"minutes_{state}" for state in GAMESTATES)
GAMESTATE_ALL_KEYS = GAMESTATE_EVENT_METRIC_KEYS + GAMESTATE_TIME_KEYS

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
    path.parent.mkdir(parents=True, exist_ok=True)
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
        fallback_competition = str(row.get("competition", "")).strip() or None
        fallback_area = "Europe" if is_european_competition_name(fallback_competition) else None

        model_games.append(
            {
                "home": home_team,
                "away": away_team,
                "match_date": row["kickoff_time"],
                "season_id": fixture.get("season_id") if fixture else None,
                "competition_name": fixture.get("competition_name") if fixture else fallback_competition,
                "area_name": fixture.get("area_name") if fixture else fallback_area,
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
        out = pd.DataFrame(mapped_games)
        if out.empty:
            return out
        # Keep response shape stable for downstream rendering when no model rows can be produced.
        default_cols: dict[str, Any] = {
            "period": None,
            "home_xg": None,
            "away_xg": None,
            "total_xg": None,
            "xgd": None,
            "xgd_perf": None,
            "strength": None,
            "total_min_xg": None,
            "total_max_xg": None,
            "home_games_used": 0,
            "away_games_used": 0,
            "model_warning": "No model games available for this fixture.",
            "xgd_competition_mismatch": False,
            "xgd_domestic_fallback": False,
        }
        for col, default_value in default_cols.items():
            if col not in out.columns:
                out[col] = default_value
        return out

    sofa_competitions: list[str] = []
    sofa_competition_by_norm: dict[str, str] = {}
    sofa_competition_set: set[str] = set()
    if "competition_name" in form_df.columns:
        sofa_competitions = sorted(
            {
                str(value).strip()
                for value in form_df["competition_name"].dropna().tolist()
                if str(value).strip()
            }
        )
        sofa_competition_set = set(sofa_competitions)
        for competition_name in sofa_competitions:
            key = normalize_competition_key(competition_name)
            if key and key not in sofa_competition_by_norm:
                sofa_competition_by_norm[key] = competition_name

    adjusted_model_games: list[dict[str, Any]] = []
    model_game_idx = 0
    for mapped in mapped_games:
        if not mapped.get("home_sofa") or not mapped.get("away_sofa"):
            continue
        model_row = dict(model_games[model_game_idx])
        model_game_idx += 1

        raw_competition = str(mapped.get("competition", "")).strip()
        tier_text = str(mapped.get("tier", "")).strip().casefold()
        is_tier_zero = tier_text.startswith("tier 0") or tier_text == "tier0"
        fixture_competition = str(model_row.get("competition_name") or "").strip()
        fixture_area = str(model_row.get("area_name") or "").strip()
        fixture_is_european = bool(fixture_competition) and (
            is_european_competition_name(fixture_competition)
            or fixture_area.casefold() == "europe"
        )
        should_prefer_cup_view = bool(raw_competition) and (
            is_tier_zero
            or (
                is_european_competition_name(raw_competition)
                and (not fixture_competition or fixture_is_european)
            )
        )
        if should_prefer_cup_view and sofa_competition_set:
            resolved_competition = resolve_sofa_competition_name(
                raw_competition=raw_competition,
                manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                sofa_competitions=sofa_competitions,
                sofa_competition_by_norm=sofa_competition_by_norm,
                sofa_competition_set=sofa_competition_set,
            )
            if resolved_competition:
                model_row["competition_name"] = resolved_competition
                # Fixture season ids can be domestic-specific; avoid filtering out cup rows.
                model_row["season_id"] = None
                fallback_area = model_row.get("area_name")
                if not str(fallback_area or "").strip() and is_european_competition_name(raw_competition):
                    fallback_area = "Europe"
                resolved_area = infer_area_for_competition(
                    form_df=form_df,
                    competition_name=resolved_competition,
                    fallback_area=fallback_area,
                )
                if resolved_area:
                    model_row["area_name"] = resolved_area

        adjusted_model_games.append(model_row)

    model_games_df = pd.DataFrame(adjusted_model_games if adjusted_model_games else model_games)
    game_tables, source_games = calc_wyscout_form_tables(
        model_games_df,
        form_df,
        periods=periods,
        return_source_games=True,
        min_games=int(min_games),
    )

    pred_rows: list[dict[str, Any]] = []

    def _season_values_equal(left: Any, right: Any) -> bool:
        left_num = pd.to_numeric(pd.Series([left]), errors="coerce").iloc[0]
        right_num = pd.to_numeric(pd.Series([right]), errors="coerce").iloc[0]
        if pd.notna(left_num) and pd.notna(right_num):
            return float(left_num) == float(right_num)
        return str(left).strip() == str(right).strip()

    def _filter_rows_by_season(rows: pd.DataFrame, season_id_value: Any) -> pd.DataFrame:
        if rows.empty or ("season_id" not in rows.columns):
            return rows.copy()
        season_num = pd.to_numeric(pd.Series([season_id_value]), errors="coerce").iloc[0]
        if pd.notna(season_num):
            mask = pd.to_numeric(rows["season_id"], errors="coerce") == season_num
            return rows[mask].copy()
        season_text = str(season_id_value).strip()
        return rows[rows["season_id"].astype(str).str.strip() == season_text].copy()

    def _infer_common_competition_season_id(
        *,
        home_team_name: str,
        away_team_name: str,
        competition_name: str,
        area_name: str,
        cutoff_time: Any,
    ) -> Any:
        if not competition_name:
            return None
        rows = form_df.copy()
        if rows.empty or ("team" not in rows.columns) or ("competition_name" not in rows.columns):
            return None
        rows = rows[
            rows["team"].astype(str).isin({str(home_team_name), str(away_team_name)})
            & (rows["competition_name"].astype(str) == str(competition_name))
        ].copy()
        if area_name and ("area_name" in rows.columns):
            rows = rows[rows["area_name"].astype(str) == str(area_name)].copy()
        if rows.empty or ("season_id" not in rows.columns):
            return None
        if not pd.isna(cutoff_time) and ("date_time" in rows.columns):
            rows["date_time"] = pd.to_datetime(rows["date_time"], errors="coerce", utc=True)
            rows = rows[rows["date_time"] < cutoff_time].copy()
        rows = rows[rows["season_id"].notna()].copy()
        if rows.empty:
            return None

        home_ids = set(
            rows.loc[rows["team"].astype(str) == str(home_team_name), "season_id"]
            .dropna()
            .tolist()
        )
        away_ids = set(
            rows.loc[rows["team"].astype(str) == str(away_team_name), "season_id"]
            .dropna()
            .tolist()
        )
        common_ids = [
            season_id
            for season_id in home_ids
            if any(_season_values_equal(season_id, other) for other in away_ids)
        ]
        if not common_ids:
            return None

        def is_common(season_val: Any) -> bool:
            return any(_season_values_equal(season_val, common_val) for common_val in common_ids)

        common_rows = rows[rows["season_id"].apply(is_common)].copy()
        sort_cols = [col for col in ("season_start_date", "date_time") if col in common_rows.columns]
        for col in ("season_start_date", "date_time"):
            if col in common_rows.columns:
                common_rows[col] = pd.to_datetime(common_rows[col], errors="coerce", utc=True)
        if sort_cols:
            common_rows = common_rows.sort_values(sort_cols, kind="mergesort")
        return common_rows.iloc[-1]["season_id"] if not common_rows.empty else common_ids[0]

    def _append_empty_period_rows_for_market(
        mapped_row: dict[str, Any],
        *,
        warning: str,
        home_games_used: int,
        away_games_used: int,
    ) -> None:
        period_values = list(periods) if periods else ["season", 5, 3]
        for period_value in period_values:
            pred_rows.append(
                {
                    **mapped_row,
                    "period": period_value,
                    "home_xg": None,
                    "away_xg": None,
                    "total_xg": None,
                    "xgd": None,
                    "xgd_perf": None,
                    "strength": None,
                    "total_min_xg": None,
                    "total_max_xg": None,
                    "home_games_used": int(home_games_used),
                    "away_games_used": int(away_games_used),
                    "model_warning": warning,
                    "xgd_competition_mismatch": False,
                    "xgd_domestic_fallback": False,
                }
            )

    model_idx = 0
    for mapped in mapped_games:
        if not mapped.get("home_sofa") or not mapped.get("away_sofa"):
            pred_rows.append({
                **mapped,
                "period": None,
                "xgd_competition_mismatch": False,
                "xgd_domestic_fallback": False,
            })
            continue
        if model_idx >= len(game_tables):
            pred_rows.append({
                **mapped,
                "period": None,
                "xgd_competition_mismatch": False,
                "xgd_domestic_fallback": False,
            })
            continue

        _, _, _, _, reduced = game_tables[model_idx]
        sources = source_games[model_idx] if model_idx < len(source_games) else {}
        model_idx += 1

        home_sofa = str(mapped.get("home_sofa") or "").strip()
        away_sofa = str(mapped.get("away_sofa") or "").strip()
        kickoff_time = mapped.get("kickoff_time")
        tier_text = str(mapped.get("tier", "")).strip().casefold()
        is_tier_zero = tier_text.startswith("tier 0") or tier_text == "tier0"

        if is_tier_zero and home_sofa and away_sofa and (not pd.isna(kickoff_time)):
            fixture_competition_text = str(mapped.get("fixture_competition") or "").strip()
            betfair_competition_text = str(mapped.get("competition", "")).strip()
            if not fixture_competition_text:
                fallback_competition = resolve_sofa_competition_name(
                    raw_competition=betfair_competition_text,
                    manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                    sofa_competitions=sofa_competitions,
                    sofa_competition_by_norm=sofa_competition_by_norm,
                    sofa_competition_set=sofa_competition_set,
                )
                if fallback_competition:
                    fixture_competition_text = str(fallback_competition).strip()

            fixture_competition_in_db = False
            if fixture_competition_text:
                fixture_competition_norm = normalize_competition_key(fixture_competition_text)
                if fixture_competition_text in sofa_competition_set:
                    fixture_competition_in_db = True
                elif fixture_competition_norm and fixture_competition_norm in sofa_competition_by_norm:
                    fixture_competition_text = str(
                        sofa_competition_by_norm.get(fixture_competition_norm) or fixture_competition_text
                    ).strip()
                    fixture_competition_in_db = True

            if not fixture_competition_in_db:
                home_domestic_comp, home_domestic_area = infer_team_domestic_competition(
                    form_df=form_df,
                    team_name=home_sofa,
                    kickoff_time=kickoff_time,
                )
                away_domestic_comp, away_domestic_area = infer_team_domestic_competition(
                    form_df=form_df,
                    team_name=away_sofa,
                    kickoff_time=kickoff_time,
                )
                if home_domestic_comp and away_domestic_comp:
                    home_domestic_rows, home_domestic_season = select_team_competition_rows(
                        form_df=form_df,
                        team_name=home_sofa,
                        competition_name=home_domestic_comp,
                        area_name=home_domestic_area,
                        kickoff_time=kickoff_time,
                    )
                    away_domestic_rows, away_domestic_season = select_team_competition_rows(
                        form_df=form_df,
                        team_name=away_sofa,
                        competition_name=away_domestic_comp,
                        area_name=away_domestic_area,
                        kickoff_time=kickoff_time,
                    )
                    if not home_domestic_rows.empty and not away_domestic_rows.empty:
                        domestic_calc_form_df = pd.concat([home_domestic_rows, away_domestic_rows], ignore_index=True)
                        same_domestic_competition = (
                            normalize_competition_key(home_domestic_comp) == normalize_competition_key(away_domestic_comp)
                            and normalize_competition_key(home_domestic_area) == normalize_competition_key(away_domestic_area)
                        )
                        if not same_domestic_competition:
                            if "season_id" in domestic_calc_form_df.columns:
                                domestic_calc_form_df["season_id"] = "__tier0_domestic__"
                            if "season_start_date" in domestic_calc_form_df.columns:
                                domestic_calc_form_df["season_start_date"] = pd.Timestamp("1900-01-01", tz="UTC")
                            if "season_end_date" in domestic_calc_form_df.columns:
                                domestic_calc_form_df["season_end_date"] = pd.Timestamp("2100-12-31 23:59:59", tz="UTC")

                        domestic_view = build_xgd_view_from_form_df(
                            view_id="domestic",
                            label="Domestic",
                            home_sofa=home_sofa,
                            away_sofa=away_sofa,
                            kickoff_time=kickoff_time,
                            calc_form_df=domestic_calc_form_df,
                            full_form_df=form_df,
                            calc_wyscout_form_tables=calc_wyscout_form_tables,
                            periods=periods,
                            min_games=min_games,
                            home_venue_filter={
                                "season_id": home_domestic_season,
                                "competition_name": home_domestic_comp,
                                "area_name": home_domestic_area,
                            },
                            away_venue_filter={
                                "season_id": away_domestic_season,
                                "competition_name": away_domestic_comp,
                                "area_name": away_domestic_area,
                            },
                        )
                        domestic_period_rows = (
                            domestic_view.get("period_rows")
                            if isinstance(domestic_view, dict)
                            else None
                        )
                        if isinstance(domestic_period_rows, list) and domestic_period_rows:
                            for domestic_period_row in domestic_period_rows:
                                pred_rows.append(
                                    {
                                        **mapped,
                                        "period": domestic_period_row.get("period"),
                                        "home_xg": domestic_period_row.get("home_xg"),
                                        "away_xg": domestic_period_row.get("away_xg"),
                                        "total_xg": domestic_period_row.get("total_xg"),
                                        "xgd": domestic_period_row.get("xgd"),
                                        "xgd_perf": domestic_period_row.get("xgd_perf"),
                                        "strength": domestic_period_row.get("strength"),
                                        "total_min_xg": domestic_period_row.get("total_min_xg"),
                                        "total_max_xg": domestic_period_row.get("total_max_xg"),
                                        "home_games_used": int(domestic_period_row.get("home_games_used", 0) or 0),
                                        "away_games_used": int(domestic_period_row.get("away_games_used", 0) or 0),
                                        "model_warning": (
                                            domestic_period_row.get("model_warning")
                                            or (domestic_view.get("warning") if isinstance(domestic_view, dict) else None)
                                        ),
                                        "xgd_competition_mismatch": False,
                                        "xgd_domestic_fallback": True,
                                    }
                                )
                            continue

        if (not is_tier_zero) and home_sofa and away_sofa and (not pd.isna(kickoff_time)):
            fixture_competition_text = str(mapped.get("fixture_competition") or "").strip()
            fixture_area_text = str(mapped.get("fixture_area") or "").strip()
            fixture_season_id = mapped.get("fixture_season_id")
            try:
                if pd.isna(fixture_season_id):
                    fixture_season_id = None
            except Exception:
                pass
            betfair_competition_text = str(mapped.get("competition", "")).strip()

            if not fixture_competition_text:
                fallback_competition = resolve_sofa_competition_name(
                    raw_competition=betfair_competition_text,
                    manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                    sofa_competitions=sofa_competitions,
                    sofa_competition_by_norm=sofa_competition_by_norm,
                    sofa_competition_set=sofa_competition_set,
                )
                if fallback_competition:
                    fixture_competition_text = str(fallback_competition).strip()

            if fixture_competition_text and not fixture_area_text:
                inferred_area = infer_area_for_competition(
                    form_df=form_df,
                    competition_name=fixture_competition_text,
                    fallback_area=None,
                )
                fixture_area_text = str(inferred_area or "").strip()

            home_comp_rows, home_comp_season = select_team_competition_rows(
                form_df=form_df,
                team_name=home_sofa,
                competition_name=fixture_competition_text,
                area_name=(fixture_area_text or None),
                kickoff_time=kickoff_time,
            )
            away_comp_rows, away_comp_season = select_team_competition_rows(
                form_df=form_df,
                team_name=away_sofa,
                competition_name=fixture_competition_text,
                area_name=(fixture_area_text or None),
                kickoff_time=kickoff_time,
            )

            if fixture_season_id is None:
                if (home_comp_season is not None) and (away_comp_season is not None):
                    if _season_values_equal(home_comp_season, away_comp_season):
                        fixture_season_id = home_comp_season
                    else:
                        fixture_season_id = _infer_common_competition_season_id(
                            home_team_name=home_sofa,
                            away_team_name=away_sofa,
                            competition_name=fixture_competition_text,
                            area_name=fixture_area_text,
                            cutoff_time=kickoff_time,
                        )
                elif home_comp_season is not None:
                    fixture_season_id = home_comp_season
                elif away_comp_season is not None:
                    fixture_season_id = away_comp_season

            strict_home_rows = _filter_rows_by_season(home_comp_rows, fixture_season_id)
            strict_away_rows = _filter_rows_by_season(away_comp_rows, fixture_season_id)

            home_team_venue_rows = build_team_venue_recent_rows(
                form_df=form_df,
                team_name=home_sofa,
                kickoff_time=kickoff_time,
                recent_n=None,
                season_id=fixture_season_id,
                competition_name=(fixture_competition_text or None),
                area_name=(fixture_area_text or None),
            )
            away_team_venue_rows = build_team_venue_recent_rows(
                form_df=form_df,
                team_name=away_sofa,
                kickoff_time=kickoff_time,
                recent_n=None,
                season_id=fixture_season_id,
                competition_name=(fixture_competition_text or None),
                area_name=(fixture_area_text or None),
            )
            home_fixture_rows = list(home_team_venue_rows.get("home") or [])
            away_fixture_rows = list(away_team_venue_rows.get("away") or [])

            if not home_fixture_rows or not away_fixture_rows:
                _append_empty_period_rows_for_market(
                    mapped,
                    warning="Season warning: one team has no matches in this competition season.",
                    home_games_used=len(home_fixture_rows),
                    away_games_used=len(away_fixture_rows),
                )
                continue

            strict_calc_form_df = pd.concat([strict_home_rows, strict_away_rows], ignore_index=True)
            strict_fixture_view = build_xgd_view_from_form_df(
                view_id="fixture",
                label="Fixture",
                home_sofa=home_sofa,
                away_sofa=away_sofa,
                kickoff_time=kickoff_time,
                calc_form_df=strict_calc_form_df,
                full_form_df=form_df,
                calc_wyscout_form_tables=calc_wyscout_form_tables,
                periods=periods,
                min_games=min_games,
                home_venue_filter={
                    "season_id": fixture_season_id,
                    "competition_name": fixture_competition_text,
                    "area_name": fixture_area_text,
                },
                away_venue_filter={
                    "season_id": fixture_season_id,
                    "competition_name": fixture_competition_text,
                    "area_name": fixture_area_text,
                },
            )
            strict_period_rows = (
                strict_fixture_view.get("period_rows")
                if isinstance(strict_fixture_view, dict)
                else None
            )
            if isinstance(strict_period_rows, list) and strict_period_rows:
                for strict_period_row in strict_period_rows:
                    pred_rows.append(
                        {
                            **mapped,
                            "period": strict_period_row.get("period"),
                            "home_xg": strict_period_row.get("home_xg"),
                            "away_xg": strict_period_row.get("away_xg"),
                            "total_xg": strict_period_row.get("total_xg"),
                            "xgd": strict_period_row.get("xgd"),
                            "xgd_perf": strict_period_row.get("xgd_perf"),
                            "strength": strict_period_row.get("strength"),
                            "total_min_xg": strict_period_row.get("total_min_xg"),
                            "total_max_xg": strict_period_row.get("total_max_xg"),
                            "home_games_used": int(strict_period_row.get("home_games_used", 0) or 0),
                            "away_games_used": int(strict_period_row.get("away_games_used", 0) or 0),
                            "model_warning": (
                                strict_period_row.get("model_warning")
                                or (strict_fixture_view.get("warning") if isinstance(strict_fixture_view, dict) else None)
                            ),
                            "xgd_competition_mismatch": False,
                            "xgd_domestic_fallback": False,
                        }
                    )
                continue

            _append_empty_period_rows_for_market(
                mapped,
                warning="No xGD output produced for this competition-season sample.",
                home_games_used=len(home_fixture_rows),
                away_games_used=len(away_fixture_rows),
            )
            continue

        xgd_competition_mismatch = source_competitions_differ_from_betfair_competition(
            betfair_competition=mapped.get("competition"),
            home_source_games=sources.get("home_source_games"),
            away_source_games=sources.get("away_source_games"),
            manual_competition_mapping_lookup=manual_competition_mapping_lookup,
        )
        home_used = len(sources.get("home_source_games", []))
        away_used = len(sources.get("away_source_games", []))
        warning = sources.get("warning")
        xg_missing = sources_indicate_missing_xg_data(
            sources.get("home_source_games"),
            sources.get("away_source_games"),
        )
        xgot_missing = (not xg_missing) and sources_indicate_missing_xgot_data(
            sources.get("home_source_games"),
            sources.get("away_source_games"),
        )
        if xg_missing:
            warning = xg_data_warning_message()
        no_common_season = should_blank_xgd_for_warning(
            warning=warning,
            home_games_used=home_used,
            away_games_used=away_used,
        )

        for _, period_row in reduced.iterrows():
            home_xg = first_numeric_value(period_row, ("Team Home Real xG", "Avg Home Real xG"), default=0.0)
            away_xg = first_numeric_value(period_row, ("Team Away Real xG", "Avg Away Real xG"), default=0.0)
            total_xg = first_numeric_value(period_row, ("Total Team Real xG", "Total Avg Real xG"), default=0.0)
            xgd = first_numeric_value(period_row, ("Avg Real xGD", "Team Real xGD"), default=0.0)
            xgd_perf = first_numeric_value(period_row, ("Team Real xGD", "Avg Real xGD"), default=0.0)
            strength = first_numeric_value(period_row, ("Strength",), default=0.0)
            total_min_xg = first_numeric_value(period_row, ("Total Min Real xG",), default=0.0)
            total_max_xg = first_numeric_value(period_row, ("Total Max Real xG",), default=0.0)
            pred_rows.append(
                {
                    **mapped,
                    "period": period_row.get("Period"),
                    "home_xg": (None if (xg_missing or xgot_missing) else home_xg),
                    "away_xg": (None if (xg_missing or xgot_missing) else away_xg),
                    "total_xg": (None if xg_missing else total_xg),
                    "xgd": (None if (xg_missing or no_common_season) else xgd),
                    "xgd_perf": (None if (xg_missing or xgot_missing or no_common_season) else xgd_perf),
                    "strength": (None if (xg_missing or no_common_season) else strength),
                    "total_min_xg": (None if (xg_missing or xgot_missing or no_common_season) else total_min_xg),
                    "total_max_xg": (None if (xg_missing or xgot_missing or no_common_season) else total_max_xg),
                    "home_games_used": int(home_used),
                    "away_games_used": int(away_used),
                    "model_warning": warning,
                    "xgd_competition_mismatch": bool(xgd_competition_mismatch),
                    "xgd_domestic_fallback": False,
                }
            )

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
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return str(value)
    return str(value)


def should_blank_xgd_for_warning(warning: Any, home_games_used: int = 0, away_games_used: int = 0) -> bool:
    lowered = str(warning or "").lower()
    if "no common active season found" not in lowered:
        return False
    return int(home_games_used) <= 0 or int(away_games_used) <= 0


def _all_rows_zero_for_metric_pair(
    source_df: Any,
    left_col: str,
    right_col: str,
    tolerance: float = 1e-9,
) -> bool:
    if not isinstance(source_df, pd.DataFrame) or source_df.empty:
        return False
    if left_col not in source_df.columns or right_col not in source_df.columns:
        return False
    left_vals = pd.to_numeric(source_df[left_col], errors="coerce")
    right_vals = pd.to_numeric(source_df[right_col], errors="coerce")
    if left_vals.isna().all() and right_vals.isna().all():
        return True
    zero_mask = (
        (left_vals.fillna(0.0).abs() <= float(tolerance))
        & (right_vals.fillna(0.0).abs() <= float(tolerance))
    )
    return bool(zero_mask.all())


def _coerce_source_games_to_df(source_df: Any) -> pd.DataFrame | None:
    if isinstance(source_df, pd.DataFrame):
        return source_df
    elif isinstance(source_df, dict):
        return pd.DataFrame([source_df])
    elif isinstance(source_df, (list, tuple)):
        return pd.DataFrame(source_df)
    return None


def source_team_has_missing_xg_data(source_df: Any) -> bool:
    parsed_df = _coerce_source_games_to_df(source_df)
    if parsed_df is None:
        return False
    return _all_rows_zero_for_metric_pair(parsed_df, "xG", "xGA")


def source_team_has_missing_xgot_data(source_df: Any) -> bool:
    parsed_df = _coerce_source_games_to_df(source_df)
    if parsed_df is None:
        return False
    return _all_rows_zero_for_metric_pair(parsed_df, "xGoT", "xGoTA")


def sources_indicate_missing_xg_data(
    home_source_games: Any,
    away_source_games: Any,
) -> bool:
    return (
        source_team_has_missing_xg_data(home_source_games)
        or source_team_has_missing_xg_data(away_source_games)
    )


def sources_indicate_missing_xgot_data(
    home_source_games: Any,
    away_source_games: Any,
) -> bool:
    return (
        source_team_has_missing_xgot_data(home_source_games)
        or source_team_has_missing_xgot_data(away_source_games)
    )


def xg_data_warning_message() -> str:
    return "Data warning: xG unavailable for this competition. xGD output disabled."


def simplify_model_warning(messages: list[str]) -> str | None:
    cleaned = [str(msg).strip() for msg in messages if str(msg).strip()]
    if not cleaned:
        return None
    lowered = [msg.lower() for msg in cleaned]

    xg_missing_idx = next(
        (i for i, msg in enumerate(lowered) if ("xg/xgot unavailable" in msg) or ("xgd output disabled" in msg)),
        None,
    )
    if xg_missing_idx is not None:
        if "xg unavailable for this competition" in lowered[xg_missing_idx]:
            return "Data warning: xG unavailable for this competition. xGD output disabled."
        return "Data warning: xG/xGoT unavailable for this competition. xGD output disabled."

    no_common_idx = next((i for i, msg in enumerate(lowered) if "no common active season found" in msg), None)
    if no_common_idx is not None:
        return "Season warning: no common active season found. xGD may be less reliable."

    team_specific_idx = next(
        (i for i, msg in enumerate(lowered) if "using team-specific active seasons" in msg),
        None,
    )
    if team_specific_idx is not None:
        return "Season warning: shared season_id unavailable; using team-specific active seasons."

    boundaries_idx = next((i for i, msg in enumerate(lowered) if "season boundaries unavailable" in msg), None)
    if boundaries_idx is not None:
        return "Season warning: season boundaries unavailable. xGD may be less reliable."

    small_sample_idx = next((i for i, msg in enumerate(lowered) if "same-season sample is small" in msg), None)
    if small_sample_idx is not None:
        original = cleaned[small_sample_idx]
        match = re.search(
            r"season_id=([^,)\s]+).*home=(\d+),\s*away=(\d+),\s*min_games=(\d+)",
            original,
            flags=re.IGNORECASE,
        )
        if match:
            season_id = match.group(1)
            home_count = match.group(2)
            away_count = match.group(3)
            minimum = match.group(4)
            return (
                f"Sample warning: same-season sample is small "
                f"(season {season_id}, home {home_count}, away {away_count}, min {minimum}). "
                "xGD may be less reliable."
            )
        return "Sample warning: same-season sample is small. xGD may be less reliable."

    primary = cleaned[0]
    return f"Model warning: {primary}"


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
    blank_all_metrics: bool = False,
    blank_xgot_derived_metrics: bool = False,
) -> list[dict[str, Any]]:
    if not isinstance(reduced_df, pd.DataFrame) or reduced_df.empty:
        return []

    no_common_season = should_blank_xgd_for_warning(
        warning=warning,
        home_games_used=home_games_used,
        away_games_used=away_games_used,
    )
    period_rows: list[dict[str, Any]] = []
    for _, period_row in reduced_df.iterrows():
        home_xg = first_numeric_value(period_row, ("Team Home Real xG", "Avg Home Real xG"), default=0.0)
        away_xg = first_numeric_value(period_row, ("Team Away Real xG", "Avg Away Real xG"), default=0.0)
        total_xg = first_numeric_value(period_row, ("Total Team Real xG", "Total Avg Real xG"), default=0.0)
        xgd = first_numeric_value(period_row, ("Avg Real xGD", "Team Real xGD"), default=0.0)
        xgd_perf = first_numeric_value(period_row, ("Team Real xGD", "Avg Real xGD"), default=0.0)
        strength = first_numeric_value(period_row, ("Strength",), default=0.0)
        total_min_xg = first_numeric_value(period_row, ("Total Min Real xG",), default=0.0)
        total_max_xg = first_numeric_value(period_row, ("Total Max Real xG",), default=0.0)
        period_rows.append(
            {
                "period": period_row.get("Period"),
                "home_xg": (None if (blank_all_metrics or blank_xgot_derived_metrics) else home_xg),
                "away_xg": (None if (blank_all_metrics or blank_xgot_derived_metrics) else away_xg),
                "total_xg": (None if blank_all_metrics else total_xg),
                "xgd": (None if (blank_all_metrics or no_common_season) else xgd),
                "xgd_perf": (
                    None if (blank_all_metrics or blank_xgot_derived_metrics or no_common_season) else xgd_perf
                ),
                "strength": (None if (blank_all_metrics or no_common_season) else strength),
                "total_min_xg": (
                    None
                    if (blank_all_metrics or blank_xgot_derived_metrics or no_common_season)
                    else total_min_xg
                ),
                "total_max_xg": (
                    None
                    if (blank_all_metrics or blank_xgot_derived_metrics or no_common_season)
                    else total_max_xg
                ),
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
        "xgd_perf": to_native(chosen.get("xgd_perf")),
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
    xg_missing = sources_indicate_missing_xg_data(home_source, away_source)
    xgot_missing = (not xg_missing) and sources_indicate_missing_xgot_data(home_source, away_source)
    if xg_missing:
        warning_raw = xg_data_warning_message()

    period_rows = period_rows_from_reduced_table(
        reduced_df=reduced,
        warning=warning_raw,
        home_games_used=home_used,
        away_games_used=away_used,
        blank_all_metrics=xg_missing,
        blank_xgot_derived_metrics=xgot_missing,
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
    xg_missing = sources_indicate_missing_xg_data(home_source, away_source)
    xgot_missing = (not xg_missing) and sources_indicate_missing_xgot_data(home_source, away_source)
    if xg_missing:
        warning = xg_data_warning_message()

    period_rows = period_rows_from_reduced_table(
        reduced_df=reduced,
        warning=warning,
        home_games_used=home_used,
        away_games_used=away_used,
        blank_all_metrics=xg_missing,
        blank_xgot_derived_metrics=xgot_missing,
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
    # Treat as mismatch only when none of the source competitions align with the fixture competition.
    return all(
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
        "season_home_xg",
        "last5_home_xg",
        "last3_home_xg",
        "season_away_xg",
        "last5_away_xg",
        "last3_away_xg",
        "season_total_xg",
        "last5_total_xg",
        "last3_total_xg",
        "season_xgd",
        "last5_xgd",
        "last3_xgd",
        "season_xgd_perf",
        "last5_xgd_perf",
        "last3_xgd_perf",
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
        "xgd_domestic_fallback",
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
    if "xgd_domestic_fallback" in base.columns:
        out["xgd_domestic_fallback"] = base["xgd_domestic_fallback"].map(coerce_bool)
    else:
        out["xgd_domestic_fallback"] = False

    metric_specs = [
        ("home_xg", "home_xg"),
        ("away_xg", "away_xg"),
        ("total_xg", "total_xg"),
        ("xgd", "xgd"),
        ("xgd_perf", "xgd_perf"),
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
        "corners_for",
        "corners_against",
        "cards_for",
        "cards_against",
        "yellow_for",
        "yellow_against",
        "red_for",
        "red_against",
        *GAMESTATE_ALL_KEYS,
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
        season_target_num = pd.to_numeric(pd.Series([season_id]), errors="coerce").iloc[0]
        if pd.notna(season_target_num):
            season_col_num = pd.to_numeric(df["season_id"], errors="coerce")
            df = df[season_col_num == season_target_num]
        else:
            season_target_text = str(season_id).strip()
            df = df[df["season_id"].astype(str).str.strip() == season_target_text]

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


def merge_recent_rows_by_venue(rows_by_venue: dict[str, list[dict[str, Any]]] | None) -> list[dict[str, Any]]:
    if not isinstance(rows_by_venue, dict):
        return []
    merged: list[dict[str, Any]] = []
    for key in ("home", "away"):
        chunk = rows_by_venue.get(key)
        if isinstance(chunk, list):
            merged.extend(chunk)
    merged.sort(key=lambda row: str(row.get("date_time", "")), reverse=True)
    return merged


def format_day_label(day_iso: str) -> str:
    try:
        dt_obj = dt.datetime.strptime(day_iso, "%Y-%m-%d")
        return dt_obj.strftime("%a %Y-%m-%d")
    except Exception:
        return day_iso
