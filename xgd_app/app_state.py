"""Application state implementation."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

import pandas as pd

from xgd_app.core import (
    APP_DIR,
    ASIAN_GOAL_MARKET_TYPES,
    DEFAULT_ALL_LEAGUES,
    DEFAULT_BETFAIR_HISTORICAL_DIR,
    DEFAULT_LEAGUE_TIER,
    DEFAULT_MANUAL_COMPETITION_MAPPINGS,
    DEFAULT_MANUAL_TEAM_MAPPINGS,
    DEFAULT_SAVED_GAMES,
    DEFAULT_SELECTED_LEAGUES,
    PERIOD_METRIC_COLUMNS,
    TeamMatcher,
    build_predictions,
    ensure_selected_leagues_file,
    extract_period_metrics,
    format_day_label,
    format_float_value,
    load_module_from_path,
    load_selected_league_entries,
    map_betfair_games,
    match_competition_name,
    normalize_competition_key,
    normalize_team_name,
    normalize_xg_metric_mode,
    parse_iso_utc,
    parse_list_csv,
    parse_periods,
    period_rows_from_reduced_table,
    source_specific_app_data_path,
    source_db_label,
    source_competitions_differ_from_betfair_competition,
    split_event_teams,
)
from xgd_app.data.sofascore_loader import load_sofascore_inputs, resolve_match_events_db_path
from xgd_app.default_paths import get_external_path
from xgd_app.data.historical_betfair import HistoricalBetfairDataService
from xgd_app.services.games import GamesService
from xgd_app.services.game_xgd import GameXgdService
from xgd_app.services.historical import HistoricalService
from xgd_app.services.mappings import MappingService


class AppState:
    def _set_source_mapping_paths(self, source_db_path: Path) -> None:
        resolved = Path(source_db_path).expanduser().resolve()
        self.manual_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_TEAM_MAPPINGS,
            resolved,
        )
        self.manual_competition_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            resolved,
        )
        self.source_db_label = source_db_label(resolved)

    def __init__(self, args: argparse.Namespace) -> None:
        init_started_at = time.perf_counter()
        self.args = args
        self.lock = threading.Lock()

        betfair_scraper_path = APP_DIR / "xgd_app" / "integrations" / "betfair_scraper.py"
        legacy_betfair_scraper_path = APP_DIR / "betfair_scraper.py"
        if (not betfair_scraper_path.exists()) and legacy_betfair_scraper_path.exists():
            betfair_scraper_path = legacy_betfair_scraper_path
        configured_betfair_scraper_path = get_external_path("betfair_scraper")
        if (not betfair_scraper_path.exists()) and configured_betfair_scraper_path is not None:
            betfair_scraper_path = configured_betfair_scraper_path

        form_model_path = APP_DIR / "xgd_app" / "integrations" / "xgd_form_model.py"
        legacy_form_model_path = APP_DIR / "xgd_form_model.py"
        if (not form_model_path.exists()) and legacy_form_model_path.exists():
            form_model_path = legacy_form_model_path
        configured_form_model_path = get_external_path("xgd_form_model")
        if (not form_model_path.exists()) and configured_form_model_path is not None:
            form_model_path = configured_form_model_path

        if not betfair_scraper_path.exists():
            raise RuntimeError(
                "Missing xgd_app/integrations/betfair_scraper.py. "
                "Set external_paths.betfair_scraper in app_data/default_paths.json if needed."
            )
        if not form_model_path.exists():
            raise RuntimeError(
                "Missing xgd_app/integrations/xgd_form_model.py. "
                "Set external_paths.xgd_form_model in app_data/default_paths.json if needed."
            )

        self.betfair_module = load_module_from_path(betfair_scraper_path, "xgd_betfair_scraper")
        self.form_model_module = load_module_from_path(form_model_path, "xgd_form_model")

        self.market_types = parse_list_csv(args.market_types)
        if not self.market_types:
            raise RuntimeError("At least one market type is required.")

        self.max_markets = max(1, min(int(args.max_markets), 1000))
        self.horizon_days = int(args.horizon_days)
        self.refresh_seconds = max(10, int(args.refresh_seconds))
        self.periods = parse_periods(args.periods)
        self.min_games = int(args.min_games)

        self.games_service = GamesService(
            state=self,
            app_dir=APP_DIR,
            default_selected_leagues=DEFAULT_SELECTED_LEAGUES,
            default_all_leagues=DEFAULT_ALL_LEAGUES,
            default_league_tier=DEFAULT_LEAGUE_TIER,
            asian_goal_market_types=ASIAN_GOAL_MARKET_TYPES,
            period_metric_columns=PERIOD_METRIC_COLUMNS,
            ensure_selected_leagues_file=ensure_selected_leagues_file,
            load_selected_league_entries=load_selected_league_entries,
            parse_iso_utc=parse_iso_utc,
            split_event_teams=split_event_teams,
            build_predictions=build_predictions,
            extract_period_metrics=extract_period_metrics,
            format_float_value=format_float_value,
            format_day_label=format_day_label,
        )
        self.credentials = self.games_service.resolve_credentials(args)

        load_sofa_started_at = time.perf_counter()
        print("[xgd_web_app] Stage 2/3: Loading match inputs...", flush=True)
        requested_source_db_path = str(getattr(args, "db_path", "")).strip()
        requested_match_events_db_path = str(getattr(args, "match_events_db_path", "")).strip()
        source_team_mappings_path = source_specific_app_data_path(DEFAULT_MANUAL_TEAM_MAPPINGS, requested_source_db_path)
        source_comp_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            requested_source_db_path,
        )
        events_mapping_db_reference = requested_match_events_db_path or requested_source_db_path
        events_team_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_TEAM_MAPPINGS,
            events_mapping_db_reference,
        )
        events_comp_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            events_mapping_db_reference,
        )
        self.form_df, self.fixtures_df, teams, resolved_db_path = load_sofascore_inputs(
            requested_source_db_path,
            match_events_db_path=(requested_match_events_db_path or None),
            source_team_mappings_path=source_team_mappings_path,
            source_competition_mappings_path=source_comp_mappings_path,
            match_events_team_mappings_path=events_team_mappings_path,
            match_events_competition_mappings_path=events_comp_mappings_path,
        )
        self._form_df_metric_cache: dict[str, pd.DataFrame] = {}
        self.npxg_available = self._detect_npxg_availability(self.form_df)
        self.source_db_path = Path(resolved_db_path).expanduser().resolve()
        # Backward-compatible alias used across existing modules/UI payloads.
        self.sofascore_db_path = self.source_db_path
        self.match_events_db_path = resolve_match_events_db_path(
            requested_match_events_db_path or None,
            self.source_db_path,
        )
        self.match_events_db_label = source_db_label(self.match_events_db_path)
        self.match_events_team_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_TEAM_MAPPINGS,
            self.match_events_db_path,
        )
        self.match_events_competition_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            self.match_events_db_path,
        )
        events_db_note = (
            f" (events from {self.match_events_db_path})"
            if self.match_events_db_path != self.source_db_path
            else ""
        )
        print(
            "[xgd_web_app] Stage 2/3: Match inputs loaded "
            f"({len(self.form_df)} form rows, {len(self.fixtures_df)} fixtures) "
            f"from {self.source_db_path}{events_db_note} "
            f"in {(time.perf_counter() - load_sofa_started_at):.1f}s",
            flush=True,
        )
        self.team_matcher = TeamMatcher(teams)
        self._set_source_mapping_paths(self.source_db_path)
        self.saved_games_path = DEFAULT_SAVED_GAMES

        sofa_competitions = self._competition_names_for_mapping(
            form_df=self.form_df,
            db_path=self.source_db_path,
        )
        self.sofa_competitions = sofa_competitions
        self.sofa_competition_set = set(self.sofa_competitions)
        self.sofa_competition_by_norm: dict[str, str] = {}
        for competition_name in self.sofa_competitions:
            norm = normalize_competition_key(competition_name)
            if norm and norm not in self.sofa_competition_by_norm:
                self.sofa_competition_by_norm[norm] = competition_name

        self.mapping_service = MappingService(
            state=self,
            normalize_team_name=normalize_team_name,
            normalize_competition_key=normalize_competition_key,
            map_betfair_games=map_betfair_games,
            match_competition_name=match_competition_name,
        )
        self.manual_team_mappings = self.mapping_service.load_manual_team_mappings()
        self.manual_mapping_lookup = self.mapping_service.build_manual_mapping_lookup(self.manual_team_mappings)
        self.manual_competition_mappings = self.mapping_service.load_manual_competition_mappings()
        self.manual_competition_mapping_lookup = self.mapping_service.build_manual_competition_mapping_lookup(
            self.manual_competition_mappings
        )
        sync_mappings_started_at = time.perf_counter()
        print(
            "[xgd_web_app] Stage 2/3: Syncing manual team mappings to DB "
            f"({len(self.manual_team_mappings)} mappings)...",
            flush=True,
        )
        self.mapping_service.sync_all_manual_team_mappings_to_db()
        print(
            "[xgd_web_app] Stage 2/3: Manual mapping sync complete "
            f"in {(time.perf_counter() - sync_mappings_started_at):.1f}s",
            flush=True,
        )

        self.historical_service = HistoricalService(
            state=self,
            period_metric_columns=PERIOD_METRIC_COLUMNS,
            extract_period_metrics=extract_period_metrics,
            period_rows_from_reduced_table=period_rows_from_reduced_table,
            source_competitions_differ_from_betfair_competition=source_competitions_differ_from_betfair_competition,
        )
        self.historical_data_service = HistoricalBetfairDataService(state=self)
        self.game_xgd_service = GameXgdService(state=self)

        self.historical_data_dir = DEFAULT_BETFAIR_HISTORICAL_DIR
        # Lazily populate day -> event-dir mappings on demand. This avoids a full
        # startup scan across all historical archive years.
        self._historical_event_day_index: dict[str, list[Path]] = {}
        self._historical_day_events_cache: dict[str, list[dict[str, Any]]] = {}
        self._historical_event_metadata_cache: dict[str, dict[str, Any] | None] = {}
        self._historical_event_stream_cache: dict[str, dict[str, Any]] = {}
        self._historical_event_price_cache: dict[str, dict[str, str]] = {}
        self._historical_match_price_cache: dict[tuple[str, str, str], dict[str, str]] = {}
        self._historical_day_event_lookup_cache: dict[str, dict[str, dict[tuple[str, str], list[dict[str, Any]]]]] = {}

        self.games_df = pd.DataFrame()
        self.historical_games_df = pd.DataFrame()
        self.upcoming_metrics_cache: dict[str, dict[str, Any]] = {}
        self.upcoming_metrics_cache_by_mode: dict[str, dict[str, dict[str, Any]]] = {
            "xg": self.upcoming_metrics_cache,
            "npxg": {},
        }
        self.saved_market_ids = self._load_saved_market_ids()
        historical_init_started_at = time.perf_counter()
        print("[xgd_web_app] Stage 2/3: Initializing historical games cache...", flush=True)
        self.historical_data_service.initialize_historical_games(initial_days=7)
        print(
            "[xgd_web_app] Stage 2/3: Historical games cache initialized "
            f"({len(self.historical_games_df)} rows) "
            f"in {(time.perf_counter() - historical_init_started_at):.1f}s",
            flush=True,
        )
        self.last_refresh: dt.datetime | None = None
        print(
            "[xgd_web_app] Stage 2/3: App state initialized "
            f"in {(time.perf_counter() - init_started_at):.1f}s",
            flush=True,
        )

    def _load_sofa_competitions_from_db(self, db_path: Path | None = None) -> list[str]:
        target_path = Path(db_path if db_path is not None else self.sofascore_db_path).expanduser().resolve()
        if not target_path.exists():
            return []
        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(target_path))
            rows = conn.execute(
                """
                SELECT id, TRIM(name) AS competition_name, TRIM(COALESCE(country, '')) AS area_name
                FROM competitions
                WHERE name IS NOT NULL
                  AND TRIM(name) <> ''
                """
            ).fetchall()
        except Exception:
            return []
        finally:
            if conn is not None:
                conn.close()

        raw_rows: list[tuple[str, str, str]] = []
        name_to_ids: dict[str, set[str]] = {}
        for row in rows:
            if not row:
                continue
            comp_id = str(row[0]).strip()
            comp_name = str(row[1]).strip() if len(row) > 1 else ""
            area_name = str(row[2]).strip() if len(row) > 2 else ""
            if not comp_id or not comp_name:
                continue
            raw_rows.append((comp_id, comp_name, area_name))
            key = comp_name.casefold()
            ids = name_to_ids.setdefault(key, set())
            ids.add(comp_id)

        out: list[str] = []
        for comp_id, comp_name, area_name in raw_rows:
            name_key = comp_name.casefold()
            is_duplicate_name = len(name_to_ids.get(name_key, set())) > 1
            if not is_duplicate_name:
                out.append(comp_name)
                continue
            if area_name:
                out.append(f"{comp_name} [{area_name} | {comp_id}]")
            else:
                out.append(f"{comp_name} [{comp_id}]")
        unique_sorted = sorted(
            {str(value).strip() for value in out if str(value).strip()},
            key=str.lower,
        )
        return unique_sorted

    @staticmethod
    def _competition_names_from_form_df(form_df: pd.DataFrame) -> list[str]:
        if not isinstance(form_df, pd.DataFrame) or form_df.empty or ("competition_name" not in form_df.columns):
            return []
        names = {
            str(value).strip()
            for value in form_df["competition_name"].dropna().tolist()
            if str(value).strip()
        }
        return sorted(names, key=str.lower)

    def _competition_names_for_mapping(self, form_df: pd.DataFrame, db_path: Path | None = None) -> list[str]:
        form_names = self._competition_names_from_form_df(form_df)
        db_names = self._load_sofa_competitions_from_db(db_path=db_path)
        merged: list[str] = []
        seen_cf: set[str] = set()
        for name in [*form_names, *db_names]:
            text = str(name).strip()
            if not text:
                continue
            key = text.casefold()
            if key in seen_cf:
                continue
            seen_cf.add(key)
            merged.append(text)
        return sorted(merged, key=str.lower)

    @staticmethod
    def _detect_npxg_availability(form_df: pd.DataFrame) -> bool:
        if not isinstance(form_df, pd.DataFrame) or form_df.empty:
            return False
        if "NPxG" not in form_df.columns or "NPxGA" not in form_df.columns:
            return False
        npxg = pd.to_numeric(form_df["NPxG"], errors="coerce")
        npxga = pd.to_numeric(form_df["NPxGA"], errors="coerce")
        return bool(npxg.notna().any() and npxga.notna().any())

    def normalize_xg_metric_mode(self, value: Any) -> str:
        mode = normalize_xg_metric_mode(value)
        if mode == "npxg" and not bool(getattr(self, "npxg_available", False)):
            return "xg"
        return mode

    def get_form_df_for_metric_mode(self, metric_mode: Any) -> pd.DataFrame:
        resolved_mode = self.normalize_xg_metric_mode(metric_mode)
        if resolved_mode == "xg":
            return self.form_df
        cached = self._form_df_metric_cache.get(resolved_mode)
        if isinstance(cached, pd.DataFrame):
            return cached

        out = self.form_df.copy()
        if out.empty:
            self._form_df_metric_cache[resolved_mode] = out
            return out
        if ("NPxG" in out.columns) and ("NPxGA" in out.columns):
            out["xG"] = out["NPxG"].where(out["NPxG"].notna(), out.get("xG"))
            out["xGA"] = out["NPxGA"].where(out["NPxGA"].notna(), out.get("xGA"))
        self._form_df_metric_cache[resolved_mode] = out
        return out

    def hard_refresh_xgd_data(self, db_path: str | None = None) -> dict[str, Any]:
        requested_db_path = str(db_path or "").strip()
        if not requested_db_path:
            requested_db_path = str(getattr(self.args, "db_path", "") or self.source_db_path)
        requested_match_events_db_path = str(getattr(self.args, "match_events_db_path", "") or "").strip()
        source_team_mappings_path = source_specific_app_data_path(DEFAULT_MANUAL_TEAM_MAPPINGS, requested_db_path)
        source_comp_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            requested_db_path,
        )
        events_mapping_db_reference = requested_match_events_db_path or requested_db_path
        events_team_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_TEAM_MAPPINGS,
            events_mapping_db_reference,
        )
        events_comp_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            events_mapping_db_reference,
        )
        form_df, fixtures_df, teams, resolved_db_path = load_sofascore_inputs(
            requested_db_path,
            match_events_db_path=(requested_match_events_db_path or None),
            source_team_mappings_path=source_team_mappings_path,
            source_competition_mappings_path=source_comp_mappings_path,
            match_events_team_mappings_path=events_team_mappings_path,
            match_events_competition_mappings_path=events_comp_mappings_path,
        )
        team_matcher = TeamMatcher(teams)
        source_db_path = Path(resolved_db_path).expanduser().resolve()
        resolved_match_events_db_path = resolve_match_events_db_path(
            requested_match_events_db_path or None,
            source_db_path,
        )
        manual_mappings_path = source_specific_app_data_path(DEFAULT_MANUAL_TEAM_MAPPINGS, source_db_path)
        manual_competition_mappings_path = source_specific_app_data_path(DEFAULT_MANUAL_COMPETITION_MAPPINGS, source_db_path)
        source_label = source_db_label(source_db_path)
        match_events_label = source_db_label(resolved_match_events_db_path)
        match_events_team_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_TEAM_MAPPINGS,
            resolved_match_events_db_path,
        )
        match_events_competition_mappings_path = source_specific_app_data_path(
            DEFAULT_MANUAL_COMPETITION_MAPPINGS,
            resolved_match_events_db_path,
        )
        sofa_competitions = self._competition_names_for_mapping(
            form_df=form_df,
            db_path=source_db_path,
        )
        sofa_competition_set = set(sofa_competitions)
        sofa_competition_by_norm: dict[str, str] = {}
        for competition_name in sofa_competitions:
            norm = normalize_competition_key(competition_name)
            if norm and norm not in sofa_competition_by_norm:
                sofa_competition_by_norm[norm] = competition_name

        with self.lock:
            self.form_df = form_df
            self._form_df_metric_cache = {}
            self.npxg_available = self._detect_npxg_availability(form_df)
            self.fixtures_df = fixtures_df
            self.team_matcher = team_matcher
            self.source_db_path = source_db_path
            self.sofascore_db_path = source_db_path
            if hasattr(self.args, "db_path"):
                self.args.db_path = str(source_db_path)
            self.manual_mappings_path = manual_mappings_path
            self.manual_competition_mappings_path = manual_competition_mappings_path
            self.source_db_label = source_label
            self.match_events_db_path = resolved_match_events_db_path
            self.match_events_db_label = match_events_label
            self.match_events_team_mappings_path = match_events_team_mappings_path
            self.match_events_competition_mappings_path = match_events_competition_mappings_path
            self.sofa_competitions = sofa_competitions
            self.sofa_competition_set = sofa_competition_set
            self.sofa_competition_by_norm = sofa_competition_by_norm
            # Clear cached xGD period metrics so they are rebuilt against fresh DB data.
            self.upcoming_metrics_cache = {}
            self.upcoming_metrics_cache_by_mode = {
                "xg": self.upcoming_metrics_cache,
                "npxg": {},
            }
            self.games_df = pd.DataFrame()
            self.historical_games_df = pd.DataFrame()
            self.last_refresh = None
            # Clear historical lookup/price caches so DB switches don't reuse stale rows.
            self._historical_event_day_index = {}
            self._historical_day_events_cache = {}
            self._historical_event_metadata_cache = {}
            self._historical_event_stream_cache = {}
            self._historical_event_price_cache = {}
            self._historical_match_price_cache = {}
            self._historical_day_event_lookup_cache = {}

        game_xgd_cache_lock = getattr(self.game_xgd_service, "_game_xgd_cache_lock", None)
        if game_xgd_cache_lock is not None:
            with game_xgd_cache_lock:
                self.game_xgd_service._game_xgd_cache = {}
                self.game_xgd_service._game_xgd_cache_stamp = ""

        self.mapping_service.refresh_mapping_paths()
        with self.lock:
            self.manual_team_mappings = self.mapping_service.load_manual_team_mappings()
            self.manual_mapping_lookup = self.mapping_service.build_manual_mapping_lookup(self.manual_team_mappings)
            self.manual_competition_mappings = self.mapping_service.load_manual_competition_mappings()
            self.manual_competition_mapping_lookup = self.mapping_service.build_manual_competition_mapping_lookup(
                self.manual_competition_mappings
            )
        self.mapping_service.sync_all_manual_team_mappings_to_db()

        # Rebuild historical and upcoming game tables using refreshed database inputs.
        self.historical_data_service.initialize_historical_games(initial_days=7)
        self.refresh_games(force=True)

        with self.lock:
            upcoming_games_count = int(len(self.games_df))
            historical_games_count = int(len(self.historical_games_df))

        return {
            "db_path": str(source_db_path),
            "source_db_label": source_label,
            "match_events_db_path": str(resolved_match_events_db_path),
            "match_events_db_label": match_events_label,
            "manual_team_mappings_path": str(manual_mappings_path),
            "manual_competition_mappings_path": str(manual_competition_mappings_path),
            "teams_count": int(len(teams)),
            "form_rows_count": int(len(form_df)),
            "fixtures_count": int(len(fixtures_df)),
            "upcoming_games_count": upcoming_games_count,
            "historical_games_count": historical_games_count,
        }

    def _load_saved_market_ids(self) -> list[str]:
        path = Path(self.saved_games_path)
        if not path.exists():
            return []
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []

        if isinstance(payload, dict):
            raw_ids = payload.get("saved_market_ids", [])
        else:
            raw_ids = payload
        if not isinstance(raw_ids, list):
            return []

        out: list[str] = []
        seen: set[str] = set()
        for raw in raw_ids:
            market_id = str(raw or "").strip()
            if not market_id or market_id in seen:
                continue
            seen.add(market_id)
            out.append(market_id)
        return out

    def _persist_saved_market_ids_locked(self) -> None:
        path = Path(self.saved_games_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"saved_market_ids": list(self.saved_market_ids)}
        tmp_path = path.with_suffix(f"{path.suffix}.tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp_path.replace(path)

    def _prune_played_saved_market_ids_locked(self) -> int:
        if not self.saved_market_ids:
            return 0

        saved_lookup = set(self.saved_market_ids)
        played_market_ids: set[str] = set()
        now_utc = pd.Timestamp(dt.datetime.now(dt.timezone.utc))

        historical_df = self.historical_games_df
        if isinstance(historical_df, pd.DataFrame) and not historical_df.empty and "market_id" in historical_df.columns:
            hist_ids = (
                historical_df["market_id"]
                .dropna()
                .astype(str)
                .str.strip()
            )
            played_market_ids.update({value for value in hist_ids.tolist() if value in saved_lookup})

        upcoming_df = self.games_df
        if (
            isinstance(upcoming_df, pd.DataFrame)
            and not upcoming_df.empty
            and "market_id" in upcoming_df.columns
            and "kickoff_time" in upcoming_df.columns
        ):
            upcoming_subset = upcoming_df.copy()
            upcoming_subset["market_id"] = upcoming_subset["market_id"].astype(str).str.strip()
            upcoming_subset = upcoming_subset[upcoming_subset["market_id"].isin(saved_lookup)].copy()
            if not upcoming_subset.empty:
                kickoff_ts = pd.to_datetime(upcoming_subset["kickoff_time"], errors="coerce", utc=True)
                played_mask = kickoff_ts.notna() & (kickoff_ts <= now_utc)
                if bool(played_mask.any()):
                    played_market_ids.update(upcoming_subset.loc[played_mask, "market_id"].tolist())

        if not played_market_ids:
            return 0

        original_count = len(self.saved_market_ids)
        self.saved_market_ids = [market_id for market_id in self.saved_market_ids if market_id not in played_market_ids]
        removed_count = original_count - len(self.saved_market_ids)
        if removed_count > 0:
            self._persist_saved_market_ids_locked()
        return removed_count

    def _prune_missing_saved_market_ids_locked(self, available_market_ids: set[str]) -> int:
        if not self.saved_market_ids:
            return 0
        if not isinstance(available_market_ids, set):
            available_market_ids = set()
        available_norm = {str(value or "").strip() for value in available_market_ids if str(value or "").strip()}
        original_count = len(self.saved_market_ids)
        self.saved_market_ids = [market_id for market_id in self.saved_market_ids if market_id in available_norm]
        removed_count = original_count - len(self.saved_market_ids)
        if removed_count > 0:
            self._persist_saved_market_ids_locked()
        return removed_count

    def _collect_available_market_ids_locked(self) -> set[str]:
        available_market_ids: set[str] = set()
        for frame in (self.games_df, self.historical_games_df):
            if not isinstance(frame, pd.DataFrame) or frame.empty or "market_id" not in frame.columns:
                continue
            values = frame["market_id"].dropna().astype(str).str.strip().tolist()
            available_market_ids.update({value for value in values if value})
        return available_market_ids

    def list_saved_market_ids(self) -> list[str]:
        with self.lock:
            self._prune_played_saved_market_ids_locked()
            self._prune_missing_saved_market_ids_locked(self._collect_available_market_ids_locked())
            return list(self.saved_market_ids)

    def save_game(self, market_id: str) -> dict[str, Any]:
        market_id_norm = str(market_id or "").strip()
        if not market_id_norm:
            raise ValueError("market_id is required")
        with self.lock:
            self._prune_played_saved_market_ids_locked()
            if market_id_norm not in self.saved_market_ids:
                self.saved_market_ids.append(market_id_norm)
                self._persist_saved_market_ids_locked()
            out_ids = list(self.saved_market_ids)
        return {"saved": True, "saved_market_ids": out_ids, "saved_count": len(out_ids)}

    def unsave_game(self, market_id: str) -> dict[str, Any]:
        market_id_norm = str(market_id or "").strip()
        if not market_id_norm:
            raise ValueError("market_id is required")
        with self.lock:
            existed = market_id_norm in self.saved_market_ids
            if existed:
                self.saved_market_ids = [value for value in self.saved_market_ids if value != market_id_norm]
                self._persist_saved_market_ids_locked()
            out_ids = list(self.saved_market_ids)
        return {
            "saved": False,
            "removed": bool(existed),
            "saved_market_ids": out_ids,
            "saved_count": len(out_ids),
        }

    def list_saved_games_grouped_by_day(self) -> dict[str, Any]:
        with self.lock:
            self._prune_played_saved_market_ids_locked()
            self._prune_missing_saved_market_ids_locked(self._collect_available_market_ids_locked())
            saved_market_ids = list(self.saved_market_ids)
            games_df = self.games_df.copy()
            historical_games_df = self.historical_games_df.copy()
            last_refresh = self.last_refresh

        if not saved_market_ids:
            return self.games_service.group_games_df_by_day(
                games_df=pd.DataFrame(),
                mode_norm="saved",
                fill_missing_days=False,
                has_more_older=False,
                added_games=0,
                last_refresh=last_refresh,
                saved_market_ids=[],
            )

        saved_lookup = set(saved_market_ids)
        frames: list[pd.DataFrame] = []
        for frame in (games_df, historical_games_df):
            if frame.empty or "market_id" not in frame.columns:
                continue
            frame_local = frame.copy()
            frame_local["market_id"] = frame_local["market_id"].astype(str)
            frame_local = frame_local[frame_local["market_id"].isin(saved_lookup)].copy()
            if frame_local.empty:
                continue
            frames.append(frame_local)

        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        if not combined.empty and "market_id" in combined.columns:
            combined["market_id"] = combined["market_id"].astype(str)
            combined = combined.drop_duplicates(subset=["market_id"], keep="first")

        return self.games_service.group_games_df_by_day(
            games_df=combined,
            mode_norm="saved",
            fill_missing_days=False,
            has_more_older=False,
            added_games=0,
            last_refresh=last_refresh,
            saved_market_ids=saved_market_ids,
        )

    def get_manual_mapping_lookup_snapshot(self) -> dict[str, str]:
        return self.mapping_service.get_manual_mapping_lookup_snapshot()

    def get_disabled_auto_team_mapping_norms_snapshot(self) -> set[str]:
        return self.mapping_service.get_disabled_auto_team_mapping_norms_snapshot()

    def get_disabled_auto_competition_mapping_norms_snapshot(self) -> set[str]:
        return self.mapping_service.get_disabled_auto_competition_mapping_norms_snapshot()

    def get_manual_competition_mapping_lookup_snapshot(self) -> dict[str, str]:
        return self.mapping_service.get_manual_competition_mapping_lookup_snapshot()

    def list_manual_team_mappings(self) -> dict[str, Any]:
        return self.mapping_service.list_manual_team_mappings()

    def upsert_manual_team_mapping(self, raw_name: str, sofa_name: str) -> None:
        self.mapping_service.upsert_manual_team_mapping(raw_name=raw_name, sofa_name=sofa_name)

    def upsert_manual_team_mappings_bulk(self, mappings: list[dict[str, Any]]) -> int:
        return self.mapping_service.upsert_manual_team_mappings_bulk(mappings=mappings)

    def delete_manual_team_mapping(self, raw_name: str) -> bool:
        return self.mapping_service.delete_manual_team_mapping(raw_name=raw_name)

    def upsert_manual_competition_mapping(self, raw_name: str, sofa_name: str) -> None:
        self.mapping_service.upsert_manual_competition_mapping(raw_name=raw_name, sofa_name=sofa_name)

    def upsert_manual_competition_mappings_bulk(self, mappings: list[dict[str, Any]]) -> int:
        return self.mapping_service.upsert_manual_competition_mappings_bulk(mappings=mappings)

    def delete_manual_competition_mapping(self, raw_name: str) -> bool:
        return self.mapping_service.delete_manual_competition_mapping(raw_name=raw_name)

    def refresh_games(self, force: bool = False) -> None:
        self.games_service.refresh(force=force)

    def list_games_grouped_by_day(
        self,
        mode: str = "upcoming",
        load_more_historical: bool = False,
        xg_metric_mode: str = "xg",
    ) -> dict[str, Any]:
        return self.games_service.list_grouped_by_day(
            mode=mode,
            load_more_historical=load_more_historical,
            xg_metric_mode=xg_metric_mode,
        )

    def calculate_historical_day_xgd(self, day_iso: str) -> dict[str, Any]:
        return self.historical_service.calculate_day_xgd(day_iso=day_iso)

    def rescan_historical_closing_prices(self) -> dict[str, Any]:
        return self.historical_data_service.rescan_loaded_historical_closing_prices()

    def get_game_xgd(
        self,
        market_id: str,
        recent_n: int = 5,
        venue_recent_n: int = 5,
        xg_metric_mode: str = "xg",
    ) -> dict[str, Any]:
        return self.game_xgd_service.get_game_xgd(
            market_id=market_id,
            recent_n=recent_n,
            venue_recent_n=venue_recent_n,
            xg_metric_mode=xg_metric_mode,
        )

    def get_game_hc_performance(
        self,
        market_id: str,
        verbose: bool = False,
        xg_metric_mode: str = "xg",
    ) -> dict[str, Any]:
        return self.game_xgd_service.get_game_hc_performance(
            market_id=market_id,
            verbose=bool(verbose),
            xg_metric_mode=xg_metric_mode,
        )

    def get_team_hc_rankings(
        self,
        xg_push_threshold: float = 0.1,
        season_id: str | None = None,
        competition_name: str | None = None,
    ) -> dict[str, Any]:
        return self.game_xgd_service.get_team_hc_rankings(
            xg_push_threshold=xg_push_threshold,
            season_id=season_id,
            competition_name=competition_name,
        )

    def get_team_hc_ranking_details(
        self,
        team_name: str,
        competition_name: str | None = None,
        season_id: str | None = None,
    ) -> dict[str, Any]:
        return self.game_xgd_service.get_team_hc_ranking_details(
            team_name=team_name,
            competition_name=competition_name,
            season_id=season_id,
        )

    def get_team_page(
        self,
        team_name: str,
        competition_name: str | None = None,
        season_id: str | None = None,
        xg_metric_mode: str = "xg",
    ) -> dict[str, Any]:
        return self.game_xgd_service.get_team_page(
            team_name=team_name,
            competition_name=competition_name,
            season_id=season_id,
            xg_metric_mode=xg_metric_mode,
        )

    def get_matchup_xgd(
        self,
        home_team: str,
        away_team: str,
        competition_name: str | None = None,
        season_id: str | None = None,
        xg_metric_mode: str = "xg",
    ) -> dict[str, Any]:
        return self.game_xgd_service.get_matchup_xgd(
            home_team=home_team,
            away_team=away_team,
            competition_name=competition_name,
            season_id=season_id,
            xg_metric_mode=xg_metric_mode,
        )

    def list_teams_directory(self) -> dict[str, Any]:
        return self.game_xgd_service.list_teams_directory()

    def _get_historical_game_xgd(self, match_id: int, recent_n: int, venue_recent_n: int) -> dict[str, Any]:
        return self.game_xgd_service._get_historical_game_xgd(match_id=match_id, recent_n=recent_n, venue_recent_n=venue_recent_n)


__all__ = ["AppState"]
