"""Application state implementation."""

from __future__ import annotations

from xgd_app.core import *  # noqa: F401,F403
from xgd_app.data.historical_betfair import HistoricalBetfairDataService
from xgd_app.services.game_xgd import GameXgdService


class AppState:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.lock = threading.Lock()

        betfair_scraper_path = APP_DIR / "betfair_scraper.py"
        if not betfair_scraper_path.exists():
            betfair_scraper_path = WORKSPACE_DIR / "Bot Finder" / "betfair_scraper.py"
        form_model_path = APP_DIR / "xgd_form_model.py"
        if not form_model_path.exists():
            form_model_path = WORKSPACE_DIR / "XGD Model" / "xgd_form_model.py"

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
            workspace_dir=WORKSPACE_DIR,
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

        self.form_df, self.fixtures_df, teams, self.sofascore_db_path = load_sofascore_inputs(args.db_path)
        self.team_matcher = TeamMatcher(teams)

        self.manual_mappings_path = DEFAULT_MANUAL_TEAM_MAPPINGS
        self.manual_competition_mappings_path = DEFAULT_MANUAL_COMPETITION_MAPPINGS
        self.saved_games_path = APP_DIR / "saved_games.json"

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
        self.saved_market_ids = self._load_saved_market_ids()
        self.historical_data_service.initialize_historical_games(initial_days=7)
        self.last_refresh: dt.datetime | None = None

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
        payload = {"saved_market_ids": list(self.saved_market_ids)}
        tmp_path = path.with_suffix(f"{path.suffix}.tmp")
        tmp_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp_path.replace(path)

    def list_saved_market_ids(self) -> list[str]:
        with self.lock:
            return list(self.saved_market_ids)

    def save_game(self, market_id: str) -> dict[str, Any]:
        market_id_norm = str(market_id or "").strip()
        if not market_id_norm:
            raise ValueError("market_id is required")
        with self.lock:
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

    def get_manual_competition_mapping_lookup_snapshot(self) -> dict[str, str]:
        return self.mapping_service.get_manual_competition_mapping_lookup_snapshot()

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
                calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
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

        cache_updates: dict[str, dict[str, Any]] = {}
        if "market_id" in games_df.columns:
            metric_cols = [col for col in PERIOD_METRIC_COLUMNS if col in games_df.columns]
            if metric_cols:
                metric_rows_df = games_df[["market_id", *metric_cols]].dropna(subset=["market_id"])
                for row in metric_rows_df.to_dict(orient="records"):
                    market_id = str(row.get("market_id", "")).strip()
                    if not market_id:
                        continue
                    cache_updates[market_id] = {col: row.get(col) for col in metric_cols}

        with self.lock:
            self.games_df = games_df
            if cache_updates:
                self.upcoming_metrics_cache.update(cache_updates)

    # Compatibility wrappers for migrated historical-data helpers.
    @staticmethod
    def _build_historical_event_day_index(base_dir: Path) -> dict[str, list[Path]]:
        return HistoricalBetfairDataService._build_historical_event_day_index(base_dir)

    @staticmethod
    def _historical_default_price_snapshot() -> dict[str, str]:
        return HistoricalBetfairDataService._historical_default_price_snapshot()

    @staticmethod
    def _stream_ltp_key(selection_id: int, handicap: float | None) -> tuple[int, float | None]:
        return HistoricalBetfairDataService._stream_ltp_key(selection_id, handicap)

    @staticmethod
    def _lookup_stream_ltp(
        ltp_by_key: dict[tuple[int, float | None], float],
        selection_id: int,
        handicap: float | None,
    ) -> float | None:
        return HistoricalBetfairDataService._lookup_stream_ltp(ltp_by_key, selection_id, handicap)

    def _read_historical_event_metadata(self, event_dir: Path) -> dict[str, Any] | None:
        return self.historical_data_service._read_historical_event_metadata(event_dir)

    def _read_historical_event_stream_cache(self, event_dir: Path) -> dict[str, Any]:
        return self.historical_data_service._read_historical_event_stream_cache(event_dir)

    def _load_historical_day_events(self, day_iso: str) -> list[dict[str, Any]]:
        return self.historical_data_service._load_historical_day_events(day_iso)

    def _historical_market_mainline_snapshot_from_stream(
        self,
        event_name: str,
        market_def: dict[str, Any] | None,
        ltp_by_key: dict[tuple[int, float | None], float],
    ) -> dict[str, Any]:
        return self.historical_data_service._historical_market_mainline_snapshot_from_stream(
            event_name,
            market_def,
            ltp_by_key,
        )

    def _historical_goal_mainline_snapshot_from_stream(
        self,
        market_def: dict[str, Any] | None,
        ltp_by_key: dict[tuple[int, float | None], float],
    ) -> dict[str, Any]:
        return self.historical_data_service._historical_goal_mainline_snapshot_from_stream(market_def, ltp_by_key)

    def _historical_event_closing_prices(self, event_meta: dict[str, Any]) -> dict[str, str]:
        return self.historical_data_service._historical_event_closing_prices(event_meta)

    def _lookup_historical_betfair_prices(self, home_team: str, away_team: str, kickoff_ts: Any) -> dict[str, str]:
        return self.historical_data_service._lookup_historical_betfair_prices(home_team, away_team, kickoff_ts)

    @staticmethod
    def _historical_market_id_from_match_id(match_id: Any) -> str:
        return HistoricalBetfairDataService._historical_market_id_from_match_id(match_id)

    @staticmethod
    def _parse_historical_match_id(market_id: Any) -> int | None:
        return HistoricalBetfairDataService._parse_historical_match_id(market_id)

    def _build_historical_games_df(self) -> pd.DataFrame:
        return self.historical_data_service._build_historical_games_df()

    def list_manual_team_mappings(self) -> dict[str, Any]:
        return self.mapping_service.list_manual_team_mappings()

    def upsert_manual_team_mapping(self, raw_name: str, sofa_name: str) -> None:
        self.mapping_service.upsert_manual_team_mapping(raw_name=raw_name, sofa_name=sofa_name)

    def delete_manual_team_mapping(self, raw_name: str) -> bool:
        return self.mapping_service.delete_manual_team_mapping(raw_name=raw_name)

    def upsert_manual_competition_mapping(self, raw_name: str, sofa_name: str) -> None:
        self.mapping_service.upsert_manual_competition_mapping(raw_name=raw_name, sofa_name=sofa_name)

    def delete_manual_competition_mapping(self, raw_name: str) -> bool:
        return self.mapping_service.delete_manual_competition_mapping(raw_name=raw_name)

    def refresh_games(self, force: bool = False) -> None:
        self.games_service.refresh(force=force)

    def list_games_grouped_by_day(self, mode: str = "upcoming", load_more_historical: bool = False) -> dict[str, Any]:
        return self.games_service.list_grouped_by_day(mode=mode, load_more_historical=load_more_historical)

    def calculate_historical_day_xgd(self, day_iso: str) -> dict[str, Any]:
        return self.historical_service.calculate_day_xgd(day_iso=day_iso)

    def rescan_historical_closing_prices(self) -> dict[str, Any]:
        return self.historical_data_service.rescan_loaded_historical_closing_prices()

    def get_game_xgd(self, market_id: str, recent_n: int = 5, venue_recent_n: int = 5) -> dict[str, Any]:
        return self.game_xgd_service.get_game_xgd(market_id=market_id, recent_n=recent_n, venue_recent_n=venue_recent_n)

    def get_game_hc_performance(self, market_id: str, verbose: bool = False) -> dict[str, Any]:
        return self.game_xgd_service.get_game_hc_performance(market_id=market_id, verbose=bool(verbose))

    def get_team_hc_rankings(self, xg_push_threshold: float = 0.1) -> dict[str, Any]:
        return self.game_xgd_service.get_team_hc_rankings(xg_push_threshold=xg_push_threshold)

    def get_team_hc_ranking_details(self, team_name: str, competition_name: str | None = None) -> dict[str, Any]:
        return self.game_xgd_service.get_team_hc_ranking_details(team_name=team_name, competition_name=competition_name)

    def _get_historical_game_xgd(self, match_id: int, recent_n: int, venue_recent_n: int) -> dict[str, Any]:
        return self.game_xgd_service._get_historical_game_xgd(match_id=match_id, recent_n=recent_n, venue_recent_n=venue_recent_n)


__all__ = ["AppState"]
