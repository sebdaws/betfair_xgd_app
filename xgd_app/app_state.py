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
        self._historical_event_day_index = self.historical_data_service._build_historical_event_day_index(
            self.historical_data_dir
        )
        self._historical_day_events_cache: dict[str, list[dict[str, Any]]] = {}
        self._historical_event_metadata_cache: dict[str, dict[str, Any] | None] = {}
        self._historical_event_stream_cache: dict[str, dict[str, Any]] = {}
        self._historical_event_price_cache: dict[str, dict[str, str]] = {}
        self._historical_match_price_cache: dict[tuple[str, str, str], dict[str, str]] = {}

        self.games_df = pd.DataFrame()
        self.historical_games_df = self.historical_data_service._build_historical_games_df()
        self.last_refresh: dt.datetime | None = None

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

        with self.lock:
            self.games_df = games_df

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

    def list_games_grouped_by_day(self, mode: str = "upcoming") -> dict[str, Any]:
        return self.games_service.list_grouped_by_day(mode=mode)

    def calculate_historical_day_xgd(self, day_iso: str) -> dict[str, Any]:
        return self.historical_service.calculate_day_xgd(day_iso=day_iso)

    def get_game_xgd(self, market_id: str, recent_n: int = 5, venue_recent_n: int = 5) -> dict[str, Any]:
        return self.game_xgd_service.get_game_xgd(market_id=market_id, recent_n=recent_n, venue_recent_n=venue_recent_n)

    def _get_historical_game_xgd(self, match_id: int, recent_n: int, venue_recent_n: int) -> dict[str, Any]:
        return self.game_xgd_service._get_historical_game_xgd(match_id=match_id, recent_n=recent_n, venue_recent_n=venue_recent_n)


__all__ = ["AppState"]
