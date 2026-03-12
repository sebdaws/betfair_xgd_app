"""Upcoming games service implementation."""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import os
import sys
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from xgd_app.markets.goals import event_goal_mainline_snapshot, is_goal_line_market
from xgd_app.markets.handicap import market_mainline_snapshot


EnsureSelectedLeaguesFileFn = Callable[[Path], None]
LoadSelectedLeagueEntriesFn = Callable[[Path], tuple[list[dict[str, str]], list[str]]]
ParseIsoUtcFn = Callable[[Any], pd.Timestamp | type(pd.NaT)]
SplitEventTeamsFn = Callable[[str | None], tuple[str | None, str | None]]
BuildPredictionsFn = Callable[..., pd.DataFrame]
ExtractPeriodMetricsFn = Callable[[pd.DataFrame], pd.DataFrame]
FormatFloatValueFn = Callable[[Any, int], str]
FormatDayLabelFn = Callable[[str], str]


class GamesService:
    """Owns Betfair credential resolution and upcoming-game refresh flow."""

    def __init__(
        self,
        state: Any,
        *,
        app_dir: Path,
        workspace_dir: Path,
        default_selected_leagues: Path,
        default_all_leagues: Path,
        default_league_tier: str,
        asian_goal_market_types: list[str],
        period_metric_columns: tuple[str, ...],
        ensure_selected_leagues_file: EnsureSelectedLeaguesFileFn,
        load_selected_league_entries: LoadSelectedLeagueEntriesFn,
        parse_iso_utc: ParseIsoUtcFn,
        split_event_teams: SplitEventTeamsFn,
        build_predictions: BuildPredictionsFn,
        extract_period_metrics: ExtractPeriodMetricsFn,
        format_float_value: FormatFloatValueFn,
        format_day_label: FormatDayLabelFn,
    ) -> None:
        self.state = state
        self.app_dir = app_dir
        self.workspace_dir = workspace_dir
        self.default_selected_leagues = default_selected_leagues
        self.default_all_leagues = default_all_leagues
        self.default_league_tier = default_league_tier
        self.asian_goal_market_types = asian_goal_market_types
        self.period_metric_columns = period_metric_columns
        self.ensure_selected_leagues_file = ensure_selected_leagues_file
        self.load_selected_league_entries = load_selected_league_entries
        self.parse_iso_utc = parse_iso_utc
        self.split_event_teams = split_event_teams
        self.build_predictions = build_predictions
        self.extract_period_metrics = extract_period_metrics
        self.format_float_value = format_float_value
        self.format_day_label = format_day_label

    @staticmethod
    def load_module_settings(module_path: Path) -> dict[str, str | None]:
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

    def resolve_credentials(self, args: argparse.Namespace) -> dict[str, str | None]:
        cfg_filename = f"{args.config_module}.py"
        local_cfg = self.app_dir / cfg_filename
        workspace_cfg = self.workspace_dir / "Bot Finder" / cfg_filename
        if local_cfg.exists():
            module_path = local_cfg
        elif workspace_cfg.exists():
            module_path = workspace_cfg
        else:
            module_path = local_cfg
        settings = self.load_module_settings(module_path)

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
        cfg = self.state.betfair_module.Config(
            username=str(self.state.credentials.get("username") or "") or None,
            password=str(self.state.credentials.get("password") or "") or None,
            app_key=str(self.state.credentials["app_key"]),
            cert_file=str(self.state.credentials.get("cert_file") or "") or None,
            key_file=str(self.state.credentials.get("key_file") or "") or None,
            session_token=(str(self.state.credentials.get("session_token") or "") or None)
            if use_session_token
            else None,
            poll_interval=3.0,
            max_markets=self.state.max_markets,
            pre_match_only=True,
            market_types=self.state.market_types,
            selected_leagues_file=str(self.default_selected_leagues),
            all_leagues_file=str(self.default_all_leagues),
            export_leagues_only=False,
        )
        return self.state.betfair_module.BetfairClient(cfg)

    def _login_client(self):
        has_session = bool(str(self.state.credentials.get("session_token") or "").strip())
        has_cert_bundle = bool(
            str(self.state.credentials.get("username") or "").strip()
            and str(self.state.credentials.get("password") or "").strip()
            and str(self.state.credentials.get("cert_file") or "").strip()
            and str(self.state.credentials.get("key_file") or "").strip()
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

    def refresh(self, force: bool = False) -> None:
        now = dt.datetime.now(dt.timezone.utc)
        with self.state.lock:
            if not force and self.state.last_refresh is not None:
                delta = (now - self.state.last_refresh).total_seconds()
                if delta < self.state.refresh_seconds:
                    return
            cached_games_df = self.state.games_df.copy()

        client = self._login_client()
        try:
            competitions = client.list_competitions()
        except RuntimeError as exc:
            has_cert_bundle = bool(
                str(self.state.credentials.get("username") or "").strip()
                and str(self.state.credentials.get("password") or "").strip()
                and str(self.state.credentials.get("cert_file") or "").strip()
                and str(self.state.credentials.get("key_file") or "").strip()
            )
            if has_cert_bundle and self._is_invalid_session_error(exc):
                client = self._build_client(use_session_token=False)
                client.login()
                competitions = client.list_competitions()
            else:
                raise
        self.state.betfair_module.write_all_leagues_file(str(self.default_all_leagues), competitions)
        self.ensure_selected_leagues_file(self.default_selected_leagues)
        selected_entries, invalid_lines = self.load_selected_league_entries(self.default_selected_leagues)
        if invalid_lines:
            print(
                "Warning: invalid lines in selected leagues file "
                "(expected competition_id|competition_name|tier): "
                + ", ".join(invalid_lines),
                file=sys.stderr,
            )
        if not selected_entries:
            raise RuntimeError(
                f"No leagues selected in {self.default_selected_leagues}. "
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
            tier_by_competition_id[competition_id] = selected.get("tier") or self.default_league_tier
        if not competition_ids:
            raise RuntimeError(
                "No valid selected leagues matched Betfair competitions. "
                f"Check {self.default_selected_leagues} and {self.default_all_leagues}."
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
            for market_type in self.state.market_types
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
        horizon = None if self.state.horizon_days <= 0 else now_ts + pd.Timedelta(days=self.state.horizon_days)

        candidate_markets: list[dict[str, Any]] = []
        for cat in deduped:
            market_id = str(cat.get("marketId", "")).strip()
            kickoff_raw = str(cat.get("marketStartTime", "")).strip()
            kickoff_ts = self.parse_iso_utc(kickoff_raw)
            if not market_id or pd.isna(kickoff_ts):
                continue
            if kickoff_ts <= now_ts:
                continue
            if horizon is not None and kickoff_ts > horizon:
                continue

            event_name = str(cat.get("event", {}).get("name", "")).strip()
            home_raw, away_raw = self.split_event_teams(event_name)
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

        goal_catalogues = client.list_markets(competition_ids, self.asian_goal_market_types)
        candidate_event_ids = {
            str(row.get("event_id", "")).strip()
            for row in candidate_markets
            if str(row.get("event_id", "")).strip()
        }
        candidate_goal_catalogues: list[dict[str, Any]] = []
        for cat in goal_catalogues:
            market_id = str(cat.get("marketId", "")).strip()
            kickoff_raw = str(cat.get("marketStartTime", "")).strip()
            kickoff_ts = self.parse_iso_utc(kickoff_raw)
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
                        str(cat.get("competition", {}).get("id", "")).strip(), self.default_league_tier
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
            cached_metric_cols = [col for col in self.period_metric_columns if col in cached_games_df.columns]
            cached_metrics_df = pd.DataFrame()
            if (not cached_games_df.empty) and ("market_id" in cached_games_df.columns) and cached_metric_cols:
                cached_metrics_df = (
                    cached_games_df[["market_id", *cached_metric_cols]]
                    .dropna(subset=["market_id"])
                    .drop_duplicates(subset=["market_id"], keep="first")
                )

            if cached_metrics_df.empty:
                manual_mapping_lookup = self.state.get_manual_mapping_lookup_snapshot()
                manual_competition_mapping_lookup = self.state.get_manual_competition_mapping_lookup_snapshot()
                try:
                    prediction_df = self.build_predictions(
                        betfair_games_df=games_df,
                        form_df=self.state.form_df,
                        fixtures_df=self.state.fixtures_df,
                        team_matcher=self.state.team_matcher,
                        calc_wyscout_form_tables=self.state.form_model_module.calc_wyscout_form_tables,
                        periods=self.state.periods,
                        min_games=self.state.min_games,
                        manual_mapping_lookup=manual_mapping_lookup,
                        manual_competition_mapping_lookup=manual_competition_mapping_lookup,
                    )
                except Exception:
                    prediction_df = pd.DataFrame()

                period_metrics_df = self.extract_period_metrics(prediction_df)
                if not period_metrics_df.empty:
                    games_df = games_df.merge(period_metrics_df, on="market_id", how="left")
            else:
                games_df = games_df.merge(cached_metrics_df, on="market_id", how="left")

            for col in self.period_metric_columns:
                if col not in games_df.columns:
                    games_df[col] = None

            games_df = games_df.sort_values(["kickoff_time", "competition", "event_name"]).reset_index(drop=True)

        with self.state.lock:
            self.state.games_df = games_df
            self.state.last_refresh = dt.datetime.now(dt.timezone.utc)

    def list_grouped_by_day(self, mode: str = "upcoming") -> dict[str, Any]:
        mode_norm = str(mode or "").strip().lower()
        if mode_norm == "historical":
            with self.state.lock:
                games_df = self.state.historical_games_df.copy()
                last_refresh = self.state.last_refresh
            fill_missing_days = False
        else:
            mode_norm = "upcoming"
            self.refresh(force=False)
            with self.state.lock:
                games_df = self.state.games_df.copy()
                last_refresh = self.state.last_refresh
            fill_missing_days = True

        if games_df.empty:
            return {
                "days": [],
                "tiers": [],
                "total_games": 0,
                "last_refresh_utc": last_refresh.isoformat() if last_refresh else None,
                "mode": mode_norm,
                "fill_missing_days": fill_missing_days,
            }

        if "tier" not in games_df.columns:
            games_df["tier"] = self.default_league_tier
        games_df["tier"] = games_df["tier"].fillna(self.default_league_tier).astype(str)
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
                        "tier": str(row.get("tier", self.default_league_tier) or self.default_league_tier),
                        "market_name": str(row.get("market_name", "")),
                        "kickoff_raw": str(row.get("kickoff_raw", "")),
                        "kickoff_utc": kickoff_utc,
                        "mainline": str(row.get("mainline", "-") or "-"),
                        "home_price": str(row.get("home_price", "-") or "-"),
                        "away_price": str(row.get("away_price", "-") or "-"),
                        "goal_mainline": str(row.get("goal_mainline", "-") or "-"),
                        "goal_under_price": str(row.get("goal_under_price", "-") or "-"),
                        "goal_over_price": str(row.get("goal_over_price", "-") or "-"),
                        "scoreline": str(row.get("scoreline", "")).strip(),
                        "season_xgd": self.format_float_value(row.get("season_xgd"), decimals=2),
                        "last5_xgd": self.format_float_value(row.get("last5_xgd"), decimals=2),
                        "last3_xgd": self.format_float_value(row.get("last3_xgd"), decimals=2),
                        "season_strength": self.format_float_value(row.get("season_strength"), decimals=2),
                        "last5_strength": self.format_float_value(row.get("last5_strength"), decimals=2),
                        "last3_strength": self.format_float_value(row.get("last3_strength"), decimals=2),
                        "xgd_competition_mismatch": (
                            bool(row.get("xgd_competition_mismatch"))
                            if not pd.isna(row.get("xgd_competition_mismatch"))
                            else False
                        ),
                        "season_min_xg": self.format_float_value(row.get("season_min_xg"), decimals=2),
                        "last5_min_xg": self.format_float_value(row.get("last5_min_xg"), decimals=2),
                        "last3_min_xg": self.format_float_value(row.get("last3_min_xg"), decimals=2),
                        "season_max_xg": self.format_float_value(row.get("season_max_xg"), decimals=2),
                        "last5_max_xg": self.format_float_value(row.get("last5_max_xg"), decimals=2),
                        "last3_max_xg": self.format_float_value(row.get("last3_max_xg"), decimals=2),
                        "is_historical": bool(row.get("is_historical")),
                        "home_goals": (
                            int(row.get("home_goals"))
                            if pd.notna(row.get("home_goals"))
                            else None
                        ),
                        "away_goals": (
                            int(row.get("away_goals"))
                            if pd.notna(row.get("away_goals"))
                            else None
                        ),
                        "home_xg_actual": self.format_float_value(row.get("home_xg_actual"), decimals=2),
                        "away_xg_actual": self.format_float_value(row.get("away_xg_actual"), decimals=2),
                        "home_corners_actual": self.format_float_value(row.get("home_corners_actual"), decimals=1),
                        "away_corners_actual": self.format_float_value(row.get("away_corners_actual"), decimals=1),
                        "home_cards_actual": self.format_float_value(row.get("home_cards_actual"), decimals=1),
                        "away_cards_actual": self.format_float_value(row.get("away_cards_actual"), decimals=1),
                    }
                )
            days_out.append({"date": day, "date_label": self.format_day_label(day), "games": games})

        return {
            "days": days_out,
            "tiers": available_tiers,
            "total_games": int(len(games_df)),
            "last_refresh_utc": last_refresh.isoformat() if last_refresh else None,
            "mode": mode_norm,
            "fill_missing_days": fill_missing_days,
        }

    def get_game_xgd(self, market_id: str, recent_n: int = 5, venue_recent_n: int = 5) -> dict[str, Any]:
        return self.state.get_game_xgd(market_id=market_id, recent_n=recent_n, venue_recent_n=venue_recent_n)


__all__ = ["GamesService"]
