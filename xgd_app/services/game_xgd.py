"""Game-level xGD detail service."""

from __future__ import annotations

import time

from xgd_app.core import *  # noqa: F401,F403

class GameXgdService:
    def __init__(self, state: Any) -> None:
        self.state = state
        self._verbose_timing = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self.state, name)

    def _log_hcperf_timing(self, label: str, step: str, started_at: float, **meta: Any) -> None:
        if not bool(getattr(self, "_verbose_timing", False)):
            return
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        parts = [f"{k}={v}" for k, v in meta.items() if v is not None]
        meta_text = f" | {' '.join(parts)}" if parts else ""
        print(f"[HC PERF TIMING] {label} | {step} | {elapsed_ms:.1f}ms{meta_text}", flush=True)

    def _parse_historical_match_id(self, market_id: Any) -> int | None:
        return self.state.historical_data_service._parse_historical_match_id(market_id)

    @staticmethod
    def _to_float_or_none(value: Any) -> float | None:
        try:
            if value is None or pd.isna(value):
                return None
        except Exception:
            pass
        try:
            out = float(value)
        except Exception:
            return None
        return out if pd.notna(out) else None

    @staticmethod
    def _handicap_verdict(value: float | None) -> str:
        if value is None:
            return "-"
        if value > 1e-9:
            return "Cover"
        if value < -1e-9:
            return "No Cover"
        return "Push"

    def _build_team_season_handicap_rows(
        self,
        team_name: str,
        season_id: Any,
        kickoff_time: Any,
        resolve_from_archive: bool = False,
        timing_label: str = "",
    ) -> list[dict[str, Any]]:
        func_started_at = time.perf_counter()
        label = timing_label or str(team_name or "team")
        team = str(team_name or "").strip()
        kickoff_ts = pd.to_datetime(kickoff_time, errors="coerce", utc=True)
        if not team or pd.isna(kickoff_ts):
            self._log_hcperf_timing(label, "early_exit_invalid_team_or_kickoff", func_started_at)
            return []
        if self.form_df.empty:
            self._log_hcperf_timing(label, "early_exit_empty_form_df", func_started_at)
            return []
        if "team" not in self.form_df.columns or "date_time" not in self.form_df.columns:
            self._log_hcperf_timing(label, "early_exit_missing_form_columns", func_started_at)
            return []

        step_started_at = time.perf_counter()
        with self.lock:
            historical_df = self.historical_games_df.copy()
        self._log_hcperf_timing(label, "copy_historical_df", step_started_at, rows=int(len(historical_df)))
        historical_by_match_id: dict[int, dict[str, str]] = {}
        step_started_at = time.perf_counter()
        if isinstance(historical_df, pd.DataFrame) and (not historical_df.empty):
            for hist_row in historical_df.to_dict(orient="records"):
                match_id_raw = hist_row.get("match_id")
                if match_id_raw is None or pd.isna(match_id_raw):
                    continue
                try:
                    match_id = int(match_id_raw)
                except Exception:
                    continue
                historical_by_match_id[match_id] = {
                    "mainline": str(hist_row.get("mainline", "-") or "-"),
                    "home_price": str(hist_row.get("home_price", "-") or "-"),
                    "away_price": str(hist_row.get("away_price", "-") or "-"),
                }
        self._log_hcperf_timing(label, "build_historical_by_match_id", step_started_at, entries=int(len(historical_by_match_id)))

        step_started_at = time.perf_counter()
        team_df = self.form_df[self.form_df["team"].astype(str) == team].copy()
        if team_df.empty:
            self._log_hcperf_timing(label, "early_exit_team_not_found", step_started_at)
            return []
        team_df["date_time"] = pd.to_datetime(team_df["date_time"], errors="coerce", utc=True)
        team_df = team_df[team_df["date_time"].notna()].copy()
        team_df = team_df[team_df["date_time"] < kickoff_ts].copy()
        if team_df.empty:
            self._log_hcperf_timing(label, "early_exit_no_games_before_kickoff", step_started_at)
            return []
        self._log_hcperf_timing(label, "filter_team_games", step_started_at, rows=int(len(team_df)))

        if season_id is not None and (not pd.isna(season_id)) and "season_id" in team_df.columns:
            step_started_at = time.perf_counter()
            season_filtered = team_df[team_df["season_id"] == season_id].copy()
            if not season_filtered.empty:
                team_df = season_filtered
            self._log_hcperf_timing(label, "season_filter", step_started_at, rows=int(len(team_df)))

        step_started_at = time.perf_counter()
        team_df = team_df.sort_values("date_time", ascending=False).reset_index(drop=True)
        self._log_hcperf_timing(label, "sort_team_games", step_started_at, rows=int(len(team_df)))
        archive_prices_by_fixture: dict[tuple[str, str, str], dict[str, str]] = {}
        if resolve_from_archive:
            step_started_at = time.perf_counter()
            fixture_rows: list[dict[str, Any]] = []
            for src_row in team_df.to_dict(orient="records"):
                venue_text = str(src_row.get("venue", "")).strip().lower()
                opponent_text = str(src_row.get("opponent", "")).strip()
                kickoff_src = src_row.get("date_time")
                if not opponent_text or pd.isna(kickoff_src):
                    continue
                is_home_src = venue_text == "home"
                fixture_rows.append(
                    {
                        "match_id": src_row.get("match_id"),
                        "home_team": team if is_home_src else opponent_text,
                        "away_team": opponent_text if is_home_src else team,
                        "kickoff_time": kickoff_src,
                    }
                )
            self._log_hcperf_timing(label, "prepare_fixture_rows", step_started_at, fixtures=int(len(fixture_rows)))
            step_started_at = time.perf_counter()
            archive_prices_by_fixture = self.state.historical_data_service._bulk_lookup_historical_betfair_prices_for_team_fixtures(
                team_name=team,
                fixtures=fixture_rows,
            )
            self._log_hcperf_timing(
                label,
                "bulk_lookup_prices",
                step_started_at,
                matched=int(len(archive_prices_by_fixture)),
                fixtures=int(len(fixture_rows)),
            )
        rows: list[dict[str, Any]] = []
        loop_started_at = time.perf_counter()
        bulk_hit_count = 0
        historical_hit_count = 0
        fallback_lookup_count = 0
        fallback_lookup_time_ms = 0.0
        for row in team_df.to_dict(orient="records"):
            venue = str(row.get("venue", "")).strip().lower()
            opponent = str(row.get("opponent", "")).strip()
            if not opponent:
                continue
            is_home = venue == "home"
            home_team = team if is_home else opponent
            away_team = opponent if is_home else team

            match_id_raw = row.get("match_id")
            closing = None
            if resolve_from_archive:
                fixture_key = self.state.historical_data_service._historical_price_lookup_key(
                    normalize_team_name(home_team),
                    normalize_team_name(away_team),
                    pd.to_datetime(row.get("date_time"), errors="coerce", utc=True),
                )
                closing = archive_prices_by_fixture.get(fixture_key)
                if isinstance(closing, dict):
                    bulk_hit_count += 1
                if match_id_raw is not None and (not pd.isna(match_id_raw)):
                    try:
                        db_closing = historical_by_match_id.get(int(match_id_raw))
                        current_mainline = str((closing or {}).get("mainline", "")).strip() if isinstance(closing, dict) else ""
                        db_mainline = str((db_closing or {}).get("mainline", "")).strip() if isinstance(db_closing, dict) else ""
                        if isinstance(db_closing, dict) and (not current_mainline or current_mainline == "-") and db_mainline and db_mainline != "-":
                            closing = db_closing
                            historical_hit_count += 1
                    except Exception:
                        pass
                closing_mainline = str((closing or {}).get("mainline", "")).strip() if isinstance(closing, dict) else ""
                if not closing_mainline or closing_mainline == "-":
                    fallback_started_at = time.perf_counter()
                    closing = self.state.historical_data_service._lookup_historical_betfair_prices(
                        home_team=home_team,
                        away_team=away_team,
                        kickoff_ts=row.get("date_time"),
                    )
                    fallback_lookup_count += 1
                    fallback_lookup_time_ms += (time.perf_counter() - fallback_started_at) * 1000.0
            else:
                if match_id_raw is not None and (not pd.isna(match_id_raw)):
                    try:
                        closing = historical_by_match_id.get(int(match_id_raw))
                    except Exception:
                        closing = None
            if not isinstance(closing, dict):
                closing = self.state.historical_data_service._historical_default_price_snapshot()
            home_handicap = parse_handicap_value((closing or {}).get("mainline"))
            team_handicap = None if home_handicap is None else (float(home_handicap) if is_home else -float(home_handicap))

            gf = self._to_float_or_none(row.get("GF"))
            ga = self._to_float_or_none(row.get("GA"))
            xg = self._to_float_or_none(row.get("xG"))
            xga = self._to_float_or_none(row.get("xGA"))

            result_margin = (gf - ga) if (gf is not None and ga is not None) else None
            xg_margin = (xg - xga) if (xg is not None and xga is not None) else None
            result_vs_hc = (
                (result_margin + team_handicap)
                if (result_margin is not None and team_handicap is not None)
                else None
            )
            xg_vs_hc = (
                (xg_margin + team_handicap)
                if (xg_margin is not None and team_handicap is not None)
                else None
            )

            match_ts = pd.to_datetime(row.get("date_time"), errors="coerce", utc=True)
            rows.append(
                {
                    "date_time": match_ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(match_ts) else to_native(row.get("date_time")),
                    "competition_name": str(row.get("competition_name", "")).strip(),
                    "team": team,
                    "opponent": opponent,
                    "venue": "home" if is_home else "away",
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_goals": (gf if is_home else ga),
                    "away_goals": (ga if is_home else gf),
                    "home_xg": (xg if is_home else xga),
                    "away_xg": (xga if is_home else xg),
                    "closing_mainline": str((closing or {}).get("mainline", "-") or "-"),
                    "closing_home_price": str((closing or {}).get("home_price", "-") or "-"),
                    "closing_away_price": str((closing or {}).get("away_price", "-") or "-"),
                    "closing_handicap_team": team_handicap,
                    "result_vs_hc": result_vs_hc,
                    "xg_vs_hc": xg_vs_hc,
                    "result_verdict": self._handicap_verdict(result_vs_hc),
                    "xg_verdict": self._handicap_verdict(xg_vs_hc),
                }
            )
        self._log_hcperf_timing(
            label,
            "build_rows_loop",
            loop_started_at,
            rows=int(len(rows)),
            bulk_hits=int(bulk_hit_count),
            historical_hits=int(historical_hit_count),
            fallback_calls=int(fallback_lookup_count),
            fallback_ms=f"{fallback_lookup_time_ms:.1f}",
        )
        self._log_hcperf_timing(label, "total_team_build", func_started_at, rows=int(len(rows)))
        return rows

    def _resolve_fixture_context_for_market(self, market_id: str) -> dict[str, Any]:
        historical_match_id = self._parse_historical_match_id(market_id)
        if historical_match_id is not None:
            with self.lock:
                historical_df = self.historical_games_df.copy()
            row_df = historical_df[pd.to_numeric(historical_df["match_id"], errors="coerce") == int(historical_match_id)].copy()
            if row_df.empty:
                raise KeyError("Historical match not found")
            row = row_df.iloc[0].to_dict()
            return {
                "market_id": str(row.get("market_id", "")),
                "home_team": str(row.get("home_sofa", "")).strip(),
                "away_team": str(row.get("away_sofa", "")).strip(),
                "kickoff_time": pd.to_datetime(row.get("kickoff_time"), errors="coerce", utc=True),
                "fixture_season_id": row.get("fixture_season_id"),
                "home_label": str(row.get("home_sofa") or row.get("home_raw") or "").strip(),
                "away_label": str(row.get("away_sofa") or row.get("away_raw") or "").strip(),
                "is_historical": True,
            }

        self.refresh_games(force=False)
        with self.lock:
            games_df = self.games_df.copy()

        if games_df.empty:
            raise KeyError("No games available")
        game_df = games_df[games_df["market_id"].astype(str) == str(market_id)].copy()
        if game_df.empty:
            raise KeyError("Market not found")

        manual_mapping_lookup = self.get_manual_mapping_lookup_snapshot()
        mapped_rows, _ = map_betfair_games(
            betfair_games_df=game_df,
            fixtures_df=self.fixtures_df,
            team_matcher=self.team_matcher,
            manual_mapping_lookup=manual_mapping_lookup,
        )
        if not mapped_rows:
            raise KeyError("Market not found")
        row = mapped_rows[0]
        return {
            "market_id": str(row.get("market_id", "")),
            "home_team": str(row.get("home_sofa") or "").strip(),
            "away_team": str(row.get("away_sofa") or "").strip(),
            "kickoff_time": pd.to_datetime(row.get("kickoff_time"), errors="coerce", utc=True),
            "fixture_season_id": row.get("fixture_season_id"),
            "home_label": str(row.get("home_sofa") or row.get("home_raw") or "").strip(),
            "away_label": str(row.get("away_sofa") or row.get("away_raw") or "").strip(),
            "is_historical": False,
        }

    def get_game_hc_performance(self, market_id: str, verbose: bool = False) -> dict[str, Any]:
        previous_verbose = bool(getattr(self, "_verbose_timing", False))
        historical_service = self.state.historical_data_service
        previous_price_verbose = bool(getattr(historical_service, "_verbose_timing", False))
        self._verbose_timing = bool(verbose)
        historical_service._verbose_timing = bool(verbose)
        try:
            req_started_at = time.perf_counter()
            req_label = f"{market_id}"
            self._log_hcperf_timing(req_label, "request_start", req_started_at)
            step_started_at = time.perf_counter()
            context = self._resolve_fixture_context_for_market(market_id=market_id)
            self._log_hcperf_timing(
                req_label,
                "resolve_fixture_context",
                step_started_at,
                home=str(context.get("home_team", "")).strip(),
                away=str(context.get("away_team", "")).strip(),
            )
            home_team = str(context.get("home_team", "")).strip()
            away_team = str(context.get("away_team", "")).strip()
            kickoff_time = context.get("kickoff_time")
            fixture_season_id = context.get("fixture_season_id")
            if not home_team or not away_team or pd.isna(kickoff_time):
                self._log_hcperf_timing(req_label, "early_exit_invalid_context", req_started_at)
                return {
                    "market_id": str(context.get("market_id", market_id)),
                    "home_label": str(context.get("home_label", "Home team")),
                    "away_label": str(context.get("away_label", "Away team")),
                    "season_handicap_rows": {"home": [], "away": []},
                    "is_historical": bool(context.get("is_historical")),
                }
            step_started_at = time.perf_counter()
            home_rows = self._build_team_season_handicap_rows(
                team_name=home_team,
                season_id=fixture_season_id,
                kickoff_time=kickoff_time,
                resolve_from_archive=True,
                timing_label=f"{req_label}|home|{home_team}",
            )
            self._log_hcperf_timing(req_label, "build_home_rows", step_started_at, rows=int(len(home_rows)))
            step_started_at = time.perf_counter()
            away_rows = self._build_team_season_handicap_rows(
                team_name=away_team,
                season_id=fixture_season_id,
                kickoff_time=kickoff_time,
                resolve_from_archive=True,
                timing_label=f"{req_label}|away|{away_team}",
            )
            self._log_hcperf_timing(req_label, "build_away_rows", step_started_at, rows=int(len(away_rows)))
            self._log_hcperf_timing(
                req_label,
                "request_total",
                req_started_at,
                home_rows=int(len(home_rows)),
                away_rows=int(len(away_rows)),
            )
            return {
                "market_id": str(context.get("market_id", market_id)),
                "home_label": str(context.get("home_label", home_team)),
                "away_label": str(context.get("away_label", away_team)),
                "season_handicap_rows": {
                    "home": home_rows,
                    "away": away_rows,
                },
                "is_historical": bool(context.get("is_historical")),
            }
        finally:
            self._verbose_timing = previous_verbose
            historical_service._verbose_timing = previous_price_verbose

    def get_game_xgd(self, market_id: str, recent_n: int = 5, venue_recent_n: int = 5) -> dict[str, Any]:
        recent_n = clamp_int(recent_n, default=5, min_value=1, max_value=20)
        venue_recent_n = clamp_int(venue_recent_n, default=5, min_value=1, max_value=20)

        historical_match_id = self._parse_historical_match_id(market_id)
        if historical_match_id is not None:
            return self._get_historical_game_xgd(
                match_id=historical_match_id,
                recent_n=recent_n,
                venue_recent_n=venue_recent_n,
            )

        self.refresh_games(force=False)
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
            calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
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
                "season_handicap_rows": {"home": [], "away": []},
                "alternate_xgd_sections": [],
                "xgd_views": [],
                "warning": "No xGD output produced for this game.",
                "is_historical": False,
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
        season_handicap_rows: dict[str, list[dict[str, Any]]] = {"home": [], "away": []}
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
                    _, source_games = self.form_model_module.calc_wyscout_form_tables(
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
                # Fallback for game-page tables if direct source extraction fails.
                if not home_recent_rows:
                    home_recent_rows = merge_recent_rows_by_venue(home_team_venue_rows)
                if not away_recent_rows:
                    away_recent_rows = merge_recent_rows_by_venue(away_team_venue_rows)

                fixture_season_id = mapping_row.get("fixture_season_id")
                season_handicap_rows = {
                    "home": self._build_team_season_handicap_rows(
                        team_name=str(home_sofa),
                        season_id=fixture_season_id,
                        kickoff_time=kickoff_time,
                    ),
                    "away": self._build_team_season_handicap_rows(
                        team_name=str(away_sofa),
                        season_id=fixture_season_id,
                        kickoff_time=kickoff_time,
                    ),
                }

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
                            calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
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
                                calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
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
                    calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
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
                        calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
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
            "season_handicap_rows": season_handicap_rows,
            "alternate_xgd_sections": alternate_xgd_sections,
            "xgd_views": xgd_views,
            "warning": warning_message,
            "is_historical": False,
        }

    def _get_historical_game_xgd(self, match_id: int, recent_n: int, venue_recent_n: int) -> dict[str, Any]:
        with self.lock:
            historical_df = self.historical_games_df.copy()

        if historical_df.empty:
            raise KeyError("No historical games available")

        row_df = historical_df[pd.to_numeric(historical_df["match_id"], errors="coerce") == int(match_id)].copy()
        if row_df.empty:
            raise KeyError("Historical match not found")

        game_row = row_df.iloc[0].to_dict()
        home_team = str(game_row.get("home_sofa", "")).strip()
        away_team = str(game_row.get("away_sofa", "")).strip()
        kickoff_time = pd.to_datetime(game_row.get("kickoff_time"), errors="coerce", utc=True)
        fixture_competition = str(game_row.get("fixture_competition", "")).strip()
        fixture_area = str(game_row.get("fixture_area", "")).strip()
        fixture_season_id = game_row.get("fixture_season_id")
        score_text = str(game_row.get("scoreline", "-") or "-")
        closing_mainline = str(game_row.get("mainline", "-") or "-")
        closing_home_price = str(game_row.get("home_price", "-") or "-")
        closing_away_price = str(game_row.get("away_price", "-") or "-")
        closing_goal_mainline = str(game_row.get("goal_mainline", "-") or "-")
        closing_goal_under_price = str(game_row.get("goal_under_price", "-") or "-")
        closing_goal_over_price = str(game_row.get("goal_over_price", "-") or "-")

        if not home_team or not away_team or pd.isna(kickoff_time):
            return {
                "market_id": str(game_row.get("market_id", "")),
                "event_name": str(game_row.get("event_name", "")),
                "competition": str(game_row.get("competition", "")),
                "kickoff_raw": str(game_row.get("kickoff_raw", "")),
                "mainline": closing_mainline,
                "home_price": closing_home_price,
                "away_price": closing_away_price,
                "goal_mainline": closing_goal_mainline,
                "goal_under_price": closing_goal_under_price,
                "goal_over_price": closing_goal_over_price,
                "summary": None,
                "period_rows": [],
                "mapping_rows": [],
                "recent_n": recent_n,
                "home_recent_rows": [],
                "away_recent_rows": [],
                "venue_recent_n": venue_recent_n,
                "home_team_venue_rows": {"home": [], "away": []},
                "away_team_venue_rows": {"home": [], "away": []},
                "season_handicap_rows": {"home": [], "away": []},
                "alternate_xgd_sections": [],
                "xgd_views": [],
                "warning": "Historical game is missing required data for xGD.",
                "is_historical": True,
                "historical_result": {
                    "score": score_text,
                    "home_goals": to_native(game_row.get("home_goals")),
                    "away_goals": to_native(game_row.get("away_goals")),
                    "home_xg": to_native(game_row.get("home_xg_actual")),
                    "away_xg": to_native(game_row.get("away_xg_actual")),
                    "home_corners": to_native(game_row.get("home_corners_actual")),
                    "away_corners": to_native(game_row.get("away_corners_actual")),
                    "home_cards": to_native(game_row.get("home_cards_actual")),
                    "away_cards": to_native(game_row.get("away_cards_actual")),
                },
                "prediction_vs_actual": None,
            }

        fixture_view = build_xgd_view_from_form_df(
            view_id="fixture",
            label="Fixture",
            home_sofa=home_team,
            away_sofa=away_team,
            kickoff_time=kickoff_time,
            calc_form_df=self.form_df,
            full_form_df=self.form_df,
            calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
            periods=self.periods,
            min_games=self.min_games,
            home_venue_filter={
                "season_id": fixture_season_id,
                "competition_name": fixture_competition,
                "area_name": fixture_area,
            },
            away_venue_filter={
                "season_id": fixture_season_id,
                "competition_name": fixture_competition,
                "area_name": fixture_area,
            },
        )

        if not fixture_view:
            return {
                "market_id": str(game_row.get("market_id", "")),
                "event_name": str(game_row.get("event_name", "")),
                "competition": str(game_row.get("competition", "")),
                "kickoff_raw": str(game_row.get("kickoff_raw", "")),
                "mainline": closing_mainline,
                "home_price": closing_home_price,
                "away_price": closing_away_price,
                "goal_mainline": closing_goal_mainline,
                "goal_under_price": closing_goal_under_price,
                "goal_over_price": closing_goal_over_price,
                "summary": None,
                "period_rows": [],
                "mapping_rows": [],
                "recent_n": recent_n,
                "home_recent_rows": [],
                "away_recent_rows": [],
                "venue_recent_n": venue_recent_n,
                "home_team_venue_rows": {"home": [], "away": []},
                "away_team_venue_rows": {"home": [], "away": []},
                "season_handicap_rows": {"home": [], "away": []},
                "alternate_xgd_sections": [],
                "xgd_views": [],
                "warning": "No xGD output produced for this historical game.",
                "is_historical": True,
                "historical_result": {
                    "score": score_text,
                    "home_goals": to_native(game_row.get("home_goals")),
                    "away_goals": to_native(game_row.get("away_goals")),
                    "home_xg": to_native(game_row.get("home_xg_actual")),
                    "away_xg": to_native(game_row.get("away_xg_actual")),
                    "home_corners": to_native(game_row.get("home_corners_actual")),
                    "away_corners": to_native(game_row.get("away_corners_actual")),
                    "home_cards": to_native(game_row.get("home_cards_actual")),
                    "away_cards": to_native(game_row.get("away_cards_actual")),
                },
                "prediction_vs_actual": None,
            }

        period_rows = fixture_view.get("period_rows") or []
        summary = fixture_view.get("summary")
        warning_message = str(fixture_view.get("warning") or "").strip() or None
        home_recent_rows = fixture_view.get("home_recent_rows") or []
        away_recent_rows = fixture_view.get("away_recent_rows") or []
        home_team_venue_rows = fixture_view.get("home_team_venue_rows") or {"home": [], "away": []}
        away_team_venue_rows = fixture_view.get("away_team_venue_rows") or {"home": [], "away": []}
        season_handicap_rows = {
            "home": self._build_team_season_handicap_rows(
                team_name=home_team,
                season_id=fixture_season_id,
                kickoff_time=kickoff_time,
            ),
            "away": self._build_team_season_handicap_rows(
                team_name=away_team,
                season_id=fixture_season_id,
                kickoff_time=kickoff_time,
            ),
        }

        mapping_rows = [
            {
                "home_raw": home_team,
                "away_raw": away_team,
                "home_sofa": home_team,
                "away_sofa": away_team,
                "home_match_method": "exact",
                "away_match_method": "exact",
                "home_match_score": 1.0,
                "away_match_score": 1.0,
                "fixture_found": True,
                "fixture_competition": fixture_competition,
                "fixture_area": fixture_area,
                "fixture_season_id": to_native(fixture_season_id),
            }
        ]

        historical_result = {
            "score": score_text,
            "home_goals": to_native(game_row.get("home_goals")),
            "away_goals": to_native(game_row.get("away_goals")),
            "home_xg": to_native(game_row.get("home_xg_actual")),
            "away_xg": to_native(game_row.get("away_xg_actual")),
            "home_xgot": to_native(game_row.get("home_xgot_actual")),
            "away_xgot": to_native(game_row.get("away_xgot_actual")),
            "home_corners": to_native(game_row.get("home_corners_actual")),
            "away_corners": to_native(game_row.get("away_corners_actual")),
            "home_cards": to_native(game_row.get("home_cards_actual")),
            "away_cards": to_native(game_row.get("away_cards_actual")),
        }

        pred_home_xg = to_native(summary.get("home_xg")) if isinstance(summary, dict) else None
        pred_away_xg = to_native(summary.get("away_xg")) if isinstance(summary, dict) else None
        pred_total_xg = to_native(summary.get("total_xg")) if isinstance(summary, dict) else None
        pred_xgd = to_native(summary.get("xgd")) if isinstance(summary, dict) else None

        actual_home_xg = to_native(game_row.get("home_xg_actual"))
        actual_away_xg = to_native(game_row.get("away_xg_actual"))
        actual_total_xg = (
            (float(actual_home_xg) + float(actual_away_xg))
            if isinstance(actual_home_xg, (int, float)) and isinstance(actual_away_xg, (int, float))
            else None
        )
        actual_xgd = (
            (float(actual_home_xg) - float(actual_away_xg))
            if isinstance(actual_home_xg, (int, float)) and isinstance(actual_away_xg, (int, float))
            else None
        )
        actual_goal_diff = (
            (float(game_row.get("home_goals")) - float(game_row.get("away_goals")))
            if pd.notna(game_row.get("home_goals")) and pd.notna(game_row.get("away_goals"))
            else None
        )

        prediction_vs_actual = {
            "pred_home_xg": pred_home_xg,
            "actual_home_xg": actual_home_xg,
            "delta_home_xg": (
                (float(actual_home_xg) - float(pred_home_xg))
                if isinstance(actual_home_xg, (int, float)) and isinstance(pred_home_xg, (int, float))
                else None
            ),
            "pred_away_xg": pred_away_xg,
            "actual_away_xg": actual_away_xg,
            "delta_away_xg": (
                (float(actual_away_xg) - float(pred_away_xg))
                if isinstance(actual_away_xg, (int, float)) and isinstance(pred_away_xg, (int, float))
                else None
            ),
            "pred_total_xg": pred_total_xg,
            "actual_total_xg": actual_total_xg,
            "delta_total_xg": (
                (float(actual_total_xg) - float(pred_total_xg))
                if isinstance(actual_total_xg, (int, float)) and isinstance(pred_total_xg, (int, float))
                else None
            ),
            "pred_xgd": pred_xgd,
            "actual_xgd": actual_xgd,
            "delta_xgd": (
                (float(actual_xgd) - float(pred_xgd))
                if isinstance(actual_xgd, (int, float)) and isinstance(pred_xgd, (int, float))
                else None
            ),
            "actual_goal_diff": actual_goal_diff,
            "delta_goal_diff_vs_pred_xgd": (
                (float(actual_goal_diff) - float(pred_xgd))
                if isinstance(actual_goal_diff, (int, float)) and isinstance(pred_xgd, (int, float))
                else None
            ),
        }

        return {
            "market_id": str(game_row.get("market_id", "")),
            "event_name": str(game_row.get("event_name", "")),
            "competition": str(game_row.get("competition", "")),
            "kickoff_raw": str(game_row.get("kickoff_raw", "")),
            "mainline": closing_mainline,
            "home_price": closing_home_price,
            "away_price": closing_away_price,
            "goal_mainline": closing_goal_mainline,
            "goal_under_price": closing_goal_under_price,
            "goal_over_price": closing_goal_over_price,
            "summary": summary,
            "period_rows": period_rows,
            "mapping_rows": mapping_rows,
            "recent_n": recent_n,
            "home_recent_rows": home_recent_rows,
            "away_recent_rows": away_recent_rows,
            "venue_recent_n": venue_recent_n,
            "home_team_venue_rows": home_team_venue_rows,
            "away_team_venue_rows": away_team_venue_rows,
            "season_handicap_rows": season_handicap_rows,
            "alternate_xgd_sections": [],
            "xgd_views": [fixture_view],
            "warning": warning_message,
            "is_historical": True,
            "historical_result": historical_result,
            "prediction_vs_actual": prediction_vs_actual,
        }



__all__ = ["GameXgdService"]
