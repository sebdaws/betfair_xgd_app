"""Game-level xGD detail service."""

from __future__ import annotations

import math
import time

from xgd_app.core import *  # noqa: F401,F403

class GameXgdService:
    def __init__(self, state: Any) -> None:
        self.state = state
        self._verbose_timing = False
        self._betfair_commission_rate = 0.02

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

    @staticmethod
    def _empty_hc_rank_counts() -> dict[str, int]:
        return {"win": 0, "half_win": 0, "half_loss": 0, "loss": 0, "push": 0, "total": 0}

    @staticmethod
    def _increment_hc_rank_counts(target: dict[str, int], verdict: str | None) -> None:
        if not isinstance(target, dict):
            return
        verdict_text = str(verdict or "").strip().lower()
        if verdict_text not in {"win", "half_win", "half_loss", "loss", "push"}:
            return
        target[verdict_text] = int(target.get(verdict_text, 0)) + 1
        target["total"] = int(target.get("total", 0)) + 1

    @staticmethod
    def _classify_result_handicap_delta(delta: float | None) -> str | None:
        if delta is None:
            return None
        try:
            delta_value = float(delta)
        except Exception:
            return None
        if not math.isfinite(delta_value):
            return None
        if abs(delta_value) <= 1e-9:
            return "push"
        abs_delta = abs(delta_value)
        is_half = abs(abs_delta - 0.25) <= 1e-6
        if delta_value > 0:
            return "half_win" if is_half else "win"
        return "half_loss" if is_half else "loss"

    @staticmethod
    def _classify_xg_handicap_delta(delta: float | None, push_threshold: float = 0.1) -> str | None:
        if delta is None:
            return None
        try:
            delta_value = float(delta)
        except Exception:
            return None
        if not math.isfinite(delta_value):
            return None
        threshold = max(0.0, float(push_threshold))
        if delta_value > threshold:
            return "win"
        if delta_value < -threshold:
            return "loss"
        return "push"

    @staticmethod
    def _result_weighted_counts(counts: dict[str, Any]) -> tuple[float, float, float]:
        win = float(counts.get("win", 0) or 0)
        half_win = float(counts.get("half_win", 0) or 0)
        half_loss = float(counts.get("half_loss", 0) or 0)
        loss = float(counts.get("loss", 0) or 0)
        push = float(counts.get("push", 0) or 0)
        weighted_win = win + (0.5 * half_win)
        weighted_loss = loss + (0.5 * half_loss)
        weighted_push = push + (0.5 * half_win) + (0.5 * half_loss)
        return weighted_win, weighted_loss, weighted_push

    @staticmethod
    def _score_from_result_counts(counts: dict[str, Any]) -> float:
        total = float(counts.get("total", 0) or 0)
        if total <= 0:
            return 0.0
        weighted_win, weighted_loss, _ = GameXgdService._result_weighted_counts(counts)
        return (weighted_win - weighted_loss) / total

    @staticmethod
    def _score_from_xg_counts(counts: dict[str, Any]) -> float:
        total = float(counts.get("total", 0) or 0)
        if total <= 0:
            return 0.0
        win = float(counts.get("win", 0) or 0)
        loss = float(counts.get("loss", 0) or 0)
        return (win - loss) / total

    @staticmethod
    def _to_serializable_int_counts(counts: dict[str, Any]) -> dict[str, int]:
        return {
            "win": int(counts.get("win", 0) or 0),
            "half_win": int(counts.get("half_win", 0) or 0),
            "half_loss": int(counts.get("half_loss", 0) or 0),
            "loss": int(counts.get("loss", 0) or 0),
            "push": int(counts.get("push", 0) or 0),
            "total": int(counts.get("total", 0) or 0),
        }

    def _result_pnl_from_verdict_and_price(self, verdict: str | None, price: float | None) -> float:
        if not verdict:
            return 0.0
        try:
            odds = float(price) if price is not None else float("nan")
        except Exception:
            odds = float("nan")
        if not math.isfinite(odds) or odds <= 1.0:
            return 0.0
        commission = float(getattr(self, "_betfair_commission_rate", 0.0) or 0.0)
        commission = min(1.0, max(0.0, commission))
        net_factor = 1.0 - commission
        verdict_text = str(verdict).strip().lower()
        if verdict_text == "win":
            return (odds - 1.0) * net_factor
        if verdict_text == "half_win":
            return (odds - 1.0) * 0.5 * net_factor
        if verdict_text == "loss":
            return -1.0
        if verdict_text == "half_loss":
            return -0.5
        return 0.0

    @staticmethod
    def _season_id_key(value: Any) -> str:
        try:
            if value is None or pd.isna(value):
                return ""
        except Exception:
            pass
        text = str(value).strip()
        if not text:
            return ""
        try:
            numeric = float(text)
        except Exception:
            return text
        if math.isfinite(numeric) and abs(numeric - round(numeric)) <= 1e-9:
            return str(int(round(numeric)))
        return text

    @staticmethod
    def _season_start_year(ts: pd.Timestamp) -> int | None:
        if pd.isna(ts):
            return None
        year = int(ts.year)
        return year if int(ts.month) >= 7 else (year - 1)

    @staticmethod
    def _has_season_id(value: Any) -> bool:
        return bool(GameXgdService._season_id_key(value))

    @staticmethod
    def _season_values_equal(left: Any, right: Any) -> bool:
        left_key = GameXgdService._season_id_key(left)
        right_key = GameXgdService._season_id_key(right)
        return bool(left_key) and bool(right_key) and left_key == right_key

    def _filter_rows_by_season_id(self, rows: pd.DataFrame, season_id_value: Any) -> pd.DataFrame:
        if not isinstance(rows, pd.DataFrame) or rows.empty:
            return pd.DataFrame()
        if "season_id" not in rows.columns:
            return rows.copy()
        season_key = self._season_id_key(season_id_value)
        if not season_key:
            return rows.copy()
        season_keys = rows["season_id"].map(self._season_id_key)
        return rows[season_keys == season_key].copy()

    def _infer_fixture_season_id(self, home_team: str, away_team: str, kickoff_time: Any) -> Any:
        home_text = str(home_team or "").strip()
        away_text = str(away_team or "").strip()
        kickoff_ts = pd.to_datetime(kickoff_time, errors="coerce", utc=True)
        if not home_text or not away_text or pd.isna(kickoff_ts):
            return None
        if self.form_df.empty:
            return None
        if "team" not in self.form_df.columns or "date_time" not in self.form_df.columns or "season_id" not in self.form_df.columns:
            return None

        rows = self.form_df.copy()
        rows = rows[rows["team"].astype(str).isin({home_text, away_text})].copy()
        if rows.empty:
            return None

        rows["date_time"] = pd.to_datetime(rows["date_time"], errors="coerce", utc=True)
        rows = rows[rows["date_time"].notna() & (rows["date_time"] < kickoff_ts)].copy()
        if rows.empty:
            return None

        rows["_season_key"] = rows["season_id"].map(self._season_id_key)
        rows = rows[rows["_season_key"] != ""].copy()
        if rows.empty:
            return None

        home_keys = set(rows.loc[rows["team"].astype(str) == home_text, "_season_key"].tolist())
        away_keys = set(rows.loc[rows["team"].astype(str) == away_text, "_season_key"].tolist())
        common_keys = home_keys & away_keys
        if not common_keys:
            return None

        common_rows = rows[rows["_season_key"].isin(common_keys)].copy()
        if common_rows.empty:
            return None
        common_rows = common_rows.sort_values("date_time", kind="mergesort")
        selected_key = str(common_rows.iloc[-1]["_season_key"]).strip()
        if not selected_key:
            return None

        selected_rows = rows[rows["_season_key"] == selected_key].copy()
        selected_rows = selected_rows[selected_rows["season_id"].notna()].copy()
        if selected_rows.empty:
            return selected_key
        selected_rows = selected_rows.sort_values("date_time", kind="mergesort")
        return selected_rows.iloc[-1]["season_id"]

    def _filter_rows_for_hc_rankings_current_season(self, rows: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(rows, pd.DataFrame) or rows.empty:
            return pd.DataFrame()
        out = rows.copy()
        if "date_time" not in out.columns:
            return out

        out["date_time"] = pd.to_datetime(out["date_time"], errors="coerce", utc=True)
        out = out[out["date_time"].notna()].copy()
        if out.empty:
            return pd.DataFrame()

        if "competition_name" in out.columns:
            out["_competition"] = out["competition_name"].fillna("").astype(str).str.strip()
            out["_competition"] = out["_competition"].replace("", "Unknown")
        else:
            out["_competition"] = "Unknown"

        if "season_id" in out.columns:
            out["_season_id_key"] = out["season_id"].map(self._season_id_key)
        else:
            out["_season_id_key"] = ""
        out["_season_start_year"] = out["date_time"].map(self._season_start_year)

        filtered_frames: list[pd.DataFrame] = []
        for _, comp_rows in out.groupby("_competition", sort=False):
            rows_with_season_id = comp_rows[comp_rows["_season_id_key"] != ""]
            if not rows_with_season_id.empty:
                latest_by_season = rows_with_season_id.groupby("_season_id_key")["date_time"].max()
                current_season_key = str(latest_by_season.idxmax())
                keep_mask = comp_rows["_season_id_key"] == current_season_key
            else:
                current_start_year = pd.to_numeric(comp_rows["_season_start_year"], errors="coerce").max()
                keep_mask = comp_rows["_season_start_year"] == current_start_year
            filtered_frames.append(comp_rows[keep_mask].copy())

        if not filtered_frames:
            return pd.DataFrame()
        filtered = pd.concat(filtered_frames, ignore_index=True)
        return filtered.drop(columns=["_competition", "_season_id_key", "_season_start_year"], errors="ignore")

    def _build_prices_by_match_id_for_home_rows(self, source_rows: pd.DataFrame) -> dict[str, dict[str, str]]:
        if not isinstance(source_rows, pd.DataFrame) or source_rows.empty:
            return {}
        historical_service = self.state.historical_data_service
        match_ids = source_rows["match_id"].dropna().tolist() if "match_id" in source_rows.columns else []
        rows_df = historical_service._query_historical_price_rows_for_match_ids(match_ids)
        entries = historical_service._build_historical_price_entries_from_rows(rows_df)
        out: dict[str, dict[str, str]] = {}
        for entry in entries:
            game_id = historical_service._normalize_game_id_value(entry.get("game_id"))
            if not game_id:
                continue
            prices = dict(entry.get("prices", {})) if isinstance(entry, dict) else {}
            mainline_text = str(prices.get("mainline", "")).strip()
            if not mainline_text or mainline_text == "-":
                continue
            out[game_id] = prices
        return out

    def _resolve_sofascore_team_name(
        self,
        team_name: str,
        competition_name: str | None = None,
    ) -> str:
        requested_team = str(team_name or "").strip()
        if not requested_team:
            return ""

        manual_lookup = self.get_manual_mapping_lookup_snapshot()
        requested_key = normalize_team_name(requested_team)
        if requested_key and manual_lookup:
            manual_target = manual_lookup.get(requested_key)
            if manual_target and manual_target in self.team_matcher.team_set:
                return manual_target

        if requested_team in self.team_matcher.team_set:
            return requested_team

        matched_team, _, _ = self.team_matcher.match(requested_team)
        if matched_team:
            return matched_team

        competition_text = str(competition_name or "").strip()
        if (
            competition_text
            and isinstance(self.form_df, pd.DataFrame)
            and (not self.form_df.empty)
            and ("team" in self.form_df.columns)
        ):
            scoped = self.form_df.copy()
            if "competition_name" in scoped.columns:
                comp_col = scoped["competition_name"].fillna("").astype(str).str.strip()
                scoped = scoped[comp_col == competition_text].copy()
            scoped_teams = sorted(
                {
                    str(value).strip()
                    for value in scoped["team"].dropna().tolist()
                    if str(value).strip()
                }
            )
            if scoped_teams:
                scoped_matcher = TeamMatcher(scoped_teams)
                scoped_match, _, _ = scoped_matcher.match(requested_team)
                if scoped_match:
                    return scoped_match

        return requested_team

    def list_teams_directory(self) -> dict[str, Any]:
        historical_service = self.state.historical_data_service
        source_rows = self._filter_rows_for_hc_rankings_current_season(
            historical_service._prepare_historical_home_rows()
        )
        if not isinstance(source_rows, pd.DataFrame) or source_rows.empty:
            return {
                "teams": [],
                "competitions": [],
                "total_teams": 0,
                "total_competitions": 0,
            }

        long_rows: list[dict[str, Any]] = []
        for row in source_rows.to_dict(orient="records"):
            competition_text = str(row.get("competition_name", "")).strip() or "Unknown"
            kickoff_ts = pd.to_datetime(row.get("date_time"), errors="coerce", utc=True)
            home_team = str(row.get("team", "")).strip()
            away_team = str(row.get("opponent", "")).strip()
            if home_team:
                long_rows.append(
                    {
                        "team": home_team,
                        "competition": competition_text,
                        "date_time": kickoff_ts,
                    }
                )
            if away_team:
                long_rows.append(
                    {
                        "team": away_team,
                        "competition": competition_text,
                        "date_time": kickoff_ts,
                    }
                )

        if not long_rows:
            return {
                "teams": [],
                "competitions": [],
                "total_teams": 0,
                "total_competitions": 0,
            }

        long_df = pd.DataFrame(long_rows)
        if long_df.empty:
            return {
                "teams": [],
                "competitions": [],
                "total_teams": 0,
                "total_competitions": 0,
            }

        long_df["team"] = long_df["team"].fillna("").astype(str).str.strip()
        long_df = long_df[long_df["team"] != ""].copy()
        if long_df.empty:
            return {
                "teams": [],
                "competitions": [],
                "total_teams": 0,
                "total_competitions": 0,
            }

        long_df["competition"] = long_df["competition"].fillna("").astype(str).str.strip().replace("", "Unknown")
        long_df["date_time"] = pd.to_datetime(long_df["date_time"], errors="coerce", utc=True)

        competition_counts = long_df.groupby("competition", dropna=False)["team"].size().sort_values(ascending=False)
        competitions = [
            {
                "competition": str(comp_name),
                "games_count": int(count),
            }
            for comp_name, count in competition_counts.items()
        ]

        teams: list[dict[str, Any]] = []
        for team_name, team_rows in long_df.groupby("team", sort=True):
            comp_counts = team_rows.groupby("competition", dropna=False)["team"].size().sort_values(ascending=False)
            comp_entries = [
                {
                    "competition": str(comp_name),
                    "games_count": int(count),
                }
                for comp_name, count in comp_counts.items()
            ]
            latest_ts = pd.to_datetime(team_rows["date_time"], errors="coerce", utc=True).max()
            latest_text = latest_ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(latest_ts) else ""
            teams.append(
                {
                    "team": str(team_name),
                    "games_count": int(len(team_rows)),
                    "competitions_count": int(len(comp_entries)),
                    "primary_competition": str(comp_entries[0]["competition"]) if comp_entries else "",
                    "latest_game_utc": latest_text,
                    "competitions": comp_entries,
                }
            )

        teams.sort(key=lambda row: str(row.get("team", "")).lower())
        return {
            "teams": teams,
            "competitions": competitions,
            "total_teams": len(teams),
            "total_competitions": len(competitions),
        }

    def get_team_hc_rankings(
        self,
        xg_push_threshold: float = 0.1,
        season_id: str | None = None,
        competition_name: str | None = None,
    ) -> dict[str, Any]:
        xg_threshold = self._to_float_or_none(xg_push_threshold)
        if xg_threshold is None or not math.isfinite(float(xg_threshold)):
            xg_threshold = 0.1
        xg_threshold = max(0.0, min(5.0, float(xg_threshold)))
        requested_season_key = self._season_id_key(season_id)
        requested_competition = str(competition_name or "").strip()
        historical_service = self.state.historical_data_service
        source_rows = historical_service._prepare_historical_home_rows()
        if not isinstance(source_rows, pd.DataFrame) or source_rows.empty:
            return {
                "season": requested_season_key,
                "seasons": [],
                "seasons_by_competition": {},
                "leagues": [],
                "rows": [],
                "total_leagues": 0,
                "total_teams": 0,
                "sort_options": ["result", "xg", "pnl", "pnl_against"],
                "venue_options": ["overall", "home", "away"],
                "xg_push_threshold": xg_threshold,
            }

        if "date_time" in source_rows.columns:
            source_rows["date_time"] = pd.to_datetime(source_rows["date_time"], errors="coerce", utc=True)
            source_rows = source_rows[source_rows["date_time"].notna()].copy()
        if source_rows.empty:
            return {
                "season": requested_season_key,
                "seasons": [],
                "seasons_by_competition": {},
                "leagues": [],
                "rows": [],
                "total_leagues": 0,
                "total_teams": 0,
                "sort_options": ["result", "xg", "pnl", "pnl_against"],
                "venue_options": ["overall", "home", "away"],
                "xg_push_threshold": xg_threshold,
            }

        if "season_id" in source_rows.columns:
            source_rows["_season_key"] = source_rows["season_id"].map(self._season_id_key)
        else:
            source_rows["_season_key"] = ""
        if not bool((source_rows["_season_key"] != "").any()):
            source_rows["_season_key"] = source_rows["date_time"].map(self._season_start_year).map(
                lambda value: "" if value is None else str(int(value))
            )
        source_rows = source_rows[source_rows["_season_key"].astype(str).str.strip() != ""].copy()
        if source_rows.empty:
            return {
                "season": requested_season_key,
                "seasons": [],
                "seasons_by_competition": {},
                "leagues": [],
                "rows": [],
                "total_leagues": 0,
                "total_teams": 0,
                "sort_options": ["result", "xg", "pnl", "pnl_against"],
                "venue_options": ["overall", "home", "away"],
                "xg_push_threshold": xg_threshold,
            }

        def build_season_summaries(rows: pd.DataFrame) -> list[dict[str, Any]]:
            out: list[dict[str, Any]] = []
            for season_key, season_rows in rows.groupby("_season_key", sort=False):
                season_key_text = str(season_key or "").strip()
                if not season_key_text:
                    continue
                latest_ts = pd.to_datetime(season_rows["date_time"], errors="coerce", utc=True).max()
                latest_text = latest_ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(latest_ts) else ""
                season_label = ""
                if "season_name" in season_rows.columns:
                    season_name_rows = season_rows.copy()
                    season_name_rows = season_name_rows.sort_values("date_time", kind="mergesort")
                    season_name_text = str(season_name_rows.iloc[-1].get("season_name", "")).strip()
                    if season_name_text:
                        season_label = season_name_text
                if not season_label:
                    season_label = f"Season {season_key_text}"
                out.append(
                    {
                        "season": season_key_text,
                        "season_label": season_label,
                        "games_count": int(len(season_rows)),
                        "latest_game_utc": latest_text,
                        "_latest_ts": latest_ts,
                    }
                )
            out.sort(
                key=lambda row: (
                    (
                        pd.to_datetime(row.get("_latest_ts"), errors="coerce", utc=True).timestamp()
                        if not pd.isna(pd.to_datetime(row.get("_latest_ts"), errors="coerce", utc=True))
                        else float("-inf")
                    ),
                    int(row.get("games_count", 0) or 0),
                    str(row.get("season", "")).lower(),
                ),
                reverse=True,
            )
            return out

        season_summaries = build_season_summaries(source_rows)
        seasons_by_competition: dict[str, list[dict[str, Any]]] = {}
        if "competition_name" in source_rows.columns:
            comp_series = source_rows["competition_name"].fillna("").astype(str).str.strip()
            source_rows["_competition_key"] = comp_series.where(comp_series != "", "Unknown")
        else:
            source_rows["_competition_key"] = "Unknown"
        for competition_key, competition_rows in source_rows.groupby("_competition_key", sort=False):
            competition_text = str(competition_key or "").strip() or "Unknown"
            seasons_by_competition[competition_text] = build_season_summaries(competition_rows)

        selected_competition_seasons = seasons_by_competition.get(requested_competition, [])
        selected_competition_available_seasons = {
            str(row.get("season", "")).strip()
            for row in selected_competition_seasons
            if str(row.get("season", "")).strip()
        }
        available_seasons = {
            str(row.get("season", "")).strip()
            for row in season_summaries
            if str(row.get("season", "")).strip()
        }
        if requested_competition and selected_competition_available_seasons:
            selected_season = (
                requested_season_key if requested_season_key in selected_competition_available_seasons else ""
            )
            if not selected_season and selected_competition_seasons:
                selected_season = str(selected_competition_seasons[0].get("season", "")).strip()
        else:
            selected_season = requested_season_key if requested_season_key in available_seasons else ""
            if not selected_season and season_summaries:
                selected_season = str(season_summaries[0].get("season", "")).strip()
        if selected_season:
            source_rows = source_rows[source_rows["_season_key"] == selected_season].copy()
        for row in season_summaries:
            row.pop("_latest_ts", None)
        for competition_rows in seasons_by_competition.values():
            for row in competition_rows:
                row.pop("_latest_ts", None)

        prices_by_match_id = self._build_prices_by_match_id_for_home_rows(source_rows)
        league_team_stats: dict[str, dict[str, dict[str, Any]]] = {}

        def get_team_bucket(competition_name: str, team_name: str) -> dict[str, Any]:
            competition_text = str(competition_name or "").strip() or "Unknown"
            team_text = str(team_name or "").strip()
            league_bucket = league_team_stats.setdefault(competition_text, {})
            if team_text not in league_bucket:
                league_bucket[team_text] = {
                    "team": team_text,
                    "games_played": {
                        "home": 0,
                        "away": 0,
                        "overall": 0,
                    },
                    "games_with_handicap": {
                        "home": 0,
                        "away": 0,
                        "overall": 0,
                    },
                    "result": {
                        "home": self._empty_hc_rank_counts(),
                        "away": self._empty_hc_rank_counts(),
                        "overall": self._empty_hc_rank_counts(),
                    },
                    "xg": {
                        "home": self._empty_hc_rank_counts(),
                        "away": self._empty_hc_rank_counts(),
                        "overall": self._empty_hc_rank_counts(),
                    },
                    "pnl": {
                        "home": 0.0,
                        "away": 0.0,
                        "overall": 0.0,
                    },
                    "pnl_against": {
                        "home": 0.0,
                        "away": 0.0,
                        "overall": 0.0,
                    },
                    "tier_counts": {},
                }
            return league_bucket[team_text]

        for row in source_rows.to_dict(orient="records"):
            home_team = str(row.get("team", "")).strip()
            away_team = str(row.get("opponent", "")).strip()
            if not home_team or not away_team:
                continue

            competition = str(row.get("competition_name", "")).strip() or "Unknown"
            tier = str(row.get("tier", "")).strip()
            match_id = historical_service._normalize_game_id_value(row.get("match_id"))
            prices = prices_by_match_id.get(match_id, {}) if match_id else {}

            home_bucket = get_team_bucket(competition, home_team)
            away_bucket = get_team_bucket(competition, away_team)

            home_games_played = home_bucket["games_played"]
            away_games_played = away_bucket["games_played"]
            home_games_played["home"] = int(home_games_played.get("home", 0)) + 1
            home_games_played["overall"] = int(home_games_played.get("overall", 0)) + 1
            away_games_played["away"] = int(away_games_played.get("away", 0)) + 1
            away_games_played["overall"] = int(away_games_played.get("overall", 0)) + 1

            if tier:
                home_tier_counts = home_bucket["tier_counts"]
                away_tier_counts = away_bucket["tier_counts"]
                home_tier_counts[tier] = int(home_tier_counts.get(tier, 0)) + 1
                away_tier_counts[tier] = int(away_tier_counts.get(tier, 0)) + 1

            home_handicap = parse_handicap_value((prices or {}).get("mainline"))
            if home_handicap is None:
                continue
            home_handicap = float(home_handicap)
            home_price = self._to_float_or_none((prices or {}).get("home_price"))
            away_price = self._to_float_or_none((prices or {}).get("away_price"))

            home_goals = self._to_float_or_none(row.get("GF"))
            away_goals = self._to_float_or_none(row.get("GA"))
            home_xg = self._to_float_or_none(row.get("xG"))
            away_xg = self._to_float_or_none(row.get("xGA"))

            result_delta_home = None
            result_delta_away = None
            if home_goals is not None and away_goals is not None:
                result_delta_home = (float(home_goals) - float(away_goals)) + home_handicap
                result_delta_away = -result_delta_home

            xg_delta_home = None
            xg_delta_away = None
            if home_xg is not None and away_xg is not None:
                xg_delta_home = (float(home_xg) - float(away_xg)) + home_handicap
                xg_delta_away = -xg_delta_home

            home_games_with_handicap = home_bucket["games_with_handicap"]
            away_games_with_handicap = away_bucket["games_with_handicap"]
            home_games_with_handicap["home"] = int(home_games_with_handicap.get("home", 0)) + 1
            home_games_with_handicap["overall"] = int(home_games_with_handicap.get("overall", 0)) + 1
            away_games_with_handicap["away"] = int(away_games_with_handicap.get("away", 0)) + 1
            away_games_with_handicap["overall"] = int(away_games_with_handicap.get("overall", 0)) + 1

            home_result_verdict = self._classify_result_handicap_delta(result_delta_home)
            away_result_verdict = self._classify_result_handicap_delta(result_delta_away)
            self._increment_hc_rank_counts(home_bucket["result"]["home"], home_result_verdict)
            self._increment_hc_rank_counts(home_bucket["result"]["overall"], home_result_verdict)
            self._increment_hc_rank_counts(away_bucket["result"]["away"], away_result_verdict)
            self._increment_hc_rank_counts(away_bucket["result"]["overall"], away_result_verdict)
            home_result_pnl = self._result_pnl_from_verdict_and_price(home_result_verdict, home_price)
            away_result_pnl = self._result_pnl_from_verdict_and_price(away_result_verdict, away_price)
            home_bucket["pnl"]["home"] = float(home_bucket["pnl"].get("home", 0.0)) + float(home_result_pnl)
            home_bucket["pnl"]["overall"] = float(home_bucket["pnl"].get("overall", 0.0)) + float(home_result_pnl)
            away_bucket["pnl"]["away"] = float(away_bucket["pnl"].get("away", 0.0)) + float(away_result_pnl)
            away_bucket["pnl"]["overall"] = float(away_bucket["pnl"].get("overall", 0.0)) + float(away_result_pnl)
            home_bucket["pnl_against"]["home"] = (
                float(home_bucket["pnl_against"].get("home", 0.0)) + float(away_result_pnl)
            )
            home_bucket["pnl_against"]["overall"] = (
                float(home_bucket["pnl_against"].get("overall", 0.0)) + float(away_result_pnl)
            )
            away_bucket["pnl_against"]["away"] = (
                float(away_bucket["pnl_against"].get("away", 0.0)) + float(home_result_pnl)
            )
            away_bucket["pnl_against"]["overall"] = (
                float(away_bucket["pnl_against"].get("overall", 0.0)) + float(home_result_pnl)
            )

            home_xg_verdict = self._classify_xg_handicap_delta(xg_delta_home, push_threshold=xg_threshold)
            away_xg_verdict = self._classify_xg_handicap_delta(xg_delta_away, push_threshold=xg_threshold)
            self._increment_hc_rank_counts(home_bucket["xg"]["home"], home_xg_verdict)
            self._increment_hc_rank_counts(home_bucket["xg"]["overall"], home_xg_verdict)
            self._increment_hc_rank_counts(away_bucket["xg"]["away"], away_xg_verdict)
            self._increment_hc_rank_counts(away_bucket["xg"]["overall"], away_xg_verdict)

        leagues: list[dict[str, Any]] = []
        rows: list[dict[str, Any]] = []
        for competition_name in sorted(league_team_stats.keys(), key=lambda value: str(value).lower()):
            competition_bucket = league_team_stats.get(competition_name, {})
            competition_rows: list[dict[str, Any]] = []
            for team_name in sorted(competition_bucket.keys(), key=lambda value: str(value).lower()):
                bucket = competition_bucket.get(team_name, {})
                if not team_name:
                    continue
                tier_counts = bucket.get("tier_counts", {})
                tier = (
                    max(tier_counts.items(), key=lambda kv: (int(kv[1]), str(kv[0]).lower()))[0]
                    if tier_counts
                    else ""
                )

                result_home = self._to_serializable_int_counts(bucket["result"]["home"])
                result_away = self._to_serializable_int_counts(bucket["result"]["away"])
                result_overall = self._to_serializable_int_counts(bucket["result"]["overall"])
                xg_home = self._to_serializable_int_counts(bucket["xg"]["home"])
                xg_away = self._to_serializable_int_counts(bucket["xg"]["away"])
                xg_overall = self._to_serializable_int_counts(bucket["xg"]["overall"])
                games_played = {
                    "home": int(bucket.get("games_played", {}).get("home", 0) or 0),
                    "away": int(bucket.get("games_played", {}).get("away", 0) or 0),
                    "overall": int(bucket.get("games_played", {}).get("overall", 0) or 0),
                }
                games_with_handicap = {
                    "home": int(bucket.get("games_with_handicap", {}).get("home", 0) or 0),
                    "away": int(bucket.get("games_with_handicap", {}).get("away", 0) or 0),
                    "overall": int(bucket.get("games_with_handicap", {}).get("overall", 0) or 0),
                }
                games_missing = {
                    "home": max(0, games_played["home"] - games_with_handicap["home"]),
                    "away": max(0, games_played["away"] - games_with_handicap["away"]),
                    "overall": max(0, games_played["overall"] - games_with_handicap["overall"]),
                }

                row_out = {
                    "team": team_name,
                    "competition": competition_name,
                    "tier": tier,
                    "games_played": games_played,
                    "games_with_handicap": games_with_handicap,
                    "games_missing": games_missing,
                    "result": {
                        "home": result_home,
                        "away": result_away,
                        "overall": result_overall,
                    },
                    "xg": {
                        "home": xg_home,
                        "away": xg_away,
                        "overall": xg_overall,
                    },
                    "scores": {
                        "result_home": self._score_from_result_counts(result_home),
                        "result_away": self._score_from_result_counts(result_away),
                        "result_overall": self._score_from_result_counts(result_overall),
                        "xg_home": self._score_from_xg_counts(xg_home),
                        "xg_away": self._score_from_xg_counts(xg_away),
                        "xg_overall": self._score_from_xg_counts(xg_overall),
                    },
                    "pnl": {
                        "home": round(float(bucket.get("pnl", {}).get("home", 0.0) or 0.0), 4),
                        "away": round(float(bucket.get("pnl", {}).get("away", 0.0) or 0.0), 4),
                        "overall": round(float(bucket.get("pnl", {}).get("overall", 0.0) or 0.0), 4),
                    },
                    "pnl_against": {
                        "home": round(float(bucket.get("pnl_against", {}).get("home", 0.0) or 0.0), 4),
                        "away": round(float(bucket.get("pnl_against", {}).get("away", 0.0) or 0.0), 4),
                        "overall": round(float(bucket.get("pnl_against", {}).get("overall", 0.0) or 0.0), 4),
                    },
                }
                competition_rows.append(row_out)
                rows.append(row_out)

            leagues.append(
                {
                    "competition": competition_name,
                    "rows": competition_rows,
                    "total_teams": len(competition_rows),
                }
            )

        return {
            "season": selected_season,
            "seasons": season_summaries,
            "seasons_by_competition": seasons_by_competition,
            "leagues": leagues,
            "rows": rows,
            "total_leagues": len(leagues),
            "total_teams": len(rows),
            "sort_options": ["result", "xg", "pnl", "pnl_against"],
            "venue_options": ["overall", "home", "away"],
            "xg_push_threshold": xg_threshold,
        }

    def get_team_hc_ranking_details(
        self,
        team_name: str,
        competition_name: str | None = None,
        season_id: str | None = None,
    ) -> dict[str, Any]:
        requested_team_text = str(team_name or "").strip()
        if not requested_team_text:
            raise ValueError("team is required")

        competition_text = str(competition_name or "").strip()
        requested_season_key = self._season_id_key(season_id)
        team_text = self._resolve_sofascore_team_name(
            requested_team_text,
            competition_name=(competition_text or None),
        )
        historical_service = self.state.historical_data_service
        source_rows_raw = historical_service._prepare_historical_home_rows()
        if requested_season_key:
            work_rows = source_rows_raw.copy() if isinstance(source_rows_raw, pd.DataFrame) else pd.DataFrame()
            if isinstance(work_rows, pd.DataFrame) and (not work_rows.empty) and ("date_time" in work_rows.columns):
                work_rows["date_time"] = pd.to_datetime(work_rows["date_time"], errors="coerce", utc=True)
                work_rows = work_rows[work_rows["date_time"].notna()].copy()
                if "season_id" in work_rows.columns:
                    work_rows["_season_key"] = work_rows["season_id"].map(self._season_id_key)
                else:
                    work_rows["_season_key"] = ""
                if not bool((work_rows["_season_key"] != "").any()):
                    work_rows["_season_key"] = work_rows["date_time"].map(self._season_start_year).map(
                        lambda value: "" if value is None else str(int(value))
                    )
                work_rows = work_rows[work_rows["_season_key"].astype(str).str.strip() != ""].copy()
                source_rows = work_rows[work_rows["_season_key"] == requested_season_key].copy()
            else:
                source_rows = pd.DataFrame()
        else:
            source_rows = self._filter_rows_for_hc_rankings_current_season(source_rows_raw)
        if not isinstance(source_rows, pd.DataFrame) or source_rows.empty:
            return {
                "team": team_text,
                "requested_team": requested_team_text,
                "competition": competition_text,
                "season": requested_season_key,
                "rows": [],
                "games_count": 0,
            }

        work = source_rows.copy()
        if competition_text and "competition_name" in work.columns:
            comp_col = work["competition_name"].fillna("").astype(str).str.strip()
            work = work[comp_col == competition_text].copy()
        if work.empty:
            return {
                "team": team_text,
                "requested_team": requested_team_text,
                "competition": competition_text,
                "season": requested_season_key,
                "rows": [],
                "games_count": 0,
            }

        team_col = work["team"].fillna("").astype(str).str.strip() if "team" in work.columns else pd.Series(dtype=str)
        opp_col = work["opponent"].fillna("").astype(str).str.strip() if "opponent" in work.columns else pd.Series(dtype=str)
        work = work[(team_col == team_text) | (opp_col == team_text)].copy()
        if work.empty:
            return {
                "team": team_text,
                "requested_team": requested_team_text,
                "competition": competition_text,
                "season": requested_season_key,
                "rows": [],
                "games_count": 0,
            }

        prices_by_match_id = self._build_prices_by_match_id_for_home_rows(work)
        default_prices = historical_service._historical_default_price_snapshot()
        rows_out: list[dict[str, Any]] = []

        for row in work.to_dict(orient="records"):
            home_team = str(row.get("team", "")).strip()
            away_team = str(row.get("opponent", "")).strip()
            if not home_team or not away_team:
                continue
            is_home = team_text == home_team
            if (not is_home) and team_text != away_team:
                continue

            match_id = historical_service._normalize_game_id_value(row.get("match_id"))
            closing = prices_by_match_id.get(match_id) if match_id else None
            if not isinstance(closing, dict):
                closing = dict(default_prices)

            home_handicap = parse_handicap_value((closing or {}).get("mainline"))
            team_handicap = None if home_handicap is None else (float(home_handicap) if is_home else -float(home_handicap))

            match_home_goals = self._to_float_or_none(row.get("GF"))
            match_away_goals = self._to_float_or_none(row.get("GA"))
            match_home_xg = self._to_float_or_none(row.get("xG"))
            match_away_xg = self._to_float_or_none(row.get("xGA"))

            result_margin = None
            if match_home_goals is not None and match_away_goals is not None:
                result_margin = (
                    (float(match_home_goals) - float(match_away_goals))
                    if is_home
                    else (float(match_away_goals) - float(match_home_goals))
                )
            xg_margin = None
            if match_home_xg is not None and match_away_xg is not None:
                xg_margin = (
                    (float(match_home_xg) - float(match_away_xg))
                    if is_home
                    else (float(match_away_xg) - float(match_home_xg))
                )
            result_vs_hc = (result_margin + team_handicap) if (result_margin is not None and team_handicap is not None) else None
            xg_vs_hc = (xg_margin + team_handicap) if (xg_margin is not None and team_handicap is not None) else None

            kickoff_ts = pd.to_datetime(row.get("date_time"), errors="coerce", utc=True)
            rows_out.append(
                {
                    "_sort_kickoff": kickoff_ts,
                    "date_time": kickoff_ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(kickoff_ts) else "",
                    "competition_name": str(row.get("competition_name", "")).strip(),
                    "team": team_text,
                    "opponent": away_team if is_home else home_team,
                    "venue": "home" if is_home else "away",
                    "home_team": home_team,
                    "away_team": away_team,
                    "home_goals": match_home_goals,
                    "away_goals": match_away_goals,
                    "home_xg": match_home_xg,
                    "away_xg": match_away_xg,
                    "closing_mainline": str((closing or {}).get("mainline", "-") or "-"),
                    "closing_home_price": str((closing or {}).get("home_price", "-") or "-"),
                    "closing_away_price": str((closing or {}).get("away_price", "-") or "-"),
                    "closing_handicap_team": team_handicap,
                    "result_vs_hc": result_vs_hc,
                    "xg_vs_hc": xg_vs_hc,
                }
            )

        rows_out = sorted(
            rows_out,
            key=lambda item: pd.to_datetime(item.get("_sort_kickoff"), errors="coerce", utc=True),
            reverse=True,
        )
        for row in rows_out:
            row.pop("_sort_kickoff", None)
        return {
            "team": team_text,
            "requested_team": requested_team_text,
            "competition": competition_text,
            "season": requested_season_key,
            "rows": rows_out,
            "games_count": len(rows_out),
        }

    def get_team_page(
        self,
        team_name: str,
        competition_name: str | None = None,
        season_id: str | None = None,
    ) -> dict[str, Any]:
        requested_team_text = str(team_name or "").strip()
        if not requested_team_text:
            raise ValueError("team is required")

        requested_competition = str(competition_name or "").strip()
        requested_season_key = self._season_id_key(season_id)
        team_text = self._resolve_sofascore_team_name(
            requested_team_text,
            competition_name=(requested_competition or None),
        )
        historical_service = self.state.historical_data_service
        source_rows = historical_service._prepare_historical_home_rows()

        empty_payload = {
            "team": team_text,
            "requested_team": requested_team_text,
            "competition": requested_competition,
            "season": requested_season_key,
            "competitions": [],
            "seasons": [],
            "recent_rows": [],
            "team_venue_rows": {"home": [], "away": []},
            "season_handicap_rows": [],
            "games_count": 0,
            "handicap_games_count": 0,
        }
        if not isinstance(source_rows, pd.DataFrame) or source_rows.empty:
            return empty_payload

        work_source = source_rows.copy()
        team_col = work_source["team"].fillna("").astype(str).str.strip() if "team" in work_source.columns else pd.Series(dtype=str)
        opp_col = (
            work_source["opponent"].fillna("").astype(str).str.strip()
            if "opponent" in work_source.columns
            else pd.Series(dtype=str)
        )
        work_source = work_source[(team_col == team_text) | (opp_col == team_text)].copy()
        if work_source.empty:
            return empty_payload

        if "date_time" in work_source.columns:
            work_source["date_time"] = pd.to_datetime(work_source["date_time"], errors="coerce", utc=True)
            work_source = work_source[work_source["date_time"].notna()].copy()
        if work_source.empty:
            return empty_payload

        competition_tier_map: dict[str, str] = {}
        if "competition_name" in source_rows.columns and "tier" in source_rows.columns:
            for row in source_rows.to_dict(orient="records"):
                competition = str(row.get("competition_name", "")).strip()
                tier = str(row.get("tier", "")).strip()
                if competition and competition not in competition_tier_map:
                    competition_tier_map[competition] = tier

        if "season_id" in work_source.columns:
            work_source["_season_key"] = work_source["season_id"].map(self._season_id_key)
        else:
            work_source["_season_key"] = ""
        if not bool((work_source["_season_key"] != "").any()):
            work_source["_season_key"] = work_source["date_time"].map(self._season_start_year).map(
                lambda value: "" if value is None else str(int(value))
            )

        work_source = work_source[work_source["_season_key"].astype(str).str.strip() != ""].copy()
        if work_source.empty:
            return empty_payload

        season_summaries: list[dict[str, Any]] = []
        season_key_to_value: dict[str, Any] = {}
        for season_key, season_rows in work_source.groupby("_season_key", sort=False):
            season_key_text = str(season_key or "").strip()
            if not season_key_text:
                continue
            latest_ts = pd.to_datetime(season_rows["date_time"], errors="coerce", utc=True).max()
            latest_text = latest_ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(latest_ts) else ""

            season_id_value: Any = season_key_text
            if "season_id" in season_rows.columns:
                season_ids = [value for value in season_rows["season_id"].dropna().tolist() if self._season_id_key(value)]
                if season_ids:
                    season_id_value = season_ids[0]
            season_key_to_value.setdefault(season_key_text, season_id_value)

            season_label = ""
            if "season_name" in season_rows.columns:
                season_name_rows = season_rows.copy()
                season_name_rows = season_name_rows.sort_values("date_time", kind="mergesort")
                season_name_text = str(season_name_rows.iloc[-1].get("season_name", "")).strip()
                if season_name_text:
                    season_label = season_name_text
            if not season_label:
                season_label = f"Season {season_key_text}"

            season_summaries.append(
                {
                    "season": season_key_text,
                    "season_label": season_label,
                    "games_count": int(len(season_rows)),
                    "latest_game_utc": latest_text,
                    "_latest_ts": latest_ts,
                }
            )

        season_summaries.sort(
            key=lambda row: (
                (
                    pd.to_datetime(row.get("_latest_ts"), errors="coerce", utc=True).timestamp()
                    if not pd.isna(pd.to_datetime(row.get("_latest_ts"), errors="coerce", utc=True))
                    else float("-inf")
                ),
                int(row.get("games_count", 0) or 0),
                str(row.get("season", "")).lower(),
            ),
            reverse=True,
        )

        available_seasons = {
            str(row.get("season", "")).strip()
            for row in season_summaries
            if str(row.get("season", "")).strip()
        }
        selected_season = requested_season_key if requested_season_key in available_seasons else ""
        if not selected_season and season_summaries:
            selected_season = str(season_summaries[0].get("season", "")).strip()
        selected_season_value = season_key_to_value.get(selected_season, selected_season)
        if selected_season:
            work_source = work_source[work_source["_season_key"] == selected_season].copy()

        competition_summaries: list[dict[str, Any]] = []
        if "competition_name" in work_source.columns:
            comp_series = work_source["competition_name"].fillna("").astype(str).str.strip()
            for competition_name_value, comp_rows in work_source.groupby(comp_series, sort=False):
                competition_name_text = str(competition_name_value or "").strip()
                if not competition_name_text:
                    continue
                latest_ts = pd.to_datetime(comp_rows["date_time"], errors="coerce", utc=True).max()
                latest_text = latest_ts.strftime("%Y-%m-%d %H:%M") if not pd.isna(latest_ts) else ""
                competition_summaries.append(
                    {
                        "competition": competition_name_text,
                        "tier": competition_tier_map.get(competition_name_text, ""),
                        "games_count": int(len(comp_rows)),
                        "latest_game_utc": latest_text,
                        "_latest_ts": latest_ts,
                    }
                )

        competition_summaries.sort(
            key=lambda row: (
                (
                    pd.to_datetime(row.get("_latest_ts"), errors="coerce", utc=True).timestamp()
                    if not pd.isna(pd.to_datetime(row.get("_latest_ts"), errors="coerce", utc=True))
                    else float("-inf")
                ),
                int(row.get("games_count", 0) or 0),
                str(row.get("competition", "")).lower(),
            ),
            reverse=True,
        )

        available_competitions = {
            str(row.get("competition", "")).strip()
            for row in competition_summaries
            if str(row.get("competition", "")).strip()
        }
        selected_competition = requested_competition if requested_competition in available_competitions else ""
        if not selected_competition and competition_summaries:
            selected_competition = str(competition_summaries[0].get("competition", "")).strip()

        team_form_rows = self.form_df.copy()
        if team_form_rows.empty or "team" not in team_form_rows.columns:
            team_form_rows = pd.DataFrame()
        else:
            team_series = team_form_rows["team"].fillna("").astype(str).str.strip()
            team_form_rows = team_form_rows[team_series == team_text].copy()

        if isinstance(team_form_rows, pd.DataFrame) and (not team_form_rows.empty):
            if "competition_name" in team_form_rows.columns and available_competitions:
                comp_values = team_form_rows["competition_name"].fillna("").astype(str).str.strip()
                team_form_rows = team_form_rows[comp_values.isin(available_competitions)].copy()
            if selected_season:
                team_form_rows = self._filter_rows_by_season_id(team_form_rows, selected_season_value)
            if selected_competition and "competition_name" in team_form_rows.columns:
                comp_values = team_form_rows["competition_name"].fillna("").astype(str).str.strip()
                team_form_rows = team_form_rows[comp_values == selected_competition].copy()
            if "date_time" in team_form_rows.columns:
                team_form_rows["date_time"] = pd.to_datetime(team_form_rows["date_time"], errors="coerce", utc=True)
                team_form_rows = team_form_rows[team_form_rows["date_time"].notna()].copy()
                team_form_rows = team_form_rows.sort_values("date_time", ascending=False).reset_index(drop=True)
        else:
            team_form_rows = pd.DataFrame()

        if selected_competition and not requested_competition:
            requested_competition = selected_competition
        if selected_season and not requested_season_key:
            requested_season_key = selected_season

        latest_team_ts = pd.to_datetime(team_form_rows.get("date_time"), errors="coerce", utc=True).max() if (
            isinstance(team_form_rows, pd.DataFrame) and (not team_form_rows.empty) and ("date_time" in team_form_rows.columns)
        ) else pd.NaT
        if pd.isna(latest_team_ts):
            latest_team_ts = pd.to_datetime(work_source.get("date_time"), errors="coerce", utc=True).max() if (
                isinstance(work_source, pd.DataFrame) and (not work_source.empty) and ("date_time" in work_source.columns)
            ) else pd.NaT
        cutoff_time = (
            (latest_team_ts + pd.Timedelta(seconds=1))
            if not pd.isna(latest_team_ts)
            else pd.Timestamp.now(tz="UTC")
        )
        team_venue_rows_raw = build_team_venue_recent_rows(
            form_df=self.form_df,
            team_name=team_text,
            kickoff_time=cutoff_time,
            recent_n=None,
            season_id=(selected_season_value if selected_season else None),
            competition_name=(selected_competition or None),
            area_name=None,
        )
        team_venue_rows = {
            "home": list(team_venue_rows_raw.get("home") or []),
            "away": list(team_venue_rows_raw.get("away") or []),
            "home_prev_season_rows": list(team_venue_rows_raw.get("home_prev_season_rows") or []),
            "away_prev_season_rows": list(team_venue_rows_raw.get("away_prev_season_rows") or []),
        }
        recent_rows = merge_recent_rows_by_venue(
            {
                "home": team_venue_rows["home"],
                "away": team_venue_rows["away"],
            }
        )

        season_handicap_rows = self._build_team_season_handicap_rows(
            team_name=team_text,
            season_id=selected_season_value,
            kickoff_time=cutoff_time,
            competition_name=(selected_competition or None),
        )

        for row in competition_summaries:
            row.pop("_latest_ts", None)
        for row in season_summaries:
            row.pop("_latest_ts", None)

        return {
            "team": team_text,
            "requested_team": requested_team_text,
            "competition": selected_competition,
            "season": selected_season,
            "competitions": competition_summaries,
            "seasons": season_summaries,
            "recent_rows": recent_rows,
            "team_venue_rows": team_venue_rows,
            "season_handicap_rows": season_handicap_rows,
            "games_count": len(recent_rows),
            "handicap_games_count": len(season_handicap_rows),
        }

    def _build_team_season_handicap_rows(
        self,
        team_name: str,
        season_id: Any,
        kickoff_time: Any,
        resolve_from_archive: bool = False,
        timing_label: str = "",
        competition_name: str | None = None,
        area_name: str | None = None,
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

        competition_text = str(competition_name or "").strip()
        if competition_text and "competition_name" in team_df.columns:
            step_started_at = time.perf_counter()
            team_df = team_df[team_df["competition_name"].astype(str) == competition_text].copy()
            self._log_hcperf_timing(
                label,
                "competition_filter",
                step_started_at,
                rows=int(len(team_df)),
                competition=competition_text,
            )
            if team_df.empty:
                self._log_hcperf_timing(
                    label,
                    "early_exit_no_games_in_competition",
                    step_started_at,
                    competition=competition_text,
                )
                return []

        area_text = str(area_name or "").strip()
        if area_text and "area_name" in team_df.columns:
            step_started_at = time.perf_counter()
            team_df = team_df[team_df["area_name"].astype(str) == area_text].copy()
            self._log_hcperf_timing(
                label,
                "area_filter",
                step_started_at,
                rows=int(len(team_df)),
                area=area_text,
            )
            if team_df.empty:
                self._log_hcperf_timing(
                    label,
                    "early_exit_no_games_in_area",
                    step_started_at,
                    area=area_text,
                )
                return []

        if self._has_season_id(season_id) and "season_id" in team_df.columns:
            step_started_at = time.perf_counter()
            team_df = self._filter_rows_by_season_id(team_df, season_id)
            self._log_hcperf_timing(label, "season_filter", step_started_at, rows=int(len(team_df)))
            if team_df.empty:
                self._log_hcperf_timing(label, "early_exit_no_games_in_fixture_season", step_started_at)
                return []

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
        blocked_auto_mapping_norms = self.state.get_disabled_auto_team_mapping_norms_snapshot()
        mapped_rows, _ = map_betfair_games(
            betfair_games_df=game_df,
            fixtures_df=self.fixtures_df,
            team_matcher=self.team_matcher,
            manual_mapping_lookup=manual_mapping_lookup,
            blocked_auto_mapping_norms=blocked_auto_mapping_norms,
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
            if not self._has_season_id(fixture_season_id):
                inferred_season_id = self._infer_fixture_season_id(
                    home_team=home_team,
                    away_team=away_team,
                    kickoff_time=kickoff_time,
                )
                if self._has_season_id(inferred_season_id):
                    fixture_season_id = inferred_season_id
            self._log_hcperf_timing(
                req_label,
                "resolve_fixture_season",
                req_started_at,
                fixture_season_id=self._season_id_key(fixture_season_id),
            )
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
        tier_text = str(game_row.get("tier", "")).strip().casefold()
        is_tier_zero = tier_text.startswith("tier 0") or tier_text == "tier0"

        def _clean_optional_text(value: Any) -> str:
            if value is None:
                return ""
            try:
                if pd.isna(value):
                    return ""
            except Exception:
                pass
            return str(value).strip()

        manual_mapping_lookup = self.get_manual_mapping_lookup_snapshot()
        manual_competition_mapping_lookup = self.get_manual_competition_mapping_lookup_snapshot()
        use_cached_main_table_metrics = True

        def _metric_value(period_key: str, metric_suffix: str) -> Any:
            col = f"{period_key}_{metric_suffix}"
            return to_native(game_row.get(col))

        period_rows: list[dict[str, Any]] = [
            {
                "period": "Season",
                "home_xg": _metric_value("season", "home_xg"),
                "away_xg": _metric_value("season", "away_xg"),
                "total_xg": _metric_value("season", "total_xg"),
                "xgd": _metric_value("season", "xgd"),
                "xgd_perf": _metric_value("season", "xgd_perf"),
                "strength": _metric_value("season", "strength"),
                "total_min_xg": _metric_value("season", "min_xg"),
                "total_max_xg": _metric_value("season", "max_xg"),
                "home_games_used": 0,
                "away_games_used": 0,
                "model_warning": None,
            },
            {
                "period": 5,
                "home_xg": _metric_value("last5", "home_xg"),
                "away_xg": _metric_value("last5", "away_xg"),
                "total_xg": _metric_value("last5", "total_xg"),
                "xgd": _metric_value("last5", "xgd"),
                "xgd_perf": _metric_value("last5", "xgd_perf"),
                "strength": _metric_value("last5", "strength"),
                "total_min_xg": _metric_value("last5", "min_xg"),
                "total_max_xg": _metric_value("last5", "max_xg"),
                "home_games_used": 0,
                "away_games_used": 0,
                "model_warning": None,
            },
            {
                "period": 3,
                "home_xg": _metric_value("last3", "home_xg"),
                "away_xg": _metric_value("last3", "away_xg"),
                "total_xg": _metric_value("last3", "total_xg"),
                "xgd": _metric_value("last3", "xgd"),
                "xgd_perf": _metric_value("last3", "xgd_perf"),
                "strength": _metric_value("last3", "strength"),
                "total_min_xg": _metric_value("last3", "min_xg"),
                "total_max_xg": _metric_value("last3", "max_xg"),
                "home_games_used": 0,
                "away_games_used": 0,
                "model_warning": None,
            },
        ]
        summary = {
            "home_xg": _metric_value("season", "home_xg"),
            "away_xg": _metric_value("season", "away_xg"),
            "total_xg": _metric_value("season", "total_xg"),
            "xgd": _metric_value("season", "xgd"),
            "xgd_perf": _metric_value("season", "xgd_perf"),
            "strength": _metric_value("season", "strength"),
        }
        warning_message: str | None = None

        mapped_rows, _ = map_betfair_games(
            betfair_games_df=game_df,
            fixtures_df=self.fixtures_df,
            team_matcher=self.team_matcher,
            manual_mapping_lookup=manual_mapping_lookup,
            blocked_auto_mapping_norms=self.state.get_disabled_auto_team_mapping_norms_snapshot(),
        )
        mapped_prediction_df = pd.DataFrame(mapped_rows)

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
        mapping_cols = [c for c in mapping_cols if c in mapped_prediction_df.columns]
        if mapping_cols:
            mapping_df = mapped_prediction_df[mapping_cols].drop_duplicates().reset_index(drop=True)
        else:
            mapping_df = pd.DataFrame(columns=mapping_cols)
        mapping_rows = [{k: to_native(v) for k, v in row.items()} for row in mapping_df.to_dict(orient="records")]

        def _empty_period_rows_for_fixture() -> list[dict[str, Any]]:
            return [
                {
                    "period": "Season",
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
                    "model_warning": None,
                },
                {
                    "period": 5,
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
                    "model_warning": None,
                },
                {
                    "period": 3,
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
                    "model_warning": None,
                },
            ]

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
                fixture_season_value = mapping_row.get("fixture_season_id")
                try:
                    if pd.isna(fixture_season_value):
                        fixture_season_value = None
                except Exception:
                    pass
                fixture_competition_value = _clean_optional_text(mapping_row.get("fixture_competition"))
                fixture_area_value = _clean_optional_text(mapping_row.get("fixture_area"))
                # Reuse cached table metrics for xGD numbers; avoid re-running model
                # on game-page open. Recent rows are built from filtered form_df below.
                home_recent_rows = []
                away_recent_rows = []

                home_team_venue_rows = build_team_venue_recent_rows(
                    form_df=self.form_df,
                    team_name=home_sofa,
                    kickoff_time=kickoff_time,
                    recent_n=None,
                    season_id=fixture_season_value,
                    competition_name=(fixture_competition_value or None),
                    area_name=(fixture_area_value or None),
                )
                away_team_venue_rows = build_team_venue_recent_rows(
                    form_df=self.form_df,
                    team_name=away_sofa,
                    kickoff_time=kickoff_time,
                    recent_n=None,
                    season_id=fixture_season_value,
                    competition_name=(fixture_competition_value or None),
                    area_name=(fixture_area_value or None),
                )
                # Fallback for game-page tables if direct source extraction fails.
                if not home_recent_rows:
                    home_recent_rows = merge_recent_rows_by_venue(home_team_venue_rows)
                if not away_recent_rows:
                    away_recent_rows = merge_recent_rows_by_venue(away_team_venue_rows)

                fixture_season_id = fixture_season_value
                season_handicap_rows = {
                    "home": self._build_team_season_handicap_rows(
                        team_name=str(home_sofa),
                        season_id=fixture_season_id,
                        kickoff_time=kickoff_time,
                        resolve_from_archive=True,
                    ),
                    "away": self._build_team_season_handicap_rows(
                        team_name=str(away_sofa),
                        season_id=fixture_season_id,
                        kickoff_time=kickoff_time,
                        resolve_from_archive=True,
                    ),
                }

                # Apply fixture competition-scoped extraction for all tiers, including tier 0.
                apply_fixture_scope = True
                if apply_fixture_scope:
                    fixture_competition_text = fixture_competition_value
                    fixture_area_text = fixture_area_value
                    fixture_season_id = fixture_season_value
                    betfair_competition_text = str(game_row.get("competition", "")).strip()

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
                        rows = self.form_df.copy()
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
                        sort_cols = [
                            col
                            for col in ("season_start_date", "date_time")
                            if col in common_rows.columns
                        ]
                        for col in ("season_start_date", "date_time"):
                            if col in common_rows.columns:
                                common_rows[col] = pd.to_datetime(common_rows[col], errors="coerce", utc=True)
                        if sort_cols:
                            common_rows = common_rows.sort_values(sort_cols, kind="mergesort")
                        return common_rows.iloc[-1]["season_id"] if not common_rows.empty else common_ids[0]

                    if not fixture_competition_text:
                        with self.lock:
                            sofa_competitions = list(self.sofa_competitions)
                            sofa_competition_by_norm = dict(self.sofa_competition_by_norm)
                            sofa_competition_set = set(self.sofa_competition_set)
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
                            form_df=self.form_df,
                            competition_name=fixture_competition_text,
                            fallback_area=None,
                        )
                        fixture_area_text = str(inferred_area or "").strip()

                    fixture_competition_in_db = False
                    if fixture_competition_text:
                        fixture_competition_norm = normalize_competition_key(fixture_competition_text)
                        with self.lock:
                            sofa_competition_set = set(self.sofa_competition_set)
                            sofa_competition_by_norm = dict(self.sofa_competition_by_norm)
                        if fixture_competition_text in sofa_competition_set:
                            fixture_competition_in_db = True
                        elif fixture_competition_norm and fixture_competition_norm in sofa_competition_by_norm:
                            fixture_competition_text = str(
                                sofa_competition_by_norm.get(fixture_competition_norm) or fixture_competition_text
                            ).strip()
                            fixture_competition_in_db = True

                    if not fixture_competition_in_db:
                        summary = None
                        period_rows = _empty_period_rows_for_fixture()
                        home_recent_rows = []
                        away_recent_rows = []
                        home_team_venue_rows = {"home": [], "away": []}
                        away_team_venue_rows = {"home": [], "away": []}
                        season_handicap_rows = {"home": [], "away": []}
                        warning_message = "Season warning: fixture competition not found in database."
                    else:
                        home_comp_rows, home_comp_season = select_team_competition_rows(
                            form_df=self.form_df,
                            team_name=str(home_sofa),
                            competition_name=fixture_competition_text,
                            area_name=(fixture_area_text or None),
                            kickoff_time=kickoff_time,
                        )
                        away_comp_rows, away_comp_season = select_team_competition_rows(
                            form_df=self.form_df,
                            team_name=str(away_sofa),
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
                                        home_team_name=str(home_sofa),
                                        away_team_name=str(away_sofa),
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
                            form_df=self.form_df,
                            team_name=home_sofa,
                            kickoff_time=kickoff_time,
                            recent_n=None,
                            season_id=fixture_season_id,
                            competition_name=(fixture_competition_text or None),
                            area_name=(fixture_area_text or None),
                        )
                        away_team_venue_rows = build_team_venue_recent_rows(
                            form_df=self.form_df,
                            team_name=away_sofa,
                            kickoff_time=kickoff_time,
                            recent_n=None,
                            season_id=fixture_season_id,
                            competition_name=(fixture_competition_text or None),
                            area_name=(fixture_area_text or None),
                        )
                        home_recent_rows = list(home_team_venue_rows.get("home") or [])
                        away_recent_rows = list(away_team_venue_rows.get("away") or [])

                        if not home_recent_rows or not away_recent_rows:
                            summary = None
                            period_rows = []
                            warning_message = (
                                "Season warning: one team has no matches in this competition season."
                            )
                        else:
                            # Keep cached period metrics from the main games table.
                            warning_message = None

                betfair_competition = str(game_row.get("competition", "")).strip()
                fixture_competition = fixture_competition_value
                fixture_area = fixture_area_value
                if fixture_competition:
                    # Prefer fixture-mapped competition context when available.
                    # This prevents Betfair competition drift from forcing
                    # European-only views on domestic fixtures.
                    is_european_fixture = (
                        is_european_competition_name(fixture_competition)
                        or fixture_area.casefold() == "europe"
                    )
                else:
                    is_european_fixture = is_european_competition_name(betfair_competition)
                if (not use_cached_main_table_metrics) and is_european_fixture:
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
            "season_handicap_rows": season_handicap_rows,
        }
        xgd_views: list[dict[str, Any]] = [base_view]

        if (
            is_tier_zero
            and primary_mapping_row
            and primary_home_sofa
            and primary_away_sofa
            and not pd.isna(kickoff_time)
        ):
            home_domestic_competition, home_domestic_area = infer_team_domestic_competition(
                form_df=self.form_df,
                team_name=primary_home_sofa,
                kickoff_time=kickoff_time,
            )
            away_domestic_competition, away_domestic_area = infer_team_domestic_competition(
                form_df=self.form_df,
                team_name=primary_away_sofa,
                kickoff_time=kickoff_time,
            )

            has_domestic_competitions = (
                bool(home_domestic_competition)
                and bool(away_domestic_competition)
            )

            if has_domestic_competitions:
                home_domestic_rows, home_domestic_season = select_team_competition_rows(
                    form_df=self.form_df,
                    team_name=primary_home_sofa,
                    competition_name=home_domestic_competition,
                    area_name=home_domestic_area,
                    kickoff_time=kickoff_time,
                )
                away_domestic_rows, away_domestic_season = select_team_competition_rows(
                    form_df=self.form_df,
                    team_name=primary_away_sofa,
                    competition_name=away_domestic_competition,
                    area_name=away_domestic_area,
                    kickoff_time=kickoff_time,
                )
                if not home_domestic_rows.empty and not away_domestic_rows.empty:
                    domestic_form_df = pd.concat([home_domestic_rows, away_domestic_rows], ignore_index=True)
                    same_domestic_competition = (
                        normalize_competition_key(home_domestic_competition)
                        == normalize_competition_key(away_domestic_competition)
                        and normalize_competition_key(home_domestic_area)
                        == normalize_competition_key(away_domestic_area)
                    )
                    if not same_domestic_competition:
                        if "season_id" in domestic_form_df.columns:
                            domestic_form_df["season_id"] = "__tier0_domestic__"
                        if "season_start_date" in domestic_form_df.columns:
                            domestic_form_df["season_start_date"] = pd.Timestamp("1900-01-01", tz="UTC")
                        if "season_end_date" in domestic_form_df.columns:
                            domestic_form_df["season_end_date"] = pd.Timestamp("2100-12-31 23:59:59", tz="UTC")
                    domestic_view = build_xgd_view_from_form_df(
                        view_id="domestic",
                        label="Domestic",
                        home_sofa=primary_home_sofa,
                        away_sofa=primary_away_sofa,
                        kickoff_time=kickoff_time,
                        calc_form_df=domestic_form_df,
                        full_form_df=self.form_df,
                        calc_wyscout_form_tables=self.form_model_module.calc_wyscout_form_tables,
                        periods=self.periods,
                        min_games=self.min_games,
                        home_venue_filter={
                            "season_id": home_domestic_season,
                            "competition_name": home_domestic_competition,
                            "area_name": home_domestic_area,
                        },
                        away_venue_filter={
                            "season_id": away_domestic_season,
                            "competition_name": away_domestic_competition,
                            "area_name": away_domestic_area,
                        },
                    )
                    if domestic_view:
                        domestic_view["context_note"] = (
                            f"Domestic leagues - {primary_home_sofa}: {home_domestic_competition} | "
                            f"{primary_away_sofa}: {away_domestic_competition}"
                        )
                        domestic_view["season_handicap_rows"] = {
                            "home": self._build_team_season_handicap_rows(
                                team_name=primary_home_sofa,
                                season_id=home_domestic_season,
                                kickoff_time=kickoff_time,
                                resolve_from_archive=True,
                                competition_name=home_domestic_competition,
                                area_name=home_domestic_area,
                            ),
                            "away": self._build_team_season_handicap_rows(
                                team_name=primary_away_sofa,
                                season_id=away_domestic_season,
                                kickoff_time=kickoff_time,
                                resolve_from_archive=True,
                                competition_name=away_domestic_competition,
                                area_name=away_domestic_area,
                            ),
                        }
                        xgd_views.append(domestic_view)

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
