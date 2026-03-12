"""Historical game calculations service."""

from __future__ import annotations

import re
from typing import Any, Callable

import pandas as pd


ExtractPeriodMetricsFn = Callable[[pd.DataFrame], pd.DataFrame]
PeriodRowsFromReducedTableFn = Callable[..., list[dict[str, Any]]]
SourceCompetitionsDifferFn = Callable[..., bool]


class HistoricalService:
    """Owns historical-day xGD calculation and updates."""

    def __init__(
        self,
        state: Any,
        *,
        period_metric_columns: tuple[str, ...],
        extract_period_metrics: ExtractPeriodMetricsFn,
        period_rows_from_reduced_table: PeriodRowsFromReducedTableFn,
        source_competitions_differ_from_betfair_competition: SourceCompetitionsDifferFn,
    ) -> None:
        self.state = state
        self.period_metric_columns = period_metric_columns
        self.extract_period_metrics = extract_period_metrics
        self.period_rows_from_reduced_table = period_rows_from_reduced_table
        self.source_competitions_differ_from_betfair_competition = (
            source_competitions_differ_from_betfair_competition
        )

    def _build_day_metrics(self, day_games_df: pd.DataFrame) -> pd.DataFrame:
        out_cols = ["market_id", *self.period_metric_columns]
        if day_games_df.empty:
            return pd.DataFrame(columns=out_cols)

        model_rows: list[dict[str, Any]] = []
        mapping_rows: list[dict[str, Any]] = []
        for row in day_games_df.to_dict(orient="records"):
            market_id = str(row.get("market_id", "")).strip()
            home_team = str(row.get("home_sofa", "")).strip()
            away_team = str(row.get("away_sofa", "")).strip()
            kickoff_time = pd.to_datetime(row.get("kickoff_time"), errors="coerce", utc=True)
            if (not market_id) or (not home_team) or (not away_team) or pd.isna(kickoff_time):
                continue
            model_rows.append(
                {
                    "home": home_team,
                    "away": away_team,
                    "match_date": kickoff_time,
                    "season_id": row.get("fixture_season_id"),
                    "competition_name": row.get("fixture_competition"),
                    "area_name": row.get("fixture_area"),
                }
            )
            mapping_rows.append(
                {
                    "market_id": market_id,
                    "competition": str(row.get("competition", "")).strip(),
                }
            )

        if not model_rows:
            return pd.DataFrame(columns=out_cols)

        try:
            game_tables, source_games = self.state.form_model_module.calc_wyscout_form_tables(
                pd.DataFrame(model_rows),
                self.state.form_df,
                periods=self.state.periods,
                return_source_games=True,
                min_games=self.state.min_games,
            )
        except Exception:
            return pd.DataFrame(columns=out_cols)

        pred_rows: list[dict[str, Any]] = []
        max_idx = min(len(mapping_rows), len(game_tables))
        for idx in range(max_idx):
            mapped = mapping_rows[idx]
            source = source_games[idx] if idx < len(source_games) else {}
            warning = source.get("warning")
            home_source = source.get("home_source_games")
            away_source = source.get("away_source_games")
            home_used = int(len(home_source)) if isinstance(home_source, pd.DataFrame) else int(len(home_source or []))
            away_used = int(len(away_source)) if isinstance(away_source, pd.DataFrame) else int(len(away_source or []))
            mismatch = self.source_competitions_differ_from_betfair_competition(
                betfair_competition=mapped.get("competition"),
                home_source_games=home_source,
                away_source_games=away_source,
                manual_competition_mapping_lookup=self.state.manual_competition_mapping_lookup,
            )

            _, _, _, _, reduced = game_tables[idx]
            period_rows = self.period_rows_from_reduced_table(
                reduced_df=reduced,
                warning=warning,
                home_games_used=home_used,
                away_games_used=away_used,
            )
            for row in period_rows:
                pred_rows.append(
                    {
                        "market_id": mapped["market_id"],
                        "period": row.get("period"),
                        "xgd": row.get("xgd"),
                        "strength": row.get("strength"),
                        "total_min_xg": row.get("total_min_xg"),
                        "total_max_xg": row.get("total_max_xg"),
                        "xgd_competition_mismatch": bool(mismatch),
                    }
                )

        if not pred_rows:
            return pd.DataFrame(columns=out_cols)

        metrics_df = self.extract_period_metrics(pd.DataFrame(pred_rows))
        for col in out_cols:
            if col not in metrics_df.columns:
                metrics_df[col] = None
        return metrics_df[out_cols]

    def calculate_day_xgd(self, day_iso: str) -> dict[str, Any]:
        day_text = str(day_iso or "").strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_text):
            raise ValueError("date must be YYYY-MM-DD")

        with self.state.lock:
            historical_df = self.state.historical_games_df.copy()
        if historical_df.empty:
            raise ValueError("No historical games available")

        day_mask = historical_df["kickoff_time"].dt.strftime("%Y-%m-%d") == day_text
        day_games_df = historical_df[day_mask].copy()
        if day_games_df.empty:
            raise ValueError("No historical games found for the selected day")

        metrics_df = self._build_day_metrics(day_games_df)
        metric_cols = [col for col in self.period_metric_columns if col in historical_df.columns]

        with self.state.lock:
            updated_df = self.state.historical_games_df.copy()
            update_mask = updated_df["kickoff_time"].dt.strftime("%Y-%m-%d") == day_text
            for col in metric_cols:
                if pd.api.types.is_bool_dtype(updated_df[col].dtype):
                    # Use nullable boolean so clearing values does not trigger dtype warnings.
                    updated_df[col] = updated_df[col].astype("boolean")
                    updated_df.loc[update_mask, col] = pd.NA
                else:
                    updated_df.loc[update_mask, col] = float("nan")

            if not metrics_df.empty:
                update_cols = ["market_id", *metric_cols]
                update_frame = (
                    metrics_df[update_cols]
                    .drop_duplicates(subset=["market_id"], keep="first")
                    .set_index("market_id")
                )
                updated_indexed = updated_df.set_index("market_id")
                for col in metric_cols:
                    if col in update_frame.columns:
                        common_ids = updated_indexed.index.intersection(update_frame.index)
                        if not common_ids.empty:
                            updated_indexed.loc[common_ids, col] = update_frame.loc[common_ids, col]
                updated_df = updated_indexed.reset_index()

            self.state.historical_games_df = updated_df

        computed_games = int(metrics_df["market_id"].nunique()) if not metrics_df.empty else 0
        return {
            "date": day_text,
            "games_count": int(len(day_games_df)),
            "computed_games": computed_games,
        }

    def list_grouped_by_day(self) -> dict[str, Any]:
        return self.state.list_games_grouped_by_day(mode="historical")

    def get_game_xgd(self, market_id: str, recent_n: int = 5, venue_recent_n: int = 5) -> dict[str, Any]:
        return self.state.get_game_xgd(market_id=market_id, recent_n=recent_n, venue_recent_n=venue_recent_n)


__all__ = ["HistoricalService"]
