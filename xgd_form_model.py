from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

GAMESTATES = ("drawing", "winning", "losing")
GAMESTATE_EVENT_METRIC_KEYS = tuple(
    f"{metric}_{direction}_{state}"
    for metric in ("corners", "cards")
    for direction in ("for", "against")
    for state in GAMESTATES
)
GAMESTATE_TIME_KEYS = tuple(f"minutes_{state}" for state in GAMESTATES)
GAMESTATE_ALL_KEYS = GAMESTATE_EVENT_METRIC_KEYS + GAMESTATE_TIME_KEYS

REQUIRED_FORM_COLUMNS = {"team", "venue", "xG", "xGA", "xGoT", "xGoTA", "GF", "GA", "date_time"}


def calc_wyscout_form_tables(games, data_df, periods=("Season", 5, 3), return_source_games=False, min_games=3):
    """Calculate xGD form tables from the project's form dataframe.

    games can be:
    - DataFrame with columns home/away and optional date_time or match_date
    - dict/list convertible to DataFrame([home, away])

    data_df must be the transformed form dataframe used by this project
    (team-level rows with xG/xGA/xGoT/xGoTA/GF/GA and date_time columns).

    If return_source_games=True, also returns a second list with the exact
    source rows used for each fixture.
    """
    if isinstance(games, dict):
        games = pd.DataFrame(games, columns=["home", "away"])
    elif not isinstance(games, pd.DataFrame):
        games = pd.DataFrame(games, columns=["home", "away"])

    if "home" not in games.columns or "away" not in games.columns:
        raise KeyError("games must include 'home' and 'away' columns")

    games = games.copy()
    if "date_time" in games.columns:
        games["date_time"] = pd.to_datetime(games["date_time"], errors="coerce")
    elif "match_date" in games.columns:
        games["match_date"] = pd.to_datetime(games["match_date"], errors="coerce")

    if not isinstance(data_df, pd.DataFrame):
        raise TypeError("data_df must be a pandas DataFrame")

    missing_cols = sorted(REQUIRED_FORM_COLUMNS - set(data_df.columns))
    if missing_cols:
        raise KeyError(
            "data_df is missing required transformed form columns: " + ", ".join(missing_cols)
        )

    form_df = data_df.copy()
    form_df["date_time"] = pd.to_datetime(form_df["date_time"], errors="coerce")
    if "season_start_date" in form_df.columns:
        form_df["season_start_date"] = pd.to_datetime(form_df["season_start_date"], errors="coerce")
    if "season_end_date" in form_df.columns:
        form_df["season_end_date"] = pd.to_datetime(form_df["season_end_date"], errors="coerce")

    def _calc_form(df):
        cols_to_float = ["xG", "GF", "GA", "xGoT", "xGA", "xGoTA"]
        case = df.copy()
        for col in cols_to_float:
            case[col] = pd.to_numeric(case[col], errors="coerce")
        case = case.dropna(subset=["xG", "GF", "GA", "xGoT", "xGA", "xGoTA"])

        if len(case) == 0:
            return pd.DataFrame(
                {
                    "xGD": [0.0],
                    "GD-xGD": [0.0],
                    "xG": [0.0],
                    "G-xG": [0.0],
                    "xGoT-xG": [0.0],
                    "GF-xGoT": [0.0],
                    "xGA": [0.0],
                    "xGA-GA": [0.0],
                    "xGoTA-xGA": [0.0],
                    "xGoTA-GA": [0.0],
                }
            )

        return pd.DataFrame(
            {
                "xGD": [np.sum(case["xG"] - case["xGA"])],
                "GD-xGD": [np.sum(case["GF"] - case["GA"]) - np.sum(case["xG"] - case["xGA"])],
                "xG": [np.sum(case["xG"])],
                "G-xG": [np.sum(case["GF"] - case["xG"])],
                "xGoT-xG": [np.sum(case["xGoT"] - case["xG"])],
                "GF-xGoT": [np.sum(case["xGoT"] - case["GF"])],
                "xGA": [np.sum(case["xGA"])],
                "xGA-GA": [np.sum(case["xGA"] - case["GA"])],
                "xGoTA-xGA": [np.sum(case["xGoTA"] - case["xGA"])],
                "xGoTA-GA": [np.sum(case["xGoTA"] - case["GA"])],
            }
        ).astype(float)

    def _build_period_df(team_case):
        period_dfs = []
        for period in periods:
            if period == "Season":
                period_case = team_case.copy()
            else:
                period_case = team_case.tail(int(period)).copy()

            metrics = _calc_form(period_case)
            denom = len(period_case) if len(period_case) > 0 else 1
            metrics_90 = np.round(metrics / denom, 2)
            metrics_90["Period"] = period
            ordered = ["Period"] + [c for c in metrics_90.columns if c != "Period"]
            period_dfs.append(metrics_90[ordered])
        return pd.concat(period_dfs, ignore_index=True)

    col_names = [
        "Period",
        "xGD",
        "xGD Perf",
        "xG",
        "xG Pef",
        "Shoot Perf",
        "Opp Keep Perf",
        "xGA",
        "Def xG Perf",
        "Opp Shoot Perf",
        "Keep Perf",
    ]
    col_nums = [
        "Period",
        "xGD",
        "GD-xGD",
        "xG",
        "G-xG",
        "xGoT-xG",
        "xGoT-GF",
        "xGA",
        "xGA-GA",
        "xGoTA-xGA",
        "xGoTA-GA",
    ]

    teams_in_games = set(games["home"].dropna().tolist()) | set(games["away"].dropna().tolist())
    if teams_in_games and "team" in form_df.columns:
        form_df = form_df[form_df["team"].isin(teams_in_games)].copy()

    if "season_id" in games.columns and "season_id" in form_df.columns:
        season_ids = set(games["season_id"].dropna().tolist())
        if season_ids:
            form_df = form_df[form_df["season_id"].isin(season_ids)].copy()

    if (
        "competition_name" in games.columns
        and "area_name" in games.columns
        and "competition_name" in form_df.columns
        and "area_name" in form_df.columns
    ):
        comp_area_pairs = set(
            zip(
                games["competition_name"].dropna().tolist(),
                games["area_name"].dropna().tolist(),
            )
        )
        if comp_area_pairs:
            pair_mask = list(zip(form_df["competition_name"], form_df["area_name"]))
            form_df = form_df[[pair in comp_area_pairs for pair in pair_mask]].copy()

    form_df = form_df.sort_values(["date_time", "match_id", "venue"], kind="mergesort").reset_index(drop=True)
    team_venue_cache = {}
    for (team, venue), grp in form_df.groupby(["team", "venue"], sort=False):
        team_venue_cache[(team, venue)] = grp.copy()

    games_tables = []
    source_games = []
    for _, game in games.iterrows():
        home = game["home"]
        away = game["away"]
        if "date_time" in games.columns:
            cutoff = game["date_time"]
        elif "match_date" in games.columns:
            cutoff = game["match_date"]
        else:
            cutoff = pd.NaT

        home_perf = team_venue_cache.get((home, "Home"), form_df.iloc[0:0].copy())
        away_perf = team_venue_cache.get((away, "Away"), form_df.iloc[0:0].copy())

        if pd.notna(cutoff):
            home_perf = home_perf[home_perf["date_time"] < cutoff]
            away_perf = away_perf[away_perf["date_time"] < cutoff]

        season_used = np.nan
        warning_message = None

        if (
            pd.notna(cutoff)
            and "season_id" in form_df.columns
            and "season_start_date" in form_df.columns
            and "season_end_date" in form_df.columns
        ):
            home_active_rows = home_perf[
                (home_perf["season_start_date"] <= cutoff)
                & (home_perf["season_end_date"] >= cutoff)
            ].copy()
            away_active_rows = away_perf[
                (away_perf["season_start_date"] <= cutoff)
                & (away_perf["season_end_date"] >= cutoff)
            ].copy()

            common_active = set(home_active_rows["season_id"].dropna().tolist()).intersection(
                set(away_active_rows["season_id"].dropna().tolist())
            )

            if common_active:
                active_rows = pd.concat(
                    [
                        home_active_rows[home_active_rows["season_id"].isin(common_active)],
                        away_active_rows[away_active_rows["season_id"].isin(common_active)],
                    ],
                    ignore_index=True,
                )
                active_rows = active_rows.sort_values(["season_start_date", "date_time"])
                season_used = active_rows.iloc[-1]["season_id"]
                home_perf = home_active_rows[home_active_rows["season_id"] == season_used]
                away_perf = away_active_rows[away_active_rows["season_id"] == season_used]

            if pd.isna(season_used):
                warning_message = (
                    f"{home} vs {away} on {cutoff.date()}: no common active season found from season boundaries."
                )
                home_perf = home_perf.iloc[0:0].copy()
                away_perf = away_perf.iloc[0:0].copy()
            elif len(home_perf) < min_games or len(away_perf) < min_games:
                warning_message = (
                    f"{home} vs {away} on {cutoff.date()}: same-season sample is small "
                    f"(season_id={season_used}, home={len(home_perf)}, away={len(away_perf)}, min_games={min_games})."
                )
        elif "season_id" in form_df.columns:
            warning_message = (
                f"{home} vs {away}: season boundaries unavailable in input; "
                "unable to enforce strict same-season filtering."
            )

        home_df = _build_period_df(home_perf)
        away_df = _build_period_df(away_perf)

        final_df = pd.DataFrame(
            {
                "Period": home_df["Period"],
                "Strength": home_df["xGD"] - away_df["xGD"],
                "Home Perf": home_df["GD-xGD"],
                "Away Perf": away_df["GD-xGD"],
                "Home Shooting": home_df["xGoT-xG"],
                "Away Shooting": away_df["xGoT-xG"],
                "Home Keeping": home_df["xGoTA-GA"],
                "Away Keeping": away_df["xGoTA-GA"],
            }
        )
        final_df["Perf"] = np.round(final_df["Strength"] + final_df["Home Perf"] - final_df["Away Perf"], 2)

        ex_calcs = pd.DataFrame(
            {
                "Period": home_df["Period"],
                "Home xG diff": away_df["xGA"] - home_df["xG"],
                "Min Home xG": np.minimum(home_df["xG"], away_df["xGA"]),
                "Max Home xG": np.maximum(home_df["xG"], away_df["xGA"]),
                "Team Home xG": home_df["xG"],
                "Avg Home xG": (home_df["xG"] + away_df["xGA"]) / 2,
            }
        )
        ex_calcs["Min Home xGoT"] = ex_calcs["Min Home xG"].values + home_df["xGoT-xG"]
        ex_calcs["Min Home Real xG"] = np.maximum(ex_calcs["Min Home xGoT"].values - away_df["xGoTA-GA"], 0)
        ex_calcs["Max Home xGoT"] = ex_calcs["Max Home xG"].values + home_df["xGoT-xG"]
        ex_calcs["Max Home Real xG"] = np.maximum(ex_calcs["Max Home xGoT"].values - away_df["xGoTA-GA"], 0)
        ex_calcs["Team Home Real xG"] = ex_calcs["Team Home xG"].values + home_df["xGoT-xG"] - away_df["xGoTA-GA"]
        ex_calcs["Avg Home Real xG"] = ex_calcs["Avg Home xG"].values + home_df["xGoT-xG"] - away_df["xGoTA-GA"]

        ex_calcs["Away xG diff"] = home_df["xGA"] - away_df["xG"]
        ex_calcs["Min Away xG"] = np.minimum(away_df["xG"], home_df["xGA"])
        ex_calcs["Max Away xG"] = np.maximum(away_df["xG"], home_df["xGA"])
        ex_calcs["Team Away xG"] = away_df["xG"]
        ex_calcs["Avg Away xG"] = (away_df["xG"] + home_df["xGA"]) / 2
        ex_calcs["Min Away xGoT"] = ex_calcs["Min Away xG"].values + away_df["xGoT-xG"]
        ex_calcs["Min Away Real xG"] = np.maximum(ex_calcs["Min Away xGoT"].values - home_df["xGoTA-GA"], 0)
        ex_calcs["Max Away xGoT"] = ex_calcs["Max Away xG"].values + away_df["xGoT-xG"]
        ex_calcs["Max Away Real xG"] = np.maximum(ex_calcs["Max Away xGoT"].values - home_df["xGoTA-GA"], 0)
        ex_calcs["Team Away Real xG"] = ex_calcs["Team Away xG"].values + away_df["xGoT-xG"] - home_df["xGoTA-GA"]
        ex_calcs["Avg Away Real xG"] = ex_calcs["Avg Away xG"].values + away_df["xGoT-xG"] - home_df["xGoTA-GA"]

        ex_calcs["Min Real xGD"] = ex_calcs["Min Home Real xG"] - ex_calcs["Min Away Real xG"]
        ex_calcs["Team Real xGD"] = np.round(ex_calcs["Team Home Real xG"] - ex_calcs["Team Away Real xG"], 2)
        ex_calcs["Avg Real xGD"] = ex_calcs["Avg Home Real xG"] - ex_calcs["Avg Away Real xG"]

        important_cols = [
            "Period",
            "Min Home xG",
            "Team Home xG",
            "Avg Home xG",
            "Min Home xGoT",
            "Min Home Real xG",
            "Team Home Real xG",
            "Avg Home Real xG",
            "Min Away xG",
            "Team Away xG",
            "Avg Away xG",
            "Min Away xGoT",
            "Min Away Real xG",
            "Team Away Real xG",
            "Avg Away Real xG",
            "Min Real xGD",
            "Team Real xGD",
            "Avg Real xGD",
        ]
        ex_calcs_out = ex_calcs[important_cols].copy()

        reduced_cols = ["Period", "Avg Home Real xG", "Avg Away Real xG", "Avg Real xGD"]
        ex_calcs_reduced = ex_calcs[reduced_cols].copy()
        ex_calcs_reduced.insert(1, "Strength", final_df["Strength"].values)
        ex_calcs_reduced["Total Avg Real xG"] = ex_calcs_reduced["Avg Home Real xG"] + ex_calcs_reduced["Avg Away Real xG"]
        ex_calcs_reduced["Total Min Real xG"] = ex_calcs["Min Home Real xG"] + ex_calcs["Min Away Real xG"]
        ex_calcs_reduced["Total Max Real xG"] = ex_calcs["Max Home Real xG"] + ex_calcs["Max Away Real xG"]

        home_df.columns = pd.MultiIndex.from_tuples(zip(col_nums, col_names))
        away_df.columns = pd.MultiIndex.from_tuples(zip(col_nums, col_names))

        for out_df in (home_df, away_df, final_df, ex_calcs_out, ex_calcs_reduced):
            num_cols = out_df.select_dtypes(include="number").columns
            out_df[num_cols] = out_df[num_cols].round(2)

        games_tables.append((home_df, away_df, final_df, ex_calcs_out, ex_calcs_reduced))

        if return_source_games:
            source_cols = [
                "match_id",
                "date_time",
                "season_id",
                "competition_name",
                "area_name",
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

            home_source = home_perf.sort_values("date_time").copy()
            away_source = away_perf.sort_values("date_time").copy()
            for col in source_cols:
                if col not in home_source.columns:
                    home_source[col] = pd.NA
                if col not in away_source.columns:
                    away_source[col] = pd.NA

            source_games.append(
                {
                    "home_source_games": home_source[source_cols].reset_index(drop=True),
                    "away_source_games": away_source[source_cols].reset_index(drop=True),
                    "season_id_used": season_used,
                    "warning": warning_message,
                }
            )

    if return_source_games:
        return games_tables, source_games
    return games_tables


__all__ = ["calc_wyscout_form_tables"]
