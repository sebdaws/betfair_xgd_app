#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pandas as pd

# Allow running this script from inside ./scripts while importing project modules.
PROJECT_DIR = Path(__file__).resolve().parents[1]
if str(PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_DIR))

import xgd_web_app as app
from xgd_app.integrations import xgd_form_model as wd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract gamestate cards/corners totals table for a given fixture."
    )
    parser.add_argument("--home", required=True, help="Home team name (Database naming).")
    parser.add_argument("--away", required=True, help="Away team name (Database naming).")
    parser.add_argument(
        "--kickoff",
        required=True,
        help="Fixture kickoff datetime (ISO-like), e.g. 2026-03-11T19:45:00Z",
    )
    parser.add_argument("--db-path", default=str(app.DEFAULT_SOFASCORE_DB))
    parser.add_argument("--season-id", type=int, default=None)
    parser.add_argument("--competition-name", default=None)
    parser.add_argument("--area-name", default=None)
    parser.add_argument(
        "--team-mappings-path",
        default="",
        help=(
            "Optional manual team mappings JSON path. "
            "Defaults to source-specific app_data file for the active --db-path."
        ),
    )
    parser.add_argument("--sample-size", type=int, default=None, help="Limit to latest N source matches per team.")
    parser.add_argument("--min-games", type=int, default=3)
    parser.add_argument(
        "--games-only",
        action="store_true",
        help="Only print the per-game source rows table (skip totals summary table).",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output instead of terminal table.")
    return parser.parse_args()


def normalize_sample_size(value: int | None) -> int | None:
    if value is None:
        return None
    if int(value) < 1:
        raise ValueError("--sample-size must be >= 1")
    return int(value)


def select_recent_rows(rows: Any, sample_size: int | None) -> pd.DataFrame:
    if isinstance(rows, list):
        rows = pd.DataFrame(rows)
    if not isinstance(rows, pd.DataFrame) or rows.empty:
        return pd.DataFrame(columns=["date_time", *app.GAMESTATE_EVENT_METRIC_KEYS])

    out = rows.copy()
    out["date_time"] = pd.to_datetime(out.get("date_time"), errors="coerce", utc=True)
    out = out.sort_values("date_time", ascending=False, na_position="last")
    if sample_size is not None:
        out = out.head(sample_size)

    for key in app.GAMESTATE_EVENT_METRIC_KEYS:
        if key not in out.columns:
            out[key] = 0.0
        out[key] = pd.to_numeric(out[key], errors="coerce").fillna(0.0)
    return out


def render_number(value: float) -> int | float:
    rounded = round(float(value))
    if abs(float(value) - rounded) < 1e-9:
        return int(rounded)
    return round(float(value), 2)


def build_team_rows(team_label: str, rows: pd.DataFrame) -> list[dict[str, Any]]:
    table_rows: list[dict[str, Any]] = []
    metric_specs = [
        ("corners", "for", "Corners For"),
        ("corners", "against", "Corners Against"),
        ("cards", "for", "Cards For"),
        ("cards", "against", "Cards Against"),
    ]
    state_specs = [("drawing", "D"), ("winning", "W"), ("losing", "L")]

    for metric, direction, label in metric_specs:
        out_row: dict[str, Any] = {"team": team_label, "metric": label}
        for state_key, state_label in state_specs:
            metric_key = f"{metric}_{direction}_{state_key}"
            out_row[state_label] = render_number(rows[metric_key].sum()) if metric_key in rows.columns else 0
        table_rows.append(out_row)
    return table_rows


def result_state_from_row(row: pd.Series) -> str:
    gf = pd.to_numeric(row.get("GF"), errors="coerce")
    ga = pd.to_numeric(row.get("GA"), errors="coerce")
    if pd.isna(gf) or pd.isna(ga):
        return "unknown"
    if gf > ga:
        return "winning"
    if gf < ga:
        return "losing"
    return "drawing"


def build_games_used_table(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame(
            columns=[
                "date_time",
                "competition_name",
                "team",
                "opponent",
                "venue",
                "GF",
                "GA",
                "result_state",
                "corners_for",
                "corners_against",
                "cards_for",
                "cards_against",
                "goal_minutes_for",
                "goal_minutes_against",
                *app.GAMESTATE_EVENT_METRIC_KEYS,
            ]
        )

    out = rows.copy()
    out["result_state"] = out.apply(result_state_from_row, axis=1)
    wanted_cols = [
        "date_time",
        "competition_name",
        "team",
        "opponent",
        "venue",
        "GF",
        "GA",
        "result_state",
        "corners_for",
        "corners_against",
        "cards_for",
        "cards_against",
        "goal_minutes_for",
        "goal_minutes_against",
        *app.GAMESTATE_EVENT_METRIC_KEYS,
    ]
    for col in wanted_cols:
        if col not in out.columns:
            out[col] = pd.NA

    if "date_time" in out.columns:
        out["date_time"] = pd.to_datetime(out["date_time"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    for col in app.GAMESTATE_EVENT_METRIC_KEYS:
        out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0).map(render_number)
    for col in ("corners_for", "corners_against", "cards_for", "cards_against"):
        out[col] = pd.to_numeric(out[col], errors="coerce").map(lambda v: render_number(v) if pd.notna(v) else v)
    for col in ("goal_minutes_for", "goal_minutes_against"):
        out[col] = out[col].apply(
            lambda v: ",".join(str(int(x)) for x in v) if isinstance(v, list) else ("" if pd.isna(v) else str(v))
        )

    return out[wanted_cols].reset_index(drop=True)


def collect_match_ids(*frames: pd.DataFrame) -> list[int]:
    out: set[int] = set()
    for frame in frames:
        if not isinstance(frame, pd.DataFrame) or frame.empty or "match_id" not in frame.columns:
            continue
        values = pd.to_numeric(frame["match_id"], errors="coerce").dropna().astype(int).tolist()
        out.update(values)
    return sorted(out)


def load_timeline_data_for_matches(db_path: Path, match_ids: list[int]) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not match_ids:
        return pd.DataFrame(), pd.DataFrame()
    placeholders = ",".join("?" for _ in match_ids)

    conn = sqlite3.connect(str(db_path))
    try:
        events_df = pd.read_sql_query(
            f"""
            SELECT
                me.id,
                me.match_id,
                LOWER(TRIM(COALESCE(me.event_type, ''))) AS event_type,
                me.minute,
                COALESCE(me.added_time, 0) AS added_time,
                me.team_side_code
            FROM match_events me
            JOIN matches m ON m.id = me.match_id
            WHERE me.match_id IN ({placeholders})
              AND LOWER(TRIM(COALESCE(me.event_type, ''))) IN ('corner', 'card')
              AND (
                UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                OR (
                    NULLIF(TRIM(COALESCE(me.minute, '')), '') IS NOT NULL
                    AND CAST(NULLIF(TRIM(COALESCE(me.minute, '')), '') AS REAL) <= 90
                )
              )
            """,
            conn,
            params=match_ids,
        )
        goals_df = pd.read_sql_query(
            f"""
            SELECT
                ms.id,
                ms.match_id,
                ms.time_minute AS minute,
                CASE
                    WHEN ms.is_home = 1 THEN 1
                    WHEN ms.is_home = 0 THEN 2
                    ELSE NULL
                END AS side
            FROM match_shots ms
            JOIN matches m ON m.id = ms.match_id
            WHERE ms.match_id IN ({placeholders})
              AND LOWER(TRIM(COALESCE(ms.shot_type, ''))) = 'goal'
              AND (
                UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                OR (
                    NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                    AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                )
              )
            """,
            conn,
            params=match_ids,
        )
    finally:
        conn.close()
    return events_df, goals_df


def recompute_gamestate_from_shot_goals(db_path: Path, match_ids: list[int]) -> pd.DataFrame:
    out_cols = (
        ["match_id"]
        + [f"home_{key}" for key in app.GAMESTATE_EVENT_METRIC_KEYS]
        + [f"away_{key}" for key in app.GAMESTATE_EVENT_METRIC_KEYS]
        + ["home_goal_minutes", "away_goal_minutes"]
    )
    if not match_ids:
        return pd.DataFrame(columns=out_cols)

    events_df, goals_df = load_timeline_data_for_matches(db_path, match_ids)
    event_by_match = {
        int(mid): grp.copy()
        for mid, grp in events_df.groupby("match_id")
        if pd.notna(mid)
    } if not events_df.empty else {}
    goal_by_match = {
        int(mid): grp.copy()
        for mid, grp in goals_df.groupby("match_id")
        if pd.notna(mid)
    } if not goals_df.empty else {}

    rows: list[dict[str, Any]] = []
    for match_id in match_ids:
        event_rows = event_by_match.get(match_id, pd.DataFrame())
        goal_rows = goal_by_match.get(match_id, pd.DataFrame())
        timeline: list[dict[str, Any]] = []

        if not goal_rows.empty:
            for _, row in goal_rows.iterrows():
                minute = pd.to_numeric(row.get("minute"), errors="coerce")
                side = pd.to_numeric(row.get("side"), errors="coerce")
                if pd.isna(minute) or pd.isna(side):
                    continue
                side_int = int(side)
                if side_int not in (1, 2):
                    continue
                sort_id_num = pd.to_numeric(row.get("id"), errors="coerce")
                timeline.append(
                    {
                        "kind": "goal",
                        "minute": int(minute),
                        "added_time": 0,
                        "sort_priority": 0,
                        "sort_id": int(sort_id_num) if pd.notna(sort_id_num) else 0,
                        "side": side_int,
                    }
                )

        if not event_rows.empty:
            for _, row in event_rows.iterrows():
                event_type = str(row.get("event_type", "")).strip().lower()
                minute = pd.to_numeric(row.get("minute"), errors="coerce")
                added_time = pd.to_numeric(row.get("added_time"), errors="coerce")
                side = pd.to_numeric(row.get("team_side_code"), errors="coerce")
                if event_type not in {"corner", "card"}:
                    continue
                if pd.isna(minute) or pd.isna(side):
                    continue
                side_int = int(side)
                if side_int not in (1, 2):
                    continue
                sort_id_num = pd.to_numeric(row.get("id"), errors="coerce")
                timeline.append(
                    {
                        "kind": event_type,
                        "minute": int(minute),
                        "added_time": 0 if pd.isna(added_time) else int(added_time),
                        "sort_priority": 1,
                        "sort_id": int(sort_id_num) if pd.notna(sort_id_num) else 0,
                        "side": side_int,
                    }
                )

        timeline.sort(
            key=lambda item: (
                int(item.get("minute", 999)),
                int(item.get("added_time", 0)),
                int(item.get("sort_priority", 1)),
                int(item.get("sort_id", 0)),
            )
        )

        score_home = 0
        score_away = 0
        side_counts = {
            1: {key: 0 for key in app.GAMESTATE_EVENT_METRIC_KEYS},
            2: {key: 0 for key in app.GAMESTATE_EVENT_METRIC_KEYS},
        }
        home_goal_minutes: list[int] = []
        away_goal_minutes: list[int] = []

        for item in timeline:
            kind = str(item.get("kind", "")).strip().lower()
            side = int(item.get("side", 0))
            if kind == "goal":
                if side == 1:
                    score_home += 1
                    home_goal_minutes.append(int(item.get("minute", 0)))
                elif side == 2:
                    score_away += 1
                    away_goal_minutes.append(int(item.get("minute", 0)))
                continue

            metric_prefix = "corners" if kind == "corner" else "cards"
            for perspective_side in (1, 2):
                gamestate = app.gamestate_for_perspective(score_home, score_away, perspective_side)
                direction = "for" if side == perspective_side else "against"
                metric_key = f"{metric_prefix}_{direction}_{gamestate}"
                side_counts[perspective_side][metric_key] += 1

        row_out: dict[str, Any] = {
            "match_id": int(match_id),
            "home_goal_minutes": sorted(home_goal_minutes),
            "away_goal_minutes": sorted(away_goal_minutes),
        }
        for key in app.GAMESTATE_EVENT_METRIC_KEYS:
            row_out[f"home_{key}"] = side_counts[1][key]
            row_out[f"away_{key}"] = side_counts[2][key]
        rows.append(row_out)

    if not rows:
        return pd.DataFrame(columns=out_cols)
    return pd.DataFrame(rows, columns=out_cols)


def apply_recomputed_gamestate(rows: pd.DataFrame, recomputed_df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(rows, pd.DataFrame) or rows.empty or recomputed_df.empty:
        return rows
    if "match_id" not in rows.columns or "match_id" not in recomputed_df.columns:
        return rows

    out = rows.copy()
    out["match_id"] = pd.to_numeric(out["match_id"], errors="coerce")
    recomputed = recomputed_df.copy()
    recomputed["match_id"] = pd.to_numeric(recomputed["match_id"], errors="coerce")
    out = out.merge(recomputed, on="match_id", how="left", suffixes=("", "_recomputed"))

    venue_norm = out.get("venue", pd.Series(index=out.index, dtype=object)).astype(str).str.lower()
    is_home_row = venue_norm.eq("home")
    for key in app.GAMESTATE_EVENT_METRIC_KEYS:
        home_col = f"home_{key}"
        away_col = f"away_{key}"
        if home_col not in out.columns or away_col not in out.columns:
            continue
        chosen_num = pd.to_numeric(out[home_col].where(is_home_row, out[away_col]), errors="coerce")
        if key in out.columns:
            existing_num = pd.to_numeric(out[key], errors="coerce")
        else:
            existing_num = pd.Series([pd.NA] * len(out), index=out.index, dtype="float64")
        out[key] = chosen_num.where(chosen_num.notna(), existing_num)

    if "home_goal_minutes" in out.columns and "away_goal_minutes" in out.columns:
        out["goal_minutes_for"] = out["home_goal_minutes"].where(is_home_row, out["away_goal_minutes"])
        out["goal_minutes_against"] = out["away_goal_minutes"].where(is_home_row, out["home_goal_minutes"])

    drop_cols = [f"home_{key}" for key in app.GAMESTATE_EVENT_METRIC_KEYS] + [
        f"away_{key}" for key in app.GAMESTATE_EVENT_METRIC_KEYS
    ]
    out = out.drop(columns=[col for col in drop_cols if col in out.columns])
    return out


def load_manual_team_mapping_lookup(path: Path, team_set: set[str]) -> dict[str, str]:
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

    lookup: dict[str, str] = {}
    for raw_name, sofa_name in items:
        raw_team = str(raw_name).strip()
        sofa_team = str(sofa_name).strip()
        if not raw_team or not sofa_team:
            continue
        if sofa_team not in team_set:
            continue
        norm = app.normalize_team_name(raw_team)
        if norm:
            lookup[norm] = sofa_team
    return lookup


def resolve_team_names(
    *,
    home_raw: str,
    away_raw: str,
    teams: list[str],
    mappings_path: str,
) -> tuple[str, str, dict[str, Any]]:
    team_set = set(teams)
    manual_lookup = load_manual_team_mapping_lookup(Path(mappings_path), team_set)

    home_input = str(home_raw).strip()
    away_input = str(away_raw).strip()

    def resolve_one(raw_name: str) -> tuple[str | None, str]:
        if not raw_name:
            return None, "missing"
        if raw_name in team_set:
            return raw_name, "exact_sofa"
        norm = app.normalize_team_name(raw_name)
        mapped = manual_lookup.get(norm) if norm else None
        if mapped and mapped in team_set:
            return mapped, "manual_mapping"
        return None, "unresolved"

    home_resolved, home_method = resolve_one(home_input)
    away_resolved, away_method = resolve_one(away_input)

    final_home = home_resolved
    final_away = away_resolved
    meta = {
        "home_input": home_input,
        "away_input": away_input,
        "home_resolved": final_home,
        "away_resolved": final_away,
        "home_method": home_method,
        "away_method": away_method,
        "team_mappings_path": str(Path(mappings_path).resolve()),
    }
    return (final_home or ""), (final_away or ""), meta


def main() -> int:
    args = parse_args()
    sample_size = normalize_sample_size(args.sample_size)
    kickoff_ts = pd.to_datetime(args.kickoff, errors="coerce", utc=True)
    if pd.isna(kickoff_ts):
        raise ValueError(f"Could not parse --kickoff value: {args.kickoff!r}")

    form_df, _, teams, db_used = app.load_sofascore_inputs(args.db_path)
    default_mapping_path = app.source_specific_app_data_path(
        app.DEFAULT_MANUAL_TEAM_MAPPINGS,
        db_used,
    )
    selected_mapping_path = Path(
        str(args.team_mappings_path).strip() or str(default_mapping_path)
    ).expanduser().resolve()

    home_team, away_team, name_match_meta = resolve_team_names(
        home_raw=args.home,
        away_raw=args.away,
        teams=teams,
        mappings_path=str(selected_mapping_path),
    )
    if not home_team:
        raise RuntimeError(
            f"Could not resolve home team {args.home!r} via exact Sofa name or manual mapping file "
            f"{name_match_meta['team_mappings_path']}. Add mapping to that file and retry."
        )
    if not away_team:
        raise RuntimeError(
            f"Could not resolve away team {args.away!r} via exact Sofa name or manual mapping file "
            f"{name_match_meta['team_mappings_path']}. Add mapping to that file and retry."
        )

    game_row: dict[str, Any] = {
        "home": home_team,
        "away": away_team,
        "match_date": kickoff_ts,
    }
    if args.season_id is not None:
        game_row["season_id"] = args.season_id
    if args.competition_name:
        game_row["competition_name"] = args.competition_name
    if args.area_name:
        game_row["area_name"] = args.area_name

    _, source_games = wd.calc_wyscout_form_tables(
        pd.DataFrame([game_row]),
        form_df,
        periods=("Season", 5, 3),
        return_source_games=True,
        min_games=max(1, int(args.min_games)),
    )
    if not source_games:
        raise RuntimeError(
            "No source games returned. Check team names or add --season-id/--competition-name/--area-name filters."
        )

    source = source_games[0]
    home_rows = select_recent_rows(source.get("home_source_games"), sample_size=sample_size)
    away_rows = select_recent_rows(source.get("away_source_games"), sample_size=sample_size)
    warning = str(source.get("warning") or "").strip()
    source_mode = "model_source_games"

    # Same fallback pattern as the app page: use team venue rows when source rows are empty.
    if home_rows.empty:
        home_venue_rows = app.build_team_venue_recent_rows(
            form_df=form_df,
            team_name=home_team,
            kickoff_time=kickoff_ts,
            recent_n=None,
            season_id=args.season_id,
            competition_name=args.competition_name,
            area_name=args.area_name,
        )
        home_rows = select_recent_rows(app.merge_recent_rows_by_venue(home_venue_rows), sample_size=sample_size)
        if not home_rows.empty:
            source_mode = "venue_fallback"

    if away_rows.empty:
        away_venue_rows = app.build_team_venue_recent_rows(
            form_df=form_df,
            team_name=away_team,
            kickoff_time=kickoff_ts,
            recent_n=None,
            season_id=args.season_id,
            competition_name=args.competition_name,
            area_name=args.area_name,
        )
        away_rows = select_recent_rows(app.merge_recent_rows_by_venue(away_venue_rows), sample_size=sample_size)
        if not away_rows.empty:
            source_mode = "venue_fallback"

    match_ids = collect_match_ids(home_rows, away_rows)
    recomputed_gamestate_df = recompute_gamestate_from_shot_goals(Path(db_used), match_ids)
    home_rows = apply_recomputed_gamestate(home_rows, recomputed_gamestate_df)
    away_rows = apply_recomputed_gamestate(away_rows, recomputed_gamestate_df)

    output_rows = build_team_rows(home_team, home_rows) + build_team_rows(away_team, away_rows)
    home_games_used_df = build_games_used_table(home_rows)
    away_games_used_df = build_games_used_table(away_rows)

    if args.json:
        payload = {
            "db_path": str(db_used),
            "home": args.home,
            "away": args.away,
            "home_resolved": home_team,
            "away_resolved": away_team,
            "team_match": name_match_meta,
            "kickoff": kickoff_ts.isoformat(),
            "sample_size": sample_size,
            "home_games_used": int(len(home_rows)),
            "away_games_used": int(len(away_rows)),
            "source_mode": source_mode,
            "goal_timeline_source": "match_shots.time_minute",
            "warning": warning or None,
            "rows": output_rows,
            "home_games_table": home_games_used_df.to_dict(orient="records"),
            "away_games_table": away_games_used_df.to_dict(orient="records"),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    table_df = pd.DataFrame(output_rows, columns=["team", "metric", "D", "W", "L"])
    print(f"DB: {db_used}")
    print(f"Fixture: {args.home} vs {args.away}")
    print(f"Resolved teams: {home_team} ({name_match_meta['home_method']}) vs {away_team} ({name_match_meta['away_method']})")
    print(f"Kickoff: {kickoff_ts.isoformat()}")
    print(f"Source matches used: home={len(home_rows)} away={len(away_rows)}")
    print(f"Source mode: {source_mode}")
    if sample_size is not None:
        print(f"Sample size limit: last {sample_size} matches per team")
    if warning:
        print(f"Model warning: {warning}")
    if home_games_used_df.empty or away_games_used_df.empty:
        print("Warning: one or both teams have zero source rows after matching/fallback.")
    print()
    if not args.games_only:
        print("Gamestate totals table")
        print(table_df.to_string(index=False))
        print()

    print(f"Source games used for {args.home}")
    print(home_games_used_df.to_string(index=False))
    print()
    print(f"Source games used for {args.away}")
    print(away_games_used_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
