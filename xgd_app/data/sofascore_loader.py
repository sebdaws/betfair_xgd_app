"""SofaScore input loading and gamestate feature extraction."""

from __future__ import annotations

import re
import sqlite3
import sys
from pathlib import Path
from typing import Any

import pandas as pd

APP_DIR = Path(__file__).resolve().parents[2]
WORKSPACE_DIR = APP_DIR.parent

FINISHED_STATUSES = {
    "ended",
    "after penalties",
    "after extra time",
    "aet",
    "penalties",
    "ft",
}

REQUIRED_SOFASCORE_TABLES = {
    "matches",
    "teams",
    "competitions",
    "seasons",
    "match_stats",
    "match_shots",
    "match_events",
    "stat_definitions",
}

GAMESTATES = ("drawing", "winning", "losing")
GAMESTATE_EVENT_METRIC_KEYS = tuple(
    f"{metric}_{direction}_{state}"
    for metric in ("corners", "cards")
    for direction in ("for", "against")
    for state in GAMESTATES
)
GAMESTATE_TIME_KEYS = tuple(f"minutes_{state}" for state in GAMESTATES)
GAMESTATE_ALL_KEYS = GAMESTATE_EVENT_METRIC_KEYS + GAMESTATE_TIME_KEYS


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

def parse_season_date(value: Any, is_end: bool) -> pd.Timestamp | pd.NaT:
    if value is None:
        return pd.NaT
    text = str(value).strip()
    if not text:
        return pd.NaT
    ts = parse_iso_utc(text)
    if pd.isna(ts):
        return pd.NaT
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) and is_end:
        return ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    return ts


def parse_score_after(value: Any) -> tuple[int, int] | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    if not text:
        return None
    match = re.match(r"^(\d+)\s*[-:]\s*(\d+)$", text)
    if not match:
        return None
    try:
        return int(match.group(1)), int(match.group(2))
    except Exception:
        return None


def gamestate_for_perspective(home_score: int, away_score: int, perspective_side: int) -> str:
    goals_for, goals_against = (home_score, away_score) if perspective_side == 1 else (away_score, home_score)
    if goals_for > goals_against:
        return "winning"
    if goals_for < goals_against:
        return "losing"
    return "drawing"


def build_match_gamestate_event_counts(
    events_df: pd.DataFrame,
    goal_shots_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out_cols = ["match_id"] + [f"home_{key}" for key in GAMESTATE_ALL_KEYS] + [
        f"away_{key}" for key in GAMESTATE_ALL_KEYS
    ]
    has_events = isinstance(events_df, pd.DataFrame) and not events_df.empty
    has_goal_shots = isinstance(goal_shots_df, pd.DataFrame) and not goal_shots_df.empty
    if not has_events and not has_goal_shots:
        return pd.DataFrame(columns=out_cols)

    work = events_df.copy() if isinstance(events_df, pd.DataFrame) else pd.DataFrame()
    expected_event_cols = [
        "id",
        "match_id",
        "event_type",
        "card_type",
        "goal_type",
        "score_after",
        "minute",
        "added_time",
        "team_side_code",
    ]
    for col in expected_event_cols:
        if col not in work.columns:
            work[col] = pd.NA

    if has_goal_shots:
        work_event_type_norm = work["event_type"].fillna("").astype(str).str.strip().str.lower()
        match_ids_with_event_goals: set[int] = set(
            pd.to_numeric(
                work.loc[work_event_type_norm == "goal", "match_id"],
                errors="coerce",
            )
            .dropna()
            .astype(int)
            .tolist()
        )

        goal_work = goal_shots_df.copy()
        if "team_side_code" not in goal_work.columns and "is_home" in goal_work.columns:
            is_home_num = pd.to_numeric(goal_work.get("is_home"), errors="coerce")
            goal_work["team_side_code"] = is_home_num.map(
                lambda v: 1 if pd.notna(v) and int(v) == 1 else (2 if pd.notna(v) and int(v) == 0 else pd.NA)
            )

        goal_work["match_id_int"] = pd.to_numeric(goal_work.get("match_id"), errors="coerce")
        goal_work["minute_int"] = pd.to_numeric(goal_work.get("minute"), errors="coerce")
        goal_work["side_int"] = pd.to_numeric(goal_work.get("team_side_code"), errors="coerce")
        goal_work["id_int"] = pd.to_numeric(goal_work.get("id"), errors="coerce")
        goal_work = goal_work[
            goal_work["match_id_int"].notna()
            & goal_work["minute_int"].notna()
            & goal_work["side_int"].isin([1, 2])
        ].copy()
        if not goal_work.empty:
            goal_work["match_id_int"] = goal_work["match_id_int"].astype(int)
            goal_work["side_int"] = goal_work["side_int"].astype(int)
            if match_ids_with_event_goals:
                goal_work = goal_work[~goal_work["match_id_int"].isin(match_ids_with_event_goals)].copy()
        if not goal_work.empty:
            fallback_ids = pd.Series(range(-1, -len(goal_work) - 1, -1), index=goal_work.index, dtype="int64")
            shot_ids = goal_work["id_int"].where(goal_work["id_int"].notna(), fallback_ids)
            shot_goal_events = pd.DataFrame(
                {
                    "id": shot_ids,
                    "match_id": goal_work["match_id_int"],
                    "event_type": "goal",
                    "card_type": pd.NA,
                    "goal_type": pd.NA,
                    "score_after": pd.NA,
                    "minute": goal_work["minute_int"],
                    "added_time": 0,
                    "team_side_code": goal_work["side_int"],
                }
            )
            work = pd.concat([work, shot_goal_events], ignore_index=True, sort=False)

    work["event_type_norm"] = work["event_type"].fillna("").astype(str).str.strip().str.lower()
    work = work[work["event_type_norm"].isin({"goal", "corner", "card"})].copy()
    if work.empty:
        return pd.DataFrame(columns=out_cols)

    work["card_type_norm"] = work.get("card_type", pd.Series(index=work.index, dtype=object)).fillna("").astype(str).str.strip().str.lower()
    work["goal_type_norm"] = work.get("goal_type", pd.Series(index=work.index, dtype=object)).fillna("").astype(str).str.strip().str.lower()
    work["minute_int"] = pd.to_numeric(work.get("minute"), errors="coerce")
    work["added_int"] = pd.to_numeric(work.get("added_time"), errors="coerce").fillna(0)
    work["side_int"] = pd.to_numeric(work.get("team_side_code"), errors="coerce")
    work["event_id_sort"] = pd.to_numeric(work.get("id"), errors="coerce")
    fallback_sort = pd.Series(range(len(work)), index=work.index, dtype="float64")
    work["event_id_sort"] = work["event_id_sort"].where(work["event_id_sort"].notna(), fallback_sort)
    work["event_sort_priority"] = work["event_type_norm"].map(lambda event_type: 0 if event_type == "goal" else 1)

    rows: list[dict[str, Any]] = []
    for match_id, match_events in work.groupby("match_id", sort=False):
        if pd.isna(match_id):
            continue

        score_home = 0
        score_away = 0
        side_counts: dict[int, dict[str, int]] = {
            1: {key: 0 for key in GAMESTATE_EVENT_METRIC_KEYS},
            2: {key: 0 for key in GAMESTATE_EVENT_METRIC_KEYS},
        }
        side_minutes: dict[int, dict[str, float]] = {
            1: {state: 0.0 for state in GAMESTATES},
            2: {state: 0.0 for state in GAMESTATES},
        }
        last_goal_time = 0.0

        ordered = match_events.sort_values(
            ["minute_int", "added_int", "event_sort_priority", "event_id_sort"],
            na_position="last",
        )
        for _, event in ordered.iterrows():
            event_type = str(event.get("event_type_norm", "")).strip().lower()
            side_raw = event.get("side_int")
            side = int(side_raw) if pd.notna(side_raw) and int(side_raw) in (1, 2) else None
            minute_known = pd.notna(event.get("minute_int"))

            if event_type in {"corner", "card"} and side in (1, 2) and minute_known:
                metric_prefix = "corners" if event_type == "corner" else "cards"
                for perspective_side in (1, 2):
                    gamestate = gamestate_for_perspective(score_home, score_away, perspective_side)
                    direction = "for" if side == perspective_side else "against"
                    metric_key = f"{metric_prefix}_{direction}_{gamestate}"
                    side_counts[perspective_side][metric_key] += 1

            if event_type == "goal":
                if minute_known:
                    event_time = float(event.get("minute_int") or 0) + float(event.get("added_int") or 0)
                    if event_time < last_goal_time:
                        event_time = last_goal_time
                    elapsed = max(0.0, event_time - last_goal_time)
                    if elapsed > 0:
                        state_home = gamestate_for_perspective(score_home, score_away, 1)
                        state_away = gamestate_for_perspective(score_home, score_away, 2)
                        side_minutes[1][state_home] += elapsed
                        side_minutes[2][state_away] += elapsed
                    last_goal_time = event_time

                score_after = parse_score_after(event.get("score_after"))
                if score_after is not None:
                    score_home, score_away = score_after
                elif side in (1, 2):
                    scoring_side = side
                    if str(event.get("goal_type_norm", "")).strip().lower() == "own-goal":
                        scoring_side = 1 if side == 2 else 2
                    if scoring_side == 1:
                        score_home += 1
                    else:
                        score_away += 1

        match_times = (
            ordered.loc[ordered["minute_int"].notna(), "minute_int"].astype(float)
            + ordered.loc[ordered["minute_int"].notna(), "added_int"].astype(float)
        )
        max_timeline_time = float(match_times.max()) if not match_times.empty else 90.0
        end_time = max(90.0, max_timeline_time, last_goal_time)
        elapsed_tail = max(0.0, end_time - last_goal_time)
        if elapsed_tail > 0:
            state_home = gamestate_for_perspective(score_home, score_away, 1)
            state_away = gamestate_for_perspective(score_home, score_away, 2)
            side_minutes[1][state_home] += elapsed_tail
            side_minutes[2][state_away] += elapsed_tail

        row: dict[str, Any] = {"match_id": match_id}
        for key in GAMESTATE_EVENT_METRIC_KEYS:
            row[f"home_{key}"] = side_counts[1][key]
            row[f"away_{key}"] = side_counts[2][key]
        for state in GAMESTATES:
            row[f"home_minutes_{state}"] = side_minutes[1][state]
            row[f"away_minutes_{state}"] = side_minutes[2][state]
        rows.append(row)

    if not rows:
        return pd.DataFrame(columns=out_cols)
    return pd.DataFrame(rows, columns=out_cols)


def _sqlite_table_names(path: Path) -> set[str]:
    conn = sqlite3.connect(str(path))
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    finally:
        conn.close()
    return {str(row[0]) for row in rows if row and row[0]}


def resolve_sofascore_db_path(db_path: str) -> Path:
    requested = Path(db_path).expanduser().resolve()
    fallback = (WORKSPACE_DIR / "Sofascore_scraper" / "sofascore_local.db").resolve()

    candidates: list[Path] = []
    for candidate in (requested, fallback):
        if candidate not in candidates:
            candidates.append(candidate)

    checked: list[tuple[Path, set[str] | None]] = []
    selected_path: Path | None = None
    for candidate in candidates:
        if not candidate.exists():
            checked.append((candidate, None))
            continue
        try:
            tables = _sqlite_table_names(candidate)
        except Exception:
            checked.append((candidate, None))
            continue
        checked.append((candidate, tables))
        if REQUIRED_SOFASCORE_TABLES.issubset(tables):
            selected_path = candidate
            break

    if selected_path is not None:
        if selected_path != requested:
            requested_status = "not found"
            requested_tables = next((tables for path, tables in checked if path == requested), None)
            if requested.exists():
                if requested_tables is None:
                    requested_status = "unreadable"
                else:
                    missing = sorted(REQUIRED_SOFASCORE_TABLES - requested_tables)
                    requested_status = (
                        "valid"
                        if not missing
                        else f"missing tables: {', '.join(missing)}"
                    )
            print(
                "Warning: using fallback SofaScore DB path "
                f"{selected_path} (requested {requested}: {requested_status}).",
                file=sys.stderr,
            )
        return selected_path

    detail_lines: list[str] = []
    for candidate, tables in checked:
        if tables is None:
            if candidate.exists():
                detail_lines.append(f"- {candidate}: unreadable or invalid sqlite file")
            else:
                detail_lines.append(f"- {candidate}: not found")
            continue
        missing = sorted(REQUIRED_SOFASCORE_TABLES - tables)
        detail_lines.append(f"- {candidate}: missing tables: {', '.join(missing)}")

    details = "\n".join(detail_lines)
    raise RuntimeError(
        "SofaScore DB is missing required schema (expected tables like 'matches'). "
        "Set --db-path to a valid sofascore_local.db.\n"
        f"Checked:\n{details}"
    )


def load_sofascore_inputs(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame, list[str], Path]:
    path = resolve_sofascore_db_path(db_path)

    finished_sql = """
    WITH xg_stats AS (
        SELECT
            ms.match_id,
            MAX(COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))) AS home_xg_stat,
            MAX(COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))) AS away_xg_stat
        FROM match_stats ms
        JOIN stat_definitions sd
            ON sd.id = ms.stat_definition_id
        WHERE LOWER(COALESCE(sd.metric, '')) = 'expected goals'
          AND UPPER(COALESCE(ms.period, '')) = 'ALL'
        GROUP BY ms.match_id
    ),
    shot_aggs AS (
        SELECT
            match_id,
            SUM(CASE WHEN is_home = 1 THEN COALESCE(CAST(NULLIF(xg, '') AS REAL), 0) ELSE 0 END) AS home_xg_shots,
            SUM(CASE WHEN is_home = 0 THEN COALESCE(CAST(NULLIF(xg, '') AS REAL), 0) ELSE 0 END) AS away_xg_shots,
            SUM(CASE WHEN is_home = 1 THEN COALESCE(CAST(NULLIF(xgot, '') AS REAL), 0) ELSE 0 END) AS home_xgot_shots,
            SUM(CASE WHEN is_home = 0 THEN COALESCE(CAST(NULLIF(xgot, '') AS REAL), 0) ELSE 0 END) AS away_xgot_shots
        FROM match_shots
        GROUP BY match_id
    ),
    keeper_aggs AS (
        SELECT
            ms.match_id,
            MAX(COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))) AS home_goalkeeper_goals_prevented,
            MAX(COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))) AS away_goalkeeper_goals_prevented
        FROM match_stats ms
        JOIN stat_definitions sd
            ON sd.id = ms.stat_definition_id
        WHERE LOWER(COALESCE(sd.metric, '')) = 'goals prevented'
          AND UPPER(COALESCE(ms.period, '')) = 'ALL'
        GROUP BY ms.match_id
    ),
    card_corner_aggs AS (
        SELECT
            ms.match_id,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(sd.metric, '')) LIKE '%corner%'
                    THEN COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))
                END
            ) AS home_corners,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(sd.metric, '')) LIKE '%corner%'
                    THEN COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))
                END
            ) AS away_corners,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(sd.metric, '')) LIKE '%yellow%' AND LOWER(COALESCE(sd.metric, '')) LIKE '%card%'
                    THEN COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))
                END
            ) AS home_yellow_cards,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(sd.metric, '')) LIKE '%yellow%' AND LOWER(COALESCE(sd.metric, '')) LIKE '%card%'
                    THEN COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))
                END
            ) AS away_yellow_cards,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(sd.metric, '')) LIKE '%red%' AND LOWER(COALESCE(sd.metric, '')) LIKE '%card%'
                    THEN COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))
                END
            ) AS home_red_cards,
            MAX(
                CASE
                    WHEN LOWER(COALESCE(sd.metric, '')) LIKE '%red%' AND LOWER(COALESCE(sd.metric, '')) LIKE '%card%'
                    THEN COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))
                END
            ) AS away_red_cards
        FROM match_stats ms
        JOIN stat_definitions sd
            ON sd.id = ms.stat_definition_id
        WHERE UPPER(COALESCE(ms.period, '')) = 'ALL'
        GROUP BY ms.match_id
    ),
    event_card_aggs AS (
        SELECT
            me.match_id,
            SUM(
                CASE
                    WHEN LOWER(COALESCE(me.event_type, '')) = 'card'
                     AND LOWER(COALESCE(me.card_type, '')) = 'second-yellow'
                     AND me.team_side_code = 1
                    THEN 1 ELSE 0
                END
            ) AS home_second_yellows,
            SUM(
                CASE
                    WHEN LOWER(COALESCE(me.event_type, '')) = 'card'
                     AND LOWER(COALESCE(me.card_type, '')) = 'second-yellow'
                     AND me.team_side_code = 2
                    THEN 1 ELSE 0
                END
            ) AS away_second_yellows,
            SUM(
                CASE
                    WHEN LOWER(COALESCE(me.event_type, '')) = 'card'
                     AND LOWER(COALESCE(me.card_type, '')) = 'red'
                     AND me.team_side_code = 1
                    THEN 1 ELSE 0
                END
            ) AS home_straight_red_events,
            SUM(
                CASE
                    WHEN LOWER(COALESCE(me.event_type, '')) = 'card'
                     AND LOWER(COALESCE(me.card_type, '')) = 'red'
                     AND me.team_side_code = 2
                    THEN 1 ELSE 0
                END
            ) AS away_straight_red_events
        FROM match_events me
        GROUP BY me.match_id
    )
    SELECT
        m.id AS match_id,
        m.season_id,
        m.kickoff_ts AS date_time,
        m.status AS match_status,
        c.name AS competition_name,
        c.country AS area_name,
        s.name AS season_name,
        s.start_date AS season_start_date,
        s.end_date AS season_end_date,
        ht.name AS home_team_name,
        at.name AS away_team_name,
        COALESCE(m.home_ft_score, m.home_score_final) AS home_goals,
        COALESCE(m.away_ft_score, m.away_score_final) AS away_goals,
        xg.home_xg_stat,
        xg.away_xg_stat,
        sa.home_xg_shots,
        sa.away_xg_shots,
        sa.home_xgot_shots,
        sa.away_xgot_shots,
        ka.home_goalkeeper_goals_prevented,
        ka.away_goalkeeper_goals_prevented,
        cca.home_corners,
        cca.away_corners,
        cca.home_yellow_cards,
        cca.away_yellow_cards,
        cca.home_red_cards,
        cca.away_red_cards,
        eca.home_second_yellows,
        eca.away_second_yellows,
        eca.home_straight_red_events,
        eca.away_straight_red_events
    FROM matches m
    JOIN teams ht ON ht.id = m.home_team_id
    JOIN teams at ON at.id = m.away_team_id
    LEFT JOIN competitions c ON c.id = m.competition_id
    LEFT JOIN seasons s ON s.id = m.season_id
    LEFT JOIN xg_stats xg ON xg.match_id = m.id
    LEFT JOIN shot_aggs sa ON sa.match_id = m.id
    LEFT JOIN keeper_aggs ka ON ka.match_id = m.id
    LEFT JOIN card_corner_aggs cca ON cca.match_id = m.id
    LEFT JOIN event_card_aggs eca ON eca.match_id = m.id
    WHERE m.kickoff_ts IS NOT NULL
      AND COALESCE(m.home_ft_score, m.home_score_final) IS NOT NULL
      AND COALESCE(m.away_ft_score, m.away_score_final) IS NOT NULL
    """

    fixtures_sql = """
    SELECT
        m.id AS match_id,
        m.season_id,
        m.kickoff_ts AS date_time,
        c.name AS competition_name,
        c.country AS area_name,
        ht.name AS home_team_name,
        at.name AS away_team_name
    FROM matches m
    JOIN teams ht ON ht.id = m.home_team_id
    JOIN teams at ON at.id = m.away_team_id
    LEFT JOIN competitions c ON c.id = m.competition_id
    WHERE LOWER(TRIM(COALESCE(m.status, ''))) = 'not started'
      AND m.kickoff_ts IS NOT NULL
    """

    conn = sqlite3.connect(str(path))
    try:
        raw_finished = pd.read_sql_query(finished_sql, conn)
        fixtures_df = pd.read_sql_query(fixtures_sql, conn)
        match_events_df = pd.read_sql_query(
            """
            SELECT
                id,
                match_id,
                event_type,
                card_type,
                goal_type,
                score_after,
                minute,
                added_time,
                team_side_code
            FROM match_events
            WHERE LOWER(COALESCE(event_type, '')) IN ('goal', 'corner', 'card')
            """,
            conn,
        )
        goal_shots_df = pd.read_sql_query(
            """
            SELECT
                id,
                match_id,
                time_minute AS minute,
                CASE
                    WHEN is_home = 1 THEN 1
                    WHEN is_home = 0 THEN 2
                    ELSE NULL
                END AS team_side_code
            FROM match_shots
            WHERE LOWER(TRIM(COALESCE(shot_type, ''))) = 'goal'
            """,
            conn,
        )
        teams_df = pd.read_sql_query(
            """
            SELECT DISTINCT t.name
            FROM teams t
            JOIN matches m ON m.home_team_id = t.id OR m.away_team_id = t.id
            WHERE t.name IS NOT NULL AND TRIM(t.name) <> ''
            ORDER BY t.name
            """,
            conn,
        )
    finally:
        conn.close()

    raw_finished["status_norm"] = raw_finished["match_status"].fillna("").astype(str).str.strip().str.lower()
    raw_finished = raw_finished[raw_finished["status_norm"].isin(FINISHED_STATUSES)].copy()
    raw_finished["date_time"] = pd.to_datetime(raw_finished["date_time"], errors="coerce", utc=True)
    raw_finished["season_start_date"] = raw_finished["season_start_date"].apply(lambda v: parse_season_date(v, False))
    raw_finished["season_end_date"] = raw_finished["season_end_date"].apply(lambda v: parse_season_date(v, True))

    numeric_cols = [
        "home_goals",
        "away_goals",
        "home_xg_stat",
        "away_xg_stat",
        "home_xg_shots",
        "away_xg_shots",
        "home_xgot_shots",
        "away_xgot_shots",
        "home_goalkeeper_goals_prevented",
        "away_goalkeeper_goals_prevented",
        "home_corners",
        "away_corners",
        "home_yellow_cards",
        "away_yellow_cards",
        "home_red_cards",
        "away_red_cards",
        "home_second_yellows",
        "away_second_yellows",
        "home_straight_red_events",
        "away_straight_red_events",
    ]
    for col in numeric_cols:
        if col in raw_finished.columns:
            raw_finished[col] = pd.to_numeric(raw_finished[col], errors="coerce")

    gamestate_counts_df = build_match_gamestate_event_counts(match_events_df, goal_shots_df=goal_shots_df)
    if not gamestate_counts_df.empty and "match_id" in raw_finished.columns:
        raw_finished = raw_finished.merge(gamestate_counts_df, on="match_id", how="left")
        gamestate_numeric_cols = [col for col in gamestate_counts_df.columns if col != "match_id"]
        for col in gamestate_numeric_cols:
            if col in raw_finished.columns:
                raw_finished[col] = pd.to_numeric(raw_finished[col], errors="coerce").fillna(0.0)

    raw_finished["home_xg"] = raw_finished["home_xg_stat"].where(raw_finished["home_xg_stat"].notna(), raw_finished["home_xg_shots"])
    raw_finished["away_xg"] = raw_finished["away_xg_stat"].where(raw_finished["away_xg_stat"].notna(), raw_finished["away_xg_shots"])
    raw_finished["home_xgot_keeper"] = raw_finished["home_goals"] + raw_finished["away_goalkeeper_goals_prevented"]
    raw_finished["away_xgot_keeper"] = raw_finished["away_goals"] + raw_finished["home_goalkeeper_goals_prevented"]

    # Prefer xGoT derived from goals + opponent goalkeeper goals prevented.
    raw_finished["home_xgot"] = raw_finished["home_xgot_keeper"]
    raw_finished.loc[raw_finished["home_xgot"].isna(), "home_xgot"] = raw_finished["home_xgot_shots"]
    raw_finished.loc[raw_finished["home_xgot"].isna(), "home_xgot"] = raw_finished["home_xg"]

    raw_finished["away_xgot"] = raw_finished["away_xgot_keeper"]
    raw_finished.loc[raw_finished["away_xgot"].isna(), "away_xgot"] = raw_finished["away_xgot_shots"]
    raw_finished.loc[raw_finished["away_xgot"].isna(), "away_xgot"] = raw_finished["away_xg"]

    raw_finished["home_cards"] = raw_finished["home_yellow_cards"].fillna(0) + (2 * raw_finished["home_red_cards"].fillna(0))
    raw_finished["away_cards"] = raw_finished["away_yellow_cards"].fillna(0) + (2 * raw_finished["away_red_cards"].fillna(0))
    home_second_yellows = raw_finished["home_second_yellows"].fillna(0)
    away_second_yellows = raw_finished["away_second_yellows"].fillna(0)
    home_red_includes_second_yellow = (
        (home_second_yellows > 0)
        & (
            raw_finished["home_red_cards"].fillna(0)
            >= (raw_finished["home_straight_red_events"].fillna(0) + home_second_yellows)
        )
    )
    away_red_includes_second_yellow = (
        (away_second_yellows > 0)
        & (
            raw_finished["away_red_cards"].fillna(0)
            >= (raw_finished["away_straight_red_events"].fillna(0) + away_second_yellows)
        )
    )
    # SofaScore can record a second-yellow as both yellow and red in aggregate stats.
    # When that pattern is detected, subtract one point per second-yellow to avoid double-counting.
    raw_finished.loc[home_red_includes_second_yellow, "home_cards"] = (
        raw_finished.loc[home_red_includes_second_yellow, "home_cards"]
        - home_second_yellows.loc[home_red_includes_second_yellow]
    )
    raw_finished.loc[away_red_includes_second_yellow, "away_cards"] = (
        raw_finished.loc[away_red_includes_second_yellow, "away_cards"]
        - away_second_yellows.loc[away_red_includes_second_yellow]
    )
    raw_finished.loc[
        raw_finished["home_yellow_cards"].isna() & raw_finished["home_red_cards"].isna(),
        "home_cards",
    ] = pd.NA
    raw_finished.loc[
        raw_finished["away_yellow_cards"].isna() & raw_finished["away_red_cards"].isna(),
        "away_cards",
    ] = pd.NA

    gamestate_match_cols = [f"home_{key}" for key in GAMESTATE_ALL_KEYS] + [
        f"away_{key}" for key in GAMESTATE_ALL_KEYS
    ]
    for col in gamestate_match_cols:
        if col not in raw_finished.columns:
            raw_finished[col] = 0.0

    finished = raw_finished[
        [
            "match_id",
            "season_id",
            "competition_name",
            "area_name",
            "season_name",
            "season_start_date",
            "season_end_date",
            "date_time",
            "home_team_name",
            "away_team_name",
            "home_goals",
            "away_goals",
            "home_xg",
            "away_xg",
            "home_xgot",
            "away_xgot",
            "home_corners",
            "away_corners",
            "home_yellow_cards",
            "away_yellow_cards",
            "home_red_cards",
            "away_red_cards",
            "home_cards",
            "away_cards",
            *gamestate_match_cols,
        ]
    ].copy()
    finished = finished.dropna(subset=["date_time", "home_team_name", "away_team_name", "home_goals", "away_goals", "home_xg", "away_xg"])

    home_rows = pd.DataFrame(
        {
            "match_id": finished["match_id"],
            "team": finished["home_team_name"],
            "opponent": finished["away_team_name"],
            "venue": "Home",
            "date_time": finished["date_time"],
            "GF": finished["home_goals"],
            "GA": finished["away_goals"],
            "xG": finished["home_xg"],
            "xGA": finished["away_xg"],
            "xGoT": finished["home_xgot"].where(finished["home_xgot"].notna(), finished["home_xg"]),
            "xGoTA": finished["away_xgot"].where(finished["away_xgot"].notna(), finished["away_xg"]),
            "corners_for": finished["home_corners"],
            "corners_against": finished["away_corners"],
            "yellow_for": finished["home_yellow_cards"],
            "yellow_against": finished["away_yellow_cards"],
            "red_for": finished["home_red_cards"],
            "red_against": finished["away_red_cards"],
            "cards_for": finished["home_cards"],
            "cards_against": finished["away_cards"],
            **{key: finished[f"home_{key}"] for key in GAMESTATE_ALL_KEYS},
            "season_id": finished["season_id"],
            "competition_name": finished["competition_name"],
            "area_name": finished["area_name"],
            "season_name": finished["season_name"],
            "season_start_date": finished["season_start_date"],
            "season_end_date": finished["season_end_date"],
        }
    )
    away_rows = pd.DataFrame(
        {
            "match_id": finished["match_id"],
            "team": finished["away_team_name"],
            "opponent": finished["home_team_name"],
            "venue": "Away",
            "date_time": finished["date_time"],
            "GF": finished["away_goals"],
            "GA": finished["home_goals"],
            "xG": finished["away_xg"],
            "xGA": finished["home_xg"],
            "xGoT": finished["away_xgot"].where(finished["away_xgot"].notna(), finished["away_xg"]),
            "xGoTA": finished["home_xgot"].where(finished["home_xgot"].notna(), finished["home_xg"]),
            "corners_for": finished["away_corners"],
            "corners_against": finished["home_corners"],
            "yellow_for": finished["away_yellow_cards"],
            "yellow_against": finished["home_yellow_cards"],
            "red_for": finished["away_red_cards"],
            "red_against": finished["home_red_cards"],
            "cards_for": finished["away_cards"],
            "cards_against": finished["home_cards"],
            **{key: finished[f"away_{key}"] for key in GAMESTATE_ALL_KEYS},
            "season_id": finished["season_id"],
            "competition_name": finished["competition_name"],
            "area_name": finished["area_name"],
            "season_name": finished["season_name"],
            "season_start_date": finished["season_start_date"],
            "season_end_date": finished["season_end_date"],
        }
    )

    form_df = pd.concat([home_rows, away_rows], ignore_index=True)
    form_df = form_df.sort_values(["date_time", "match_id", "venue"], kind="mergesort").reset_index(drop=True)

    fixtures_df["date_time"] = pd.to_datetime(fixtures_df["date_time"], errors="coerce", utc=True)
    fixtures_df = fixtures_df.dropna(subset=["date_time", "home_team_name", "away_team_name"]).reset_index(drop=True)

    # Restrict matcher pool to teams that actually appear in SofaScore matches.
    teams = teams_df["name"].dropna().astype(str).str.strip()
    team_pool: set[str] = {name for name in teams if name}

    if not team_pool:
        # Fallback for unusual schemas where match-linked team extraction returns empty.
        if "team" in form_df.columns:
            team_pool.update(
                str(name).strip()
                for name in form_df["team"].dropna().tolist()
                if str(name).strip()
            )
        for col in ("home_team_name", "away_team_name"):
            if col in fixtures_df.columns:
                team_pool.update(
                    str(name).strip()
                    for name in fixtures_df[col].dropna().tolist()
                    if str(name).strip()
                )

    team_list = sorted(team_pool)
    return form_df, fixtures_df, team_list, path

__all__ = [
    "GAMESTATE_ALL_KEYS",
    "GAMESTATE_EVENT_METRIC_KEYS",
    "GAMESTATE_TIME_KEYS",
    "GAMESTATES",
    "_sqlite_table_names",
    "build_match_gamestate_event_counts",
    "gamestate_for_perspective",
    "load_sofascore_inputs",
    "parse_score_after",
    "parse_season_date",
    "resolve_sofascore_db_path",
]
