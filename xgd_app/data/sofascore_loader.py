"""SofaScore input loading and gamestate feature extraction."""

from __future__ import annotations

import re
import math
import json
import sqlite3
import unicodedata
from pathlib import Path
from typing import Any, Callable

import pandas as pd

FINISHED_STATUSES = {
    "ended",
    "after penalties",
    "after extra time",
    "aet",
    "ap",
    "penalties",
    "ft",
    "full-time",
    "full time",
    "finished",
}

NOT_STARTED_STATUSES = {
    "not started",
    "notstarted",
    "not_started",
    "not-started",
    "scheduled",
    "upcoming",
    "fixture",
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

REQUIRED_MATCH_EVENTS_TABLES = {
    "matches",
    "teams",
    "competitions",
    "match_events",
}

TEAM_TOKEN_REPLACEMENTS = {
    "utd": "united",
    "munchen": "munich",
    "st": "saint",
}

TEAM_STOP_WORDS = {
    "fc",
    "cf",
    "ac",
    "sc",
    "afc",
    "club",
    "de",
    "the",
}

GAMESTATES = ("drawing", "winning", "losing")
GAMESTATE_EVENT_METRIC_KEYS = tuple(
    f"{metric}_{direction}_{state}"
    for metric in ("corners", "cards", "shots", "shots_on_target", "xg")
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


def concat_event_frames_without_all_na_columns(frames: list[pd.DataFrame]) -> pd.DataFrame:
    concat_frames = [
        frame.dropna(axis=1, how="all")
        for frame in frames
        if isinstance(frame, pd.DataFrame) and not frame.empty
    ]
    if not concat_frames:
        return pd.DataFrame()
    if len(concat_frames) == 1:
        return concat_frames[0].copy()
    return pd.concat(concat_frames, ignore_index=True, sort=False)


def build_match_gamestate_event_counts(
    events_df: pd.DataFrame,
    goal_shots_df: pd.DataFrame | None = None,
    shots_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    out_cols = ["match_id"] + [f"home_{key}" for key in GAMESTATE_ALL_KEYS] + [
        f"away_{key}" for key in GAMESTATE_ALL_KEYS
    ]
    has_events = isinstance(events_df, pd.DataFrame) and not events_df.empty
    has_goal_shots = isinstance(goal_shots_df, pd.DataFrame) and not goal_shots_df.empty
    has_shots = isinstance(shots_df, pd.DataFrame) and not shots_df.empty
    if not has_events and not has_goal_shots and not has_shots:
        return pd.DataFrame(columns=out_cols)

    work = events_df.copy() if isinstance(events_df, pd.DataFrame) else pd.DataFrame()
    expected_event_cols = [
        "id",
        "match_id",
        "event_type",
        "card_type",
        "goal_type",
        "shot_type",
        "xg",
        "score_after",
        "minute",
        "added_time",
        "team_side_code",
    ]
    for col in expected_event_cols:
        if col not in work.columns:
            work[col] = pd.NA

    goal_source_df: pd.DataFrame | None = None
    if has_goal_shots:
        goal_source_df = goal_shots_df.copy()
    elif has_shots:
        goal_source_df = shots_df.copy()
        goal_shot_type = goal_source_df.get("shot_type", pd.Series(index=goal_source_df.index, dtype=object))
        goal_shot_type = goal_shot_type.fillna("").astype(str).str.strip().str.lower()
        goal_source_df = goal_source_df[goal_shot_type == "goal"].copy()

    if isinstance(goal_source_df, pd.DataFrame) and not goal_source_df.empty:
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

        goal_work = goal_source_df.copy()
        if "minute" not in goal_work.columns and "time_minute" in goal_work.columns:
            goal_work["minute"] = goal_work["time_minute"]
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
            if work.empty:
                work = shot_goal_events.copy()
            else:
                work = concat_event_frames_without_all_na_columns([work, shot_goal_events])

    if has_shots:
        shot_work = shots_df.copy()
        if "minute" not in shot_work.columns and "time_minute" in shot_work.columns:
            shot_work["minute"] = shot_work["time_minute"]
        if "team_side_code" not in shot_work.columns and "is_home" in shot_work.columns:
            is_home_num = pd.to_numeric(shot_work.get("is_home"), errors="coerce")
            shot_work["team_side_code"] = is_home_num.map(
                lambda v: 1 if pd.notna(v) and int(v) == 1 else (2 if pd.notna(v) and int(v) == 0 else pd.NA)
            )

        shot_work["match_id_int"] = pd.to_numeric(shot_work.get("match_id"), errors="coerce")
        shot_work["minute_int"] = pd.to_numeric(shot_work.get("minute"), errors="coerce")
        shot_work["side_int"] = pd.to_numeric(shot_work.get("team_side_code"), errors="coerce")
        shot_work["id_int"] = pd.to_numeric(shot_work.get("id"), errors="coerce")
        shot_work["xg_num"] = pd.to_numeric(shot_work.get("xg"), errors="coerce")
        shot_work["shot_type_norm"] = shot_work.get("shot_type", pd.Series(index=shot_work.index, dtype=object)).fillna(
            ""
        ).astype(str).str.strip().str.lower()
        shot_work = shot_work[
            shot_work["match_id_int"].notna()
            & shot_work["minute_int"].notna()
            & shot_work["side_int"].isin([1, 2])
        ].copy()
        if not shot_work.empty:
            shot_work["match_id_int"] = shot_work["match_id_int"].astype(int)
            shot_work["side_int"] = shot_work["side_int"].astype(int)
            fallback_ids = pd.Series(range(-1, -len(shot_work) - 1, -1), index=shot_work.index, dtype="int64")
            shot_ids = shot_work["id_int"].where(shot_work["id_int"].notna(), fallback_ids)
            shot_events = pd.DataFrame(
                {
                    "id": shot_ids,
                    "match_id": shot_work["match_id_int"],
                    "event_type": "shot",
                    "card_type": pd.NA,
                    "goal_type": pd.NA,
                    "shot_type": shot_work["shot_type_norm"],
                    "xg": shot_work["xg_num"],
                    "score_after": pd.NA,
                    "minute": shot_work["minute_int"],
                    "added_time": 0,
                    "team_side_code": shot_work["side_int"],
                }
            )
            if work.empty:
                work = shot_events.copy()
            else:
                work = concat_event_frames_without_all_na_columns([work, shot_events])

    work["event_type_norm"] = work["event_type"].fillna("").astype(str).str.strip().str.lower()
    work = work[work["event_type_norm"].isin({"goal", "corner", "card", "shot"})].copy()
    if work.empty:
        return pd.DataFrame(columns=out_cols)

    work["card_type_norm"] = work.get("card_type", pd.Series(index=work.index, dtype=object)).fillna("").astype(str).str.strip().str.lower()
    work["goal_type_norm"] = work.get("goal_type", pd.Series(index=work.index, dtype=object)).fillna("").astype(str).str.strip().str.lower()
    work["shot_type_norm"] = work.get("shot_type", pd.Series(index=work.index, dtype=object)).fillna("").astype(str).str.strip().str.lower()
    work["shot_xg_num"] = pd.to_numeric(work.get("xg"), errors="coerce")
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
        side_counts: dict[int, dict[str, float]] = {
            1: {key: 0.0 for key in GAMESTATE_EVENT_METRIC_KEYS},
            2: {key: 0.0 for key in GAMESTATE_EVENT_METRIC_KEYS},
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

            if event_type in {"corner", "card", "shot"} and side in (1, 2) and minute_known:
                if event_type == "corner":
                    metric_prefix = "corners"
                elif event_type == "card":
                    metric_prefix = "cards"
                else:
                    metric_prefix = "shots"
                shot_xg = float(event.get("shot_xg_num")) if pd.notna(event.get("shot_xg_num")) else 0.0
                for perspective_side in (1, 2):
                    gamestate = gamestate_for_perspective(score_home, score_away, perspective_side)
                    direction = "for" if side == perspective_side else "against"
                    metric_key = f"{metric_prefix}_{direction}_{gamestate}"
                    side_counts[perspective_side][metric_key] += 1
                    if event_type == "shot" and str(event.get("shot_type_norm", "")) in {"goal", "attempt saved"}:
                        on_target_key = f"shots_on_target_{direction}_{gamestate}"
                        side_counts[perspective_side][on_target_key] += 1
                    if event_type == "shot" and shot_xg > 0:
                        xg_key = f"xg_{direction}_{gamestate}"
                        side_counts[perspective_side][xg_key] += shot_xg

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
    requested_text = str(db_path or "").strip()
    if not requested_text:
        raise RuntimeError("A source DB path is required (--db-path).")

    requested = Path(requested_text).expanduser()
    requested = requested.resolve() if requested.is_absolute() else (Path.cwd() / requested).resolve()
    if not requested.exists():
        raise RuntimeError(
            f"Source DB not found: {requested}\n"
            "Set --db-path in app_data/launcher_config.json to a valid DB file."
        )

    try:
        tables = _sqlite_table_names(requested)
    except Exception as exc:
        raise RuntimeError(f"Source DB is unreadable: {requested} ({exc})") from exc

    missing = sorted(REQUIRED_SOFASCORE_TABLES - tables)
    if missing:
        raise RuntimeError(
            "Source DB is missing required schema "
            f"(missing tables: {', '.join(missing)}): {requested}"
        )
    return requested


def resolve_match_events_db_path(match_events_db_path: str | None, source_db_path: str | Path) -> Path:
    source = Path(source_db_path).expanduser()
    source = source.resolve() if source.is_absolute() else (Path.cwd() / source).resolve()
    requested_text = str(match_events_db_path or "").strip()
    if not requested_text:
        requested = source
    else:
        requested = Path(requested_text).expanduser()
        requested = requested.resolve() if requested.is_absolute() else (Path.cwd() / requested).resolve()

    if not requested.exists():
        raise RuntimeError(
            f"Match-events DB not found: {requested}\n"
            "Set --match-events-db-path in app_data/launcher_config.json to a valid DB file."
        )

    try:
        tables = _sqlite_table_names(requested)
    except Exception as exc:
        raise RuntimeError(f"Match-events DB is unreadable: {requested} ({exc})") from exc

    missing = sorted(REQUIRED_MATCH_EVENTS_TABLES - tables)
    if missing:
        raise RuntimeError(
            "Match-events DB is missing required schema "
            f"(missing tables: {', '.join(missing)}): {requested}"
        )
    return requested


def _normalize_team_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens: list[str] = []
    for token in text.split():
        token = TEAM_TOKEN_REPLACEMENTS.get(token, token)
        if token in TEAM_STOP_WORDS:
            continue
        tokens.append(token)
    return " ".join(tokens)


def _normalize_competition_key(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"\[[^\]]*\]", " ", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def _load_manual_mapping_pairs(path: str | Path | None) -> list[tuple[str, str]]:
    if path is None:
        return []
    candidate = Path(path).expanduser().resolve()
    if not candidate.exists():
        return []
    try:
        raw_data = json.loads(candidate.read_text(encoding="utf-8"))
    except Exception:
        return []

    if isinstance(raw_data, dict):
        return [
            (str(raw_name).strip(), str(source_name).strip())
            for raw_name, source_name in raw_data.items()
            if str(raw_name).strip() and str(source_name).strip()
        ]

    if isinstance(raw_data, list):
        pairs = []
        for row in raw_data:
            if not isinstance(row, dict):
                continue
            raw_name = str(row.get("raw_name", "")).strip()
            source_name = str(row.get("sofa_name", "")).strip()
            if raw_name and source_name:
                pairs.append((raw_name, source_name))
        return pairs

    return []


def _build_source_to_raw_lookup(
    mapping_path: str | Path | None,
    normalize_key: Callable[[Any], str],
) -> dict[str, str]:
    out: dict[str, str] = {}
    for raw_name, source_name in _load_manual_mapping_pairs(mapping_path):
        source_norm = normalize_key(source_name)
        raw_text = str(raw_name).strip()
        if not source_norm or not raw_text:
            continue
        out.setdefault(source_norm, raw_text)
    return out


def _canonical_from_source_name(
    source_name: Any,
    source_to_raw_lookup: dict[str, str],
    normalize_key: Callable[[Any], str],
) -> str:
    source_text = str(source_name or "").strip()
    if not source_text:
        return ""
    source_norm = normalize_key(source_text)
    if not source_norm:
        return ""
    mapped_raw = str(source_to_raw_lookup.get(source_norm, source_text)).strip()
    return normalize_key(mapped_raw)


def _to_utc_ts(value: Any) -> pd.Timestamp | pd.NaT:
    try:
        return pd.to_datetime(value, errors="coerce", utc=True)
    except Exception:
        return pd.NaT


def _build_cross_db_match_map(
    primary_matches: pd.DataFrame,
    event_matches: pd.DataFrame,
    *,
    primary_team_map: dict[str, str],
    primary_comp_map: dict[str, str],
    event_team_map: dict[str, str],
    event_comp_map: dict[str, str],
    max_delta_minutes: float = 24.0 * 60.0,
) -> pd.DataFrame:
    if (
        not isinstance(primary_matches, pd.DataFrame)
        or primary_matches.empty
        or not isinstance(event_matches, pd.DataFrame)
        or event_matches.empty
    ):
        return pd.DataFrame(columns=["primary_match_id", "event_match_id", "swap_sides", "delta_minutes"])

    primary = primary_matches.copy()
    events = event_matches.copy()

    primary["primary_match_id"] = pd.to_numeric(primary.get("match_id"), errors="coerce")
    primary["kickoff_ts"] = primary.get("date_time").map(_to_utc_ts)
    primary["home_key"] = primary.get("home_team_name").map(
        lambda value: _canonical_from_source_name(value, primary_team_map, _normalize_team_key)
    )
    primary["away_key"] = primary.get("away_team_name").map(
        lambda value: _canonical_from_source_name(value, primary_team_map, _normalize_team_key)
    )
    primary["comp_key"] = primary.get("competition_name").map(
        lambda value: _canonical_from_source_name(value, primary_comp_map, _normalize_competition_key)
    )
    primary = primary[
        primary["primary_match_id"].notna()
        & primary["kickoff_ts"].notna()
        & (primary["home_key"] != "")
        & (primary["away_key"] != "")
    ].copy()
    if primary.empty:
        return pd.DataFrame(columns=["primary_match_id", "event_match_id", "swap_sides", "delta_minutes"])

    events["event_match_id"] = pd.to_numeric(events.get("match_id"), errors="coerce")
    events["kickoff_ts"] = events.get("date_time").map(_to_utc_ts)
    events["home_key"] = events.get("home_team_name").map(
        lambda value: _canonical_from_source_name(value, event_team_map, _normalize_team_key)
    )
    events["away_key"] = events.get("away_team_name").map(
        lambda value: _canonical_from_source_name(value, event_team_map, _normalize_team_key)
    )
    events["comp_key"] = events.get("competition_name").map(
        lambda value: _canonical_from_source_name(value, event_comp_map, _normalize_competition_key)
    )
    events = events[
        events["event_match_id"].notna()
        & events["kickoff_ts"].notna()
        & (events["home_key"] != "")
        & (events["away_key"] != "")
    ].copy()
    if events.empty:
        return pd.DataFrame(columns=["primary_match_id", "event_match_id", "swap_sides", "delta_minutes"])

    event_records = events[
        ["event_match_id", "kickoff_ts", "comp_key", "home_key", "away_key"]
    ].to_dict(orient="records")
    by_comp_pair: dict[tuple[str, str, str], list[int]] = {}
    by_pair: dict[tuple[str, str], list[int]] = {}
    for idx, row in enumerate(event_records):
        comp_key = str(row.get("comp_key") or "")
        home_key = str(row.get("home_key") or "")
        away_key = str(row.get("away_key") or "")
        if not home_key or not away_key:
            continue
        by_pair.setdefault((home_key, away_key), []).append(idx)
        if comp_key:
            by_comp_pair.setdefault((comp_key, home_key, away_key), []).append(idx)

    primary_records = primary[
        ["primary_match_id", "kickoff_ts", "comp_key", "home_key", "away_key"]
    ].sort_values("kickoff_ts", kind="mergesort").to_dict(orient="records")

    used_event_indices: set[int] = set()
    rows: list[dict[str, Any]] = []
    for row in primary_records:
        primary_match_id = int(row["primary_match_id"])
        kickoff_ts = row["kickoff_ts"]
        comp_key = str(row.get("comp_key") or "")
        home_key = str(row.get("home_key") or "")
        away_key = str(row.get("away_key") or "")

        candidate_groups: list[tuple[list[int], bool]] = []
        if comp_key:
            candidate_groups.append((by_comp_pair.get((comp_key, home_key, away_key), []), False))
        candidate_groups.append((by_pair.get((home_key, away_key), []), False))
        if comp_key:
            candidate_groups.append((by_comp_pair.get((comp_key, away_key, home_key), []), True))
        candidate_groups.append((by_pair.get((away_key, home_key), []), True))

        best_idx: int | None = None
        best_swap = False
        best_delta_minutes: float | None = None
        for candidate_indices, swap_sides in candidate_groups:
            for candidate_idx in candidate_indices:
                if candidate_idx in used_event_indices:
                    continue
                candidate = event_records[candidate_idx]
                candidate_ts = candidate.get("kickoff_ts")
                if pd.isna(candidate_ts):
                    continue
                delta_minutes = abs((candidate_ts - kickoff_ts).total_seconds()) / 60.0
                if delta_minutes > float(max_delta_minutes):
                    continue
                if (best_delta_minutes is None) or (delta_minutes < best_delta_minutes):
                    best_idx = candidate_idx
                    best_swap = bool(swap_sides)
                    best_delta_minutes = float(delta_minutes)
            if best_idx is not None:
                break

        if best_idx is None:
            continue
        used_event_indices.add(best_idx)
        rows.append(
            {
                "primary_match_id": primary_match_id,
                "event_match_id": int(event_records[best_idx]["event_match_id"]),
                "swap_sides": bool(best_swap),
                "delta_minutes": float(best_delta_minutes or 0.0),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["primary_match_id", "event_match_id", "swap_sides", "delta_minutes"])
    return pd.DataFrame(rows)


def _load_cross_db_event_inputs(
    *,
    events_db_path: Path,
    primary_matches: pd.DataFrame,
    source_team_mappings_path: str | Path | None,
    source_competition_mappings_path: str | Path | None,
    match_events_team_mappings_path: str | Path | None,
    match_events_competition_mappings_path: str | Path | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    conn = sqlite3.connect(str(events_db_path))
    try:
        event_matches = pd.read_sql_query(
            """
            SELECT
                m.id AS match_id,
                m.kickoff_ts AS date_time,
                c.name AS competition_name,
                ht.name AS home_team_name,
                at.name AS away_team_name
            FROM matches m
            JOIN teams ht ON ht.id = m.home_team_id
            JOIN teams at ON at.id = m.away_team_id
            LEFT JOIN competitions c ON c.id = m.competition_id
            WHERE m.kickoff_ts IS NOT NULL
            """,
            conn,
        )
        event_rows = pd.read_sql_query(
            """
            SELECT
                me.id,
                me.match_id,
                me.event_type,
                me.card_type,
                me.goal_type,
                me.score_after,
                me.minute,
                me.added_time,
                me.team_side_code
            FROM match_events me
            WHERE LOWER(COALESCE(me.event_type, '')) IN ('goal', 'corner', 'card')
            """,
            conn,
        )
        tables = _sqlite_table_names(events_db_path)
        if "match_shots" in tables:
            shot_rows = pd.read_sql_query(
                """
                SELECT
                    ms.id,
                    ms.match_id,
                    ms.time_minute AS minute,
                    CASE
                        WHEN ms.is_home = 1 THEN 1
                        WHEN ms.is_home = 0 THEN 2
                        ELSE NULL
                    END AS team_side_code,
                    ms.shot_type,
                    ms.xg
                FROM match_shots ms
                """,
                conn,
            )
        else:
            shot_rows = pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code", "shot_type", "xg"])
    finally:
        conn.close()

    primary_team_lookup = _build_source_to_raw_lookup(source_team_mappings_path, _normalize_team_key)
    primary_comp_lookup = _build_source_to_raw_lookup(source_competition_mappings_path, _normalize_competition_key)
    events_team_lookup = _build_source_to_raw_lookup(match_events_team_mappings_path, _normalize_team_key)
    events_comp_lookup = _build_source_to_raw_lookup(match_events_competition_mappings_path, _normalize_competition_key)

    cross_map = _build_cross_db_match_map(
        primary_matches=primary_matches,
        event_matches=event_matches,
        primary_team_map=primary_team_lookup,
        primary_comp_map=primary_comp_lookup,
        event_team_map=events_team_lookup,
        event_comp_map=events_comp_lookup,
    )
    if cross_map.empty:
        return (
            pd.DataFrame(columns=["id", "match_id", "event_type", "card_type", "goal_type", "score_after", "minute", "added_time", "team_side_code"]),
            pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code"]),
            pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code", "shot_type", "xg"]),
        )

    primary_status = primary_matches[["match_id", "match_status"]].copy()
    primary_status["match_id"] = pd.to_numeric(primary_status["match_id"], errors="coerce")
    primary_status = primary_status.dropna(subset=["match_id"])
    primary_status["match_id"] = primary_status["match_id"].astype(int)
    status_lookup = dict(zip(primary_status["match_id"], primary_status["match_status"]))

    mapped_events = event_rows.merge(
        cross_map,
        left_on="match_id",
        right_on="event_match_id",
        how="inner",
    )
    if mapped_events.empty:
        return (
            pd.DataFrame(columns=["id", "match_id", "event_type", "card_type", "goal_type", "score_after", "minute", "added_time", "team_side_code"]),
            pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code"]),
            pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code", "shot_type", "xg"]),
        )
    mapped_events["match_id"] = pd.to_numeric(mapped_events["primary_match_id"], errors="coerce")
    mapped_events["team_side_code"] = pd.to_numeric(mapped_events["team_side_code"], errors="coerce")
    swap_mask = mapped_events["swap_sides"].astype(bool) & mapped_events["team_side_code"].isin([1, 2])
    if bool(swap_mask.any()):
        mapped_events.loc[swap_mask, "team_side_code"] = mapped_events.loc[swap_mask, "team_side_code"].map(
            lambda side: 1 if int(side) == 2 else 2
        )
    mapped_events = mapped_events.dropna(subset=["match_id"]).copy()
    mapped_events["match_id"] = mapped_events["match_id"].astype(int)
    mapped_events["match_status"] = mapped_events["match_id"].map(status_lookup)
    mapped_events["match_status_norm"] = mapped_events["match_status"].fillna("").astype(str).str.strip().str.upper()
    mapped_events["minute_num"] = pd.to_numeric(mapped_events["minute"], errors="coerce")
    is_aet = mapped_events["match_status_norm"].isin({"AET", "AP"})
    keep_mask = (~is_aet) | (mapped_events["minute_num"].notna() & (mapped_events["minute_num"] <= 90))
    mapped_events = mapped_events[keep_mask].copy()
    mapped_events = mapped_events[
        ["id", "match_id", "event_type", "card_type", "goal_type", "score_after", "minute", "added_time", "team_side_code"]
    ]

    mapped_shots = shot_rows.merge(
        cross_map,
        left_on="match_id",
        right_on="event_match_id",
        how="inner",
    )
    if mapped_shots.empty:
        return (
            mapped_events,
            pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code"]),
            pd.DataFrame(columns=["id", "match_id", "minute", "team_side_code", "shot_type", "xg"]),
        )
    mapped_shots["match_id"] = pd.to_numeric(mapped_shots["primary_match_id"], errors="coerce")
    mapped_shots["team_side_code"] = pd.to_numeric(mapped_shots["team_side_code"], errors="coerce")
    swap_goal_mask = mapped_shots["swap_sides"].astype(bool) & mapped_shots["team_side_code"].isin([1, 2])
    if bool(swap_goal_mask.any()):
        mapped_shots.loc[swap_goal_mask, "team_side_code"] = mapped_shots.loc[
            swap_goal_mask, "team_side_code"
        ].map(lambda side: 1 if int(side) == 2 else 2)
    mapped_shots = mapped_shots.dropna(subset=["match_id"]).copy()
    mapped_shots["match_id"] = mapped_shots["match_id"].astype(int)
    mapped_shots["match_status"] = mapped_shots["match_id"].map(status_lookup)
    mapped_shots["match_status_norm"] = mapped_shots["match_status"].fillna("").astype(str).str.strip().str.upper()
    mapped_shots["minute_num"] = pd.to_numeric(mapped_shots["minute"], errors="coerce")
    is_aet_goal = mapped_shots["match_status_norm"].isin({"AET", "AP"})
    keep_goal_mask = (~is_aet_goal) | (mapped_shots["minute_num"].notna() & (mapped_shots["minute_num"] <= 90))
    mapped_shots = mapped_shots[keep_goal_mask].copy()
    mapped_shots["shot_type_norm"] = mapped_shots.get("shot_type", pd.Series(index=mapped_shots.index, dtype=object)).fillna(
        ""
    ).astype(str).str.strip().str.lower()

    mapped_goal_shots = mapped_shots[mapped_shots["shot_type_norm"] == "goal"].copy()
    mapped_goal_shots = mapped_goal_shots[["id", "match_id", "minute", "team_side_code"]]
    mapped_shots = mapped_shots[["id", "match_id", "minute", "team_side_code", "shot_type", "xg"]]

    return mapped_events, mapped_goal_shots, mapped_shots


def _competition_id_key(value: Any) -> str:
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


def _disambiguate_competition_names_by_id(
    raw_finished: pd.DataFrame,
    fixtures_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if not isinstance(raw_finished, pd.DataFrame) or raw_finished.empty:
        return raw_finished, fixtures_df
    required_cols = {"competition_name", "competition_id"}
    if not required_cols.issubset(set(raw_finished.columns)):
        return raw_finished, fixtures_df

    work = raw_finished.copy()
    work["_comp_name"] = work["competition_name"].fillna("").astype(str).str.strip()
    work["_comp_id"] = work["competition_id"].map(_competition_id_key)
    work = work[(work["_comp_name"] != "") & (work["_comp_id"] != "")].copy()
    if work.empty:
        return raw_finished, fixtures_df

    id_counts = work.groupby("_comp_name", dropna=False)["_comp_id"].nunique()
    duplicate_names = {str(name).strip() for name, count in id_counts.items() if int(count or 0) > 1}
    if not duplicate_names:
        return raw_finished, fixtures_df

    def _build_label(name: str, comp_id: str, area: str) -> str:
        comp_name = str(name or "").strip()
        if not comp_name:
            return ""
        if comp_name not in duplicate_names:
            return comp_name
        comp_id_text = str(comp_id or "").strip()
        area_text = str(area or "").strip()
        if comp_id_text and area_text:
            return f"{comp_name} [{area_text} | {comp_id_text}]"
        if comp_id_text:
            return f"{comp_name} [{comp_id_text}]"
        return comp_name

    def _apply(df: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(df, pd.DataFrame) or df.empty:
            return df
        if not {"competition_name", "competition_id"}.issubset(set(df.columns)):
            return df
        out = df.copy()
        out["_comp_name"] = out["competition_name"].fillna("").astype(str).str.strip()
        out["_comp_id"] = out["competition_id"].map(_competition_id_key)
        if "area_name" in out.columns:
            out["_comp_area"] = out["area_name"].fillna("").astype(str).str.strip()
        else:
            out["_comp_area"] = ""
        out["competition_name"] = out.apply(
            lambda row: _build_label(row.get("_comp_name"), row.get("_comp_id"), row.get("_comp_area")),
            axis=1,
        )
        return out.drop(columns=["_comp_name", "_comp_id", "_comp_area"], errors="ignore")

    return _apply(raw_finished), _apply(fixtures_df)


def load_sofascore_inputs(
    db_path: str,
    *,
    match_events_db_path: str | None = None,
    source_team_mappings_path: str | Path | None = None,
    source_competition_mappings_path: str | Path | None = None,
    match_events_team_mappings_path: str | Path | None = None,
    match_events_competition_mappings_path: str | Path | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str], Path]:
    path = resolve_sofascore_db_path(db_path)
    resolved_match_events_path = resolve_match_events_db_path(match_events_db_path, path)

    finished_sql = """
    WITH xg_stats AS (
        SELECT
            ms.match_id,
            MAX(
                CASE
                    WHEN LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE 'expected goals%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%non%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%without%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%excluding%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%excl%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%no%penalty%'
                     AND UPPER(COALESCE(ms.period, '')) = 'ALL'
                    THEN COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))
                END
            ) AS home_xg_stat,
            MAX(
                CASE
                    WHEN LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE 'expected goals%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%non%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%without%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%excluding%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%excl%pen%'
                     AND LOWER(TRIM(COALESCE(sd.metric, ''))) NOT LIKE '%no%penalty%'
                     AND UPPER(COALESCE(ms.period, '')) = 'ALL'
                    THEN COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))
                END
            ) AS away_xg_stat,
            MAX(
                CASE
                    WHEN LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE 'expected goals%'
                     AND (
                        LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%non%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%without%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%excluding%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%excl%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%no%penalty%'
                     )
                     AND UPPER(COALESCE(ms.period, '')) = 'ALL'
                    THEN COALESCE(CAST(NULLIF(ms.home_value_num, '') AS REAL), CAST(NULLIF(ms.home_value_text, '') AS REAL))
                END
            ) AS home_npxg_stat,
            MAX(
                CASE
                    WHEN LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE 'expected goals%'
                     AND (
                        LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%non%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%without%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%excluding%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%excl%pen%'
                        OR LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE '%no%penalty%'
                     )
                     AND UPPER(COALESCE(ms.period, '')) = 'ALL'
                    THEN COALESCE(CAST(NULLIF(ms.away_value_num, '') AS REAL), CAST(NULLIF(ms.away_value_text, '') AS REAL))
                END
            ) AS away_npxg_stat
        FROM match_stats ms
        JOIN stat_definitions sd
            ON sd.id = ms.stat_definition_id
        WHERE LOWER(TRIM(COALESCE(sd.metric, ''))) LIKE 'expected goals%'
        GROUP BY ms.match_id
    ),
    shot_aggs AS (
        SELECT
            ms.match_id,
            SUM(
                CASE
                    WHEN ms.is_home = 1
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN 1
                    ELSE 0
                END
            ) AS home_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 0
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN 1
                    ELSE 0
                END
            ) AS away_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 1
                     AND LOWER(TRIM(COALESCE(ms.shot_type, ''))) IN ('goal', 'attempt saved')
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN 1
                    ELSE 0
                END
            ) AS home_shots_on_target,
            SUM(
                CASE
                    WHEN ms.is_home = 0
                     AND LOWER(TRIM(COALESCE(ms.shot_type, ''))) IN ('goal', 'attempt saved')
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN 1
                    ELSE 0
                END
            ) AS away_shots_on_target,
            SUM(
                CASE
                    WHEN ms.is_home = 1
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN CAST(NULLIF(ms.xg, '') AS REAL)
                END
            ) AS home_xg_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 1
                     AND LOWER(TRIM(COALESCE(ms.situation, ''))) NOT LIKE '%penalt%'
                     AND LOWER(TRIM(COALESCE(ms.shot_type, ''))) NOT LIKE '%penalt%'
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN CAST(NULLIF(ms.xg, '') AS REAL)
                END
            ) AS home_npxg_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 0
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN CAST(NULLIF(ms.xg, '') AS REAL)
                END
            ) AS away_xg_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 0
                     AND LOWER(TRIM(COALESCE(ms.situation, ''))) NOT LIKE '%penalt%'
                     AND LOWER(TRIM(COALESCE(ms.shot_type, ''))) NOT LIKE '%penalt%'
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN CAST(NULLIF(ms.xg, '') AS REAL)
                END
            ) AS away_npxg_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 1
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN CAST(NULLIF(ms.xgot, '') AS REAL)
                END
            ) AS home_xgot_shots,
            SUM(
                CASE
                    WHEN ms.is_home = 0
                     AND (
                        UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                        OR (
                            NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                            AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                        )
                     )
                    THEN CAST(NULLIF(ms.xgot, '') AS REAL)
                END
            ) AS away_xgot_shots
        FROM match_shots ms
        JOIN matches m ON m.id = ms.match_id
        GROUP BY ms.match_id
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
    normal_time_event_aggs AS (
        SELECT
            evt.match_id,
            SUM(
                CASE
                    WHEN evt.event_type_norm = 'corner'
                     AND evt.team_side_code = 1
                     AND evt.minute_num IS NOT NULL
                     AND evt.minute_num <= 90
                    THEN 1 ELSE 0
                END
            ) AS home_corners_90,
            SUM(
                CASE
                    WHEN evt.event_type_norm = 'corner'
                     AND evt.team_side_code = 2
                     AND evt.minute_num IS NOT NULL
                     AND evt.minute_num <= 90
                    THEN 1 ELSE 0
                END
            ) AS away_corners_90,
            SUM(
                CASE
                    WHEN evt.event_type_norm = 'card'
                     AND evt.card_type_norm IN ('yellow', 'second-yellow')
                     AND evt.team_side_code = 1
                     AND evt.minute_num IS NOT NULL
                     AND evt.minute_num <= 90
                    THEN 1 ELSE 0
                END
            ) AS home_yellow_cards_90,
            SUM(
                CASE
                    WHEN evt.event_type_norm = 'card'
                     AND evt.card_type_norm IN ('yellow', 'second-yellow')
                     AND evt.team_side_code = 2
                     AND evt.minute_num IS NOT NULL
                     AND evt.minute_num <= 90
                    THEN 1 ELSE 0
                END
            ) AS away_yellow_cards_90,
            SUM(
                CASE
                    WHEN evt.event_type_norm = 'card'
                     AND evt.card_type_norm IN ('red', 'second-yellow')
                     AND evt.team_side_code = 1
                     AND evt.minute_num IS NOT NULL
                     AND evt.minute_num <= 90
                    THEN 1 ELSE 0
                END
            ) AS home_red_cards_90,
            SUM(
                CASE
                    WHEN evt.event_type_norm = 'card'
                     AND evt.card_type_norm IN ('red', 'second-yellow')
                     AND evt.team_side_code = 2
                     AND evt.minute_num IS NOT NULL
                     AND evt.minute_num <= 90
                    THEN 1 ELSE 0
                END
            ) AS away_red_cards_90
        FROM (
            SELECT
                me.match_id,
                LOWER(TRIM(COALESCE(me.event_type, ''))) AS event_type_norm,
                LOWER(TRIM(COALESCE(me.card_type, ''))) AS card_type_norm,
                me.team_side_code AS team_side_code,
                CAST(NULLIF(TRIM(COALESCE(me.minute, '')), '') AS REAL) AS minute_num
            FROM match_events me
            JOIN matches m ON m.id = me.match_id
            WHERE UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
        ) evt
        GROUP BY evt.match_id
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
        JOIN matches m ON m.id = me.match_id
        WHERE (
            UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
            OR (
                NULLIF(TRIM(COALESCE(me.minute, '')), '') IS NOT NULL
                AND CAST(NULLIF(TRIM(COALESCE(me.minute, '')), '') AS REAL) <= 90
            )
        )
        GROUP BY me.match_id
    )
    SELECT
        m.id AS match_id,
        m.competition_id AS competition_id,
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
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN m.home_ft_score
            ELSE COALESCE(m.home_ft_score, m.home_score_final)
        END AS home_goals,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN m.away_ft_score
            ELSE COALESCE(m.away_ft_score, m.away_score_final)
        END AS away_goals,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN NULL
            ELSE xg.home_xg_stat
        END AS home_xg_stat,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN NULL
            ELSE xg.away_xg_stat
        END AS away_xg_stat,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN NULL
            ELSE xg.home_npxg_stat
        END AS home_npxg_stat,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN NULL
            ELSE xg.away_npxg_stat
        END AS away_npxg_stat,
        sa.home_shots,
        sa.away_shots,
        sa.home_shots_on_target,
        sa.away_shots_on_target,
        sa.home_xg_shots,
        sa.away_xg_shots,
        sa.home_npxg_shots,
        sa.away_npxg_shots,
        sa.home_xgot_shots,
        sa.away_xgot_shots,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN NULL
            ELSE ka.home_goalkeeper_goals_prevented
        END AS home_goalkeeper_goals_prevented,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN NULL
            ELSE ka.away_goalkeeper_goals_prevented
        END AS away_goalkeeper_goals_prevented,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN COALESCE(ntea.home_corners_90, cca.home_corners)
            ELSE cca.home_corners
        END AS home_corners,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN COALESCE(ntea.away_corners_90, cca.away_corners)
            ELSE cca.away_corners
        END AS away_corners,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN COALESCE(ntea.home_yellow_cards_90, cca.home_yellow_cards)
            ELSE cca.home_yellow_cards
        END AS home_yellow_cards,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN COALESCE(ntea.away_yellow_cards_90, cca.away_yellow_cards)
            ELSE cca.away_yellow_cards
        END AS away_yellow_cards,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN COALESCE(ntea.home_red_cards_90, cca.home_red_cards)
            ELSE cca.home_red_cards
        END AS home_red_cards,
        CASE
            WHEN UPPER(TRIM(COALESCE(m.status, ''))) IN ('AET', 'AP')
            THEN COALESCE(ntea.away_red_cards_90, cca.away_red_cards)
            ELSE cca.away_red_cards
        END AS away_red_cards,
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
    LEFT JOIN normal_time_event_aggs ntea ON ntea.match_id = m.id
    LEFT JOIN event_card_aggs eca ON eca.match_id = m.id
    WHERE m.kickoff_ts IS NOT NULL
      AND COALESCE(m.home_ft_score, m.home_score_final) IS NOT NULL
      AND COALESCE(m.away_ft_score, m.away_score_final) IS NOT NULL
    """

    fixtures_sql = """
    SELECT
        m.id AS match_id,
        m.competition_id AS competition_id,
        m.season_id,
        m.kickoff_ts AS date_time,
        m.status AS match_status,
        c.name AS competition_name,
        c.country AS area_name,
        ht.name AS home_team_name,
        at.name AS away_team_name
    FROM matches m
    JOIN teams ht ON ht.id = m.home_team_id
    JOIN teams at ON at.id = m.away_team_id
    LEFT JOIN competitions c ON c.id = m.competition_id
    WHERE m.kickoff_ts IS NOT NULL
    """

    conn = sqlite3.connect(str(path))
    try:
        raw_finished = pd.read_sql_query(finished_sql, conn)
        fixtures_df = pd.read_sql_query(fixtures_sql, conn)
        primary_match_events_df = pd.read_sql_query(
            """
            SELECT
                me.id,
                me.match_id,
                me.event_type,
                me.card_type,
                me.goal_type,
                me.score_after,
                me.minute,
                me.added_time,
                me.team_side_code
            FROM match_events me
            JOIN matches m ON m.id = me.match_id
            WHERE LOWER(COALESCE(me.event_type, '')) IN ('goal', 'corner', 'card')
              AND (
                UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                OR (
                    NULLIF(TRIM(COALESCE(me.minute, '')), '') IS NOT NULL
                    AND CAST(NULLIF(TRIM(COALESCE(me.minute, '')), '') AS REAL) <= 90
                )
              )
            """,
            conn,
        )
        primary_shots_df = pd.read_sql_query(
            """
            SELECT
                ms.id,
                ms.match_id,
                ms.time_minute AS minute,
                CASE
                    WHEN ms.is_home = 1 THEN 1
                    WHEN ms.is_home = 0 THEN 2
                    ELSE NULL
                END AS team_side_code,
                ms.shot_type,
                ms.xg
            FROM match_shots ms
            JOIN matches m ON m.id = ms.match_id
            WHERE (
                UPPER(TRIM(COALESCE(m.status, ''))) NOT IN ('AET', 'AP')
                OR (
                    NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') IS NOT NULL
                    AND CAST(NULLIF(TRIM(COALESCE(ms.time_minute, '')), '') AS REAL) <= 90
                )
              )
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

    primary_goal_shots_df = primary_shots_df.copy()
    primary_goal_shots_df["shot_type_norm"] = primary_goal_shots_df.get(
        "shot_type", pd.Series(index=primary_goal_shots_df.index, dtype=object)
    ).fillna("").astype(str).str.strip().str.lower()
    primary_goal_shots_df = primary_goal_shots_df[primary_goal_shots_df["shot_type_norm"] == "goal"].copy()
    primary_goal_shots_df = primary_goal_shots_df[["id", "match_id", "minute", "team_side_code"]]

    match_events_df = primary_match_events_df
    goal_shots_df = primary_goal_shots_df
    shots_df = primary_shots_df

    if resolved_match_events_path != path:
        mapped_match_events_df, mapped_goal_shots_df, mapped_shots_df = _load_cross_db_event_inputs(
            events_db_path=resolved_match_events_path,
            primary_matches=raw_finished[
                [
                    "match_id",
                    "date_time",
                    "match_status",
                    "competition_name",
                    "home_team_name",
                    "away_team_name",
                ]
            ].copy(),
            source_team_mappings_path=source_team_mappings_path,
            source_competition_mappings_path=source_competition_mappings_path,
            match_events_team_mappings_path=match_events_team_mappings_path,
            match_events_competition_mappings_path=match_events_competition_mappings_path,
        )
        mapped_match_ids: set[int] = set()
        if isinstance(mapped_match_events_df, pd.DataFrame) and (not mapped_match_events_df.empty):
            mapped_match_ids.update(
                pd.to_numeric(mapped_match_events_df.get("match_id"), errors="coerce")
                .dropna()
                .astype(int)
                .tolist()
            )
        if isinstance(mapped_goal_shots_df, pd.DataFrame) and (not mapped_goal_shots_df.empty):
            mapped_match_ids.update(
                pd.to_numeric(mapped_goal_shots_df.get("match_id"), errors="coerce")
                .dropna()
                .astype(int)
                .tolist()
            )
        if isinstance(mapped_shots_df, pd.DataFrame) and (not mapped_shots_df.empty):
            mapped_match_ids.update(
                pd.to_numeric(mapped_shots_df.get("match_id"), errors="coerce")
                .dropna()
                .astype(int)
                .tolist()
            )

        if mapped_match_ids:
            primary_match_events_keep = primary_match_events_df.copy()
            if "match_id" in primary_match_events_keep.columns:
                primary_match_events_keep = primary_match_events_keep[
                    ~pd.to_numeric(primary_match_events_keep["match_id"], errors="coerce")
                    .fillna(-1)
                    .astype(int)
                    .isin(mapped_match_ids)
                ].copy()
            primary_goal_shots_keep = primary_goal_shots_df.copy()
            if "match_id" in primary_goal_shots_keep.columns:
                primary_goal_shots_keep = primary_goal_shots_keep[
                    ~pd.to_numeric(primary_goal_shots_keep["match_id"], errors="coerce")
                    .fillna(-1)
                    .astype(int)
                    .isin(mapped_match_ids)
                ].copy()
            primary_shots_keep = primary_shots_df.copy()
            if "match_id" in primary_shots_keep.columns:
                primary_shots_keep = primary_shots_keep[
                    ~pd.to_numeric(primary_shots_keep["match_id"], errors="coerce")
                    .fillna(-1)
                    .astype(int)
                    .isin(mapped_match_ids)
                ].copy()

            match_event_frames = [
                frame
                for frame in (mapped_match_events_df, primary_match_events_keep)
                if isinstance(frame, pd.DataFrame) and (not frame.empty)
            ]
            if match_event_frames:
                match_events_df = pd.concat(match_event_frames, ignore_index=True, sort=False)
            else:
                match_events_df = primary_match_events_df.iloc[0:0].copy()

            goal_shot_frames = [
                frame
                for frame in (mapped_goal_shots_df, primary_goal_shots_keep)
                if isinstance(frame, pd.DataFrame) and (not frame.empty)
            ]
            if goal_shot_frames:
                goal_shots_df = pd.concat(goal_shot_frames, ignore_index=True, sort=False)
            else:
                goal_shots_df = primary_goal_shots_df.iloc[0:0].copy()

            shot_frames = [
                frame
                for frame in (mapped_shots_df, primary_shots_keep)
                if isinstance(frame, pd.DataFrame) and (not frame.empty)
            ]
            if shot_frames:
                shots_df = pd.concat(shot_frames, ignore_index=True, sort=False)
            else:
                shots_df = primary_shots_df.iloc[0:0].copy()

        mapped_match_count = int(
            pd.to_numeric(mapped_match_events_df.get("match_id"), errors="coerce").dropna().astype(int).nunique()
        )
        fallback_match_count = int(
            pd.to_numeric(primary_match_events_df.get("match_id"), errors="coerce")
            .dropna()
            .astype(int)
            .nunique()
            - len(mapped_match_ids)
        )
        final_event_match_count = int(
            pd.to_numeric(match_events_df.get("match_id"), errors="coerce").dropna().astype(int).nunique()
        )
        print(
            "[xgd_web_app] Stage 2/3: Mapped external match-events "
            f"({len(mapped_match_events_df)} events, {mapped_match_count} matches) "
            f"from {resolved_match_events_path}; "
            f"fallback primary matches={max(0, fallback_match_count)}, "
            f"final event coverage matches={final_event_match_count}",
            flush=True,
        )

    raw_finished, fixtures_df = _disambiguate_competition_names_by_id(raw_finished, fixtures_df)

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
        "home_npxg_stat",
        "away_npxg_stat",
        "home_shots",
        "away_shots",
        "home_shots_on_target",
        "away_shots_on_target",
        "home_xg_shots",
        "away_xg_shots",
        "home_npxg_shots",
        "away_npxg_shots",
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

    gamestate_counts_df = build_match_gamestate_event_counts(
        match_events_df,
        goal_shots_df=goal_shots_df,
        shots_df=shots_df,
    )
    if not gamestate_counts_df.empty and "match_id" in raw_finished.columns:
        raw_finished = raw_finished.merge(gamestate_counts_df, on="match_id", how="left")
        gamestate_numeric_cols = [col for col in gamestate_counts_df.columns if col != "match_id"]
        for col in gamestate_numeric_cols:
            if col in raw_finished.columns:
                raw_finished[col] = pd.to_numeric(raw_finished[col], errors="coerce").fillna(0.0)

    raw_finished["home_xg"] = raw_finished["home_xg_stat"].where(raw_finished["home_xg_stat"].notna(), raw_finished["home_xg_shots"])
    raw_finished["away_xg"] = raw_finished["away_xg_stat"].where(raw_finished["away_xg_stat"].notna(), raw_finished["away_xg_shots"])
    raw_finished["home_npxg"] = raw_finished["home_npxg_stat"].where(
        raw_finished["home_npxg_stat"].notna(),
        raw_finished["home_npxg_shots"],
    )
    raw_finished["home_npxg"] = raw_finished["home_npxg"].where(
        raw_finished["home_npxg"].notna(),
        raw_finished["home_xg"],
    )
    raw_finished["away_npxg"] = raw_finished["away_npxg_stat"].where(
        raw_finished["away_npxg_stat"].notna(),
        raw_finished["away_npxg_shots"],
    )
    raw_finished["away_npxg"] = raw_finished["away_npxg"].where(
        raw_finished["away_npxg"].notna(),
        raw_finished["away_xg"],
    )
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
            "competition_id",
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
            "home_npxg",
            "away_npxg",
            "home_xgot",
            "away_xgot",
            "home_shots",
            "away_shots",
            "home_shots_on_target",
            "away_shots_on_target",
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
            "competition_id": finished["competition_id"],
            "team": finished["home_team_name"],
            "opponent": finished["away_team_name"],
            "venue": "Home",
            "date_time": finished["date_time"],
            "GF": finished["home_goals"],
            "GA": finished["away_goals"],
            "xG": finished["home_xg"],
            "xGA": finished["away_xg"],
            "NPxG": finished["home_npxg"].where(finished["home_npxg"].notna(), finished["home_xg"]),
            "NPxGA": finished["away_npxg"].where(finished["away_npxg"].notna(), finished["away_xg"]),
            "xGoT": finished["home_xgot"].where(finished["home_xgot"].notna(), finished["home_xg"]),
            "xGoTA": finished["away_xgot"].where(finished["away_xgot"].notna(), finished["away_xg"]),
            "shots_for": finished["home_shots"],
            "shots_against": finished["away_shots"],
            "shots_on_target_for": finished["home_shots_on_target"],
            "shots_on_target_against": finished["away_shots_on_target"],
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
            "competition_id": finished["competition_id"],
            "team": finished["away_team_name"],
            "opponent": finished["home_team_name"],
            "venue": "Away",
            "date_time": finished["date_time"],
            "GF": finished["away_goals"],
            "GA": finished["home_goals"],
            "xG": finished["away_xg"],
            "xGA": finished["home_xg"],
            "NPxG": finished["away_npxg"].where(finished["away_npxg"].notna(), finished["away_xg"]),
            "NPxGA": finished["home_npxg"].where(finished["home_npxg"].notna(), finished["home_xg"]),
            "xGoT": finished["away_xgot"].where(finished["away_xgot"].notna(), finished["away_xg"]),
            "xGoTA": finished["home_xgot"].where(finished["home_xgot"].notna(), finished["home_xg"]),
            "shots_for": finished["away_shots"],
            "shots_against": finished["home_shots"],
            "shots_on_target_for": finished["away_shots_on_target"],
            "shots_on_target_against": finished["home_shots_on_target"],
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
    fixtures_df["status_norm"] = fixtures_df["match_status"].fillna("").astype(str).str.strip().str.lower()
    fixtures_df = fixtures_df[fixtures_df["status_norm"].isin(NOT_STARTED_STATUSES)].copy()
    fixtures_df = fixtures_df.drop(columns=["match_status", "status_norm"], errors="ignore")
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
    "resolve_match_events_db_path",
    "resolve_sofascore_db_path",
]
