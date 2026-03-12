from pathlib import Path
from ast import literal_eval
import json
import warnings

import numpy as np
import pandas as pd
from sqlalchemy import bindparam, create_engine, text

GAMESTATES = ("drawing", "winning", "losing")
GAMESTATE_EVENT_METRIC_KEYS = tuple(
    f"{metric}_{direction}_{state}"
    for metric in ("corners", "cards")
    for direction in ("for", "against")
    for state in GAMESTATES
)
GAMESTATE_TIME_KEYS = tuple(f"minutes_{state}" for state in GAMESTATES)
GAMESTATE_ALL_KEYS = GAMESTATE_EVENT_METRIC_KEYS + GAMESTATE_TIME_KEYS

DEFAULT_REMOTE_CONFIG = {
    "host": "35.234.132.229",
    "port": "5432",
    "database": "prematch-football",
    "user": "wsdreadonly_wyscout",
    "password": "blue-elephant-head",
}

BASE_QUERY = """
    SELECT
        m.*,
        md.*,
        c.name AS competition_name,
        a.name AS area_name,
        r.*,
        s.*,
        r.id AS round_table_id,
        r.competition_id AS round_competition_id,
        r.season_id AS round_season_id,
        r.name AS round_name,
        r.start_date AS round_start_date,
        r.end_date AS round_end_date,
        r.type AS round_type,
        s.id AS season_table_id,
        s.competition_id AS season_competition_id,
        s.active AS season_active,
        s.name AS season_name,
        s.start_date AS season_start_date,
        s.end_date AS season_end_date,
        th.name AS home_team_name,
        ta.name AS away_team_name
FROM "match" AS m
JOIN competition AS c
    ON c.id = m.competition_id
JOIN area AS a
    ON a.id = c.area_id
LEFT JOIN match_data AS md
    ON md.id = m.id
LEFT JOIN "round" AS r
    ON r.id = m.round_id
LEFT JOIN season AS s
    ON s.id = m.season_id
LEFT JOIN team AS th
    ON th.id = m.home_team_id
LEFT JOIN team AS ta
    ON ta.id = m.away_team_id
WHERE (c.name, a.name) IN (
    ('Premier League', 'England'),
    ('La Liga', 'Spain'),
    ('Serie A', 'Italy'),
    ('Bundesliga', 'Germany'),
    ('Ligue 1', 'France'),
    ('Championship', 'England')
)
"""

NEXT_LEAGUE_FIXTURES_QUERY = text("""
    WITH filtered_fixtures AS (
        SELECT
            m.id,
            m.competition_id,
            m.season_id,
            m.gameweek,
            m.date_time,
            m.home_team_id,
            m.away_team_id
        FROM "match" AS m
        JOIN competition AS c
            ON c.id = m.competition_id
        JOIN area AS a
            ON a.id = c.area_id
        WHERE m.status = :fixture_status
          AND m.gameweek IS NOT NULL
          AND (:competition_id IS NULL OR c.id = :competition_id)
          AND (:competition_name IS NULL OR c.name = :competition_name)
          AND (:area_name IS NULL OR a.name = :area_name)
    ),
    next_gameweek AS (
        SELECT ff.gameweek
        FROM filtered_fixtures AS ff
        ORDER BY ff.date_time
        LIMIT 1
    )
    SELECT
        ff.id AS match_id,
        ff.competition_id,
        ff.season_id,
        ff.home_team_id,
        ff.away_team_id,
        ff.date_time,
        ff.gameweek,
        th.name AS home,
        ta.name AS away,
        c.name AS competition_name,
        a.name AS area_name
    FROM filtered_fixtures AS ff
    JOIN competition AS c
        ON c.id = ff.competition_id
    JOIN area AS a
        ON a.id = c.area_id
    LEFT JOIN team AS th
        ON th.id = ff.home_team_id
    LEFT JOIN team AS ta
        ON ta.id = ff.away_team_id
    WHERE ff.gameweek = (SELECT gameweek FROM next_gameweek)
    ORDER BY ff.date_time, ff.id
""")

FAST_GAME_RAW_QUERY = text("""
    SELECT
        m.id,
        m.competition_id,
        m.season_id,
        m.date_time,
        md.home_ft_score,
        md.away_ft_score,
        md.home_total,
        md.away_total,
        md.home_average,
        md.away_average,
        md.home_general,
        md.away_general,
        c.name AS competition_name,
        a.name AS area_name,
        s.name AS season_name,
        s.start_date AS season_start_date,
        s.end_date AS season_end_date,
        th.name AS home_team_name,
        ta.name AS away_team_name
    FROM "match" AS m
    LEFT JOIN match_data AS md
        ON md.id = m.id
    JOIN competition AS c
        ON c.id = m.competition_id
    JOIN area AS a
        ON a.id = c.area_id
    LEFT JOIN season AS s
        ON s.id = m.season_id
    LEFT JOIN team AS th
        ON th.id = m.home_team_id
    LEFT JOIN team AS ta
        ON ta.id = m.away_team_id
    WHERE m.season_id = :season_id
      AND (m.home_team_id IN :team_ids OR m.away_team_id IN :team_ids)
      AND m.date_time <= :cutoff_date
      AND (:competition_id IS NULL OR m.competition_id = :competition_id)
    ORDER BY m.date_time
""").bindparams(bindparam("team_ids", expanding=True))

def _parse_dict_value(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
        try:
            parsed = literal_eval(value)
            if isinstance(parsed, dict):
                return parsed
        except (ValueError, SyntaxError):
            pass
    return {}


def _normalize_stat_key(key):
    return "".join(ch for ch in str(key).lower() if ch.isalnum())


def _extract_stat(stats_dict, keys, default=np.nan):
    if not isinstance(stats_dict, dict):
        return default
    normalized = {_normalize_stat_key(k): v for k, v in stats_dict.items()}
    for key in keys:
        lookup = _normalize_stat_key(key)
        if lookup in normalized and normalized[lookup] is not None:
            try:
                return float(normalized[lookup])
            except (TypeError, ValueError):
                continue
    return default


def get_wyscout_engine(use_local=False, local_db_path="../Football Data/local_football.db", remote_config=None):
    if use_local:
        db_path = Path(local_db_path).resolve()
        if not db_path.exists():
            raise FileNotFoundError(f"Database not found: {db_path}")
        return create_engine(f"sqlite:///{db_path.as_posix()}")

    config = remote_config or DEFAULT_REMOTE_CONFIG
    return create_engine(
        f"postgresql+psycopg2://{config['user']}:{config['password']}@"
        f"{config['host']}:{config['port']}/{config['database']}"
    )


def load_matches_df(
    use_local=False,
    local_db_path="../Football Data/local_football.db",
    remote_config=None,
    date_from=None,
    date_to=None,
    order_by_date=True,
):
    engine = get_wyscout_engine(
        use_local=use_local,
        local_db_path=local_db_path,
        remote_config=remote_config,
    )
    where_clauses = []
    params = {}
    if date_from is not None:
        where_clauses.append("m.date_time >= :date_from")
        params["date_from"] = date_from
    if date_to is not None:
        where_clauses.append("m.date_time <= :date_to")
        params["date_to"] = date_to

    sql = BASE_QUERY
    if where_clauses:
        sql += "\nAND " + "\nAND ".join(where_clauses)
    if order_by_date:
        sql += "\nORDER BY a.name, c.name, m.date_time"

    with engine.begin() as conn:
        out = pd.read_sql(text(sql), conn, params=params)
    return out.reset_index(drop=True)


def load_next_league_games(
    competition_name=None,
    area_name=None,
    competition_id=None,
    use_local=False,
    local_db_path="../Football Data/local_football.db",
    remote_config=None,
    fixture_status="Fixture",
):
    """
    Return the next fixture gameweek for a league as a dataframe with:
    match_date, home, away.

    Filter options:
    - competition_id (preferred if known)
    - competition_name (+ area_name when names are ambiguous)
    """
    if competition_id is None and competition_name is None:
        raise ValueError("Provide either competition_id or competition_name.")

    engine = get_wyscout_engine(
        use_local=use_local,
        local_db_path=local_db_path,
        remote_config=remote_config,
    )
    params = {
        "competition_id": competition_id,
        "competition_name": competition_name,
        "area_name": area_name,
        "fixture_status": fixture_status,
    }

    with engine.begin() as conn:
        out = pd.read_sql(NEXT_LEAGUE_FIXTURES_QUERY, conn, params=params)

    if out.empty:
        return pd.DataFrame(
            columns=[
                "match_id",
                "competition_id",
                "season_id",
                "home_team_id",
                "away_team_id",
                "date_time",
                "gameweek",
                "competition_name",
                "area_name",
                "home",
                "away",
                "match_date",
            ]
        )

    out["match_date"] = pd.to_datetime(out["date_time"], errors="coerce").dt.normalize()
    return out.reset_index(drop=True)


def load_matches_for_teams_season(
    home_team_id,
    away_team_id,
    season_id,
    cutoff_date=None,
    use_local=False,
    local_db_path="../Football Data/local_football.db",
    remote_config=None,
):
    engine = get_wyscout_engine(
        use_local=use_local,
        local_db_path=local_db_path,
        remote_config=remote_config,
    )
    sql = (
        BASE_QUERY
        + "\nAND m.season_id = :season_id"
        + "\nAND (m.home_team_id IN :team_ids OR m.away_team_id IN :team_ids)"
    )
    params = {
        "season_id": season_id,
        "team_ids": [home_team_id, away_team_id],
    }
    if cutoff_date is not None:
        sql += "\nAND m.date_time <= :cutoff_date"
        params["cutoff_date"] = cutoff_date
    sql += "\nORDER BY m.date_time"

    q = text(sql).bindparams(bindparam("team_ids", expanding=True))
    with engine.begin() as conn:
        out = pd.read_sql(q, conn, params=params)
    return out.reset_index(drop=True)


def calc_single_game_form_tables_fast(
    home,
    away,
    match_date,
    home_team_id,
    away_team_id,
    season_id,
    competition_id=None,
    periods=("Season", 5, 3),
    return_source_games=False,
    min_games=3,
    use_local=False,
    local_db_path="../Football Data/local_football.db",
    remote_config=None,
):
    raw_matches_df = load_single_game_raw_context(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
        season_id=season_id,
        match_date=match_date,
        competition_id=competition_id,
        use_local=use_local,
        local_db_path=local_db_path,
        remote_config=remote_config,
    )

    form_df = transform_wyscout_form_data(raw_matches_df)
    games_df = pd.DataFrame(
        [
            {
                "home": home,
                "away": away,
                "match_date": match_date,
                "season_id": season_id,
            }
        ]
    )
    return calc_wyscout_form_tables(
        games_df,
        form_df,
        periods=periods,
        return_source_games=return_source_games,
        min_games=min_games,
    )


def load_single_game_raw_context(
    home_team_id,
    away_team_id,
    season_id,
    match_date,
    competition_id=None,
    use_local=False,
    local_db_path="../Football Data/local_football.db",
    remote_config=None,
):
    engine = get_wyscout_engine(
        use_local=use_local,
        local_db_path=local_db_path,
        remote_config=remote_config,
    )
    params = {
        "season_id": season_id,
        "team_ids": [home_team_id, away_team_id],
        "cutoff_date": match_date,
        "competition_id": competition_id,
    }
    with engine.begin() as conn:
        raw_matches_df = pd.read_sql(FAST_GAME_RAW_QUERY, conn, params=params)
    return raw_matches_df.reset_index(drop=True)


def build_team_corners_cards_table(raw_matches_df):
    """
    Build team-level match table with corners/cards for and against.
    One row per team per match.
    """
    if raw_matches_df is None or raw_matches_df.empty:
        return pd.DataFrame(
            columns=[
                "match_id", "date_time", "team", "opponent", "venue",
                "corners_for", "corners_against",
                "yc_for", "yc_against", "rc_for", "rc_against",
                "cards_for", "cards_against",
                "competition_name", "area_name", "season_id",
            ]
        )

    rows = []
    for _, row in raw_matches_df.iterrows():
        home_total = _parse_dict_value(row.get("home_total"))
        away_total = _parse_dict_value(row.get("away_total"))
        home_average = _parse_dict_value(row.get("home_average"))
        away_average = _parse_dict_value(row.get("away_average"))
        home_general = _parse_dict_value(row.get("home_general"))
        away_general = _parse_dict_value(row.get("away_general"))

        home_stats = {**home_general, **home_average, **home_total}
        away_stats = {**away_general, **away_average, **away_total}

        home_corners = _extract_stat(home_stats, ["corners", "top_stats_corners"])
        away_corners = _extract_stat(away_stats, ["corners", "top_stats_corners"])
        home_yc = _extract_stat(home_stats, ["yellowCards", "yellow_cards", "discipline_yellow_cards"])
        away_yc = _extract_stat(away_stats, ["yellowCards", "yellow_cards", "discipline_yellow_cards"])
        home_rc = _extract_stat(home_stats, ["redCards", "red_cards", "discipline_red_cards"])
        away_rc = _extract_stat(away_stats, ["redCards", "red_cards", "discipline_red_cards"])

        def _total_cards(yc, rc):
            if pd.isna(yc) and pd.isna(rc):
                return np.nan
            yc_val = 0.0 if pd.isna(yc) else yc
            rc_val = 0.0 if pd.isna(rc) else rc
            return yc_val + 2 * rc_val

        rows.append(
            {
                "match_id": row.get("id"),
                "date_time": pd.to_datetime(row.get("date_time"), errors="coerce"),
                "team": row.get("home_team_name"),
                "opponent": row.get("away_team_name"),
                "venue": "Home",
                "corners_for": home_corners,
                "corners_against": away_corners,
                "yc_for": home_yc,
                "yc_against": away_yc,
                "rc_for": home_rc,
                "rc_against": away_rc,
                "cards_for": _total_cards(home_yc, home_rc),
                "cards_against": _total_cards(away_yc, away_rc),
                "competition_name": row.get("competition_name"),
                "area_name": row.get("area_name"),
                "season_id": row.get("season_id"),
            }
        )
        rows.append(
            {
                "match_id": row.get("id"),
                "date_time": pd.to_datetime(row.get("date_time"), errors="coerce"),
                "team": row.get("away_team_name"),
                "opponent": row.get("home_team_name"),
                "venue": "Away",
                "corners_for": away_corners,
                "corners_against": home_corners,
                "yc_for": away_yc,
                "yc_against": home_yc,
                "rc_for": away_rc,
                "rc_against": home_rc,
                "cards_for": _total_cards(away_yc, away_rc),
                "cards_against": _total_cards(home_yc, home_rc),
                "competition_name": row.get("competition_name"),
                "area_name": row.get("area_name"),
                "season_id": row.get("season_id"),
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["date_time", "match_id", "venue"], kind="mergesort").reset_index(drop=True)
    return out


def transform_wyscout_form_data(raw_matches_df):
    """
    Transform raw Wyscout match rows into a per-team form table.

    Returns a dataframe with one row per team per match and columns:
    team, opponent, venue, date_time, GF, GA, xG, xGA, xGoT, xGoTA
    plus season/competition metadata when present in raw data.
    """
    if raw_matches_df is None or len(raw_matches_df) == 0:
        return pd.DataFrame(
            columns=["match_id", "team", "opponent", "venue", "date_time", "GF", "GA", "xG", "xGA", "xGoT", "xGoTA"]
        )

    df = raw_matches_df.copy()

    for required_col in ["id", "home_team_name", "away_team_name", "home_ft_score", "away_ft_score"]:
        if required_col not in df.columns:
            raise KeyError(f"Missing required column in raw Wyscout data: {required_col}")

    def _maybe_dict(value):
        if isinstance(value, pd.Series):
            for v in value.tolist():
                if isinstance(v, dict):
                    return v
            for v in value.tolist():
                if isinstance(v, str):
                    try:
                        parsed = json.loads(v)
                        if isinstance(parsed, dict):
                            return parsed
                    except json.JSONDecodeError:
                        pass
                    try:
                        parsed = literal_eval(v)
                        if isinstance(parsed, dict):
                            return parsed
                    except (ValueError, SyntaxError):
                        continue
            return {}
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                pass
            try:
                parsed = literal_eval(value)
                return parsed if isinstance(parsed, dict) else {}
            except (ValueError, SyntaxError):
                return {}
        return {}

    def _normalize_key(key):
        return "".join(ch for ch in str(key).lower() if ch.isalnum())

    def _get_num(stats_dict, keys, fallback=np.nan):
        if not isinstance(stats_dict, dict):
            return fallback

        normalized = {_normalize_key(k): v for k, v in stats_dict.items()}
        for key in keys:
            lookup_key = _normalize_key(key)
            if lookup_key in normalized and normalized[lookup_key] is not None:
                try:
                    return float(normalized[lookup_key])
                except (TypeError, ValueError):
                    continue
        return fallback

    def _scalar_value(row, key, default=np.nan):
        value = row.get(key, default)
        if isinstance(value, pd.Series):
            for v in value.tolist():
                if isinstance(v, (dict, list, pd.Series)):
                    continue
                if pd.notna(v):
                    return v
            return default
        return value

    def _extract_xg(stats):
        return _get_num(stats, ["xg", "xG", "expected_goals"])

    def _extract_xgot(stats, fallback):
        # xGoT naming differs across feeds; fallback keeps downstream calcs stable.
        return _get_num(
            stats,
            [
                "xgot",
                "xGoT",
                "xgOT",
                "expected_goals_on_target",
                "xg_on_target",
                "xgOnTarget",
                "post_shot_xg",
                "postShotXg",
                "postShotExpectedGoals",
            ],
            fallback=fallback,
        )

    if "date_time" in df.columns:
        df["date_time"] = pd.to_datetime(df["date_time"], errors="coerce")

    rows = []
    for _, row in df.iterrows():
        home_total = _maybe_dict(row.get("home_total"))
        away_total = _maybe_dict(row.get("away_total"))
        home_average = _maybe_dict(row.get("home_average"))
        away_average = _maybe_dict(row.get("away_average"))
        home_general = _maybe_dict(row.get("home_general"))
        away_general = _maybe_dict(row.get("away_general"))

        match_id = _scalar_value(row, "id")
        season_id = _scalar_value(row, "season_id", default=np.nan)
        competition_name = _scalar_value(row, "competition_name", default=None)
        area_name = _scalar_value(row, "area_name", default=None)
        season_name = _scalar_value(row, "season_name", default=None)
        season_start_date = _scalar_value(row, "season_start_date", default=pd.NaT)
        season_end_date = _scalar_value(row, "season_end_date", default=pd.NaT)
        home_team_name = _scalar_value(row, "home_team_name")
        away_team_name = _scalar_value(row, "away_team_name")
        home_ft_score = _scalar_value(row, "home_ft_score")
        away_ft_score = _scalar_value(row, "away_ft_score")
        date_time = _scalar_value(row, "date_time")

        # Prefer total/average stat blocks over general; merge so keys can be found consistently.
        home_stats = {**home_general, **home_average, **home_total}
        away_stats = {**away_general, **away_average, **away_total}

        home_xg = _extract_xg(home_stats)
        away_xg = _extract_xg(away_stats)
        # If xGoT is unavailable in the feed, fallback to xG (keeps downstream logic stable).
        # When xGoT exists under variant key names, _extract_xgot now captures it.
        home_xgot = _extract_xgot(home_stats, fallback=home_xg)
        away_xgot = _extract_xgot(away_stats, fallback=away_xg)

        rows.append(
            {
                "match_id": match_id,
                "team": home_team_name,
                "opponent": away_team_name,
                "venue": "Home",
                "date_time": date_time,
                "GF": home_ft_score,
                "GA": away_ft_score,
                "xG": home_xg,
                "xGA": away_xg,
                "xGoT": home_xgot,
                "xGoTA": away_xgot,
                "season_id": season_id,
                "competition_name": competition_name,
                "area_name": area_name,
                "season_name": season_name,
                "season_start_date": season_start_date,
                "season_end_date": season_end_date,
            }
        )
        rows.append(
            {
                "match_id": match_id,
                "team": away_team_name,
                "opponent": home_team_name,
                "venue": "Away",
                "date_time": date_time,
                "GF": away_ft_score,
                "GA": home_ft_score,
                "xG": away_xg,
                "xGA": home_xg,
                "xGoT": away_xgot,
                "xGoTA": home_xgot,
                "season_id": season_id,
                "competition_name": competition_name,
                "area_name": area_name,
                "season_name": season_name,
                "season_start_date": season_start_date,
                "season_end_date": season_end_date,
            }
        )

    out = pd.DataFrame(rows)
    for col in ["GF", "GA", "xG", "xGA", "xGoT", "xGoTA"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    if "season_start_date" in out.columns:
        out["season_start_date"] = pd.to_datetime(out["season_start_date"], errors="coerce")
    if "season_end_date" in out.columns:
        out["season_end_date"] = pd.to_datetime(out["season_end_date"], errors="coerce")

    out = out.sort_values(["date_time", "match_id", "venue"], kind="mergesort").reset_index(drop=True)
    return out


def calc_wyscout_form_tables(games, data_df, periods=("Season", 5, 3), return_source_games=False, min_games=3):
    """
    Rebuild calc_form_table using Wyscout raw match data.

    games can be:
    - DataFrame with columns home/away and optional date_time
    - dict/list convertible to DataFrame([home, away])

    data_df can be either:
    - raw Wyscout matches dataframe (output of load_matches_df)
    - transformed form dataframe (output of transform_wyscout_form_data)

    If return_source_games=True, also returns a second list with the exact
    source rows used for each fixture:
      {"home_source_games": df, "away_source_games": df, "season_id_used": value}

    If the fixture season has no data (or fewer than min_games for either side),
    a warning is emitted. No cross-season fallback is used.
    """
    if isinstance(games, dict):
        games = pd.DataFrame(games, columns=["home", "away"])
    elif not isinstance(games, pd.DataFrame):
        games = pd.DataFrame(games, columns=["home", "away"])

    if "home" not in games.columns or "away" not in games.columns:
        raise KeyError("games must include 'home' and 'away' columns")

    if "date_time" in games.columns:
        games = games.copy()
        games["date_time"] = pd.to_datetime(games["date_time"], errors="coerce")
    elif "match_date" in games.columns:
        games = games.copy()
        games["match_date"] = pd.to_datetime(games["match_date"], errors="coerce")

    form_cols = {"team", "venue", "xG", "xGA", "xGoT", "xGoTA", "GF", "GA"}
    if isinstance(data_df, pd.DataFrame) and form_cols.issubset(set(data_df.columns)):
        form_df = data_df.copy()
        if "date_time" in form_df.columns:
            form_df["date_time"] = pd.to_datetime(form_df["date_time"], errors="coerce")
        if "season_start_date" in form_df.columns:
            form_df["season_start_date"] = pd.to_datetime(form_df["season_start_date"], errors="coerce")
        if "season_end_date" in form_df.columns:
            form_df["season_end_date"] = pd.to_datetime(form_df["season_end_date"], errors="coerce")
    else:
        form_df = transform_wyscout_form_data(data_df)

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

    col_names = ["Period", "xGD", "xGD Perf", "xG", "xG Pef", "Shoot Perf", "Opp Keep Perf", "xGA", "Def xG Perf", "Opp Shoot Perf", "Keep Perf"]
    col_nums = ["Period", "xGD", "GD-xGD", "xG", "G-xG", "xGoT-xG", "xGoT-GF", "xGA", "xGA-GA", "xGoTA-xGA", "xGoTA-GA"]

    # ---- Pre-filter and cache to avoid repeated full-data scans per fixture ----
    games = games.copy()
    if "home" in games.columns and "away" in games.columns:
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
        # Restrict both teams to the fixture season only.
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
                warnings.warn(warning_message, UserWarning)
                home_perf = home_perf.iloc[0:0].copy()
                away_perf = away_perf.iloc[0:0].copy()
            elif len(home_perf) < min_games or len(away_perf) < min_games:
                warning_message = (
                    f"{home} vs {away} on {cutoff.date()}: same-season sample is small "
                    f"(season_id={season_used}, home={len(home_perf)}, away={len(away_perf)}, min_games={min_games})."
                )
                warnings.warn(warning_message, UserWarning)
        elif "season_id" in form_df.columns:
            # No season boundaries available, so exact fixture-season match cannot be enforced reliably.
            warning_message = (
                f"{home} vs {away}: season boundaries unavailable in input; "
                "unable to enforce strict same-season filtering."
            )
            warnings.warn(warning_message, UserWarning)

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
