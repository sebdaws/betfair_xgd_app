"""Historical Betfair archive loading and pricing helpers."""

from __future__ import annotations

import sqlite3
import time

from xgd_app.core import *  # noqa: F401,F403

class HistoricalBetfairDataService:
    def __init__(self, state: Any) -> None:
        self.state = state
        self._verbose_timing = False
        self._historical_price_db_loaded = False
        self._historical_price_db_available = False
        self._historical_price_db_table_name: str | None = None
        self._historical_team_mapping_table_name: str | None = None
        self._historical_price_db_columns: dict[str, str] = {}
        self._historical_price_db_team_rows_cache: dict[str, pd.DataFrame] = {}
        self._historical_team_mapping_cache: dict[str, dict[str, set[str]]] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self.state, name)

    def _log_price_timing(self, label: str, step: str, started_at: float, **meta: Any) -> None:
        if not bool(getattr(self, "_verbose_timing", False)):
            return
        elapsed_ms = (time.perf_counter() - started_at) * 1000.0
        parts = [f"{k}={v}" for k, v in meta.items() if v is not None]
        meta_text = f" | {' '.join(parts)}" if parts else ""
        print(f"[HC PRICE TIMING] {label} | {step} | {elapsed_ms:.1f}ms{meta_text}", flush=True)

    @staticmethod
    def _historical_month_num_from_dir_name(value: str) -> int | None:
        text = str(value or "").strip().lower()
        if not text:
            return None
        month_num = MONTH_ABBR_TO_NUM.get(text)
        if month_num is not None:
            return int(month_num)
        if len(text) >= 3:
            month_num = MONTH_ABBR_TO_NUM.get(text[:3])
            if month_num is not None:
                return int(month_num)
        if text.isdigit():
            month_raw = int(text)
            if 1 <= month_raw <= 12:
                return month_raw
        return None

    @staticmethod
    def _build_historical_event_day_index(base_dir: Path) -> dict[str, list[Path]]:
        index: dict[str, list[Path]] = {}
        if not base_dir.exists():
            return index

        for year_dir in sorted(base_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            year_text = year_dir.name.strip()
            if not year_text.isdigit():
                continue
            year = int(year_text)

            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue
                month_num = HistoricalBetfairDataService._historical_month_num_from_dir_name(month_dir.name)
                if month_num is None:
                    continue

                for day_dir in sorted(month_dir.iterdir()):
                    if not day_dir.is_dir():
                        continue
                    day_text = day_dir.name.strip()
                    if not day_text.isdigit():
                        continue
                    day = int(day_text)
                    try:
                        day_iso = dt.date(year, month_num, day).isoformat()
                    except Exception:
                        continue

                    event_dirs = [path for path in sorted(day_dir.iterdir()) if path.is_dir()]
                    if event_dirs:
                        index[day_iso] = event_dirs
        return index

    def _historical_event_dirs_for_day(self, day_iso: str) -> list[Path]:
        day_key = str(day_iso or "").strip()
        if not day_key:
            return []

        day_ts = pd.to_datetime(day_key, errors="coerce", utc=True)
        if pd.isna(day_ts):
            return []

        base_dir = Path(self.historical_data_dir)
        if not base_dir.exists():
            return []

        year_dir = base_dir / f"{int(day_ts.year):04d}"
        if not year_dir.is_dir():
            return []

        month_num = int(day_ts.month)
        day_num = int(day_ts.day)

        month_dirs: list[Path] = []
        month_short = day_ts.strftime("%b")
        month_long = day_ts.strftime("%B")
        month_candidates = (
            month_short.lower(),
            month_short.title(),
            month_short.upper(),
            month_long.lower(),
            month_long.title(),
            month_long.upper(),
            f"{month_num:02d}",
            str(month_num),
        )
        seen_month_dirs: set[Path] = set()
        for month_name in month_candidates:
            month_dir = year_dir / month_name
            if month_dir in seen_month_dirs:
                continue
            seen_month_dirs.add(month_dir)
            if month_dir.is_dir():
                month_dirs.append(month_dir)
        if not month_dirs:
            for month_dir in sorted(year_dir.iterdir()):
                if not month_dir.is_dir():
                    continue
                resolved_month = self._historical_month_num_from_dir_name(month_dir.name)
                if resolved_month == month_num:
                    month_dirs.append(month_dir)

        if not month_dirs:
            return []

        day_candidates = (str(day_num), f"{day_num:02d}")
        out: list[Path] = []
        seen_event_dirs: set[Path] = set()
        for month_dir in month_dirs:
            for day_name in day_candidates:
                day_dir = month_dir / day_name
                if not day_dir.is_dir():
                    continue
                for event_dir in sorted(day_dir.iterdir()):
                    if not event_dir.is_dir():
                        continue
                    if event_dir in seen_event_dirs:
                        continue
                    seen_event_dirs.add(event_dir)
                    out.append(event_dir)
        return out

    @staticmethod
    def _historical_default_price_snapshot() -> dict[str, str]:
        return {
            "mainline": "-",
            "home_price": "-",
            "away_price": "-",
            "goal_mainline": "-",
            "goal_under_price": "-",
            "goal_over_price": "-",
        }

    @staticmethod
    def _historical_price_db_table_candidates() -> tuple[str, ...]:
        return (
            "betfair_historical_prices",
        )

    @staticmethod
    def _normalize_game_id_value(value: Any) -> str:
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except Exception:
            pass
        text = str(value).strip()
        if not text:
            return ""
        try:
            num = float(text)
            if pd.notna(num) and float(num).is_integer():
                return str(int(num))
        except Exception:
            pass
        return text

    @staticmethod
    def _pick_first_existing_column(columns: list[str], aliases: tuple[str, ...]) -> str | None:
        if not columns:
            return None
        by_casefold = {str(col).casefold(): str(col) for col in columns}
        for alias in aliases:
            found = by_casefold.get(str(alias).casefold())
            if found:
                return found
        return None

    @staticmethod
    def _format_db_mainline_value(value: Any) -> str:
        parsed = parse_handicap_value(value)
        if parsed is None:
            return "-"
        return format_handicap_value(parsed)

    @staticmethod
    def _format_db_goal_line_value(value: Any) -> str:
        parsed = parse_handicap_value(value)
        if parsed is None:
            return "-"
        return format_goal_line_value(parsed)

    @staticmethod
    def _format_db_price_value(value: Any) -> str:
        if value is None:
            return "-"
        try:
            if pd.isna(value):
                return "-"
        except Exception:
            pass
        text = str(value).strip()
        if not text or text == "-":
            return "-"
        try:
            return format_price_value(float(text))
        except Exception:
            return text

    def _load_historical_price_db_cache(self, force: bool = False) -> bool:
        started_at = time.perf_counter()
        label = "db_metadata"
        if self._historical_price_db_loaded and (not force):
            self._log_price_timing(label, "cache_hit", started_at, available=bool(self._historical_price_db_available))
            return bool(self._historical_price_db_available)

        self._historical_price_db_loaded = True
        self._historical_price_db_available = False
        self._historical_price_db_table_name = None
        self._historical_team_mapping_table_name = None
        self._historical_price_db_columns = {}
        self._historical_price_db_team_rows_cache = {}
        self._historical_team_mapping_cache = {}

        db_path_raw = getattr(self, "sofascore_db_path", None)
        if db_path_raw is None:
            self._log_price_timing(label, "no_db_path", started_at)
            return False
        db_path = Path(db_path_raw).expanduser().resolve()
        if not db_path.exists():
            self._log_price_timing(label, "db_missing", started_at, path=str(db_path))
            return False

        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(db_path))
            table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = [str(row[0]).strip() for row in table_rows if row and str(row[0]).strip()]
            by_casefold = {name.casefold(): name for name in table_names}

            table_name = None
            for candidate in self._historical_price_db_table_candidates():
                found = by_casefold.get(candidate.casefold())
                if found:
                    table_name = found
                    break
            if not table_name:
                self._log_price_timing(label, "table_not_found", started_at)
                return False

            self._historical_price_db_table_name = table_name
            self._historical_team_mapping_table_name = by_casefold.get("betfair_team_id_mappings")
            columns_df = pd.read_sql_query(f'PRAGMA table_info("{table_name}")', conn)
            if columns_df.empty or ("name" not in columns_df.columns):
                self._log_price_timing(label, "table_no_columns", started_at, table=table_name)
                return False
            columns = [
                str(value).strip()
                for value in columns_df["name"].tolist()
                if str(value).strip()
            ]
            if not columns:
                self._log_price_timing(label, "table_empty_columns", started_at, table=table_name)
                return False

            home_col = self._pick_first_existing_column(
                columns,
                (
                    "home_team",
                    "home",
                    "team_home",
                    "home_name",
                    "home_team_name",
                    "home_raw",
                    "home_sofa",
                    "bf_home",
                    "betfair_home_team_name",
                    "sofascore_home_team_name",
                ),
            )
            away_col = self._pick_first_existing_column(
                columns,
                (
                    "away_team",
                    "away",
                    "team_away",
                    "away_name",
                    "away_team_name",
                    "away_raw",
                    "away_sofa",
                    "bf_away",
                    "betfair_away_team_name",
                    "sofascore_away_team_name",
                ),
            )
            home_team_id_col = self._pick_first_existing_column(
                columns,
                ("home_team_id", "betfair_home_team_id"),
            )
            away_team_id_col = self._pick_first_existing_column(
                columns,
                ("away_team_id", "betfair_away_team_id"),
            )
            kickoff_col = self._pick_first_existing_column(
                columns,
                (
                    "kickoff_time",
                    "kickoff_ts",
                    "kickoff_utc",
                    "match_time",
                    "date_time",
                    "start_time",
                    "market_time",
                    "kickoff",
                    "event_time",
                    "match_datetime",
                    "utc_kickoff",
                ),
            )
            day_col = self._pick_first_existing_column(
                columns,
                ("match_day", "day", "date", "kickoff_date", "event_date"),
            )
            updated_col = self._pick_first_existing_column(
                columns,
                (
                    "timestamp",
                    "ts",
                    "pt",
                    "publish_time",
                    "price_timestamp",
                    "price_time",
                    "last_update_time",
                    "update_time",
                    "updated_at",
                ),
            )
            status_col = self._pick_first_existing_column(
                columns,
                ("status", "market_status", "selection_status", "state"),
            )
            game_id_col = self._pick_first_existing_column(
                columns,
                ("game_id", "match_id", "sofascore_match_id", "event_id", "fixture_id", "market_id"),
            )

            if not home_col or not away_col or (not kickoff_col and not day_col):
                self._log_price_timing(label, "missing_required_columns", started_at, table=table_name)
                return False

            mainline_col = self._pick_first_existing_column(
                columns,
                ("mainline", "hc_mainline", "closing_mainline", "handicap_mainline", "handicap_line", "hc_line"),
            )
            home_price_col = self._pick_first_existing_column(
                columns,
                ("home_price", "home_px", "closing_home_price", "home_odds", "h_px"),
            )
            away_price_col = self._pick_first_existing_column(
                columns,
                ("away_price", "away_px", "closing_away_price", "away_odds", "a_px"),
            )
            goal_mainline_col = self._pick_first_existing_column(
                columns,
                ("goal_mainline", "goals_mainline", "closing_goal_mainline", "goal_line", "ou_line"),
            )
            goal_under_price_col = self._pick_first_existing_column(
                columns,
                ("goal_under_price", "under_price", "ou_under_price", "closing_goal_under_price", "u_px"),
            )
            goal_over_price_col = self._pick_first_existing_column(
                columns,
                ("goal_over_price", "over_price", "ou_over_price", "closing_goal_over_price", "o_px"),
            )

            self._historical_price_db_columns = {
                "home_team": str(home_col),
                "away_team": str(away_col),
                "home_team_id": str(home_team_id_col) if home_team_id_col else "",
                "away_team_id": str(away_team_id_col) if away_team_id_col else "",
                "kickoff_time": str(kickoff_col) if kickoff_col else "",
                "match_day": str(day_col) if day_col else "",
                "updated_at": str(updated_col) if updated_col else "",
                "status": str(status_col) if status_col else "",
                "game_id": str(game_id_col) if game_id_col else "",
                "mainline": str(mainline_col) if mainline_col else "",
                "home_price": str(home_price_col) if home_price_col else "",
                "away_price": str(away_price_col) if away_price_col else "",
                "goal_mainline": str(goal_mainline_col) if goal_mainline_col else "",
                "goal_under_price": str(goal_under_price_col) if goal_under_price_col else "",
                "goal_over_price": str(goal_over_price_col) if goal_over_price_col else "",
            }
            self._historical_price_db_available = True
            self._log_price_timing(
                label,
                "metadata_loaded",
                started_at,
                table=table_name,
                columns=int(len(columns)),
            )
            return True
        except Exception:
            self._historical_price_db_available = False
            self._historical_price_db_table_name = None
            self._historical_team_mapping_table_name = None
            self._historical_price_db_columns = {}
            self._historical_price_db_team_rows_cache = {}
            self._historical_team_mapping_cache = {}
            self._log_price_timing(label, "metadata_error", started_at)
            return False
        finally:
            if conn is not None:
                conn.close()

    def _historical_price_db_select_columns(self) -> list[str]:
        col = self._historical_price_db_columns
        select_cols: list[str] = []
        for key in (
            "home_team",
            "away_team",
            "home_team_id",
            "away_team_id",
            "kickoff_time",
            "match_day",
            "updated_at",
            "status",
            "game_id",
            "mainline",
            "home_price",
            "away_price",
            "goal_mainline",
            "goal_under_price",
            "goal_over_price",
        ):
            name = str(col.get(key, "")).strip()
            if name and name not in select_cols:
                select_cols.append(name)
        return select_cols

    def _resolve_betfair_team_candidates(self, team_name: str) -> dict[str, set[str]]:
        started_at = time.perf_counter()
        team_text = str(team_name or "").strip()
        team_norm = normalize_team_name(team_text)
        label = f"team_map|{team_text}"
        empty_out = {"names": set(), "ids": set()}
        if not team_norm:
            self._log_price_timing(label, "empty_team", started_at)
            return empty_out

        cached = self._historical_team_mapping_cache.get(team_norm)
        if isinstance(cached, dict):
            out_cached = {
                "names": set(cached.get("names", set())),
                "ids": set(cached.get("ids", set())),
            }
            self._log_price_timing(
                label,
                "cache_hit",
                started_at,
                names=int(len(out_cached["names"])),
                ids=int(len(out_cached["ids"])),
            )
            return out_cached

        if not self._load_historical_price_db_cache():
            self._log_price_timing(label, "db_unavailable", started_at)
            return empty_out

        table_name = str(self._historical_team_mapping_table_name or "").strip()
        if not table_name:
            self._historical_team_mapping_cache[team_norm] = {"names": set(), "ids": set()}
            self._log_price_timing(label, "mapping_table_missing", started_at)
            return empty_out

        db_path = Path(getattr(self, "sofascore_db_path")).expanduser().resolve()
        conn: sqlite3.Connection | None = None
        out_names: set[str] = set()
        out_ids: set[str] = set()
        try:
            conn = sqlite3.connect(str(db_path))
            rows = conn.execute(
                f"""
                SELECT betfair_team_id, betfair_team_name, sofascore_team_name
                FROM "{table_name}"
                WHERE sofascore_team_name IS NOT NULL
                  AND LOWER(sofascore_team_name) = LOWER(?)
                """,
                (team_text,),
            ).fetchall()

            # Fallback: include rows where the Betfair name already matches this team text.
            if not rows:
                rows = conn.execute(
                    f"""
                    SELECT betfair_team_id, betfair_team_name, sofascore_team_name
                    FROM "{table_name}"
                    WHERE betfair_team_name IS NOT NULL
                      AND LOWER(betfair_team_name) = LOWER(?)
                    """,
                    (team_text,),
                ).fetchall()
        except Exception:
            rows = []
        finally:
            if conn is not None:
                conn.close()

        for row in rows:
            betfair_team_id = self._normalize_game_id_value(row[0] if len(row) > 0 else None)
            betfair_team_name = str(row[1] if len(row) > 1 else "").strip()
            sofascore_team_name = str(row[2] if len(row) > 2 else "").strip()
            if sofascore_team_name and normalize_team_name(sofascore_team_name) != team_norm:
                # Guard against accidental cross-matches from lower() fallback noise.
                continue
            if betfair_team_id:
                out_ids.add(betfair_team_id)
            if betfair_team_name:
                out_names.add(betfair_team_name)

        out = {"names": out_names, "ids": out_ids}
        self._historical_team_mapping_cache[team_norm] = {"names": set(out_names), "ids": set(out_ids)}
        self._log_price_timing(label, "resolved", started_at, names=int(len(out_names)), ids=int(len(out_ids)))
        return out

    def _query_historical_price_rows_for_match_ids(self, match_ids: list[Any]) -> pd.DataFrame:
        started_at = time.perf_counter()
        clean_ids = [self._normalize_game_id_value(value) for value in (match_ids or [])]
        clean_ids = sorted({value for value in clean_ids if value})
        label = f"match_id_query|count={len(clean_ids)}"
        if not clean_ids:
            self._log_price_timing(label, "empty_ids", started_at)
            return pd.DataFrame()
        if not self._load_historical_price_db_cache():
            self._log_price_timing(label, "db_unavailable", started_at)
            return pd.DataFrame()

        db_path = Path(getattr(self, "sofascore_db_path")).expanduser().resolve()
        table_name = str(self._historical_price_db_table_name or "").strip()
        col = self._historical_price_db_columns
        game_id_col = str(col.get("game_id", "")).strip()
        if not table_name or not game_id_col:
            self._log_price_timing(label, "missing_table_or_game_id_col", started_at)
            return pd.DataFrame()

        select_cols = self._historical_price_db_select_columns()
        if not select_cols:
            self._log_price_timing(label, "no_select_columns", started_at)
            return pd.DataFrame()

        quoted_select = ", ".join([f'"{name}"' for name in select_cols])
        placeholders = ", ".join(["?"] * len(clean_ids))
        sql = (
            f'SELECT rowid AS "__rowid", {quoted_select} '
            f'FROM "{table_name}" '
            f'WHERE CAST("{game_id_col}" AS TEXT) IN ({placeholders})'
        )

        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(db_path))
            out = pd.read_sql_query(sql, conn, params=clean_ids)
        except Exception:
            out = pd.DataFrame()
        finally:
            if conn is not None:
                conn.close()

        self._log_price_timing(label, "query_complete", started_at, rows=int(len(out)))
        return out

    @staticmethod
    def _historical_price_lookup_key(home_norm: str, away_norm: str, kickoff_time: pd.Timestamp) -> tuple[str, str, str]:
        kickoff_ts = pd.to_datetime(kickoff_time, errors="coerce", utc=True)
        kickoff_key = kickoff_ts.strftime("%Y-%m-%dT%H:%M") if not pd.isna(kickoff_ts) else ""
        return (
            str(home_norm or "").strip(),
            str(away_norm or "").strip(),
            kickoff_key,
        )

    def _historical_price_snapshot_from_db_row(self, row: dict[str, Any]) -> dict[str, str]:
        col = self._historical_price_db_columns
        return {
            "mainline": self._format_db_mainline_value(row.get(col.get("mainline", ""))) if col.get("mainline") else "-",
            "home_price": self._format_db_price_value(row.get(col.get("home_price", ""))) if col.get("home_price") else "-",
            "away_price": self._format_db_price_value(row.get(col.get("away_price", ""))) if col.get("away_price") else "-",
            "goal_mainline": (
                self._format_db_goal_line_value(row.get(col.get("goal_mainline", ""))) if col.get("goal_mainline") else "-"
            ),
            "goal_under_price": (
                self._format_db_price_value(row.get(col.get("goal_under_price", ""))) if col.get("goal_under_price") else "-"
            ),
            "goal_over_price": (
                self._format_db_price_value(row.get(col.get("goal_over_price", ""))) if col.get("goal_over_price") else "-"
            ),
        }

    def _query_historical_price_rows_for_team(self, team_name: str) -> pd.DataFrame:
        started_at = time.perf_counter()
        label = f"team_query|{str(team_name or '').strip()}"
        if not self._load_historical_price_db_cache():
            self._log_price_timing(label, "db_unavailable", started_at)
            return pd.DataFrame()
        team_text = str(team_name or "").strip()
        if not team_text:
            self._log_price_timing(label, "empty_team", started_at)
            return pd.DataFrame()
        team_norm = normalize_team_name(team_text)
        cached = self._historical_price_db_team_rows_cache.get(team_norm)
        if isinstance(cached, pd.DataFrame):
            self._log_price_timing(label, "cache_hit", started_at, rows=int(len(cached)))
            return cached.copy()

        db_path = Path(getattr(self, "sofascore_db_path")).expanduser().resolve()
        table_name = str(self._historical_price_db_table_name or "").strip()
        col = self._historical_price_db_columns
        if not table_name or not col.get("home_team") or not col.get("away_team"):
            self._log_price_timing(label, "missing_table_or_columns", started_at)
            return pd.DataFrame()

        select_cols = self._historical_price_db_select_columns()
        if not select_cols:
            self._log_price_timing(label, "no_select_columns", started_at)
            return pd.DataFrame()

        quoted_select = ", ".join([f'"{name}"' for name in select_cols])
        home_col = str(col.get("home_team", "")).strip()
        away_col = str(col.get("away_team", "")).strip()
        home_id_col = str(col.get("home_team_id", "")).strip()
        away_id_col = str(col.get("away_team_id", "")).strip()
        mapped = self._resolve_betfair_team_candidates(team_text)
        query_names = {team_text, *mapped.get("names", set())}
        query_names = {str(value).strip() for value in query_names if str(value).strip()}
        query_name_lc = sorted({value.casefold() for value in query_names if value})
        query_ids = sorted({str(value).strip() for value in mapped.get("ids", set()) if str(value).strip()})

        where_parts: list[str] = []
        params: list[Any] = []
        if query_name_lc:
            placeholders = ", ".join(["?"] * len(query_name_lc))
            where_parts.append(f'LOWER("{home_col}") IN ({placeholders})')
            params.extend(query_name_lc)
            where_parts.append(f'LOWER("{away_col}") IN ({placeholders})')
            params.extend(query_name_lc)
        if query_ids and home_id_col and away_id_col:
            placeholders = ", ".join(["?"] * len(query_ids))
            where_parts.append(f'CAST("{home_id_col}" AS TEXT) IN ({placeholders})')
            params.extend(query_ids)
            where_parts.append(f'CAST("{away_id_col}" AS TEXT) IN ({placeholders})')
            params.extend(query_ids)
        if not where_parts:
            self._log_price_timing(label, "no_query_candidates", started_at)
            return pd.DataFrame()
        sql = (
            f'SELECT rowid AS "__rowid", {quoted_select} '
            f'FROM "{table_name}" '
            f'WHERE ' + " OR ".join(f"({part})" for part in where_parts)
        )

        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(db_path))
            out = pd.read_sql_query(sql, conn, params=params)
        except Exception:
            out = pd.DataFrame()
        finally:
            if conn is not None:
                conn.close()

        self._historical_price_db_team_rows_cache[team_norm] = out.copy()
        self._log_price_timing(
            label,
            "query_complete",
            started_at,
            rows=int(len(out)),
            names=int(len(query_name_lc)),
            ids=int(len(query_ids)),
        )
        return out

    def _build_historical_price_entries_from_rows(self, rows_df: pd.DataFrame) -> list[dict[str, Any]]:
        started_at = time.perf_counter()
        input_rows = int(len(rows_df)) if isinstance(rows_df, pd.DataFrame) else 0
        label = "build_entries"
        if not isinstance(rows_df, pd.DataFrame) or rows_df.empty:
            self._log_price_timing(label, "empty_input", started_at, rows=input_rows)
            return []
        col = self._historical_price_db_columns
        home_col = str(col.get("home_team", "")).strip()
        away_col = str(col.get("away_team", "")).strip()
        kickoff_col = str(col.get("kickoff_time", "")).strip()
        day_col = str(col.get("match_day", "")).strip()
        updated_col = str(col.get("updated_at", "")).strip()
        status_col = str(col.get("status", "")).strip()
        game_id_col = str(col.get("game_id", "")).strip()
        if not home_col or not away_col:
            self._log_price_timing(label, "missing_home_away_columns", started_at, rows=input_rows)
            return []

        work = rows_df.copy()
        work["_home_norm"] = work[home_col].apply(normalize_team_name)
        work["_away_norm"] = work[away_col].apply(normalize_team_name)
        work["_kickoff_time"] = (
            pd.to_datetime(work[kickoff_col], errors="coerce", utc=True) if kickoff_col and kickoff_col in work.columns else pd.NaT
        )
        if day_col and day_col in work.columns:
            day_ts = pd.to_datetime(work[day_col], errors="coerce", utc=True)
            work["_kickoff_time"] = work["_kickoff_time"].where(work["_kickoff_time"].notna(), day_ts)
        work = work[
            work["_home_norm"].astype(str).str.strip().ne("")
            & work["_away_norm"].astype(str).str.strip().ne("")
            & work["_kickoff_time"].notna()
        ].copy()
        if work.empty:
            self._log_price_timing(label, "empty_after_team_kickoff_filter", started_at, rows=input_rows)
            return []

        if updated_col and updated_col in work.columns:
            work["_updated_at"] = pd.to_datetime(work[updated_col], errors="coerce", utc=True)
            updated_key = str(updated_col).strip().casefold()
            # Only apply "last update before kickoff" logic for columns that
            # represent market tick/update timestamps. Columns like "updated_at"
            # in this project are import timestamps and can be after kickoff.
            pre_kickoff_safe_keys = {
                "pt",
                "timestamp",
                "ts",
                "publish_time",
                "price_timestamp",
                "price_time",
                "last_update_time",
                "update_time",
            }
            if updated_key in pre_kickoff_safe_keys:
                work = work[(work["_updated_at"].isna()) | (work["_updated_at"] <= work["_kickoff_time"])].copy()
                if work.empty:
                    self._log_price_timing(label, "empty_after_pre_kickoff_filter", started_at, rows=input_rows)
                    return []
            else:
                self._log_price_timing(label, "skip_pre_kickoff_filter", started_at, updated_col=updated_col, rows=int(len(work)))
        else:
            work["_updated_at"] = pd.NaT

        if status_col and status_col in work.columns:
            status_series = work[status_col].fillna("").astype(str).str.strip().str.upper()
            # Keep rows with neutral/known tradable statuses and exclude explicit in-play snapshots.
            work = work[~status_series.isin({"IN_PLAY", "INPLAY"})].copy()
            if work.empty:
                self._log_price_timing(label, "empty_after_status_filter", started_at, rows=input_rows)
                return []

        work["_kickoff_key"] = work["_kickoff_time"].dt.strftime("%Y-%m-%dT%H:%M")
        if game_id_col and game_id_col in work.columns:
            gid = work[game_id_col].fillna("").astype(str).str.strip()
            work["_group_key"] = gid.where(gid.ne(""), work["_home_norm"] + "|" + work["_away_norm"] + "|" + work["_kickoff_key"])
        else:
            work["_group_key"] = work["_home_norm"] + "|" + work["_away_norm"] + "|" + work["_kickoff_key"]
        if "__rowid" not in work.columns:
            work["__rowid"] = range(1, len(work) + 1)
        work = work.sort_values(["_group_key", "_updated_at", "__rowid"], na_position="first")
        best = work.groupby("_group_key", as_index=False).tail(1).copy()

        entries: list[dict[str, Any]] = []
        for row in best.to_dict(orient="records"):
            game_id_value = self._normalize_game_id_value(row.get(game_id_col)) if game_id_col else ""
            entries.append(
                {
                    "home_norm": str(row.get("_home_norm", "")).strip(),
                    "away_norm": str(row.get("_away_norm", "")).strip(),
                    "kickoff_time": pd.to_datetime(row.get("_kickoff_time"), errors="coerce", utc=True),
                    "game_id": game_id_value,
                    "prices": self._historical_price_snapshot_from_db_row(row),
                }
            )
        self._log_price_timing(label, "complete", started_at, rows=input_rows, grouped=int(len(best)), entries=int(len(entries)))
        return entries

    def _bulk_lookup_historical_betfair_prices_for_team_fixtures(
        self,
        team_name: str,
        fixtures: list[dict[str, Any]],
    ) -> dict[tuple[str, str, str], dict[str, str]]:
        started_at = time.perf_counter()
        label = f"bulk_lookup|{str(team_name or '').strip()}"
        if not fixtures:
            self._log_price_timing(label, "empty_fixtures", started_at)
            return {}
        fixture_match_ids = [fixture.get("match_id") for fixture in fixtures]
        rows_df = self._query_historical_price_rows_for_match_ids(fixture_match_ids)
        if rows_df.empty:
            rows_df = self._query_historical_price_rows_for_team(team_name)
        entries = self._build_historical_price_entries_from_rows(rows_df)
        if not entries:
            self._log_price_timing(label, "no_entries", started_at, fixtures=int(len(fixtures)))
            return {}

        by_game_id: dict[str, list[dict[str, Any]]] = {}
        by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for entry in entries:
            game_id = self._normalize_game_id_value(entry.get("game_id"))
            if game_id:
                by_game_id.setdefault(game_id, []).append(entry)
            home_norm = str(entry.get("home_norm", "")).strip()
            away_norm = str(entry.get("away_norm", "")).strip()
            kickoff_ts = pd.to_datetime(entry.get("kickoff_time"), errors="coerce", utc=True)
            if not home_norm or not away_norm or pd.isna(kickoff_ts):
                continue
            by_pair.setdefault((home_norm, away_norm), []).append(entry)

        out: dict[tuple[str, str, str], dict[str, str]] = {}
        for fixture in fixtures:
            home_team = str(fixture.get("home_team", "")).strip()
            away_team = str(fixture.get("away_team", "")).strip()
            kickoff_time = pd.to_datetime(fixture.get("kickoff_time"), errors="coerce", utc=True)
            home_norm = normalize_team_name(home_team)
            away_norm = normalize_team_name(away_team)
            if not home_norm or not away_norm or pd.isna(kickoff_time):
                continue

            fixture_key = self._historical_price_lookup_key(home_norm, away_norm, kickoff_time)
            fixture_match_id = self._normalize_game_id_value(fixture.get("match_id"))
            pair_entries = by_game_id.get(fixture_match_id, []) if fixture_match_id else []
            if not pair_entries:
                pair_entries = by_pair.get((home_norm, away_norm), [])
            best_entry: dict[str, Any] | None = None
            best_delta_minutes = float("inf")
            kickoff_day_iso = kickoff_time.strftime("%Y-%m-%d")
            for entry in pair_entries:
                entry_kickoff = pd.to_datetime(entry.get("kickoff_time"), errors="coerce", utc=True)
                if pd.isna(entry_kickoff):
                    continue
                day_iso = entry_kickoff.strftime("%Y-%m-%d")
                delta_minutes = abs(float((entry_kickoff - kickoff_time).total_seconds()) / 60.0)
                if delta_minutes < best_delta_minutes:
                    best_delta_minutes = delta_minutes
                    best_entry = entry
                if delta_minutes <= 180 and day_iso == kickoff_day_iso:
                    break
            if best_entry is not None and best_delta_minutes <= (24 * 60):
                out[fixture_key] = dict(best_entry.get("prices", {}))
        self._log_price_timing(
            label,
            "complete",
            started_at,
            fixtures=int(len(fixtures)),
            entries=int(len(entries)),
            matched=int(len(out)),
        )
        return out

    def _lookup_historical_betfair_prices_from_db(
        self,
        home_team: str,
        away_team: str,
        home_norm: str,
        away_norm: str,
        kickoff_time: pd.Timestamp,
    ) -> dict[str, str] | None:
        started_at = time.perf_counter()
        label = f"single_lookup|{str(home_team or '').strip()}|{str(away_team or '').strip()}"
        if not self._load_historical_price_db_cache():
            self._log_price_timing(label, "db_unavailable", started_at)
            return None

        home_rows = self._query_historical_price_rows_for_team(home_team)
        away_rows = self._query_historical_price_rows_for_team(away_team)
        frames: list[pd.DataFrame] = []
        for df in (home_rows, away_rows):
            if not isinstance(df, pd.DataFrame) or df.empty:
                continue
            # Exclude all-NA frames to avoid pandas concat FutureWarning.
            if df.dropna(how="all").empty:
                continue
            frames.append(df)
        if not frames:
            combined = pd.DataFrame()
        elif len(frames) == 1:
            combined = frames[0].copy()
        else:
            combined = pd.concat(frames, ignore_index=True)
        if "__rowid" in combined.columns:
            combined = combined.drop_duplicates(subset=["__rowid"], keep="last")
        entries = self._build_historical_price_entries_from_rows(combined)
        if not entries:
            self._log_price_timing(label, "no_entries", started_at)
            return self._historical_default_price_snapshot()

        by_pair: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for entry in entries:
            pair_key = (str(entry.get("home_norm", "")).strip(), str(entry.get("away_norm", "")).strip())
            by_pair.setdefault(pair_key, []).append(entry)

        pair_entries = by_pair.get((home_norm, away_norm), [])
        best_prices: dict[str, str] | None = None
        best_delta_minutes = float("inf")
        kickoff_day_iso = kickoff_time.strftime("%Y-%m-%d")
        for entry in pair_entries:
            entry_kickoff = pd.to_datetime(entry.get("kickoff_time"), errors="coerce", utc=True)
            if pd.isna(entry_kickoff):
                continue
            day_iso = entry_kickoff.strftime("%Y-%m-%d")
            delta_minutes = abs(float((entry_kickoff - kickoff_time).total_seconds()) / 60.0)
            if delta_minutes < best_delta_minutes:
                best_delta_minutes = delta_minutes
                best_prices = dict(entry.get("prices", {}))
            if best_prices is not None and best_delta_minutes <= 180 and day_iso == kickoff_day_iso:
                break

        if best_prices is None or best_delta_minutes > (24 * 60):
            self._log_price_timing(label, "no_match", started_at, pair_entries=int(len(pair_entries)))
            return self._historical_default_price_snapshot()
        self._log_price_timing(
            label,
            "complete",
            started_at,
            pair_entries=int(len(pair_entries)),
            delta_min=f"{best_delta_minutes:.1f}",
        )
        return best_prices

    @staticmethod
    def _stream_ltp_key(selection_id: int, handicap: float | None) -> tuple[int, float | None]:
        return (selection_id, None if handicap is None else round(float(handicap), 6))

    @staticmethod
    def _lookup_stream_ltp(
        ltp_by_key: dict[tuple[int, float | None], float],
        selection_id: int,
        handicap: float | None,
    ) -> float | None:
        key_exact = HistoricalBetfairDataService._stream_ltp_key(selection_id, handicap)
        if key_exact in ltp_by_key:
            return ltp_by_key[key_exact]
        key_fallback = HistoricalBetfairDataService._stream_ltp_key(selection_id, None)
        return ltp_by_key.get(key_fallback)

    @staticmethod
    def _historical_event_stream_files(event_dir: Path) -> list[Path]:
        event_file = event_dir / f"{event_dir.name}.bz2"
        if event_file.exists():
            # Fast path for archives that provide a single consolidated event stream.
            return [event_file]
        # Some archives (e.g. older/year-specific exports) only provide
        # per-market streams inside the event folder.
        return [path for path in sorted(event_dir.glob("*.bz2")) if path.is_file()]

    def _read_historical_event_metadata(self, event_dir: Path) -> dict[str, Any] | None:
        cache_key = str(event_dir.resolve())
        if cache_key in self._historical_event_metadata_cache:
            return self._historical_event_metadata_cache[cache_key]

        event_files = self._historical_event_stream_files(event_dir)
        if not event_files:
            self._historical_event_metadata_cache[cache_key] = None
            return None

        event_name = ""
        kickoff_ts = pd.NaT
        asian_market_ids: set[str] = set()
        goal_market_ids: set[str] = set()

        try:
            for event_file in event_files:
                with bz2.open(event_file, mode="rt", encoding="utf-8", errors="replace") as f:
                    for line_no, raw_line in enumerate(f):
                        if line_no > 25:
                            break
                        line = str(raw_line or "").strip()
                        if not line:
                            continue
                        try:
                            payload = json.loads(line)
                        except Exception:
                            continue
                        for mc in payload.get("mc", []):
                            if not isinstance(mc, dict):
                                continue
                            market_id = str(mc.get("id", "")).strip()
                            market_def = mc.get("marketDefinition")
                            if not isinstance(market_def, dict):
                                continue
                            event_type = str(market_def.get("eventTypeId", "")).strip()
                            if event_type and event_type != "1":
                                continue

                            if not event_name:
                                event_name = str(market_def.get("eventName", "")).strip()
                            if pd.isna(kickoff_ts):
                                kickoff_ts = parse_iso_utc(market_def.get("marketTime"))

                            market_type = str(market_def.get("marketType", "")).strip().upper()
                            if market_id and market_type == "ASIAN_HANDICAP":
                                asian_market_ids.add(market_id)
                            elif market_id and market_type == "ALT_TOTAL_GOALS":
                                goal_market_ids.add(market_id)

                        if event_name and not pd.isna(kickoff_ts) and asian_market_ids and goal_market_ids:
                            break
                if event_name and not pd.isna(kickoff_ts) and asian_market_ids and goal_market_ids:
                    break
        except Exception:
            self._historical_event_metadata_cache[cache_key] = None
            return None

        if not event_name or pd.isna(kickoff_ts):
            self._historical_event_metadata_cache[cache_key] = None
            return None

        home_raw, away_raw = split_event_teams(event_name)
        home_sofa, _, _ = apply_manual_or_match(
            home_raw,
            team_matcher=self.team_matcher,
            manual_mapping_lookup=self.manual_mapping_lookup,
        )
        away_sofa, _, _ = apply_manual_or_match(
            away_raw,
            team_matcher=self.team_matcher,
            manual_mapping_lookup=self.manual_mapping_lookup,
            disallow={home_sofa} if home_sofa else None,
        )

        out = {
            "event_dir": event_dir,
            "event_id": str(event_dir.name),
            "event_name": event_name,
            "kickoff_time": kickoff_ts,
            "home_raw": home_raw,
            "away_raw": away_raw,
            "home_norm": normalize_team_name(home_raw),
            "away_norm": normalize_team_name(away_raw),
            "home_sofa": home_sofa,
            "away_sofa": away_sofa,
            "asian_market_ids": sorted(asian_market_ids),
            "goal_market_ids": sorted(goal_market_ids),
        }
        self._historical_event_metadata_cache[cache_key] = out
        return out

    def _read_historical_event_stream_cache(self, event_dir: Path) -> dict[str, Any]:
        cache_key = str(event_dir.resolve())
        cached = self._historical_event_stream_cache.get(cache_key)
        if cached is not None:
            return cached

        market_defs: dict[str, dict[str, Any]] = {}
        market_defs_pre: dict[str, dict[str, Any]] = {}
        ltp_by_market: dict[str, dict[tuple[int, float | None], float]] = {}
        ltp_by_market_pre: dict[str, dict[tuple[int, float | None], float]] = {}
        event_files = self._historical_event_stream_files(event_dir)
        if event_files:
            try:
                found_asian_market = False
                found_goal_market = False
                latest_relevant_market_time = pd.NaT
                cutoff_publish_ts = pd.NaT
                for event_file in event_files:
                    with bz2.open(event_file, mode="rt", encoding="utf-8", errors="replace") as f:
                        for raw_line in f:
                            line = str(raw_line or "").strip()
                            if not line:
                                continue
                            try:
                                payload = json.loads(line)
                            except Exception:
                                continue
                            publish_ts = pd.to_datetime(payload.get("pt"), unit="ms", utc=True, errors="coerce")
                            if pd.notna(cutoff_publish_ts) and pd.notna(publish_ts) and publish_ts > cutoff_publish_ts:
                                # We only need pre-kickoff prices; once we are safely past
                                # kickoff for the relevant markets, stop scanning this stream.
                                break
                            for mc in payload.get("mc", []):
                                if not isinstance(mc, dict):
                                    continue
                                market_id = str(mc.get("id", "")).strip()
                                if not market_id:
                                    continue

                                market_def = mc.get("marketDefinition")
                                if isinstance(market_def, dict):
                                    market_defs[market_id] = market_def
                                    market_type = str(market_def.get("marketType", "")).strip().upper()
                                    market_time_ts = parse_iso_utc(market_def.get("marketTime"))
                                    if market_type == "ASIAN_HANDICAP":
                                        found_asian_market = True
                                        if not pd.isna(market_time_ts):
                                            if pd.isna(latest_relevant_market_time) or market_time_ts > latest_relevant_market_time:
                                                latest_relevant_market_time = market_time_ts
                                    elif market_type == "ALT_TOTAL_GOALS":
                                        found_goal_market = True
                                        if not pd.isna(market_time_ts):
                                            if pd.isna(latest_relevant_market_time) or market_time_ts > latest_relevant_market_time:
                                                latest_relevant_market_time = market_time_ts
                                current_market_def = market_defs.get(market_id, {})
                                market_time_ts = parse_iso_utc(current_market_def.get("marketTime"))
                                market_status = str(current_market_def.get("status", "")).strip().upper()
                                in_play = bool(current_market_def.get("inPlay", False))
                                is_pre_kickoff_open = (
                                    isinstance(current_market_def, dict)
                                    and market_status == "OPEN"
                                    and (not in_play)
                                    and (pd.isna(market_time_ts) or (not pd.isna(publish_ts) and publish_ts <= market_time_ts))
                                )
                                if is_pre_kickoff_open:
                                    market_defs_pre[market_id] = dict(current_market_def)

                                rc_rows = mc.get("rc", [])
                                if not isinstance(rc_rows, list):
                                    continue
                                ltp_rows = ltp_by_market.setdefault(market_id, {})
                                pre_ltp_rows = ltp_by_market_pre.setdefault(market_id, {})
                                for rc in rc_rows:
                                    if not isinstance(rc, dict):
                                        continue
                                    ltp_value = rc.get("ltp")
                                    selection_raw = rc.get("id")
                                    if not isinstance(ltp_value, (int, float)):
                                        continue
                                    if not isinstance(selection_raw, int):
                                        continue
                                    handicap_value = parse_handicap_value(rc.get("hc"))
                                    ltp_rows[self._stream_ltp_key(int(selection_raw), handicap_value)] = float(ltp_value)
                                    if is_pre_kickoff_open:
                                        pre_ltp_rows[self._stream_ltp_key(int(selection_raw), handicap_value)] = float(ltp_value)
                            if (
                                found_asian_market
                                and found_goal_market
                                and pd.notna(latest_relevant_market_time)
                            ):
                                cutoff_publish_ts = latest_relevant_market_time + pd.Timedelta(minutes=10)
            except Exception:
                pass

        out = {
            "market_defs": market_defs,
            "market_defs_pre": market_defs_pre,
            "ltp_by_market": ltp_by_market,
            "ltp_by_market_pre": ltp_by_market_pre,
        }
        self._historical_event_stream_cache[cache_key] = out
        return out

    def _load_historical_day_events(self, day_iso: str) -> list[dict[str, Any]]:
        day_key = str(day_iso or "").strip()
        if not day_key:
            return []
        cached = self._historical_day_events_cache.get(day_key)
        if cached is not None:
            return cached

        if day_key in self._historical_event_day_index:
            event_dirs = self._historical_event_day_index.get(day_key, [])
        else:
            event_dirs = self._historical_event_dirs_for_day(day_key)
            self._historical_event_day_index[day_key] = list(event_dirs)
        out: list[dict[str, Any]] = []
        for event_dir in event_dirs:
            metadata = self._read_historical_event_metadata(event_dir)
            if metadata is None:
                continue
            out.append(metadata)

        out = sorted(
            out,
            key=lambda row: (
                str(row.get("kickoff_time", "")),
                str(row.get("event_name", "")).casefold(),
            ),
        )
        self._historical_day_events_cache[day_key] = out
        return out

    def _load_historical_day_event_lookup(
        self,
        day_iso: str,
    ) -> dict[str, dict[tuple[str, str], list[dict[str, Any]]]]:
        day_key = str(day_iso or "").strip()
        if not day_key:
            return {"by_sofa": {}, "by_raw_norm": {}}

        cached = self._historical_day_event_lookup_cache.get(day_key)
        if cached is not None:
            return cached

        by_sofa: dict[tuple[str, str], list[dict[str, Any]]] = {}
        by_raw_norm: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for event_meta in self._load_historical_day_events(day_key):
            home_sofa = str(event_meta.get("home_sofa", "")).strip()
            away_sofa = str(event_meta.get("away_sofa", "")).strip()
            if home_sofa and away_sofa:
                by_sofa.setdefault((home_sofa, away_sofa), []).append(event_meta)

            home_norm = str(event_meta.get("home_norm", "")).strip() or normalize_team_name(event_meta.get("home_raw"))
            away_norm = str(event_meta.get("away_norm", "")).strip() or normalize_team_name(event_meta.get("away_raw"))
            if home_norm and away_norm:
                by_raw_norm.setdefault((home_norm, away_norm), []).append(event_meta)

        out = {"by_sofa": by_sofa, "by_raw_norm": by_raw_norm}
        self._historical_day_event_lookup_cache[day_key] = out
        return out

    def _historical_market_mainline_snapshot_from_stream(
        self,
        event_name: str,
        market_def: dict[str, Any] | None,
        ltp_by_key: dict[tuple[int, float | None], float],
    ) -> dict[str, Any]:
        default = {
            "mainline": "-",
            "home_price": "-",
            "away_price": "-",
            "score": float("inf"),
        }
        if not isinstance(market_def, dict):
            return default

        home_team, away_team = split_event_teams(event_name)
        if not home_team or not away_team:
            return default

        home_rows: dict[float, float] = {}
        away_rows: dict[float, float] = {}
        for runner in market_def.get("runners", []):
            if not isinstance(runner, dict):
                continue
            selection_id = runner.get("id")
            if not isinstance(selection_id, int):
                continue
            runner_name = str(runner.get("name", "")).strip()
            if not runner_name:
                continue
            handicap_value = parse_handicap_value(runner.get("hc"))
            if handicap_value is None:
                handicap_value = 0.0
            price = self._lookup_stream_ltp(ltp_by_key, int(selection_id), handicap_value)
            if price is None:
                continue

            handicap_key = round(float(handicap_value), 6)
            if runner_name_matches(runner_name, home_team):
                existing = home_rows.get(handicap_key)
                if existing is None or abs(price - 2.0) < abs(existing - 2.0):
                    home_rows[handicap_key] = float(price)
            elif runner_name_matches(runner_name, away_team):
                existing = away_rows.get(handicap_key)
                if existing is None or abs(price - 2.0) < abs(existing - 2.0):
                    away_rows[handicap_key] = float(price)

        lines: list[dict[str, float]] = []
        for home_hcp, home_price in home_rows.items():
            away_hcp = round(-float(home_hcp), 6)
            away_price = away_rows.get(away_hcp)
            if away_price is None:
                continue
            score = abs(float(home_price) - 2.0) + abs(float(away_price) - 2.0)
            lines.append(
                {
                    "home_handicap": float(home_hcp),
                    "home_price": float(home_price),
                    "away_price": float(away_price),
                    "score": float(score),
                }
            )

        if not lines:
            return default

        best = min(lines, key=lambda row: (float(row["score"]), abs(float(row["home_handicap"]))))
        return {
            "mainline": format_handicap_value(best.get("home_handicap")),
            "home_price": format_price_value(best.get("home_price")),
            "away_price": format_price_value(best.get("away_price")),
            "score": float(best.get("score", float("inf"))),
        }

    def _historical_goal_mainline_snapshot_from_stream(
        self,
        market_def: dict[str, Any] | None,
        ltp_by_key: dict[tuple[int, float | None], float],
    ) -> dict[str, Any]:
        default = {
            "goal_mainline": "-",
            "goal_under_price": "-",
            "goal_over_price": "-",
            "score": float("inf"),
        }
        if not isinstance(market_def, dict):
            return default

        catalogue = {
            "marketName": str(market_def.get("name", "")).strip(),
            "marketType": str(market_def.get("marketType", "")).strip(),
            "runners": [
                {
                    "runnerName": str(runner.get("name", "")).strip(),
                    "handicap": runner.get("hc"),
                }
                for runner in market_def.get("runners", [])
                if isinstance(runner, dict)
            ],
        }
        market_level_line = parse_goal_line_from_catalogue(catalogue)

        over_by_line: dict[float, float] = {}
        under_by_line: dict[float, float] = {}
        for runner in market_def.get("runners", []):
            if not isinstance(runner, dict):
                continue
            selection_id = runner.get("id")
            if not isinstance(selection_id, int):
                continue
            runner_name = str(runner.get("name", "")).strip().lower()
            if not runner_name:
                continue
            line_value = parse_handicap_value(runner.get("hc"))
            if line_value is None:
                line_value = market_level_line
            if line_value is None:
                continue
            price = self._lookup_stream_ltp(ltp_by_key, int(selection_id), line_value)
            if price is None:
                continue
            line_key = round(float(line_value), 6)

            if "over" in runner_name:
                existing = over_by_line.get(line_key)
                if existing is None or abs(price - 2.0) < abs(existing - 2.0):
                    over_by_line[line_key] = float(price)
            elif "under" in runner_name:
                existing = under_by_line.get(line_key)
                if existing is None or abs(price - 2.0) < abs(existing - 2.0):
                    under_by_line[line_key] = float(price)

        candidate_lines = sorted(set(over_by_line.keys()) & set(under_by_line.keys()))
        if not candidate_lines:
            return default

        lines: list[dict[str, float]] = []
        for line in candidate_lines:
            over_price = over_by_line.get(line)
            under_price = under_by_line.get(line)
            if over_price is None or under_price is None:
                continue
            score = abs(float(over_price) - 2.0) + abs(float(under_price) - 2.0)
            lines.append(
                {
                    "goal_line": float(line),
                    "under_price": float(under_price),
                    "over_price": float(over_price),
                    "score": float(score),
                }
            )
        if not lines:
            return default

        best = min(lines, key=lambda row: (float(row["score"]), abs(float(row["goal_line"]) - 2.5)))
        return {
            "goal_mainline": format_goal_line_value(best.get("goal_line")),
            "goal_under_price": format_price_value(best.get("under_price")),
            "goal_over_price": format_price_value(best.get("over_price")),
            "score": float(best.get("score", float("inf"))),
        }

    def _historical_event_closing_prices(self, event_meta: dict[str, Any]) -> dict[str, str]:
        event_dir = event_meta.get("event_dir")
        if not isinstance(event_dir, Path):
            return self._historical_default_price_snapshot()
        event_id = str(event_meta.get("event_id", "")).strip()
        if event_id:
            cached_prices = self._historical_event_price_cache.get(event_id)
            if cached_prices is not None:
                return dict(cached_prices)

        stream_cache = self._read_historical_event_stream_cache(event_dir)
        market_defs: dict[str, dict[str, Any]] = stream_cache.get("market_defs", {})
        market_defs_pre: dict[str, dict[str, Any]] = stream_cache.get("market_defs_pre", {})
        ltp_by_market: dict[str, dict[tuple[int, float | None], float]] = stream_cache.get("ltp_by_market", {})
        ltp_by_market_pre: dict[str, dict[tuple[int, float | None], float]] = stream_cache.get("ltp_by_market_pre", {})

        event_name = str(event_meta.get("event_name", "")).strip()
        asian_market_ids = {
            str(value).strip()
            for value in event_meta.get("asian_market_ids", [])
            if str(value).strip()
        }
        goal_market_ids = {
            str(value).strip()
            for value in event_meta.get("goal_market_ids", [])
            if str(value).strip()
        }
        if not asian_market_ids or not goal_market_ids:
            for market_id, md in market_defs.items():
                if not isinstance(md, dict):
                    continue
                market_type = str(md.get("marketType", "")).strip().upper()
                if market_type == "ASIAN_HANDICAP":
                    asian_market_ids.add(str(market_id))
                elif market_type == "ALT_TOTAL_GOALS":
                    goal_market_ids.add(str(market_id))

        handicap_best = {
            "mainline": "-",
            "home_price": "-",
            "away_price": "-",
            "score": float("inf"),
        }
        for market_id in sorted(asian_market_ids):
            market_key = str(market_id)
            md = market_defs_pre.get(market_key) or market_defs.get(market_key, {})
            ltp_rows = ltp_by_market_pre.get(market_key) or ltp_by_market.get(market_key, {})
            snap = self._historical_market_mainline_snapshot_from_stream(event_name, md, ltp_rows)
            if float(snap.get("score", float("inf"))) < float(handicap_best.get("score", float("inf"))):
                handicap_best = snap

        goal_best = {
            "goal_mainline": "-",
            "goal_under_price": "-",
            "goal_over_price": "-",
            "score": float("inf"),
        }
        for market_id in sorted(goal_market_ids):
            market_key = str(market_id)
            md = market_defs_pre.get(market_key) or market_defs.get(market_key, {})
            ltp_rows = ltp_by_market_pre.get(market_key) or ltp_by_market.get(market_key, {})
            snap = self._historical_goal_mainline_snapshot_from_stream(md, ltp_rows)
            if float(snap.get("score", float("inf"))) < float(goal_best.get("score", float("inf"))):
                goal_best = snap

        out = {
            "mainline": str(handicap_best.get("mainline", "-") or "-"),
            "home_price": str(handicap_best.get("home_price", "-") or "-"),
            "away_price": str(handicap_best.get("away_price", "-") or "-"),
            "goal_mainline": str(goal_best.get("goal_mainline", "-") or "-"),
            "goal_under_price": str(goal_best.get("goal_under_price", "-") or "-"),
            "goal_over_price": str(goal_best.get("goal_over_price", "-") or "-"),
        }
        if event_id:
            self._historical_event_price_cache[event_id] = dict(out)
        return out

    def _lookup_historical_betfair_prices(
        self,
        home_team: str,
        away_team: str,
        kickoff_ts: Any,
    ) -> dict[str, str]:
        started_at = time.perf_counter()
        label = f"lookup|{str(home_team or '').strip()}|{str(away_team or '').strip()}"
        kickoff_time = pd.to_datetime(kickoff_ts, errors="coerce", utc=True)
        if pd.isna(kickoff_time):
            self._log_price_timing(label, "invalid_kickoff", started_at)
            return self._historical_default_price_snapshot()

        home_norm = normalize_team_name(home_team)
        away_norm = normalize_team_name(away_team)
        if not home_norm or not away_norm:
            self._log_price_timing(label, "invalid_team_names", started_at)
            return self._historical_default_price_snapshot()

        cache_key = (
            home_norm,
            away_norm,
            kickoff_time.strftime("%Y-%m-%dT%H:%M"),
        )
        cached = self._historical_match_price_cache.get(cache_key)
        if cached is not None:
            self._log_price_timing(label, "match_cache_hit", started_at)
            return dict(cached)

        db_prices = self._lookup_historical_betfair_prices_from_db(
            home_team=home_team,
            away_team=away_team,
            home_norm=home_norm,
            away_norm=away_norm,
            kickoff_time=kickoff_time,
        )
        if db_prices is not None:
            self._historical_match_price_cache[cache_key] = dict(db_prices)
            self._log_price_timing(label, "db_lookup_complete", started_at)
            return db_prices

        day_candidates = [
            kickoff_time.normalize(),
            (kickoff_time - pd.Timedelta(days=1)).normalize(),
            (kickoff_time + pd.Timedelta(days=1)).normalize(),
        ]
        best_event: dict[str, Any] | None = None
        best_delta_minutes = float("inf")

        for day_ts in day_candidates:
            day_iso = pd.to_datetime(day_ts, utc=True).strftime("%Y-%m-%d")
            day_lookup = self._load_historical_day_event_lookup(day_iso)
            candidates = list(day_lookup.get("by_sofa", {}).get((home_team, away_team), []))
            if not candidates:
                candidates = list(day_lookup.get("by_raw_norm", {}).get((home_norm, away_norm), []))
            # Fallback for any naming edge-cases not covered by the keyed lookups.
            if not candidates:
                candidates = self._load_historical_day_events(day_iso)

            for event_meta in candidates:
                event_home_sofa = str(event_meta.get("home_sofa", "")).strip()
                event_away_sofa = str(event_meta.get("away_sofa", "")).strip()
                event_home_norm = str(event_meta.get("home_norm", "")).strip() or normalize_team_name(event_meta.get("home_raw"))
                event_away_norm = str(event_meta.get("away_norm", "")).strip() or normalize_team_name(event_meta.get("away_raw"))
                if event_home_sofa and event_away_sofa:
                    if event_home_sofa != home_team or event_away_sofa != away_team:
                        if event_home_norm != home_norm or event_away_norm != away_norm:
                            continue
                else:
                    if event_home_norm != home_norm or event_away_norm != away_norm:
                        continue
                event_kickoff = pd.to_datetime(event_meta.get("kickoff_time"), errors="coerce", utc=True)
                if pd.isna(event_kickoff):
                    continue
                delta_minutes = abs(float((event_kickoff - kickoff_time).total_seconds()) / 60.0)
                if delta_minutes < best_delta_minutes:
                    best_delta_minutes = delta_minutes
                    best_event = event_meta
            if best_event is not None and best_delta_minutes <= 180 and day_iso == kickoff_time.strftime("%Y-%m-%d"):
                break

        if best_event is None or best_delta_minutes > (24 * 60):
            out = self._historical_default_price_snapshot()
            self._historical_match_price_cache[cache_key] = dict(out)
            self._log_price_timing(label, "archive_no_match", started_at)
            return out

        out = self._historical_event_closing_prices(best_event)
        self._historical_match_price_cache[cache_key] = dict(out)
        self._log_price_timing(label, "archive_lookup_complete", started_at, delta_min=f"{best_delta_minutes:.1f}")
        return out

    @staticmethod
    def _historical_market_id_from_match_id(match_id: Any) -> str:
        return f"hist:{int(match_id)}"

    @staticmethod
    def _parse_historical_match_id(market_id: Any) -> int | None:
        value = str(market_id or "").strip()
        if not value.lower().startswith("hist:"):
            return None
        raw_match_id = value.split(":", 1)[1].strip()
        if not raw_match_id:
            return None
        try:
            return int(raw_match_id)
        except Exception:
            return None

    def _selected_league_entries(self) -> list[dict[str, str]]:
        selected_entries, _ = load_selected_league_entries(DEFAULT_SELECTED_LEAGUES)
        out: list[dict[str, str]] = []
        for row in selected_entries:
            competition_name = str(row.get("competition_name", "")).strip()
            if not competition_name:
                continue
            tier = str(row.get("tier", DEFAULT_LEAGUE_TIER) or DEFAULT_LEAGUE_TIER).strip() or DEFAULT_LEAGUE_TIER
            out.append({"competition_name": competition_name, "tier": tier})
        return out

    def _selected_league_names(self) -> set[str]:
        return {
            str(row.get("competition_name", "")).strip()
            for row in self._selected_league_entries()
            if str(row.get("competition_name", "")).strip()
        }

    def _historical_competition_pool(self) -> tuple[list[str], dict[str, str], set[str]]:
        with self.lock:
            fixture_competitions = list(self.sofa_competitions)
            form_df = self.form_df.copy()

        form_competitions: set[str] = set()
        if isinstance(form_df, pd.DataFrame) and (not form_df.empty) and ("competition_name" in form_df.columns):
            form_competitions = {
                str(value).strip()
                for value in form_df["competition_name"].dropna().tolist()
                if str(value).strip()
            }

        competition_pool = sorted({*fixture_competitions, *form_competitions})
        competition_by_norm: dict[str, str] = {}
        for competition_name in competition_pool:
            comp_norm = normalize_competition_key(competition_name)
            if comp_norm and comp_norm not in competition_by_norm:
                competition_by_norm[comp_norm] = competition_name
        competition_set = set(competition_pool)
        return competition_pool, competition_by_norm, competition_set

    def _resolve_historical_competition_tiers(self) -> dict[str, str]:
        selected_entries = self._selected_league_entries()
        if not selected_entries:
            return {}

        with self.lock:
            competition_mapping_lookup = dict(self.manual_competition_mapping_lookup)
        competition_pool, competition_by_norm, competition_set = self._historical_competition_pool()

        competition_tiers: dict[str, str] = {}
        for row in selected_entries:
            raw_competition = str(row.get("competition_name", "")).strip()
            tier = str(row.get("tier", DEFAULT_LEAGUE_TIER) or DEFAULT_LEAGUE_TIER).strip() or DEFAULT_LEAGUE_TIER
            raw_norm = normalize_competition_key(raw_competition)
            if not raw_norm:
                continue
            manual_target = competition_mapping_lookup.get(raw_norm)
            if manual_target and manual_target in competition_set:
                competition_tiers.setdefault(manual_target, tier)
                continue
            matched_name, _, match_method = match_competition_name(
                raw_name=raw_competition,
                competition_names=competition_pool,
                competition_by_norm=competition_by_norm,
            )
            if matched_name and match_method not in {"missing", "unmatched"}:
                competition_tiers.setdefault(matched_name, tier)
        return competition_tiers

    def _resolve_allowed_historical_competitions(self) -> set[str]:
        return set(self._resolve_historical_competition_tiers().keys())

    def _prepare_historical_home_rows(self) -> pd.DataFrame:
        if self.form_df.empty:
            return pd.DataFrame()

        home_rows = self.form_df[
            self.form_df["venue"].fillna("").astype(str).str.strip().str.lower() == "home"
        ].copy()
        if home_rows.empty:
            return pd.DataFrame()

        home_rows["date_time"] = pd.to_datetime(home_rows["date_time"], errors="coerce", utc=True)
        home_rows = home_rows.dropna(subset=["match_id", "date_time", "team", "opponent"]).copy()
        if home_rows.empty:
            return pd.DataFrame()

        # Keep historical list aligned with selected_leagues.txt scope.
        selected_entries = self._selected_league_entries()
        if selected_entries and "competition_name" in home_rows.columns:
            competition_tiers = self._resolve_historical_competition_tiers()
            comp_names = home_rows["competition_name"].fillna("").astype(str).str.strip()
            if competition_tiers:
                home_rows = home_rows[comp_names.isin(set(competition_tiers.keys()))].copy()
                if not home_rows.empty:
                    filtered_comp_names = home_rows["competition_name"].fillna("").astype(str).str.strip()
                    home_rows["tier"] = filtered_comp_names.map(
                        lambda name: competition_tiers.get(name, DEFAULT_LEAGUE_TIER)
                    )
            else:
                # Fallback for naming drift (e.g. Betfair vs SofaScore naming variants).
                # Keep filtering strict to selected leagues via normalized key containment.
                selected_norm_to_tier: dict[str, str] = {}
                for row in selected_entries:
                    comp_name = str(row.get("competition_name", "")).strip()
                    comp_norm = normalize_competition_key(comp_name)
                    if not comp_norm:
                        continue
                    selected_norm_to_tier.setdefault(
                        comp_norm,
                        str(row.get("tier", DEFAULT_LEAGUE_TIER) or DEFAULT_LEAGUE_TIER).strip() or DEFAULT_LEAGUE_TIER,
                    )
                if not selected_norm_to_tier:
                    return pd.DataFrame()
                comp_norms = comp_names.apply(normalize_competition_key)

                def _match_selected_tier(comp_norm: str) -> str | None:
                    comp_norm_text = str(comp_norm or "").strip()
                    if not comp_norm_text:
                        return None
                    for selected_norm, tier in selected_norm_to_tier.items():
                        if competition_keys_match(comp_norm_text, selected_norm):
                            return tier
                    return None

                matched_tiers = comp_norms.apply(_match_selected_tier)
                mask = matched_tiers.notna()
                home_rows = home_rows[mask].copy()
                if not home_rows.empty:
                    home_rows["tier"] = matched_tiers[mask].astype(str).values
            if home_rows.empty:
                return pd.DataFrame()
        else:
            home_rows["tier"] = DEFAULT_LEAGUE_TIER

        for numeric_col in (
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
        ):
            if numeric_col in home_rows.columns:
                home_rows[numeric_col] = pd.to_numeric(home_rows[numeric_col], errors="coerce")

        home_rows["match_day"] = home_rows["date_time"].dt.strftime("%Y-%m-%d")
        return home_rows

    def _build_historical_games_chunk(self, home_rows: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(home_rows, pd.DataFrame) or home_rows.empty:
            return pd.DataFrame()

        rows: list[dict[str, Any]] = []
        for row in home_rows.to_dict(orient="records"):
            kickoff_ts = row.get("date_time")
            if pd.isna(kickoff_ts):
                continue

            home_goals_val = row.get("GF")
            away_goals_val = row.get("GA")
            if pd.isna(home_goals_val) or pd.isna(away_goals_val):
                continue

            home_goals = int(round(float(home_goals_val)))
            away_goals = int(round(float(away_goals_val)))
            match_id_raw = row.get("match_id")
            if pd.isna(match_id_raw):
                continue
            match_id = int(match_id_raw)
            kickoff_str = pd.to_datetime(kickoff_ts, utc=True).strftime("%Y-%m-%d %H:%M:%S UTC")
            home_team = str(row.get("team", "")).strip()
            away_team = str(row.get("opponent", "")).strip()
            competition_name = str(row.get("competition_name", "")).strip()
            area_name = str(row.get("area_name", "")).strip()
            closing_prices = self._lookup_historical_betfair_prices(
                home_team=home_team,
                away_team=away_team,
                kickoff_ts=kickoff_ts,
            )
            scoreline = f"{home_goals}-{away_goals}"

            rows.append(
                {
                    "market_id": self._historical_market_id_from_match_id(match_id),
                    "match_id": match_id,
                    "event_name": f"{home_team} v {away_team}",
                    "competition": competition_name,
                    "tier": str(row.get("tier", DEFAULT_LEAGUE_TIER) or DEFAULT_LEAGUE_TIER),
                    "market_name": "Finished",
                    "kickoff_time": kickoff_ts,
                    "kickoff_raw": kickoff_str,
                    "kickoff_utc": pd.to_datetime(kickoff_ts, utc=True).strftime("%H:%M"),
                    "home_raw": home_team,
                    "away_raw": away_team,
                    "home_sofa": home_team,
                    "away_sofa": away_team,
                    "mainline": str(closing_prices.get("mainline", "-") or "-"),
                    "home_price": str(closing_prices.get("home_price", "-") or "-"),
                    "away_price": str(closing_prices.get("away_price", "-") or "-"),
                    "goal_mainline": str(closing_prices.get("goal_mainline", "-") or "-"),
                    "goal_under_price": str(closing_prices.get("goal_under_price", "-") or "-"),
                    "goal_over_price": str(closing_prices.get("goal_over_price", "-") or "-"),
                    "scoreline": scoreline,
                    "season_xgd": None,
                    "last5_xgd": None,
                    "last3_xgd": None,
                    "season_strength": None,
                    "last5_strength": None,
                    "last3_strength": None,
                    "xgd_competition_mismatch": False,
                    "season_min_xg": None,
                    "last5_min_xg": None,
                    "last3_min_xg": None,
                    "season_max_xg": None,
                    "last5_max_xg": None,
                    "last3_max_xg": None,
                    "is_historical": True,
                    "home_goals": home_goals,
                    "away_goals": away_goals,
                    "home_xg_actual": to_native(row.get("xG")),
                    "away_xg_actual": to_native(row.get("xGA")),
                    "home_xgot_actual": to_native(row.get("xGoT")),
                    "away_xgot_actual": to_native(row.get("xGoTA")),
                    "home_corners_actual": to_native(row.get("corners_for")),
                    "away_corners_actual": to_native(row.get("corners_against")),
                    "home_cards_actual": to_native(row.get("cards_for")),
                    "away_cards_actual": to_native(row.get("cards_against")),
                    "fixture_competition": competition_name,
                    "fixture_area": area_name,
                    "fixture_season_id": to_native(row.get("season_id")),
                }
            )

        if not rows:
            return pd.DataFrame()
        out = pd.DataFrame(rows).drop_duplicates(subset=["match_id"], keep="first")
        out = out.sort_values(["kickoff_time", "competition", "event_name"]).reset_index(drop=True)
        return out

    def initialize_historical_games(self, initial_days: int = 7) -> None:
        source_rows = self._prepare_historical_home_rows()
        with self.lock:
            self._historical_source_home_rows = source_rows
            self._historical_day_values = []
            self._historical_min_day = None
            self._historical_max_day = None
            self._historical_loaded_start_day = None
            self._historical_loaded_end_day = None
            self._historical_has_more_older = False

        if source_rows.empty:
            with self.lock:
                self.state.historical_games_df = pd.DataFrame()
            return

        day_values = sorted(
            {
                str(value).strip()
                for value in source_rows["match_day"].dropna().tolist()
                if str(value).strip()
            }
        )
        if not day_values:
            with self.lock:
                self.state.historical_games_df = pd.DataFrame()
            return

        # Load the latest match day initially. Older calendar days are loaded
        # one-by-one as the user navigates backwards.
        latest_day = day_values[-1]
        day_series = source_rows["match_day"].astype(str)
        chunk_rows = source_rows[day_series == latest_day].copy()
        games_df = self._build_historical_games_chunk(chunk_rows)
        loaded_day = latest_day

        with self.lock:
            self.state.historical_games_df = games_df
            self._historical_day_values = day_values
            self._historical_min_day = day_values[0]
            self._historical_max_day = day_values[-1]
            self._historical_loaded_start_day = str(loaded_day)
            self._historical_loaded_end_day = str(loaded_day)
            self._historical_has_more_older = bool(
                self._historical_min_day
                and self._historical_loaded_start_day
                and str(self._historical_loaded_start_day) > str(self._historical_min_day)
            )

    def load_previous_historical_days(self, days: int = 7) -> dict[str, Any]:
        with self.lock:
            source_rows = self._historical_source_home_rows.copy() if isinstance(
                getattr(self, "_historical_source_home_rows", pd.DataFrame()), pd.DataFrame
            ) else pd.DataFrame()
            loaded_start = str(getattr(self, "_historical_loaded_start_day", "") or "").strip()
            min_day = str(getattr(self, "_historical_min_day", "") or "").strip()
            current_df = self.state.historical_games_df.copy()

        if source_rows.empty or not loaded_start or not min_day:
            return {"added_games": 0, "has_more_older": False}
        if loaded_start <= min_day:
            with self.lock:
                self._historical_has_more_older = False
            return {"added_games": 0, "has_more_older": False}

        loaded_start_ts = pd.to_datetime(loaded_start, errors="coerce", utc=True)
        min_day_ts = pd.to_datetime(min_day, errors="coerce", utc=True)
        if pd.isna(loaded_start_ts) or pd.isna(min_day_ts):
            with self.lock:
                self._historical_has_more_older = False
            return {"added_games": 0, "has_more_older": False}

        previous_day_ts = loaded_start_ts - pd.Timedelta(days=1)
        if previous_day_ts < min_day_ts:
            with self.lock:
                self._historical_has_more_older = False
            return {"added_games": 0, "has_more_older": False}

        previous_day = previous_day_ts.strftime("%Y-%m-%d")
        day_series = source_rows["match_day"].astype(str)
        chunk_rows = source_rows[day_series == previous_day].copy()
        chunk_df = self._build_historical_games_chunk(chunk_rows)

        if chunk_df.empty:
            with self.lock:
                self._historical_loaded_start_day = previous_day
                self._historical_has_more_older = bool(
                    self._historical_loaded_start_day
                    and str(self._historical_loaded_start_day) > str(min_day)
                )
                has_more = self._historical_has_more_older
            return {"added_games": 0, "has_more_older": bool(has_more)}

        merged_df = pd.concat([chunk_df, current_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=["match_id"], keep="first")
        merged_df = merged_df.sort_values(["kickoff_time", "competition", "event_name"]).reset_index(drop=True)

        with self.lock:
            self.state.historical_games_df = merged_df
            self._historical_loaded_start_day = previous_day
            self._historical_has_more_older = bool(previous_day > min_day)
            has_more = self._historical_has_more_older

        added_games = int(chunk_df["match_id"].nunique())
        return {"added_games": added_games, "has_more_older": bool(has_more)}

    def has_more_older_historical_days(self) -> bool:
        with self.lock:
            return bool(getattr(self, "_historical_has_more_older", False))

    def loaded_historical_day_bounds(self) -> tuple[str | None, str | None]:
        with self.lock:
            start_day = str(getattr(self, "_historical_loaded_start_day", "") or "").strip() or None
            end_day = str(getattr(self, "_historical_loaded_end_day", "") or "").strip() or None
            return start_day, end_day

    def _build_historical_games_df(self) -> pd.DataFrame:
        # Compatibility helper retained for callers expecting a full build.
        home_rows = self._prepare_historical_home_rows()
        return self._build_historical_games_chunk(home_rows)

    def rescan_loaded_historical_closing_prices(self) -> dict[str, Any]:
        with self.lock:
            historical_df = self.state.historical_games_df.copy()

        if historical_df.empty:
            return {"updated_games": 0, "changed_games": 0, "total_games": 0}

        required_cols = ("home_raw", "away_raw", "kickoff_time")
        missing = [col for col in required_cols if col not in historical_df.columns]
        if missing:
            return {
                "updated_games": 0,
                "changed_games": 0,
                "total_games": int(len(historical_df)),
                "warning": f"Missing required columns: {', '.join(missing)}",
            }

        # Force fresh archive reads for price extraction.
        with self.lock:
            self._historical_event_day_index = {}
            self._historical_day_events_cache = {}
            self._historical_day_event_lookup_cache = {}
            self._historical_event_metadata_cache = {}
            self._historical_event_stream_cache = {}
            self._historical_event_price_cache = {}
            self._historical_match_price_cache = {}
            self._historical_price_db_loaded = False
            self._historical_price_db_available = False
            self._historical_price_db_table_name = None
            self._historical_team_mapping_table_name = None
            self._historical_price_db_columns = {}
            self._historical_price_db_team_rows_cache = {}
            self._historical_team_mapping_cache = {}

        updated_df = historical_df.copy()
        changed_games = 0
        updated_games = 0
        price_cols = (
            "mainline",
            "home_price",
            "away_price",
            "goal_mainline",
            "goal_under_price",
            "goal_over_price",
        )
        for idx, row in updated_df.iterrows():
            kickoff_ts = row.get("kickoff_time")
            home_team = str(row.get("home_raw", "")).strip()
            away_team = str(row.get("away_raw", "")).strip()
            if not home_team or not away_team or pd.isna(kickoff_ts):
                continue

            closing = self._lookup_historical_betfair_prices(
                home_team=home_team,
                away_team=away_team,
                kickoff_ts=kickoff_ts,
            )
            new_values = {
                "mainline": str(closing.get("mainline", "-") or "-"),
                "home_price": str(closing.get("home_price", "-") or "-"),
                "away_price": str(closing.get("away_price", "-") or "-"),
                "goal_mainline": str(closing.get("goal_mainline", "-") or "-"),
                "goal_under_price": str(closing.get("goal_under_price", "-") or "-"),
                "goal_over_price": str(closing.get("goal_over_price", "-") or "-"),
            }
            updated_games += 1
            if any(str(row.get(col, "-") or "-") != new_values[col] for col in price_cols):
                changed_games += 1
            for col, value in new_values.items():
                updated_df.at[idx, col] = value

        with self.lock:
            self.state.historical_games_df = updated_df

        return {
            "updated_games": int(updated_games),
            "changed_games": int(changed_games),
            "total_games": int(len(updated_df)),
        }


__all__ = ["HistoricalBetfairDataService"]
