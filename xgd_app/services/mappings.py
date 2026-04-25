"""Manual team/competition mapping service."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Callable

import pandas as pd

NormalizeTeamNameFn = Callable[[str], str]
NormalizeCompetitionKeyFn = Callable[[str], str]
MapBetfairGamesFn = Callable[..., tuple[list[dict[str, Any]], Any]]
MatchCompetitionNameFn = Callable[..., tuple[str | None, float | None, str]]

MAX_UNMATCHED_TEAM_ROWS_PAYLOAD = 5000
MAX_UNMATCHED_COMPETITION_ROWS_PAYLOAD = 600
DISABLED_AUTO_TEAM_MAPPINGS_FILENAME = "disabled_auto_team_mappings.json"
DISABLED_AUTO_COMPETITION_MAPPINGS_FILENAME = "disabled_auto_competition_mappings.json"


class MappingService:
    """Owns loading, persistence, and listing for manual mappings."""

    def __init__(
        self,
        state: Any,
        *,
        normalize_team_name: NormalizeTeamNameFn,
        normalize_competition_key: NormalizeCompetitionKeyFn,
        map_betfair_games: MapBetfairGamesFn,
        match_competition_name: MatchCompetitionNameFn,
    ) -> None:
        self.state = state
        self.normalize_team_name = normalize_team_name
        self.normalize_competition_key = normalize_competition_key
        self.map_betfair_games = map_betfair_games
        self.match_competition_name = match_competition_name
        self.disabled_auto_team_mappings_path = Path(DISABLED_AUTO_TEAM_MAPPINGS_FILENAME)
        self.disabled_auto_team_mappings_by_norm: dict[str, str] = {}
        self.disabled_auto_competition_mappings_path = Path(DISABLED_AUTO_COMPETITION_MAPPINGS_FILENAME)
        self.disabled_auto_competition_mappings_by_norm: dict[str, str] = {}
        self.refresh_mapping_paths()

    def refresh_mapping_paths(self) -> None:
        manual_path = Path(getattr(self.state, "manual_mappings_path", "manual_team_mappings.json"))
        source_suffix = ""
        manual_stem = str(manual_path.stem or "").strip()
        prefix = "manual_team_mappings."
        if manual_stem.startswith(prefix):
            source_suffix = manual_stem[len(prefix):].strip()
        if source_suffix:
            disabled_filename = f"disabled_auto_team_mappings.{source_suffix}{manual_path.suffix}"
        else:
            disabled_filename = DISABLED_AUTO_TEAM_MAPPINGS_FILENAME
        self.disabled_auto_team_mappings_path = manual_path.with_name(disabled_filename)
        self.disabled_auto_team_mappings_by_norm = self._load_disabled_auto_team_mappings()

        manual_comp_path = Path(
            getattr(self.state, "manual_competition_mappings_path", "manual_competition_mappings.json")
        )
        comp_source_suffix = ""
        manual_comp_stem = str(manual_comp_path.stem or "").strip()
        comp_prefix = "manual_competition_mappings."
        if manual_comp_stem.startswith(comp_prefix):
            comp_source_suffix = manual_comp_stem[len(comp_prefix):].strip()
        if comp_source_suffix:
            disabled_comp_filename = (
                f"disabled_auto_competition_mappings.{comp_source_suffix}{manual_comp_path.suffix}"
            )
        else:
            disabled_comp_filename = DISABLED_AUTO_COMPETITION_MAPPINGS_FILENAME
        self.disabled_auto_competition_mappings_path = manual_comp_path.with_name(disabled_comp_filename)
        self.disabled_auto_competition_mappings_by_norm = self._load_disabled_auto_competition_mappings()

    def _active_db_path(self) -> Path:
        db_path_raw = getattr(
            self.state,
            "source_db_path",
            getattr(self.state, "sofascore_db_path", ""),
        )
        return Path(db_path_raw).expanduser().resolve()

    @staticmethod
    def _parse_mapping_file(path: Path) -> list[tuple[str, str]]:
        if not path.exists():
            return []
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return []

        if isinstance(raw_data, dict):
            return [(str(k), str(v)) for k, v in raw_data.items()]

        if not isinstance(raw_data, list):
            return []

        pairs: list[tuple[str, str]] = []
        for row in raw_data:
            if not isinstance(row, dict):
                continue
            raw_name = str(row.get("raw_name", "")).strip()
            sofa_name = str(row.get("sofa_name", "")).strip()
            if raw_name and sofa_name:
                pairs.append((raw_name, sofa_name))
        return pairs

    @staticmethod
    def _save_mapping_file(path: Path, mappings: dict[str, str]) -> None:
        ordered = {key: mappings[key] for key in sorted(mappings, key=str.lower)}
        body = json.dumps(ordered, indent=2, ensure_ascii=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(body + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def load_manual_team_mappings(self) -> dict[str, str]:
        pairs = self._parse_mapping_file(self.state.manual_mappings_path)
        out: dict[str, str] = {}
        for raw_name, sofa_name in pairs:
            raw_team = str(raw_name).strip()
            sofa_team = str(sofa_name).strip()
            if not raw_team or not sofa_team:
                continue
            if sofa_team not in self.state.team_matcher.team_set:
                continue
            out[raw_team] = sofa_team
        return out

    def load_manual_competition_mappings(self) -> dict[str, str]:
        pairs = self._parse_mapping_file(self.state.manual_competition_mappings_path)
        out: dict[str, str] = {}
        for raw_name, sofa_name in pairs:
            raw_comp = str(raw_name).strip()
            sofa_comp = str(sofa_name).strip()
            if not raw_comp or not sofa_comp:
                continue
            if sofa_comp not in self.state.sofa_competition_set:
                continue
            out[raw_comp] = sofa_comp
        return out

    def build_manual_mapping_lookup(self, mappings: dict[str, str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for raw_name, sofa_name in mappings.items():
            norm = self.normalize_team_name(raw_name)
            if norm:
                out[norm] = sofa_name
        return out

    def build_manual_competition_mapping_lookup(self, mappings: dict[str, str]) -> dict[str, str]:
        out: dict[str, str] = {}
        for raw_name, sofa_name in mappings.items():
            norm = self.normalize_competition_key(raw_name)
            if norm:
                out[norm] = sofa_name
        return out

    def _save_manual_team_mappings(self) -> None:
        self._save_mapping_file(self.state.manual_mappings_path, self.state.manual_team_mappings)

    def _save_manual_competition_mappings(self) -> None:
        self._save_mapping_file(
            self.state.manual_competition_mappings_path,
            self.state.manual_competition_mappings,
        )

    def _load_disabled_auto_team_mappings(self) -> dict[str, str]:
        path = Path(self.disabled_auto_team_mappings_path)
        if not path.exists():
            return {}
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

        out: dict[str, str] = {}
        if isinstance(raw_data, dict):
            for raw_norm, raw_name in raw_data.items():
                raw_text = str(raw_name).strip()
                norm_text = str(raw_norm).strip()
                if raw_text and not norm_text:
                    norm_text = self.normalize_team_name(raw_text)
                if not norm_text:
                    continue
                out[norm_text] = raw_text or norm_text
            return out

        if isinstance(raw_data, list):
            for raw_name in raw_data:
                raw_text = str(raw_name).strip()
                norm_text = self.normalize_team_name(raw_text)
                if not raw_text or not norm_text:
                    continue
                out[norm_text] = raw_text
        return out

    def _save_disabled_auto_team_mappings_locked(self) -> None:
        ordered = {
            norm: self.disabled_auto_team_mappings_by_norm[norm]
            for norm in sorted(
                self.disabled_auto_team_mappings_by_norm,
                key=lambda key: (
                    str(self.disabled_auto_team_mappings_by_norm.get(key, "")).lower(),
                    str(key).lower(),
                ),
            )
        }
        body = json.dumps(ordered, indent=2, ensure_ascii=True)
        path = Path(self.disabled_auto_team_mappings_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(body + "\n", encoding="utf-8")
        tmp_path.replace(path)

    def _load_disabled_auto_competition_mappings(self) -> dict[str, str]:
        path = Path(self.disabled_auto_competition_mappings_path)
        if not path.exists():
            return {}
        try:
            raw_data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

        out: dict[str, str] = {}
        if isinstance(raw_data, dict):
            for raw_norm, raw_name in raw_data.items():
                raw_text = str(raw_name).strip()
                norm_text = str(raw_norm).strip()
                if raw_text and not norm_text:
                    norm_text = self.normalize_competition_key(raw_text)
                if not norm_text:
                    continue
                out[norm_text] = raw_text or norm_text
            return out

        if isinstance(raw_data, list):
            for raw_name in raw_data:
                raw_text = str(raw_name).strip()
                norm_text = self.normalize_competition_key(raw_text)
                if not raw_text or not norm_text:
                    continue
                out[norm_text] = raw_text
        return out

    def _save_disabled_auto_competition_mappings_locked(self) -> None:
        ordered = {
            norm: self.disabled_auto_competition_mappings_by_norm[norm]
            for norm in sorted(
                self.disabled_auto_competition_mappings_by_norm,
                key=lambda key: (
                    str(self.disabled_auto_competition_mappings_by_norm.get(key, "")).lower(),
                    str(key).lower(),
                ),
            )
        }
        body = json.dumps(ordered, indent=2, ensure_ascii=True)
        path = Path(self.disabled_auto_competition_mappings_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(body + "\n", encoding="utf-8")
        tmp_path.replace(path)

    @staticmethod
    def _resolve_table_name_casefold(conn: sqlite3.Connection, table_name: str) -> str | None:
        target = str(table_name or "").strip().casefold()
        if not target:
            return None
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        for row in rows:
            candidate = str(row[0] if row else "").strip()
            if candidate and candidate.casefold() == target:
                return candidate
        return None

    @staticmethod
    def _table_columns_casefold(conn: sqlite3.Connection, table_name: str) -> dict[str, str]:
        rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        return {
            str(row[1]).strip().casefold(): str(row[1]).strip()
            for row in rows
            if len(row) > 1 and str(row[1]).strip()
        }

    def _sync_manual_team_mapping_to_db(self, raw_team: str, sofa_team: str) -> None:
        raw_text = str(raw_team).strip()
        sofa_text = str(sofa_team).strip()
        if not raw_text or not sofa_text:
            return

        db_path = self._active_db_path()
        if not db_path.exists():
            return

        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(db_path))
            table_name = self._resolve_table_name_casefold(conn, "betfair_team_id_mappings")
            if not table_name:
                return
            cols = self._table_columns_casefold(conn, table_name)
            betfair_name_col = cols.get("betfair_team_name")
            sofa_name_col = cols.get("sofascore_team_name")
            if not betfair_name_col or not sofa_name_col:
                return

            mapping_method_col = cols.get("mapping_method")
            mapping_score_col = cols.get("mapping_score")
            seen_count_col = cols.get("seen_count")
            updated_at_col = cols.get("updated_at")

            set_parts: list[str] = [f'"{sofa_name_col}" = ?']
            set_params: list[Any] = [sofa_text]
            if mapping_method_col:
                set_parts.append(f'"{mapping_method_col}" = ?')
                set_params.append("manual")
            if mapping_score_col:
                set_parts.append(f'"{mapping_score_col}" = ?')
                set_params.append(1.0)
            if updated_at_col:
                set_parts.append(f'"{updated_at_col}" = CURRENT_TIMESTAMP')

            update_sql = (
                f'UPDATE "{table_name}" '
                f'SET {", ".join(set_parts)} '
                f'WHERE LOWER(TRIM(COALESCE("{betfair_name_col}", \'\'))) = LOWER(TRIM(?))'
            )
            update_params = [*set_params, raw_text]
            cur = conn.execute(update_sql, update_params)
            updated_rows = int(cur.rowcount or 0)

            if updated_rows <= 0:
                insert_cols: list[str] = [f'"{betfair_name_col}"', f'"{sofa_name_col}"']
                insert_placeholders: list[str] = ["?", "?"]
                insert_params: list[Any] = [raw_text, sofa_text]

                if mapping_method_col:
                    insert_cols.append(f'"{mapping_method_col}"')
                    insert_placeholders.append("?")
                    insert_params.append("manual")
                if mapping_score_col:
                    insert_cols.append(f'"{mapping_score_col}"')
                    insert_placeholders.append("?")
                    insert_params.append(1.0)
                if seen_count_col:
                    insert_cols.append(f'"{seen_count_col}"')
                    insert_placeholders.append("?")
                    insert_params.append(0)
                if updated_at_col:
                    insert_cols.append(f'"{updated_at_col}"')
                    insert_placeholders.append("CURRENT_TIMESTAMP")

                insert_sql = (
                    f'INSERT INTO "{table_name}" ({", ".join(insert_cols)}) '
                    f'VALUES ({", ".join(insert_placeholders)})'
                )
                conn.execute(insert_sql, insert_params)

            conn.commit()
        except Exception:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
        finally:
            if conn is not None:
                conn.close()

    def _remove_manual_team_mapping_from_db(self, raw_team: str, sofa_team: str | None = None) -> None:
        raw_text = str(raw_team).strip()
        sofa_text = str(sofa_team).strip() if sofa_team is not None else ""
        if not raw_text:
            return

        db_path = self._active_db_path()
        if not db_path.exists():
            return

        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(db_path))
            table_name = self._resolve_table_name_casefold(conn, "betfair_team_id_mappings")
            if not table_name:
                return
            cols = self._table_columns_casefold(conn, table_name)
            betfair_name_col = cols.get("betfair_team_name")
            sofa_name_col = cols.get("sofascore_team_name")
            mapping_method_col = cols.get("mapping_method")
            mapping_score_col = cols.get("mapping_score")
            updated_at_col = cols.get("updated_at")
            if not betfair_name_col or not sofa_name_col or not mapping_method_col:
                return

            set_parts: list[str] = [
                f'"{sofa_name_col}" = NULL',
                f'"{mapping_method_col}" = ?',
            ]
            set_params: list[Any] = ["manual_removed"]
            if mapping_score_col:
                set_parts.append(f'"{mapping_score_col}" = NULL')
            if updated_at_col:
                set_parts.append(f'"{updated_at_col}" = CURRENT_TIMESTAMP')

            where_parts: list[str] = [
                f'LOWER(TRIM(COALESCE("{betfair_name_col}", \'\'))) = LOWER(TRIM(?))',
                f'LOWER(TRIM(COALESCE("{mapping_method_col}", \'\'))) = ?',
            ]
            where_params: list[Any] = [raw_text, "manual"]
            if sofa_text:
                where_parts.append(f'LOWER(TRIM(COALESCE("{sofa_name_col}", \'\'))) = LOWER(TRIM(?))')
                where_params.append(sofa_text)

            sql = (
                f'UPDATE "{table_name}" '
                f'SET {", ".join(set_parts)} '
                f'WHERE {" AND ".join(where_parts)}'
            )
            conn.execute(sql, [*set_params, *where_params])
            conn.commit()
        except Exception:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
        finally:
            if conn is not None:
                conn.close()

    def sync_all_manual_team_mappings_to_db(self) -> None:
        with self.state.lock:
            mappings = list(self.state.manual_team_mappings.items())
        if not mappings:
            return

        db_path = self._active_db_path()
        if not db_path.exists():
            return

        conn: sqlite3.Connection | None = None
        try:
            # Single connection + transaction keeps startup fast even with large mapping sets.
            conn = sqlite3.connect(str(db_path), timeout=1.0)
            conn.execute("PRAGMA busy_timeout = 1000")

            table_name = self._resolve_table_name_casefold(conn, "betfair_team_id_mappings")
            if not table_name:
                return
            cols = self._table_columns_casefold(conn, table_name)
            betfair_name_col = cols.get("betfair_team_name")
            sofa_name_col = cols.get("sofascore_team_name")
            if not betfair_name_col or not sofa_name_col:
                return

            mapping_method_col = cols.get("mapping_method")
            mapping_score_col = cols.get("mapping_score")
            seen_count_col = cols.get("seen_count")
            updated_at_col = cols.get("updated_at")

            set_parts: list[str] = [f'"{sofa_name_col}" = ?']
            set_param_template: list[Any] = []
            if mapping_method_col:
                set_parts.append(f'"{mapping_method_col}" = ?')
                set_param_template.append("manual")
            if mapping_score_col:
                set_parts.append(f'"{mapping_score_col}" = ?')
                set_param_template.append(1.0)
            if updated_at_col:
                set_parts.append(f'"{updated_at_col}" = CURRENT_TIMESTAMP')

            update_sql = (
                f'UPDATE "{table_name}" '
                f'SET {", ".join(set_parts)} '
                f'WHERE LOWER(TRIM(COALESCE("{betfair_name_col}", \'\'))) = LOWER(TRIM(?))'
            )

            insert_cols: list[str] = [f'"{betfair_name_col}"', f'"{sofa_name_col}"']
            insert_placeholders: list[str] = ["?", "?"]
            insert_tail_params: list[Any] = []
            if mapping_method_col:
                insert_cols.append(f'"{mapping_method_col}"')
                insert_placeholders.append("?")
                insert_tail_params.append("manual")
            if mapping_score_col:
                insert_cols.append(f'"{mapping_score_col}"')
                insert_placeholders.append("?")
                insert_tail_params.append(1.0)
            if seen_count_col:
                insert_cols.append(f'"{seen_count_col}"')
                insert_placeholders.append("?")
                insert_tail_params.append(0)
            if updated_at_col:
                insert_cols.append(f'"{updated_at_col}"')
                insert_placeholders.append("CURRENT_TIMESTAMP")

            insert_sql = (
                f'INSERT INTO "{table_name}" ({", ".join(insert_cols)}) '
                f'VALUES ({", ".join(insert_placeholders)})'
            )

            conn.execute("BEGIN IMMEDIATE")
            for raw_team, sofa_team in mappings:
                raw_text = str(raw_team).strip()
                sofa_text = str(sofa_team).strip()
                if not raw_text or not sofa_text:
                    continue

                update_params = [sofa_text, *set_param_template, raw_text]
                cur = conn.execute(update_sql, update_params)
                if int(cur.rowcount or 0) <= 0:
                    insert_params = [raw_text, sofa_text, *insert_tail_params]
                    conn.execute(insert_sql, insert_params)
            conn.commit()
        except sqlite3.OperationalError as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            print(
                "[xgd_web_app] Warning: manual mapping DB sync skipped due to SQLite lock/error: "
                f"{exc}",
                flush=True,
            )
        except Exception as exc:
            if conn is not None:
                try:
                    conn.rollback()
                except Exception:
                    pass
            print(
                "[xgd_web_app] Warning: manual mapping DB sync failed: "
                f"{exc}",
                flush=True,
            )
        finally:
            if conn is not None:
                conn.close()

    def get_manual_mapping_lookup_snapshot(self) -> dict[str, str]:
        with self.state.lock:
            return dict(self.state.manual_mapping_lookup)

    def get_disabled_auto_team_mapping_norms_snapshot(self) -> set[str]:
        with self.state.lock:
            return set(self.disabled_auto_team_mappings_by_norm.keys())

    def get_manual_competition_mapping_lookup_snapshot(self) -> dict[str, str]:
        with self.state.lock:
            return dict(self.state.manual_competition_mapping_lookup)

    def get_disabled_auto_competition_mapping_norms_snapshot(self) -> set[str]:
        with self.state.lock:
            return set(self.disabled_auto_competition_mappings_by_norm.keys())

    def _selected_betfair_competition_names(self) -> set[str]:
        games_service = getattr(self.state, "games_service", None)
        if games_service is None:
            return set()
        selected_path = getattr(games_service, "default_selected_leagues", None)
        loader = getattr(games_service, "load_selected_league_entries", None)
        if selected_path is None or not callable(loader):
            return set()
        try:
            entries, _ = loader(selected_path)
        except Exception:
            return set()
        return {
            str(row.get("competition_name", "")).strip()
            for row in entries
            if str(row.get("competition_name", "")).strip()
        }

    def _selected_betfair_competition_ids(self) -> set[str]:
        games_service = getattr(self.state, "games_service", None)
        if games_service is None:
            return set()
        selected_path = getattr(games_service, "default_selected_leagues", None)
        loader = getattr(games_service, "load_selected_league_entries", None)
        if selected_path is None or not callable(loader):
            return set()
        try:
            entries, _ = loader(selected_path)
        except Exception:
            return set()
        return {
            str(row.get("competition_id", "")).strip()
            for row in entries
            if str(row.get("competition_id", "")).strip()
        }

    def _load_historical_db_unmatched_team_rows(
        self,
        *,
        allowed_competitions: set[str] | None = None,
        allowed_competition_ids: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        db_path = self._active_db_path()
        if not db_path.exists():
            return []

        allowed_competition_norms = {
            self.normalize_competition_key(name)
            for name in (allowed_competitions or set())
            if self.normalize_competition_key(name)
        }
        allowed_competition_id_values = {
            str(value).strip()
            for value in (allowed_competition_ids or set())
            if str(value).strip()
        }

        conn: sqlite3.Connection | None = None
        try:
            conn = sqlite3.connect(str(db_path))
            table_rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = {
                str(row[0]).strip().casefold()
                for row in table_rows
                if row and str(row[0]).strip()
            }
            has_hist = "betfair_historical_prices" in table_names
            if not has_hist:
                return []

            hist_col_rows = conn.execute("PRAGMA table_info(betfair_historical_prices)").fetchall()
            hist_cols = {
                str(row[1]).strip().casefold(): str(row[1]).strip()
                for row in hist_col_rows
                if len(row) > 1 and str(row[1]).strip()
            }
            competition_col = None
            for candidate in (
                "betfair_competition_name",
                "competition_name",
                "competition",
                "event_type_name",
                "event_competition_name",
            ):
                found = hist_cols.get(candidate.casefold())
                if found:
                    competition_col = found
                    break
            competition_id_col = None
            for candidate in (
                "betfair_competition_id",
                "competition_id",
            ):
                found = hist_cols.get(candidate.casefold())
                if found:
                    competition_id_col = found
                    break

            if (
                (allowed_competition_norms or allowed_competition_id_values)
                and (not competition_col)
                and (not competition_id_col)
            ):
                return []

            comp_expr = f"TRIM(COALESCE({competition_col}, ''))" if competition_col else "''"
            comp_id_expr = f"TRIM(COALESCE({competition_id_col}, ''))" if competition_id_col else "''"

            query = """
                SELECT DISTINCT
                    TRIM(raw_name) AS raw_name,
                    TRIM(competition) AS competition,
                    TRIM(competition_id) AS competition_id,
                    0 AS seen_count
                FROM (
                    SELECT betfair_home_team_name AS raw_name, {comp_expr} AS competition, {comp_id_expr} AS competition_id
                    FROM betfair_historical_prices
                    WHERE betfair_home_team_name IS NOT NULL AND TRIM(betfair_home_team_name) <> ''
                    UNION ALL
                    SELECT betfair_away_team_name AS raw_name, {comp_expr} AS competition, {comp_id_expr} AS competition_id
                    FROM betfair_historical_prices
                    WHERE betfair_away_team_name IS NOT NULL AND TRIM(betfair_away_team_name) <> ''
                )
                WHERE raw_name IS NOT NULL AND TRIM(raw_name) <> ''
            """.format(comp_expr=comp_expr, comp_id_expr=comp_id_expr)
            params: list[Any] = []
            rows = conn.execute(query, params).fetchall()
        except Exception:
            return []
        finally:
            if conn is not None:
                conn.close()

        out: list[dict[str, Any]] = []
        seen_raw_lower: set[str] = set()
        for row in rows:
            raw_name = str(row[0] if len(row) > 0 else "").strip()
            if not raw_name:
                continue
            raw_lower = raw_name.casefold()
            if raw_lower in seen_raw_lower:
                continue
            seen_raw_lower.add(raw_lower)
            seen_count = 0
            try:
                seen_count = int(row[3] if len(row) > 3 else 0)
            except Exception:
                seen_count = 0
            competition_name = str(row[1] if len(row) > 1 else "").strip()
            competition_id = str(row[2] if len(row) > 2 else "").strip()
            if allowed_competition_norms or allowed_competition_id_values:
                matches_name = False
                matches_id = False
                if allowed_competition_norms:
                    competition_norm = self.normalize_competition_key(competition_name)
                    matches_name = bool(competition_norm and competition_norm in allowed_competition_norms)
                if allowed_competition_id_values:
                    matches_id = bool(competition_id and competition_id in allowed_competition_id_values)
                if not (matches_name or matches_id):
                    continue
            event_label = "Historical Betfair Prices"
            if seen_count > 0:
                event_label = f"Historical Betfair Prices (seen {seen_count})"
            out.append(
                {
                    "raw_name": raw_name,
                    "side": "Any",
                    "event_name": event_label,
                    "competition": competition_name or competition_id or "Historical DB",
                    "kickoff_raw": "",
                }
            )
        return out

    def _collapse_unmatched_team_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        collapsed: dict[str, dict[str, Any]] = {}
        for row in rows:
            raw_name = str(row.get("raw_name", "")).strip()
            if not raw_name:
                continue
            raw_key = raw_name.casefold()
            if not raw_key:
                continue

            side = str(row.get("side", "")).strip() or "Any"
            event_name = str(row.get("event_name", "")).strip()
            competition = str(row.get("competition", "")).strip()
            kickoff_raw = str(row.get("kickoff_raw", "")).strip()

            existing = collapsed.get(raw_key)
            if existing is None:
                collapsed[raw_key] = {
                    "raw_name": raw_name,
                    "side": side,
                    "event_name": event_name,
                    "competition": competition,
                    "kickoff_raw": kickoff_raw,
                    "occurrences": 1,
                }
                continue

            existing["occurrences"] = int(existing.get("occurrences", 1)) + 1
            existing_side = str(existing.get("side", "")).strip() or "Any"
            if existing_side != side:
                existing["side"] = "Any"

            current_kickoff = str(existing.get("kickoff_raw", "")).strip()
            should_replace_sample = False
            if kickoff_raw and (not current_kickoff or kickoff_raw < current_kickoff):
                should_replace_sample = True
            if should_replace_sample:
                existing["event_name"] = event_name
                existing["competition"] = competition
                existing["kickoff_raw"] = kickoff_raw

        return list(collapsed.values())

    def list_manual_team_mappings(self) -> dict[str, Any]:
        self.state.refresh_games(force=False)
        with self.state.lock:
            mappings = dict(self.state.manual_team_mappings)
            games_df = self.state.games_df.copy()
            form_df = self.state.form_df.copy()
            fixtures_df = self.state.fixtures_df.copy()
            manual_mapping_lookup = dict(self.state.manual_mapping_lookup)
            disabled_auto_team_norms = set(self.disabled_auto_team_mappings_by_norm.keys())
            disabled_auto_competition_norms = set(self.disabled_auto_competition_mappings_by_norm.keys())
            sofa_teams = list(self.state.team_matcher.teams)
            competition_mappings = dict(self.state.manual_competition_mappings)
            competition_mapping_lookup = dict(self.state.manual_competition_mapping_lookup)
            sofa_competitions = list(self.state.sofa_competitions)
            sofa_competition_by_norm = dict(self.state.sofa_competition_by_norm)
            sofa_competition_set = set(self.state.sofa_competitions)

        selected_betfair_competitions: set[str] = self._selected_betfair_competition_names()
        if (not selected_betfair_competitions) and (not games_df.empty) and ("competition" in games_df.columns):
            selected_betfair_competitions = {
                str(value).strip()
                for value in games_df["competition"].dropna().tolist()
                if str(value).strip()
            }

        allowed_sofa_competitions: set[str] = set()
        for raw_competition in selected_betfair_competitions:
            raw_norm = self.normalize_competition_key(raw_competition)
            if not raw_norm:
                continue
            manual_target = competition_mapping_lookup.get(raw_norm)
            if manual_target and manual_target in sofa_competition_set:
                allowed_sofa_competitions.add(manual_target)
                continue

            matched_name, _, match_method = self.match_competition_name(
                raw_name=raw_competition,
                competition_names=sofa_competitions,
                competition_by_norm=sofa_competition_by_norm,
            )
            if matched_name and match_method not in {"missing", "unmatched"}:
                allowed_sofa_competitions.add(matched_name)

        if allowed_sofa_competitions:
            filtered_sofa_team_pool: set[str] = set()
            if (
                not form_df.empty
                and "competition_name" in form_df.columns
                and "team" in form_df.columns
            ):
                form_comp_mask = (
                    form_df["competition_name"].fillna("").astype(str).str.strip().isin(allowed_sofa_competitions)
                )
                filtered_sofa_team_pool.update(
                    str(name).strip()
                    for name in form_df.loc[form_comp_mask, "team"].dropna().tolist()
                    if str(name).strip()
                )
            if not fixtures_df.empty and "competition_name" in fixtures_df.columns:
                fixture_comp_mask = (
                    fixtures_df["competition_name"].fillna("").astype(str).str.strip().isin(allowed_sofa_competitions)
                )
                for team_col in ("home_team_name", "away_team_name"):
                    if team_col not in fixtures_df.columns:
                        continue
                    filtered_sofa_team_pool.update(
                        str(name).strip()
                        for name in fixtures_df.loc[fixture_comp_mask, team_col].dropna().tolist()
                        if str(name).strip()
                    )
            filtered_sofa_team_pool.update(
                str(team_name).strip()
                for team_name in mappings.values()
                if str(team_name).strip()
            )
            if filtered_sofa_team_pool:
                sofa_teams = sorted(filtered_sofa_team_pool)

        mapped_raw_norms: set[str] = set()
        unmatched_rows: list[dict[str, Any]] = []
        seen_unmatched: set[tuple[str, str, str]] = set()
        auto_mapping_candidates: dict[str, dict[str, Any]] = {}
        manual_raw_norms = {
            self.normalize_team_name(raw_name)
            for raw_name in mappings.keys()
            if self.normalize_team_name(raw_name)
        }

        def method_rank(method: str) -> int:
            method_norm = str(method or "").strip().lower()
            if method_norm == "exact":
                return 0
            if method_norm == "fuzzy":
                return 1
            return 2

        if not games_df.empty:
            try:
                mapped_rows, _ = self.map_betfair_games(
                    betfair_games_df=games_df,
                    fixtures_df=fixtures_df,
                    team_matcher=self.state.team_matcher,
                    manual_mapping_lookup=manual_mapping_lookup,
                    blocked_auto_mapping_norms=disabled_auto_team_norms,
                )
            except Exception:
                mapped_rows = []

            if mapped_rows:
                prediction_df = pd.DataFrame(mapped_rows)
                cols = [
                    "market_id",
                    "event_name",
                    "competition",
                    "kickoff_raw",
                    "home_raw",
                    "away_raw",
                    "home_sofa",
                    "away_sofa",
                    "home_match_score",
                    "away_match_score",
                    "home_match_method",
                    "away_match_method",
                ]
                cols = [col for col in cols if col in prediction_df.columns]
                row_df = prediction_df[cols].drop_duplicates(subset=["market_id"], keep="first")
                for row in row_df.to_dict(orient="records"):
                    for side in ("home", "away"):
                        raw_name = str(row.get(f"{side}_raw", "")).strip()
                        raw_norm = self.normalize_team_name(raw_name)
                        sofa_name = str(row.get(f"{side}_sofa", "")).strip()
                        method = str(row.get(f"{side}_match_method", "")).strip().lower()
                        score_raw = row.get(f"{side}_match_score")
                        try:
                            score = float(score_raw)
                        except Exception:
                            score = -1.0
                        if not raw_name:
                            continue
                        has_mapping = bool(sofa_name and method not in {"missing", "unmatched"})
                        is_auto_mapping = bool(has_mapping and method != "manual")
                        if is_auto_mapping and raw_norm and raw_norm in disabled_auto_team_norms:
                            has_mapping = False
                        if has_mapping:
                            if raw_norm:
                                mapped_raw_norms.add(raw_norm)
                            if (
                                is_auto_mapping
                                and raw_norm
                                and raw_norm not in manual_raw_norms
                                and raw_norm not in disabled_auto_team_norms
                            ):
                                candidate = {
                                    "raw_name": raw_name,
                                    "sofa_name": sofa_name,
                                    "is_manual": False,
                                    "match_method": method or "auto",
                                    "_rank": (method_rank(method), -score, str(raw_name).lower()),
                                }
                                existing = auto_mapping_candidates.get(raw_norm)
                                if existing is None or candidate["_rank"] < existing["_rank"]:
                                    auto_mapping_candidates[raw_norm] = candidate
                            continue
                        if raw_norm and raw_norm in manual_raw_norms:
                            continue
                        unique_key = (
                            str(row.get("market_id", "")).strip(),
                            side,
                            raw_name,
                        )
                        if unique_key in seen_unmatched:
                            continue
                        seen_unmatched.add(unique_key)
                        unmatched_rows.append(
                            {
                                "raw_name": raw_name,
                                "side": "Home" if side == "home" else "Away",
                                "event_name": str(row.get("event_name", "")).strip(),
                                "competition": str(row.get("competition", "")).strip(),
                                "kickoff_raw": str(row.get("kickoff_raw", "")).strip(),
                            }
                        )

        unmatched_rows = [
            row
            for row in unmatched_rows
            if (
                self.normalize_team_name(str(row.get("raw_name", "")).strip()) not in mapped_raw_norms
                and self.normalize_team_name(str(row.get("raw_name", "")).strip()) not in manual_raw_norms
            )
        ]
        unmatched_rows = self._collapse_unmatched_team_rows(unmatched_rows)

        if auto_mapping_candidates:
            auto_mapping_candidates = {
                raw_norm: candidate
                for raw_norm, candidate in auto_mapping_candidates.items()
                if raw_norm not in manual_raw_norms and raw_norm not in disabled_auto_team_norms
            }

        mapping_rows = [
            {
                "raw_name": raw_name,
                "sofa_name": sofa_name,
                "is_manual": True,
                "match_method": "manual",
            }
            for raw_name, sofa_name in sorted(mappings.items(), key=lambda kv: kv[0].lower())
        ]
        auto_mapping_rows = sorted(
            [
                {
                    "raw_name": row.get("raw_name", ""),
                    "sofa_name": row.get("sofa_name", ""),
                    "is_manual": False,
                    "match_method": row.get("match_method", "auto"),
                }
                for row in auto_mapping_candidates.values()
            ],
            key=lambda row: str(row.get("raw_name", "")).lower(),
        )
        mapping_rows.extend(auto_mapping_rows)
        unmatched_rows = sorted(
            unmatched_rows,
            key=lambda row: (
                str(row.get("kickoff_raw", "")).strip(),
                str(row.get("event_name", "")).strip().lower(),
                str(row.get("side", "")).strip(),
                str(row.get("raw_name", "")).strip().lower(),
            ),
        )
        unmatched_total_count = len(unmatched_rows)
        if unmatched_total_count > MAX_UNMATCHED_TEAM_ROWS_PAYLOAD:
            unmatched_rows = unmatched_rows[:MAX_UNMATCHED_TEAM_ROWS_PAYLOAD]

        competition_manual_norms = {
            self.normalize_competition_key(raw_name)
            for raw_name in competition_mappings.keys()
            if self.normalize_competition_key(raw_name)
        }
        mapped_competition_norms: set[str] = set()
        unmatched_competitions: list[dict[str, Any]] = []
        auto_competition_candidates: dict[str, dict[str, Any]] = {}

        def competition_method_rank(method: str) -> int:
            method_norm = str(method or "").strip().lower()
            if method_norm == "exact":
                return 0
            if method_norm == "fuzzy":
                return 1
            return 2

        competition_stats: dict[str, dict[str, Any]] = {}

        def ensure_competition_entry(raw_name: str, kickoff_raw: str = "") -> dict[str, Any] | None:
            competition_name = str(raw_name).strip()
            if not competition_name:
                return None
            comp_norm = self.normalize_competition_key(competition_name)
            if not comp_norm:
                return None
            entry = competition_stats.get(comp_norm)
            if entry is None:
                entry = {
                    "raw_name": competition_name,
                    "raw_norm": comp_norm,
                    "games_count": 0,
                    "next_kickoff": "",
                }
                competition_stats[comp_norm] = entry
            kickoff_value = str(kickoff_raw).strip()
            if kickoff_value:
                current_next = str(entry.get("next_kickoff", "")).strip()
                if not current_next or kickoff_value < current_next:
                    entry["next_kickoff"] = kickoff_value
            return entry

        for raw_competition in sorted(selected_betfair_competitions, key=str.lower):
            ensure_competition_entry(raw_competition)

        for row in games_df.to_dict(orient="records"):
            competition_name = str(row.get("competition", "")).strip()
            if not competition_name:
                continue
            kickoff_raw = str(row.get("kickoff_raw", "")).strip()
            entry = ensure_competition_entry(competition_name, kickoff_raw)
            if entry is None:
                continue
            entry["games_count"] = int(entry.get("games_count", 0)) + 1

        for entry in competition_stats.values():
            raw_name = str(entry.get("raw_name", "")).strip()
            raw_norm = str(entry.get("raw_norm", "")).strip()
            if not raw_name or not raw_norm:
                continue
            if raw_norm in disabled_auto_competition_norms and raw_norm not in competition_manual_norms:
                unmatched_competitions.append(
                    {
                        "raw_name": raw_name,
                        "games_count": int(entry.get("games_count", 0)),
                        "next_kickoff": str(entry.get("next_kickoff", "")).strip(),
                    }
                )
                continue

            manual_target = competition_mapping_lookup.get(raw_norm)
            if manual_target and manual_target in sofa_competitions:
                mapped_competition_norms.add(raw_norm)
                continue

            matched_name, matched_score, match_method = self.match_competition_name(
                raw_name=raw_name,
                competition_names=sofa_competitions,
                competition_by_norm=sofa_competition_by_norm,
            )
            if matched_name and match_method not in {"missing", "unmatched"}:
                mapped_competition_norms.add(raw_norm)
                if raw_norm not in competition_manual_norms and raw_norm not in disabled_auto_competition_norms:
                    candidate = {
                        "raw_name": raw_name,
                        "sofa_name": matched_name,
                        "is_manual": False,
                        "match_method": match_method or "auto",
                        "_rank": (
                            competition_method_rank(match_method),
                            -(float(matched_score) if matched_score is not None else 0.0),
                            str(raw_name).lower(),
                        ),
                    }
                    existing = auto_competition_candidates.get(raw_norm)
                    if existing is None or candidate["_rank"] < existing["_rank"]:
                        auto_competition_candidates[raw_norm] = candidate
                continue

            if raw_norm in competition_manual_norms:
                continue
            unmatched_competitions.append(
                {
                    "raw_name": raw_name,
                    "games_count": int(entry.get("games_count", 0)),
                    "next_kickoff": str(entry.get("next_kickoff", "")).strip(),
                }
            )

        competition_mapping_rows = [
            {
                "raw_name": raw_name,
                "sofa_name": sofa_name,
                "is_manual": True,
                "match_method": "manual",
            }
            for raw_name, sofa_name in sorted(competition_mappings.items(), key=lambda kv: kv[0].lower())
        ]
        auto_competition_rows = sorted(
            [
                {
                    "raw_name": row.get("raw_name", ""),
                    "sofa_name": row.get("sofa_name", ""),
                    "is_manual": False,
                    "match_method": row.get("match_method", "auto"),
                }
                for row in auto_competition_candidates.values()
            ],
            key=lambda row: str(row.get("raw_name", "")).lower(),
        )
        competition_mapping_rows.extend(auto_competition_rows)
        unmatched_competitions = [
            row
            for row in unmatched_competitions
            if self.normalize_competition_key(str(row.get("raw_name", "")).strip()) not in mapped_competition_norms
            and self.normalize_competition_key(str(row.get("raw_name", "")).strip()) not in competition_manual_norms
        ]
        unmatched_competitions = sorted(
            unmatched_competitions,
            key=lambda row: (
                str(row.get("next_kickoff", "")).strip(),
                str(row.get("raw_name", "")).strip().lower(),
            ),
        )
        unmatched_competitions_total_count = len(unmatched_competitions)
        if unmatched_competitions_total_count > MAX_UNMATCHED_COMPETITION_ROWS_PAYLOAD:
            unmatched_competitions = unmatched_competitions[:MAX_UNMATCHED_COMPETITION_ROWS_PAYLOAD]

        return {
            "mappings": mapping_rows,
            "unmatched": unmatched_rows,
            "unmatched_total": unmatched_total_count,
            "sofa_teams": sofa_teams,
            "source_db_path": str(self._active_db_path()),
            "database_db_path": str(self._active_db_path()),
            "sofascore_db_path": str(self.state.sofascore_db_path),
            "manual_count": len(mappings),
            "auto_count": len(auto_mapping_rows),
            "competition_mappings": competition_mapping_rows,
            "unmatched_competitions": unmatched_competitions,
            "unmatched_competitions_total": unmatched_competitions_total_count,
            "sofa_competitions": sofa_competitions,
            "manual_competition_count": len(competition_mappings),
            "auto_competition_count": len(auto_competition_rows),
        }

    def upsert_manual_team_mapping(self, raw_name: str, sofa_name: str) -> None:
        raw_team = str(raw_name).strip()
        sofa_team = str(sofa_name).strip()
        if not raw_team:
            raise ValueError("raw_name is required")
        if not sofa_team:
            raise ValueError("sofa_name is required")
        if sofa_team not in self.state.team_matcher.team_set:
            raise ValueError(f"Unknown Database team: {sofa_team}")

        raw_norm = self.normalize_team_name(raw_team)
        with self.state.lock:
            if raw_norm:
                conflicting_keys = [
                    existing_raw
                    for existing_raw, existing_sofa in self.state.manual_team_mappings.items()
                    if (
                        str(existing_raw).strip()
                        and str(existing_raw).strip() != raw_team
                        and self.normalize_team_name(existing_raw) == raw_norm
                        and str(existing_sofa).strip() != sofa_team
                    )
                ]
                for existing_raw in conflicting_keys:
                    self.state.manual_team_mappings.pop(existing_raw, None)
                if raw_norm in self.disabled_auto_team_mappings_by_norm:
                    self.disabled_auto_team_mappings_by_norm.pop(raw_norm, None)
                    self._save_disabled_auto_team_mappings_locked()
            self.state.manual_team_mappings[raw_team] = sofa_team
            self.state.manual_mapping_lookup = self.build_manual_mapping_lookup(self.state.manual_team_mappings)
            self._save_manual_team_mappings()
        self._sync_manual_team_mapping_to_db(raw_team=raw_team, sofa_team=sofa_team)

    def upsert_manual_team_mappings_bulk(self, mappings: list[dict[str, Any]]) -> int:
        if not isinstance(mappings, list) or not mappings:
            raise ValueError("mappings must be a non-empty list")

        deduped_by_raw: dict[str, str] = {}
        for row in mappings:
            if not isinstance(row, dict):
                raise ValueError("Each mapping must be an object with raw_name and sofa_name")
            raw_team = str(row.get("raw_name", "")).strip()
            sofa_team = str(row.get("sofa_name", "")).strip()
            if not raw_team:
                raise ValueError("raw_name is required for each mapping")
            if not sofa_team:
                raise ValueError(f"sofa_name is required for '{raw_team}'")
            deduped_by_raw[raw_team] = sofa_team

        saved_count = 0
        for raw_team, sofa_team in deduped_by_raw.items():
            self.upsert_manual_team_mapping(raw_name=raw_team, sofa_name=sofa_team)
            saved_count += 1
        return saved_count

    def delete_manual_team_mapping(self, raw_name: str) -> bool:
        raw_team = str(raw_name).strip()
        if not raw_team:
            raise ValueError("raw_name is required")
        raw_norm = self.normalize_team_name(raw_team)
        deleted = False
        disabled_changed = False
        removed_sofa_team = ""
        with self.state.lock:
            keys_to_remove: list[str] = []
            if raw_norm:
                keys_to_remove = [
                    existing_raw
                    for existing_raw in list(self.state.manual_team_mappings.keys())
                    if (
                        str(existing_raw).strip()
                        and self.normalize_team_name(existing_raw) == raw_norm
                    )
                ]
            elif raw_team in self.state.manual_team_mappings:
                keys_to_remove = [raw_team]

            for key in keys_to_remove:
                if not removed_sofa_team:
                    removed_sofa_team = str(self.state.manual_team_mappings.get(key, "")).strip()
                self.state.manual_team_mappings.pop(key, None)
            if keys_to_remove:
                deleted = True

            if raw_norm:
                current = str(self.disabled_auto_team_mappings_by_norm.get(raw_norm, "")).strip()
                if current != raw_team:
                    self.disabled_auto_team_mappings_by_norm[raw_norm] = raw_team
                    disabled_changed = True
                if disabled_changed:
                    self._save_disabled_auto_team_mappings_locked()

            self.state.manual_mapping_lookup = self.build_manual_mapping_lookup(self.state.manual_team_mappings)
            self._save_manual_team_mappings()
        if deleted:
            self._remove_manual_team_mapping_from_db(raw_team=raw_team, sofa_team=removed_sofa_team or None)
        return bool(deleted or disabled_changed)

    def upsert_manual_competition_mapping(self, raw_name: str, sofa_name: str) -> None:
        raw_comp = str(raw_name).strip()
        sofa_comp = str(sofa_name).strip()
        if not raw_comp:
            raise ValueError("raw_name is required")
        if not sofa_comp:
            raise ValueError("sofa_name is required")
        if sofa_comp not in self.state.sofa_competition_set:
            raise ValueError(f"Unknown Database competition: {sofa_comp}")

        raw_norm = self.normalize_competition_key(raw_comp)
        with self.state.lock:
            if raw_norm:
                conflicting_keys = [
                    existing_raw
                    for existing_raw, existing_sofa in self.state.manual_competition_mappings.items()
                    if (
                        str(existing_raw).strip()
                        and str(existing_raw).strip() != raw_comp
                        and self.normalize_competition_key(existing_raw) == raw_norm
                        and str(existing_sofa).strip() != sofa_comp
                    )
                ]
                for existing_raw in conflicting_keys:
                    self.state.manual_competition_mappings.pop(existing_raw, None)
                if raw_norm in self.disabled_auto_competition_mappings_by_norm:
                    self.disabled_auto_competition_mappings_by_norm.pop(raw_norm, None)
                    self._save_disabled_auto_competition_mappings_locked()
            self.state.manual_competition_mappings[raw_comp] = sofa_comp
            self.state.manual_competition_mapping_lookup = self.build_manual_competition_mapping_lookup(
                self.state.manual_competition_mappings
            )
            self._save_manual_competition_mappings()

    def upsert_manual_competition_mappings_bulk(self, mappings: list[dict[str, Any]]) -> int:
        if not isinstance(mappings, list) or not mappings:
            raise ValueError("mappings must be a non-empty list")

        deduped_by_raw: dict[str, str] = {}
        for row in mappings:
            if not isinstance(row, dict):
                raise ValueError("Each mapping must be an object with raw_name and sofa_name")
            raw_comp = str(row.get("raw_name", "")).strip()
            sofa_comp = str(row.get("sofa_name", "")).strip()
            if not raw_comp:
                raise ValueError("raw_name is required for each mapping")
            if not sofa_comp:
                raise ValueError(f"sofa_name is required for '{raw_comp}'")
            deduped_by_raw[raw_comp] = sofa_comp

        saved_count = 0
        for raw_comp, sofa_comp in deduped_by_raw.items():
            self.upsert_manual_competition_mapping(raw_name=raw_comp, sofa_name=sofa_comp)
            saved_count += 1
        return saved_count

    def delete_manual_competition_mapping(self, raw_name: str) -> bool:
        raw_comp = str(raw_name).strip()
        if not raw_comp:
            raise ValueError("raw_name is required")
        deleted = False
        disabled_changed = False
        raw_norm = self.normalize_competition_key(raw_comp)
        with self.state.lock:
            keys_to_remove: list[str] = []
            if raw_norm:
                keys_to_remove = [
                    existing_raw
                    for existing_raw in list(self.state.manual_competition_mappings.keys())
                    if (
                        str(existing_raw).strip()
                        and self.normalize_competition_key(existing_raw) == raw_norm
                    )
                ]
            elif raw_comp in self.state.manual_competition_mappings:
                keys_to_remove = [raw_comp]

            for key in keys_to_remove:
                self.state.manual_competition_mappings.pop(key, None)
            if keys_to_remove:
                deleted = True

            if raw_norm:
                current = str(self.disabled_auto_competition_mappings_by_norm.get(raw_norm, "")).strip()
                if current != raw_comp:
                    self.disabled_auto_competition_mappings_by_norm[raw_norm] = raw_comp
                    disabled_changed = True
                if disabled_changed:
                    self._save_disabled_auto_competition_mappings_locked()

            self.state.manual_competition_mapping_lookup = self.build_manual_competition_mapping_lookup(
                self.state.manual_competition_mappings
            )
            self._save_manual_competition_mappings()
        return bool(deleted or disabled_changed)


__all__ = ["MappingService"]
