"""Manual team/competition mapping service."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

import pandas as pd

NormalizeTeamNameFn = Callable[[str], str]
NormalizeCompetitionKeyFn = Callable[[str], str]
MapBetfairGamesFn = Callable[..., tuple[list[dict[str, Any]], Any]]
MatchCompetitionNameFn = Callable[..., tuple[str | None, float | None, str]]


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

    def get_manual_mapping_lookup_snapshot(self) -> dict[str, str]:
        with self.state.lock:
            return dict(self.state.manual_mapping_lookup)

    def get_manual_competition_mapping_lookup_snapshot(self) -> dict[str, str]:
        with self.state.lock:
            return dict(self.state.manual_competition_mapping_lookup)

    def list_manual_mappings(self) -> dict[str, Any]:
        return self.list_manual_team_mappings()

    def list_manual_team_mappings(self) -> dict[str, Any]:
        self.state.refresh_games(force=False)
        with self.state.lock:
            mappings = dict(self.state.manual_team_mappings)
            games_df = self.state.games_df.copy()
            form_df = self.state.form_df.copy()
            fixtures_df = self.state.fixtures_df.copy()
            manual_mapping_lookup = dict(self.state.manual_mapping_lookup)
            sofa_teams = list(self.state.team_matcher.teams)
            competition_mappings = dict(self.state.manual_competition_mappings)
            competition_mapping_lookup = dict(self.state.manual_competition_mapping_lookup)
            sofa_competitions = list(self.state.sofa_competitions)
            sofa_competition_by_norm = dict(self.state.sofa_competition_by_norm)
            sofa_competition_set = set(self.state.sofa_competitions)

        selected_betfair_competitions: set[str] = set()
        if not games_df.empty and "competition" in games_df.columns:
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
                    fixtures_df=self.state.fixtures_df,
                    team_matcher=self.state.team_matcher,
                    manual_mapping_lookup=manual_mapping_lookup,
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
                        if sofa_name and method not in {"missing", "unmatched"}:
                            if raw_norm:
                                mapped_raw_norms.add(raw_norm)
                            if method != "manual" and raw_norm and raw_norm not in manual_raw_norms:
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

        if auto_mapping_candidates:
            with self.state.lock:
                changed = False
                for raw_norm, candidate in auto_mapping_candidates.items():
                    if raw_norm in self.state.manual_mapping_lookup:
                        continue
                    raw_name = str(candidate.get("raw_name", "")).strip()
                    sofa_name = str(candidate.get("sofa_name", "")).strip()
                    if not raw_name or not sofa_name:
                        continue
                    if sofa_name not in self.state.team_matcher.team_set:
                        continue
                    self.state.manual_team_mappings[raw_name] = sofa_name
                    changed = True
                if changed:
                    self.state.manual_mapping_lookup = self.build_manual_mapping_lookup(
                        self.state.manual_team_mappings
                    )
                    self._save_manual_team_mappings()
                mappings = dict(self.state.manual_team_mappings)
                manual_mapping_lookup = dict(self.state.manual_mapping_lookup)

            manual_raw_norms = {
                self.normalize_team_name(raw_name)
                for raw_name in mappings.keys()
                if self.normalize_team_name(raw_name)
            }
            auto_mapping_candidates = {
                raw_norm: candidate
                for raw_norm, candidate in auto_mapping_candidates.items()
                if raw_norm not in manual_raw_norms
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
        unmatched_rows = [
            row
            for row in unmatched_rows
            if (
                self.normalize_team_name(str(row.get("raw_name", "")).strip()) not in mapped_raw_norms
                and self.normalize_team_name(str(row.get("raw_name", "")).strip()) not in manual_raw_norms
            )
        ]
        unmatched_rows = sorted(
            unmatched_rows,
            key=lambda row: (
                str(row.get("kickoff_raw", "")).strip(),
                str(row.get("event_name", "")).strip().lower(),
                str(row.get("side", "")).strip(),
                str(row.get("raw_name", "")).strip().lower(),
            ),
        )

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
        for row in games_df.to_dict(orient="records"):
            competition_name = str(row.get("competition", "")).strip()
            if not competition_name:
                continue
            comp_norm = self.normalize_competition_key(competition_name)
            if not comp_norm:
                continue
            entry = competition_stats.setdefault(
                competition_name,
                {
                    "raw_name": competition_name,
                    "raw_norm": comp_norm,
                    "games_count": 0,
                    "next_kickoff": str(row.get("kickoff_raw", "")).strip(),
                },
            )
            entry["games_count"] = int(entry.get("games_count", 0)) + 1
            kickoff_raw = str(row.get("kickoff_raw", "")).strip()
            if kickoff_raw:
                current_next = str(entry.get("next_kickoff", "")).strip()
                if not current_next or kickoff_raw < current_next:
                    entry["next_kickoff"] = kickoff_raw

        for entry in competition_stats.values():
            raw_name = str(entry.get("raw_name", "")).strip()
            raw_norm = str(entry.get("raw_norm", "")).strip()
            if not raw_name or not raw_norm:
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
                if raw_norm not in competition_manual_norms:
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

        return {
            "mappings": mapping_rows,
            "unmatched": unmatched_rows,
            "sofa_teams": sofa_teams,
            "sofascore_db_path": str(self.state.sofascore_db_path),
            "manual_count": len(mappings),
            "auto_count": len(auto_mapping_rows),
            "competition_mappings": competition_mapping_rows,
            "unmatched_competitions": unmatched_competitions,
            "sofa_competitions": sofa_competitions,
            "manual_competition_count": len(competition_mappings),
            "auto_competition_count": len(auto_competition_rows),
        }

    def upsert_team_mapping(self, raw_name: str, sofa_name: str) -> None:
        self.upsert_manual_team_mapping(raw_name=raw_name, sofa_name=sofa_name)

    def delete_team_mapping(self, raw_name: str) -> bool:
        return self.delete_manual_team_mapping(raw_name=raw_name)

    def upsert_competition_mapping(self, raw_name: str, sofa_name: str) -> None:
        self.upsert_manual_competition_mapping(raw_name=raw_name, sofa_name=sofa_name)

    def delete_competition_mapping(self, raw_name: str) -> bool:
        return self.delete_manual_competition_mapping(raw_name=raw_name)

    def upsert_manual_team_mapping(self, raw_name: str, sofa_name: str) -> None:
        raw_team = str(raw_name).strip()
        sofa_team = str(sofa_name).strip()
        if not raw_team:
            raise ValueError("raw_name is required")
        if not sofa_team:
            raise ValueError("sofa_name is required")
        if sofa_team not in self.state.team_matcher.team_set:
            raise ValueError(f"Unknown SofaScore team: {sofa_team}")

        with self.state.lock:
            self.state.manual_team_mappings[raw_team] = sofa_team
            self.state.manual_mapping_lookup = self.build_manual_mapping_lookup(self.state.manual_team_mappings)
            self._save_manual_team_mappings()

    def delete_manual_team_mapping(self, raw_name: str) -> bool:
        raw_team = str(raw_name).strip()
        if not raw_team:
            raise ValueError("raw_name is required")
        deleted = False
        with self.state.lock:
            if raw_team in self.state.manual_team_mappings:
                self.state.manual_team_mappings.pop(raw_team, None)
                deleted = True
            self.state.manual_mapping_lookup = self.build_manual_mapping_lookup(self.state.manual_team_mappings)
            self._save_manual_team_mappings()
        return deleted

    def upsert_manual_competition_mapping(self, raw_name: str, sofa_name: str) -> None:
        raw_comp = str(raw_name).strip()
        sofa_comp = str(sofa_name).strip()
        if not raw_comp:
            raise ValueError("raw_name is required")
        if not sofa_comp:
            raise ValueError("sofa_name is required")
        if sofa_comp not in self.state.sofa_competition_set:
            raise ValueError(f"Unknown SofaScore competition: {sofa_comp}")

        with self.state.lock:
            self.state.manual_competition_mappings[raw_comp] = sofa_comp
            self.state.manual_competition_mapping_lookup = self.build_manual_competition_mapping_lookup(
                self.state.manual_competition_mappings
            )
            self._save_manual_competition_mappings()

    def delete_manual_competition_mapping(self, raw_name: str) -> bool:
        raw_comp = str(raw_name).strip()
        if not raw_comp:
            raise ValueError("raw_name is required")
        deleted = False
        with self.state.lock:
            if raw_comp in self.state.manual_competition_mappings:
                self.state.manual_competition_mappings.pop(raw_comp, None)
                deleted = True
            self.state.manual_competition_mapping_lookup = self.build_manual_competition_mapping_lookup(
                self.state.manual_competition_mappings
            )
            self._save_manual_competition_mappings()
        return deleted


__all__ = ["MappingService"]
