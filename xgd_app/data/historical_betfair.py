"""Historical Betfair archive loading and pricing helpers."""

from __future__ import annotations

from xgd_app.core import *  # noqa: F401,F403

class HistoricalBetfairDataService:
    def __init__(self, state: Any) -> None:
        self.state = state

    def __getattr__(self, name: str) -> Any:
        return getattr(self.state, name)

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
                month_num = MONTH_ABBR_TO_NUM.get(month_dir.name.strip().lower())
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

    def _read_historical_event_metadata(self, event_dir: Path) -> dict[str, Any] | None:
        cache_key = str(event_dir.resolve())
        if cache_key in self._historical_event_metadata_cache:
            return self._historical_event_metadata_cache[cache_key]

        event_file = event_dir / f"{event_dir.name}.bz2"
        if not event_file.exists():
            self._historical_event_metadata_cache[cache_key] = None
            return None

        event_name = ""
        kickoff_ts = pd.NaT
        asian_market_ids: set[str] = set()
        goal_market_ids: set[str] = set()

        try:
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

        event_file = event_dir / f"{event_dir.name}.bz2"
        market_defs: dict[str, dict[str, Any]] = {}
        ltp_by_market: dict[str, dict[tuple[int, float | None], float]] = {}
        if event_file.exists():
            try:
                with bz2.open(event_file, mode="rt", encoding="utf-8", errors="replace") as f:
                    for raw_line in f:
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
                            if not market_id:
                                continue

                            market_def = mc.get("marketDefinition")
                            if isinstance(market_def, dict):
                                market_defs[market_id] = market_def

                            rc_rows = mc.get("rc", [])
                            if not isinstance(rc_rows, list):
                                continue
                            ltp_rows = ltp_by_market.setdefault(market_id, {})
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
            except Exception:
                pass

        out = {
            "market_defs": market_defs,
            "ltp_by_market": ltp_by_market,
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

        event_dirs = self._historical_event_day_index.get(day_key, [])
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
        ltp_by_market: dict[str, dict[tuple[int, float | None], float]] = stream_cache.get("ltp_by_market", {})

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
            md = market_defs.get(str(market_id), {})
            ltp_rows = ltp_by_market.get(str(market_id), {})
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
            md = market_defs.get(str(market_id), {})
            ltp_rows = ltp_by_market.get(str(market_id), {})
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
        kickoff_time = pd.to_datetime(kickoff_ts, errors="coerce", utc=True)
        if pd.isna(kickoff_time):
            return self._historical_default_price_snapshot()

        home_norm = normalize_team_name(home_team)
        away_norm = normalize_team_name(away_team)
        if not home_norm or not away_norm:
            return self._historical_default_price_snapshot()

        cache_key = (
            home_norm,
            away_norm,
            kickoff_time.strftime("%Y-%m-%dT%H:%M"),
        )
        cached = self._historical_match_price_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        day_candidates = [
            kickoff_time.normalize(),
            (kickoff_time - pd.Timedelta(days=1)).normalize(),
            (kickoff_time + pd.Timedelta(days=1)).normalize(),
        ]
        best_event: dict[str, Any] | None = None
        best_delta_minutes = float("inf")

        for day_ts in day_candidates:
            day_iso = pd.to_datetime(day_ts, utc=True).strftime("%Y-%m-%d")
            for event_meta in self._load_historical_day_events(day_iso):
                event_home_sofa = str(event_meta.get("home_sofa", "")).strip()
                event_away_sofa = str(event_meta.get("away_sofa", "")).strip()
                if event_home_sofa and event_away_sofa:
                    if event_home_sofa != home_team or event_away_sofa != away_team:
                        continue
                else:
                    event_home_norm = normalize_team_name(event_meta.get("home_raw"))
                    event_away_norm = normalize_team_name(event_meta.get("away_raw"))
                    if event_home_norm != home_norm or event_away_norm != away_norm:
                        continue
                event_kickoff = pd.to_datetime(event_meta.get("kickoff_time"), errors="coerce", utc=True)
                if pd.isna(event_kickoff):
                    continue
                delta_minutes = abs(float((event_kickoff - kickoff_time).total_seconds()) / 60.0)
                if delta_minutes < best_delta_minutes:
                    best_delta_minutes = delta_minutes
                    best_event = event_meta

        if best_event is None or best_delta_minutes > (24 * 60):
            out = self._historical_default_price_snapshot()
            self._historical_match_price_cache[cache_key] = dict(out)
            return out

        out = self._historical_event_closing_prices(best_event)
        self._historical_match_price_cache[cache_key] = dict(out)
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

    def _build_historical_games_df(self) -> pd.DataFrame:
        if self.form_df.empty:
            return pd.DataFrame()

        home_rows = self.form_df[
            self.form_df["venue"].fillna("").astype(str).str.strip().str.lower() == "home"
        ].copy()
        if home_rows.empty:
            return pd.DataFrame()

        home_rows["date_time"] = pd.to_datetime(home_rows["date_time"], errors="coerce", utc=True)
        home_rows = home_rows.dropna(
            subset=["match_id", "date_time", "team", "opponent"]
        ).copy()
        if home_rows.empty:
            return pd.DataFrame()

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
                    "tier": DEFAULT_LEAGUE_TIER,
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


__all__ = ["HistoricalBetfairDataService"]
