#!/usr/bin/env python3
"""
Fetch pre-match football handicap prices from Betfair Exchange API.

League selection workflow:
1) Run once with --export-leagues-only to discover competitions.
2) Open all_leagues.txt and copy leagues you want into selected_leagues.txt.
3) Run normally; only leagues in selected_leagues.txt are tracked.

Usage:
  python betfair_scraper.py --export-leagues-only
  python betfair_scraper.py --selected-leagues-file selected_leagues.txt
  python betfair_scraper.py --market-types ASIAN_HANDICAP,HANDICAP
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

IDENTITY_URL = "https://identitysso-cert.betfair.com/api/certlogin"
BETTING_API_URL = "https://api.betfair.com/exchange/betting/json-rpc/v1"
FOOTBALL_EVENT_TYPE_ID = "1"
DEFAULT_MAIN_LEAGUES = [
    "Premier League",
    "La Liga",
    "Serie A",
    "Bundesliga",
    "Ligue 1",
    "UEFA Champions League",
]


@dataclass
class Competition:
    comp_id: str
    name: str


@dataclass
class Config:
    username: str | None
    password: str | None
    app_key: str
    cert_file: str | None
    key_file: str | None
    session_token: str | None
    poll_interval: float
    max_markets: int
    pre_match_only: bool
    market_types: list[str]
    selected_leagues_file: str
    all_leagues_file: str
    export_leagues_only: bool


class BetfairClient:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.session_token: str | None = None

    def login(self) -> None:
        if self.config.session_token:
            self.session_token = self.config.session_token
            return

        if not self.config.username or not self.config.password:
            raise RuntimeError(
                "No session token provided, and username/password are missing for cert login."
            )
        if not self.config.cert_file or not self.config.key_file:
            raise RuntimeError(
                "No session token provided, and certificate credentials are missing."
            )

        headers = {
            "X-Application": self.config.app_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = urllib.parse.urlencode(
            {"username": self.config.username, "password": self.config.password}
        ).encode("utf-8")

        payload = self._post_json(
            IDENTITY_URL,
            headers=headers,
            data=data,
            cert_file=self.config.cert_file,
            key_file=self.config.key_file,
        )

        if payload.get("loginStatus") != "SUCCESS":
            raise RuntimeError(f"Betfair login failed: {payload}")

        token = payload.get("sessionToken")
        if not token:
            raise RuntimeError(f"No session token returned: {payload}")

        self.session_token = token

    def _rpc(self, method: str, params: dict[str, Any]) -> Any:
        if not self.session_token:
            raise RuntimeError("Not logged in. Call login() first.")

        headers = {
            "X-Application": self.config.app_key,
            "X-Authentication": self.session_token,
            "Content-Type": "application/json",
        }
        body = {
            "jsonrpc": "2.0",
            "method": f"SportsAPING/v1.0/{method}",
            "params": params,
            "id": 1,
        }

        payload = self._post_json(
            BETTING_API_URL,
            headers=headers,
            data=json.dumps(body).encode("utf-8"),
        )

        if "error" in payload:
            raise RuntimeError(f"Betfair API error ({method}): {payload['error']}")

        return payload.get("result")

    def _post_json(
        self,
        url: str,
        headers: dict[str, str],
        data: bytes,
        cert_file: str | None = None,
        key_file: str | None = None,
    ) -> dict[str, Any]:
        request = urllib.request.Request(url=url, data=data, headers=headers, method="POST")
        context = None
        if cert_file and key_file:
            context = ssl.create_default_context()
            context.load_cert_chain(certfile=cert_file, keyfile=key_file)

        try:
            with urllib.request.urlopen(request, timeout=20, context=context) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP error {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Network error: {exc.reason}") from exc

        try:
            return json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON response from {url}: {raw[:200]}") from exc

    def list_competitions(self) -> list[Competition]:
        params = {"filter": {"eventTypeIds": [FOOTBALL_EVENT_TYPE_ID]}}
        result = self._rpc("listCompetitions", params) or []

        rows: list[Competition] = []
        for entry in result:
            comp = entry.get("competition", {})
            comp_id = comp.get("id")
            name = comp.get("name")
            if comp_id and name:
                rows.append(Competition(comp_id=str(comp_id), name=str(name)))

        rows.sort(key=lambda row: row.name.lower())
        return rows

    def list_handicap_markets(self, competition_ids: list[str]) -> list[dict[str, Any]]:
        market_filter: dict[str, Any] = {
            "eventTypeIds": [FOOTBALL_EVENT_TYPE_ID],
            "marketTypeCodes": self.config.market_types,
            "inPlayOnly": (not self.config.pre_match_only),
            "competitionIds": competition_ids,
        }

        params = {
            "filter": market_filter,
            "maxResults": str(self.config.max_markets),
            "marketProjection": [
                "RUNNER_DESCRIPTION",
                "EVENT",
                "COMPETITION",
                "MARKET_START_TIME",
            ],
            "sort": "FIRST_TO_START",
        }
        result = self._rpc("listMarketCatalogue", params)
        return result or []

    def list_market_books(self, market_ids: list[str], chunk_size: int = 25) -> list[dict[str, Any]]:
        if not market_ids:
            return []

        all_books: list[dict[str, Any]] = []
        step = max(1, chunk_size)
        for i in range(0, len(market_ids), step):
            chunk = market_ids[i : i + step]
            params = {
                "marketIds": chunk,
                "priceProjection": {
                    "priceData": ["EX_BEST_OFFERS"],
                    "virtualise": True,
                    "exBestOffersOverrides": {"bestPricesDepth": 1},
                },
            }
            result = self._rpc("listMarketBook", params) or []
            all_books.extend(result)
        return all_books


def get_best_price(ex_data: dict[str, Any], side: str) -> float | None:
    offers = ex_data.get(side, [])
    if not offers:
        return None
    prices = [offer.get("price") for offer in offers if isinstance(offer.get("price"), (int, float))]
    if not prices:
        return None
    return max(prices) if side == "availableToBack" else min(prices)


def to_utc_now_str() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_price(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "-"


def parse_handicap(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def price_midpoint(back: float | None, lay: float | None) -> float | None:
    if back is None or lay is None:
        return None
    return (back + lay) / 2.0


def format_handicap(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return ""
    if float(value).is_integer():
        return f"{int(value):+d}"
    return f"{value:+g}"


def build_runner_info_map(catalogue: dict[str, Any]) -> dict[int, dict[str, Any]]:
    info: dict[int, dict[str, Any]] = {}
    for runner in catalogue.get("runners", []):
        selection_id = runner.get("selectionId")
        runner_name = runner.get("runnerName")
        if isinstance(selection_id, int) and isinstance(runner_name, str):
            info[selection_id] = {
                "name": runner_name,
                "handicap": runner.get("handicap"),
            }
    return info


def split_event_teams(event_name: str) -> tuple[str | None, str | None]:
    separators = [" v ", " vs ", " @ "]
    lowered = event_name.lower()
    for sep in separators:
        idx = lowered.find(sep)
        if idx != -1:
            left = event_name[:idx].strip()
            right = event_name[idx + len(sep) :].strip()
            if left and right:
                return left, right
    return None, None


def normalize_name(value: str) -> str:
    return " ".join(value.lower().replace(".", " ").replace("-", " ").split())


def name_matches(candidate: str, target: str) -> bool:
    c = normalize_name(candidate)
    t = normalize_name(target)
    return c == t or c in t or t in c


def build_line_window_rows(
    catalogue: dict[str, Any],
    book: dict[str, Any],
    window_each_side: int = 5,
) -> list[dict[str, Any]]:
    event_name = str(catalogue.get("event", {}).get("name", ""))
    home_team, away_team = split_event_teams(event_name)
    runner_info = build_runner_info_map(catalogue)

    home_candidates: list[dict[str, Any]] = []
    away_candidates: list[dict[str, Any]] = []
    for runner in book.get("runners", []):
        sid = runner.get("selectionId")
        info = runner_info.get(sid, {})
        base_name = str(info.get("name", f"Selection {sid}"))
        handicap = parse_handicap(runner.get("handicap", info.get("handicap")))
        if handicap is None:
            continue
        ex = runner.get("ex", {})
        back = get_best_price(ex, "availableToBack")
        lay = get_best_price(ex, "availableToLay")
        if back is None or lay is None:
            continue

        row = {
            "base_name": base_name,
            "handicap": handicap,
            "back": back,
            "lay": lay,
            "status": runner.get("status", "UNKNOWN"),
        }
        if home_team and name_matches(base_name, home_team):
            home_candidates.append(row)
        elif away_team and name_matches(base_name, away_team):
            away_candidates.append(row)

    # Keep the best row per handicap key for each side (tightest spread).
    def best_by_handicap(rows: list[dict[str, Any]]) -> dict[float, dict[str, Any]]:
        out: dict[float, dict[str, Any]] = {}
        for row in rows:
            key = round(float(row["handicap"]), 6)
            spread = float(row["lay"]) - float(row["back"])
            existing = out.get(key)
            if existing is None:
                out[key] = row
                continue
            existing_spread = float(existing["lay"]) - float(existing["back"])
            if spread < existing_spread:
                out[key] = row
        return out

    home_by_hcp = best_by_handicap(home_candidates)
    away_by_hcp = best_by_handicap(away_candidates)

    # Build ordered lines keyed by home handicap.
    lines: list[dict[str, Any]] = []
    for home_hcp_key, home_row in home_by_hcp.items():
        away_hcp_key = round(-home_hcp_key, 6)
        away_row = away_by_hcp.get(away_hcp_key)
        if away_row is None:
            continue

        home_mid = price_midpoint(home_row["back"], home_row["lay"])
        away_mid = price_midpoint(away_row["back"], away_row["lay"])
        if home_mid is None or away_mid is None:
            continue

        lines.append(
            {
                "home_handicap": float(home_row["handicap"]),
                "score": abs(home_mid - 2.0) + abs(away_mid - 2.0),
                "rows": [home_row, away_row],
            }
        )

    if not lines:
        return []

    lines.sort(key=lambda line: line["home_handicap"])

    main_index = 0
    best_score = lines[0]["score"]
    for idx, line in enumerate(lines):
        if line["score"] < best_score:
            best_score = line["score"]
            main_index = idx

    start_idx = max(0, main_index - window_each_side)
    end_idx = min(len(lines), main_index + window_each_side + 1)

    output_rows: list[dict[str, Any]] = []
    for line in lines[start_idx:end_idx]:
        for row in line["rows"]:
            handicap_label = format_handicap(row["handicap"])
            display_name = (
                f"{row['base_name']} ({handicap_label})" if handicap_label else row["base_name"]
            )
            output_rows.append(
                {
                    "runner": display_name,
                    "back": row["back"],
                    "lay": row["lay"],
                    "status": row["status"],
                }
            )
    return output_rows


def parse_list_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def write_all_leagues_file(path: str, competitions: list[Competition]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("# Auto-generated from Betfair listCompetitions\n")
        f.write("# Format: competition_id|competition_name\n")
        for comp in competitions:
            f.write(f"{comp.comp_id}|{comp.name}\n")


def ensure_selected_leagues_file(path: str) -> None:
    if os.path.exists(path):
        return

    with open(path, "w", encoding="utf-8") as f:
        f.write("# Format: competition_id|competition_name\n")
        f.write("# Lines starting with # are ignored\n")
        f.write("# Example:\n")
        f.write("# 10932509|English Premier League\n")


def load_selected_leagues(path: str) -> tuple[list[Competition], list[str]]:
    if not os.path.exists(path):
        return [], []

    selected: list[Competition] = []
    invalid_lines: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            value = line.strip()
            if not value or value.startswith("#"):
                continue
            if "|" not in value:
                invalid_lines.append(value)
                continue
            comp_id, comp_name = value.split("|", 1)
            comp_id = comp_id.strip()
            comp_name = comp_name.strip()
            if not comp_id or not comp_name:
                invalid_lines.append(value)
                continue
            selected.append(Competition(comp_id=comp_id, name=comp_name))
    return selected, invalid_lines


def find_selected_competition_ids(
    competitions: list[Competition], selected_values: list[Competition]
) -> tuple[list[str], list[str]]:
    by_id = {comp.comp_id: comp for comp in competitions}

    ids: list[str] = []
    not_found: list[str] = []

    for selected in selected_values:
        comp = by_id.get(selected.comp_id)
        if comp:
            ids.append(selected.comp_id)
        else:
            not_found.append(f"{selected.comp_id}|{selected.name}")

    deduped: list[str] = []
    seen: set[str] = set()
    for comp_id in ids:
        if comp_id not in seen:
            seen.add(comp_id)
            deduped.append(comp_id)

    return deduped, not_found


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pre-match football handicap prices from Betfair API")
    parser.add_argument("--config-module", default="betfair_credentials")
    parser.add_argument("--username")
    parser.add_argument("--password")
    parser.add_argument("--app-key")
    parser.add_argument("--session-token")
    parser.add_argument("--cert-file")
    parser.add_argument("--key-file")
    parser.add_argument("--poll-interval", type=float, default=3.0)
    parser.add_argument("--max-markets", type=int, default=40)
    parser.add_argument("--include-live", action="store_true")
    parser.add_argument("--market-types", default="ASIAN_HANDICAP")
    parser.add_argument("--selected-leagues-file", default="selected_leagues.txt")
    parser.add_argument("--all-leagues-file", default="all_leagues.txt")
    parser.add_argument("--export-leagues-only", action="store_true")
    return parser.parse_args()


def load_module_settings(module_name: str) -> dict[str, str | None]:
    module_filename = f"{module_name}.py"
    if not os.path.exists(module_filename):
        return {}

    spec = importlib.util.spec_from_file_location(module_name, module_filename)
    if spec is None or spec.loader is None:
        raise ValueError(f"Could not load config module from {module_filename}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {
        "username": getattr(module, "BETFAIR_USERNAME", None),
        "password": getattr(module, "BETFAIR_PASSWORD", None),
        "app_key": getattr(module, "BETFAIR_APP_KEY", None),
        "session_token": getattr(module, "BETFAIR_SESSION_TOKEN", None),
        "cert_file": getattr(module, "BETFAIR_CERT_FILE", None),
        "key_file": getattr(module, "BETFAIR_KEY_FILE", None),
    }


def resolve_setting(cli_value: str | None, env_name: str, file_value: str | None) -> str | None:
    return cli_value or os.getenv(env_name) or file_value


def validate_config(args: argparse.Namespace) -> Config:
    file_settings = load_module_settings(args.config_module)

    resolved = {
        "username": resolve_setting(args.username, "BETFAIR_USERNAME", file_settings.get("username")),
        "password": resolve_setting(args.password, "BETFAIR_PASSWORD", file_settings.get("password")),
        "app_key": resolve_setting(args.app_key, "BETFAIR_APP_KEY", file_settings.get("app_key")),
        "session_token": resolve_setting(
            args.session_token, "BETFAIR_SESSION_TOKEN", file_settings.get("session_token")
        ),
        "cert_file": resolve_setting(args.cert_file, "BETFAIR_CERT_FILE", file_settings.get("cert_file")),
        "key_file": resolve_setting(args.key_file, "BETFAIR_KEY_FILE", file_settings.get("key_file")),
    }

    if not resolved["app_key"]:
        raise ValueError("Missing app_key. Provide --app-key, BETFAIR_APP_KEY, or config module value.")

    has_session_token = bool(resolved["session_token"])
    has_cert_login_bundle = bool(
        resolved["username"] and resolved["password"] and resolved["cert_file"] and resolved["key_file"]
    )
    if not has_session_token and not has_cert_login_bundle:
        raise ValueError(
            "Provide BETFAIR_SESSION_TOKEN (or --session-token), or provide username/password + "
            "BETFAIR_CERT_FILE + BETFAIR_KEY_FILE for cert login."
        )

    market_types = parse_list_csv(args.market_types)
    if not market_types:
        raise ValueError("At least one market type is required (e.g. ASIAN_HANDICAP).")

    return Config(
        username=(str(resolved["username"]) if resolved["username"] else None),
        password=(str(resolved["password"]) if resolved["password"] else None),
        app_key=str(resolved["app_key"]),
        session_token=(str(resolved["session_token"]) if resolved["session_token"] else None),
        cert_file=(str(resolved["cert_file"]) if resolved["cert_file"] else None),
        key_file=(str(resolved["key_file"]) if resolved["key_file"] else None),
        poll_interval=args.poll_interval,
        max_markets=args.max_markets,
        pre_match_only=(not args.include_live),
        market_types=market_types,
        selected_leagues_file=args.selected_leagues_file,
        all_leagues_file=args.all_leagues_file,
        export_leagues_only=args.export_leagues_only,
    )


def run() -> None:
    args = parse_args()
    config = validate_config(args)
    client = BetfairClient(config)

    print("Authenticating with Betfair...")
    client.login()
    if config.session_token:
        print("Session token loaded.")
    else:
        print("Login successful.")

    competitions = client.list_competitions()
    write_all_leagues_file(config.all_leagues_file, competitions)
    print(f"Exported {len(competitions)} leagues to {config.all_leagues_file}")

    ensure_selected_leagues_file(config.selected_leagues_file)
    selected_values, invalid_lines = load_selected_leagues(config.selected_leagues_file)

    if config.export_leagues_only:
        print(f"Edit {config.selected_leagues_file} to pick leagues, then run again without --export-leagues-only.")
        return

    if invalid_lines:
        print(
            "Warning: invalid lines in selected leagues file (expected competition_id|competition_name): "
            + ", ".join(invalid_lines),
            file=sys.stderr,
        )

    if not selected_values:
        print(
            f"No leagues selected in {config.selected_leagues_file}. Add competition_id|competition_name lines and rerun.",
            file=sys.stderr,
        )
        return

    competition_ids, not_found = find_selected_competition_ids(competitions, selected_values)
    if not_found:
        print(
            f"Warning: {len(not_found)} selected league entries were not found: {', '.join(not_found)}",
            file=sys.stderr,
        )

    if not competition_ids:
        print("No valid selected leagues matched Betfair competitions.", file=sys.stderr)
        return

    print(f"Using {len(competition_ids)} selected leagues from {config.selected_leagues_file}")
    print(f"Market types: {', '.join(config.market_types)}")
    print(f"Pre-match only: {config.pre_match_only}")

    while True:
        try:
            catalogues = client.list_handicap_markets(competition_ids)
            if not catalogues:
                print(f"[{to_utc_now_str()}] No handicap markets found for selected leagues.")
                time.sleep(config.poll_interval)
                continue

            market_ids = [c["marketId"] for c in catalogues if "marketId" in c]
            books = client.list_market_books(market_ids)
            books_by_id = {b["marketId"]: b for b in books if "marketId" in b}

            print(f"\n[{to_utc_now_str()}] Football handicap prices")
            print("-" * 110)
            for cat in catalogues:
                market_id = cat["marketId"]
                event_name = cat.get("event", {}).get("name", "Unknown event")
                competition_name = cat.get("competition", {}).get("name", "Unknown competition")
                market_name = cat.get("marketName", "Unknown market")
                start_time = cat.get("marketStartTime", "")
                book = books_by_id.get(market_id, {})
                line_rows = build_line_window_rows(cat, book, window_each_side=5)
                filtered_rows = [
                    f"  - {row['runner']:<32} BACK {format_price(row['back']):>5} | "
                    f"LAY {format_price(row['lay']):>5} | {row['status']}"
                    for row in line_rows
                ]

                if not filtered_rows:
                    continue

                print(
                    f"{competition_name} | {event_name} | {market_name} | {market_id} | Start: {start_time}"
                )
                for row in filtered_rows:
                    print(row)
                print()

            time.sleep(config.poll_interval)
        except KeyboardInterrupt:
            print("\nStopped.")
            return
        except Exception as exc:
            print(f"[{to_utc_now_str()}] Error: {exc}", file=sys.stderr)
            time.sleep(config.poll_interval)


if __name__ == "__main__":
    run()
