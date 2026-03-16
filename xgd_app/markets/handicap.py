"""Asian handicap market helpers."""

from __future__ import annotations

import re
import unicodedata
from typing import Any

TOKEN_REPLACEMENTS = {
    "utd": "united",
    "munchen": "munich",
    "st": "saint",
}

STOP_WORDS = {
    "fc",
    "cf",
    "ac",
    "sc",
    "afc",
    "club",
    "de",
    "the",
}


def split_event_teams(event_name: str | None) -> tuple[str | None, str | None]:
    if not event_name:
        return None, None
    separators = [" v ", " vs ", " @ "]
    lowered = event_name.lower()
    for sep in separators:
        idx = lowered.find(sep)
        if idx == -1:
            continue
        left = event_name[:idx].strip()
        right = event_name[idx + len(sep) :].strip()
        if left and right:
            return left, right
    return None, None


def _normalize_team_name(value: str | None) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = []
    for token in text.split():
        token = TOKEN_REPLACEMENTS.get(token, token)
        if token in STOP_WORDS:
            continue
        tokens.append(token)
    return " ".join(tokens)


def normalize_runner_name(value: str | None) -> str:
    return _normalize_team_name(value)


def runner_name_matches(candidate: str | None, target: str | None) -> bool:
    c = normalize_runner_name(candidate)
    t = normalize_runner_name(target)
    if not c or not t:
        return False
    return c == t or c in t or t in c


def parse_handicap_value(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace(",", "")
    try:
        return float(text)
    except Exception:
        return None


def get_best_offer_price(ex_data: dict[str, Any], side: str) -> float | None:
    offers = ex_data.get(side, [])
    if not offers:
        return None
    prices = [offer.get("price") for offer in offers if isinstance(offer.get("price"), (int, float))]
    if not prices:
        return None
    return max(prices) if side == "availableToBack" else min(prices)


def format_handicap_value(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    if float(value).is_integer():
        return f"{int(value):+d}"
    return f"{float(value):+g}"


def format_price_value(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    return f"{float(value):.2f}"


def market_mainline_snapshot(catalogue: dict[str, Any], market_book: dict[str, Any] | None) -> dict[str, str]:
    default = {"mainline": "-", "home_price": "-", "away_price": "-"}
    if not market_book:
        return default

    event_name = str(catalogue.get("event", {}).get("name", "")).strip()
    market_name = str(catalogue.get("marketName", "")).strip().lower()
    is_match_odds_market = market_name == "match odds"
    home_team, away_team = split_event_teams(event_name)
    if not home_team or not away_team:
        return default

    runner_info: dict[int, dict[str, Any]] = {}
    for runner in catalogue.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue
        runner_info[selection_id] = {
            "name": str(runner.get("runnerName", "")).strip(),
            "handicap": runner.get("handicap"),
        }

    home_candidates: list[dict[str, Any]] = []
    away_candidates: list[dict[str, Any]] = []
    home_match_odds_candidates: list[dict[str, Any]] = []
    away_match_odds_candidates: list[dict[str, Any]] = []
    for runner in market_book.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue
        info = runner_info.get(selection_id, {})
        runner_name = str(info.get("name", "")).strip() or f"Selection {selection_id}"

        ex = runner.get("ex", {})
        back = get_best_offer_price(ex, "availableToBack")
        lay = get_best_offer_price(ex, "availableToLay")
        mid_price = (
            (float(back) + float(lay)) / 2.0
            if (back is not None and lay is not None)
            else (float(back) if back is not None else (float(lay) if lay is not None else None))
        )
        spread = (float(lay) - float(back)) if (back is not None and lay is not None) else None

        handicap = parse_handicap_value(runner.get("handicap", info.get("handicap")))
        if is_match_odds_market:
            handicap = None
        if handicap is None:
            if runner_name_matches(runner_name, home_team):
                home_match_odds_candidates.append(
                    {
                        "mid": mid_price,
                        "spread": spread,
                    }
                )
            elif runner_name_matches(runner_name, away_team):
                away_match_odds_candidates.append(
                    {
                        "mid": mid_price,
                        "spread": spread,
                    }
                )
            continue

        row = {
            "handicap": float(handicap),
            "back": float(back) if back is not None else None,
            "lay": float(lay) if lay is not None else None,
            "spread": spread,
        }
        if runner_name_matches(runner_name, home_team):
            home_candidates.append(row)
        elif runner_name_matches(runner_name, away_team):
            away_candidates.append(row)

    def best_by_handicap(rows: list[dict[str, Any]]) -> dict[float, dict[str, Any]]:
        out: dict[float, dict[str, Any]] = {}
        for row in rows:
            key = round(float(row["handicap"]), 6)
            existing = out.get(key)
            if existing is None:
                out[key] = row
                continue

            row_spread = row.get("spread")
            existing_spread = existing.get("spread")
            if row_spread is None and existing_spread is None:
                continue
            if existing_spread is None:
                out[key] = row
                continue
            if row_spread is None:
                continue
            if float(row_spread) < float(existing_spread):
                out[key] = row
        return out

    home_by_hcp = best_by_handicap(home_candidates)
    away_by_hcp = best_by_handicap(away_candidates)

    lines: list[dict[str, Any]] = []
    if home_by_hcp and away_by_hcp:
        for home_hcp_key, home_row in home_by_hcp.items():
            away_hcp_key = round(-home_hcp_key, 6)
            away_row = away_by_hcp.get(away_hcp_key)
            if away_row is None:
                continue

            home_back = home_row.get("back")
            home_lay = home_row.get("lay")
            away_back = away_row.get("back")
            away_lay = away_row.get("lay")
            home_mid = (
                (float(home_back) + float(home_lay)) / 2.0
                if (home_back is not None and home_lay is not None)
                else None
            )
            away_mid = (
                (float(away_back) + float(away_lay)) / 2.0
                if (away_back is not None and away_lay is not None)
                else None
            )
            score = (
                abs(home_mid - 2.0) + abs(away_mid - 2.0)
                if (home_mid is not None and away_mid is not None)
                else float("inf")
            )
            lines.append(
                {
                    "home_handicap": float(home_row["handicap"]),
                    "home_mid": home_mid,
                    "away_mid": away_mid,
                    "score": score,
                }
            )

    def select_match_odds_mid(rows: list[dict[str, Any]]) -> float | None:
        if not rows:
            return None
        valid = [row for row in rows if row.get("mid") is not None]
        if not valid:
            return None

        def sort_key(row: dict[str, Any]) -> tuple[float, float]:
            spread_value = row.get("spread")
            spread_rank = float(spread_value) if spread_value is not None else float("inf")
            mid = float(row.get("mid"))
            return (spread_rank, abs(mid - 2.0))

        best = min(valid, key=sort_key)
        return float(best.get("mid"))

    if not lines:
        home_match_odds_mid = select_match_odds_mid(home_match_odds_candidates)
        away_match_odds_mid = select_match_odds_mid(away_match_odds_candidates)
        if home_match_odds_mid is not None and away_match_odds_mid is not None:
            return {
                "mainline": "-",
                "home_price": format_price_value(home_match_odds_mid),
                "away_price": format_price_value(away_match_odds_mid),
            }
        return default

    priced_lines = [line for line in lines if line.get("home_mid") is not None and line.get("away_mid") is not None]
    if priced_lines:
        best_line = min(priced_lines, key=lambda line: (line["score"], abs(line["home_handicap"])))
    else:
        # Fallback: still show a handicap line even if prices are currently unavailable.
        best_line = min(lines, key=lambda line: abs(line["home_handicap"]))

    return {
        "mainline": format_handicap_value(best_line["home_handicap"]),
        "home_price": format_price_value(best_line["home_mid"]),
        "away_price": format_price_value(best_line["away_mid"]),
    }


__all__ = [
    "format_handicap_value",
    "format_price_value",
    "get_best_offer_price",
    "market_mainline_snapshot",
    "parse_handicap_value",
    "runner_name_matches",
    "split_event_teams",
]
