"""Goal line market helpers."""

from __future__ import annotations

import re
from typing import Any

from .handicap import format_price_value, get_best_offer_price, parse_handicap_value

GOAL_MARKET_NAME_RE = re.compile(
    r"(?:over\s*/\s*under|goal\s*line|total\s*goals?)\s*([0-9]+(?:\.[0-9]+)?)",
    flags=re.IGNORECASE,
)
GOAL_MARKET_TYPE_RE = re.compile(r"OVER_UNDER_(\d+)$", flags=re.IGNORECASE)
GOAL_RUNNER_RE = re.compile(r"\b(?:over|under)\s*([0-9]+(?:\.[0-9]+)?)\b", flags=re.IGNORECASE)
NON_GOAL_MARKET_TERMS = {
    "corner",
    "booking",
    "bookings",
    "card",
    "cards",
    "offside",
    "offsides",
    "throw",
    "shots",
    "shot",
    "foul",
    "penalty",
}


def format_goal_line_value(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "-"
    number = float(value)
    if number.is_integer():
        return f"{int(number)}.0"
    return f"{number:g}"


def parse_goal_line_from_catalogue(catalogue: dict[str, Any]) -> float | None:
    market_name = str(catalogue.get("marketName", "")).strip()
    match = GOAL_MARKET_NAME_RE.search(market_name)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass

    market_type_raw = str(
        catalogue.get("marketType")
        or (catalogue.get("description", {}) or {}).get("marketType")
        or ""
    ).strip()
    match = GOAL_MARKET_TYPE_RE.search(market_type_raw)
    if match:
        try:
            return float(match.group(1)) / 10.0
        except ValueError:
            pass

    found_values: set[float] = set()
    for runner in catalogue.get("runners", []):
        runner_name = str(runner.get("runnerName", "")).strip()
        runner_match = GOAL_RUNNER_RE.search(runner_name)
        if not runner_match:
            continue
        try:
            found_values.add(float(runner_match.group(1)))
        except ValueError:
            continue
    if len(found_values) == 1:
        return next(iter(found_values))
    return None


def extract_goal_runner_handicaps(catalogue: dict[str, Any]) -> set[float]:
    values: set[float] = set()
    for runner in catalogue.get("runners", []):
        handicap = parse_handicap_value(runner.get("handicap"))
        if handicap is not None:
            values.add(float(handicap))
    return values


def is_goal_line_market(catalogue: dict[str, Any]) -> bool:
    market_name = str(catalogue.get("marketName", "")).strip().lower()
    if not market_name:
        return False
    has_goal_line_label = (
        ("over/under" in market_name)
        or ("over under" in market_name)
        or ("goal line" in market_name)
        or ("total goals" in market_name)
    )
    if not has_goal_line_label:
        return False
    if any(term in market_name for term in NON_GOAL_MARKET_TERMS):
        return False

    market_type = str(
        (catalogue.get("description", {}) or {}).get("marketType")
        or catalogue.get("marketType")
        or ""
    ).strip().upper()
    runner_lines = extract_goal_runner_handicaps(catalogue)
    if market_type == "ALT_TOTAL_GOALS":
        if not runner_lines:
            return False
        return any(abs((line * 4) - round(line * 4)) < 1e-6 for line in runner_lines)

    goal_line = parse_goal_line_from_catalogue(catalogue)
    if goal_line is None and not runner_lines:
        return False

    candidate_lines = set(runner_lines)
    if goal_line is not None:
        candidate_lines.add(float(goal_line))
    return any(abs((line * 4) - round(line * 4)) < 1e-6 for line in candidate_lines)


def goal_market_snapshot(catalogue: dict[str, Any], market_book: dict[str, Any] | None) -> dict[str, Any]:
    market_level_line = parse_goal_line_from_catalogue(catalogue)
    if not market_book:
        return {
            "goal_line": market_level_line,
            "score": float("inf"),
            "under_mid": None,
            "over_mid": None,
        }

    runner_info: dict[int, str] = {}
    for runner in catalogue.get("runners", []):
        selection_id = runner.get("selectionId")
        if isinstance(selection_id, int):
            runner_info[selection_id] = str(runner.get("runnerName", "")).strip()

    over_rows_by_line: dict[float, dict[str, float | None]] = {}
    under_rows_by_line: dict[float, dict[str, float | None]] = {}

    def store_best(bucket: dict[float, dict[str, float | None]], line: float, row: dict[str, float | None]) -> None:
        key = round(float(line), 6)
        existing = bucket.get(key)
        if existing is None:
            bucket[key] = row
            return
        row_spread = row.get("spread")
        existing_spread = existing.get("spread")
        if row_spread is None and existing_spread is None:
            return
        if existing_spread is None:
            bucket[key] = row
            return
        if row_spread is None:
            return
        if float(row_spread) < float(existing_spread):
            bucket[key] = row

    for runner in market_book.get("runners", []):
        selection_id = runner.get("selectionId")
        if not isinstance(selection_id, int):
            continue

        name = runner_info.get(selection_id, "").strip().lower()
        if not name:
            continue

        line_value = parse_handicap_value(runner.get("handicap"))
        if line_value is None:
            line_value = market_level_line
        if line_value is None:
            continue

        ex = runner.get("ex", {})
        back = get_best_offer_price(ex, "availableToBack")
        lay = get_best_offer_price(ex, "availableToLay")
        row = {
            "mid": ((float(back) + float(lay)) / 2.0) if (back is not None and lay is not None) else None,
            "spread": (float(lay) - float(back)) if (back is not None and lay is not None) else None,
        }
        if "over" in name:
            store_best(over_rows_by_line, float(line_value), row)
        elif "under" in name:
            store_best(under_rows_by_line, float(line_value), row)

    candidate_lines = sorted(set(over_rows_by_line.keys()) & set(under_rows_by_line.keys()))
    if not candidate_lines:
        return {
            "goal_line": market_level_line,
            "score": float("inf"),
            "under_mid": None,
            "over_mid": None,
        }

    lines: list[dict[str, float | None]] = []
    for line in candidate_lines:
        over_row = over_rows_by_line.get(line, {})
        under_row = under_rows_by_line.get(line, {})
        over_mid = over_row.get("mid")
        under_mid = under_row.get("mid")
        score = (
            abs(float(over_mid) - 2.0) + abs(float(under_mid) - 2.0)
            if (over_mid is not None and under_mid is not None)
            else float("inf")
        )
        lines.append(
            {
                "goal_line": float(line),
                "score": float(score),
                "under_mid": (float(under_mid) if under_mid is not None else None),
                "over_mid": (float(over_mid) if over_mid is not None else None),
            }
        )

    priced_lines = [line for line in lines if line.get("under_mid") is not None and line.get("over_mid") is not None]
    if priced_lines:
        best = min(priced_lines, key=lambda row: (float(row["score"]), abs(float(row["goal_line"]) - 2.5)))
    else:
        best = min(lines, key=lambda row: abs(float(row["goal_line"]) - 2.5))
    return {
        "goal_line": best["goal_line"],
        "score": best["score"],
        "under_mid": best.get("under_mid"),
        "over_mid": best.get("over_mid"),
    }


def event_goal_mainline_snapshot(
    goal_catalogues: list[dict[str, Any]],
    goal_books_by_market_id: dict[str, dict[str, Any]],
) -> dict[str, str]:
    snapshots: list[dict[str, Any]] = []
    for cat in goal_catalogues:
        market_id = str(cat.get("marketId", "")).strip()
        snapshot = goal_market_snapshot(cat, goal_books_by_market_id.get(market_id))
        if snapshot.get("goal_line") is None:
            continue
        snapshots.append(snapshot)

    if not snapshots:
        return {"goal_mainline": "-", "goal_under_price": "-", "goal_over_price": "-"}

    priced = [row for row in snapshots if row.get("score", float("inf")) != float("inf")]
    if priced:
        best = min(priced, key=lambda row: (float(row["score"]), abs(float(row["goal_line"]) - 2.5)))
    else:
        best = min(snapshots, key=lambda row: abs(float(row["goal_line"]) - 2.5))
    return {
        "goal_mainline": format_goal_line_value(best["goal_line"]),
        "goal_under_price": format_price_value(best.get("under_mid")),
        "goal_over_price": format_price_value(best.get("over_mid")),
    }


__all__ = [
    "event_goal_mainline_snapshot",
    "extract_goal_runner_handicaps",
    "format_goal_line_value",
    "goal_market_snapshot",
    "is_goal_line_market",
    "parse_goal_line_from_catalogue",
]
