"""Competition matching adapters."""

from xgd_app.core import (
    competition_keys_match,
    competition_token_set,
    infer_area_for_competition,
    infer_shared_domestic_competition,
    infer_team_domestic_competition,
    match_competition_name,
    normalize_competition_key,
    resolve_sofa_competition_name,
    select_team_competition_rows,
    source_competitions_differ_from_betfair_competition,
)

__all__ = [
    "competition_keys_match",
    "competition_token_set",
    "infer_area_for_competition",
    "infer_shared_domestic_competition",
    "infer_team_domestic_competition",
    "match_competition_name",
    "normalize_competition_key",
    "resolve_sofa_competition_name",
    "select_team_competition_rows",
    "source_competitions_differ_from_betfair_competition",
]
