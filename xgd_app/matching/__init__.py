"""Team and competition matching interfaces."""

from .competitions import (
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
from .teams import (
    MatchMapping,
    TeamMatcher,
    apply_manual_or_match,
    map_betfair_games,
    normalize_team_name,
    pick_best_fixture,
    token_set,
)

__all__ = [
    "MatchMapping",
    "TeamMatcher",
    "apply_manual_or_match",
    "competition_keys_match",
    "competition_token_set",
    "infer_area_for_competition",
    "infer_shared_domestic_competition",
    "infer_team_domestic_competition",
    "map_betfair_games",
    "match_competition_name",
    "normalize_competition_key",
    "normalize_team_name",
    "pick_best_fixture",
    "resolve_sofa_competition_name",
    "select_team_competition_rows",
    "source_competitions_differ_from_betfair_competition",
    "token_set",
]

