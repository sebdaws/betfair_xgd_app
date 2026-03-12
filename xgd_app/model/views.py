"""xGD view and presentation helpers."""

from xgd_app.core import (
    build_competition_specific_xgd_section,
    build_recent_form_rows,
    build_team_venue_recent_rows,
    build_xgd_view_from_form_df,
    merge_recent_rows_by_venue,
    simplify_model_warning,
)

__all__ = [
    "build_competition_specific_xgd_section",
    "build_recent_form_rows",
    "build_team_venue_recent_rows",
    "build_xgd_view_from_form_df",
    "merge_recent_rows_by_venue",
    "simplify_model_warning",
]
