"""xGD model calculation and view interfaces."""

from .predictions import (
    build_predictions,
    extract_period_metrics,
    first_numeric_value,
    period_rows_from_reduced_table,
    summary_from_period_rows,
)
from .views import (
    build_competition_specific_xgd_section,
    build_recent_form_rows,
    build_team_venue_recent_rows,
    build_xgd_view_from_form_df,
    merge_recent_rows_by_venue,
    simplify_model_warning,
)

__all__ = [
    "build_competition_specific_xgd_section",
    "build_predictions",
    "build_recent_form_rows",
    "build_team_venue_recent_rows",
    "build_xgd_view_from_form_df",
    "extract_period_metrics",
    "first_numeric_value",
    "merge_recent_rows_by_venue",
    "period_rows_from_reduced_table",
    "simplify_model_warning",
    "summary_from_period_rows",
]

