# Modularization Phase 1

This phase introduces a modular package layout without changing `xgd_web_app.py`.

## Goal

- Preserve existing behavior and avoid code loss.
- Create stable module boundaries for the future split.
- Keep all runtime paths intact while new imports become available.

## New Package

`xgd_app/` now exposes:

- `xgd_app.config`
- `xgd_app.cli`
- `xgd_app.app_state`
- `xgd_app.data.sofascore_loader`
- `xgd_app.matching.teams`
- `xgd_app.matching.competitions`
- `xgd_app.markets.handicap`
- `xgd_app.markets.goals`
- `xgd_app.model.predictions`
- `xgd_app.model.views`
- `xgd_app.services.games`
- `xgd_app.services.historical`
- `xgd_app.services.mappings`
- `xgd_app.web.handler`
- `xgd_app.entrypoint`

All modules are currently adapters over the existing implementations in
`xgd_web_app.py`.

## Next Phase

Move implementation blocks from `xgd_web_app.py` into their corresponding
`xgd_app/*` modules incrementally, then invert imports so `xgd_web_app.py`
becomes a thin compatibility entrypoint.

