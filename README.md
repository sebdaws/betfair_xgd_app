# Football Handicap Viewer

Web app version of the Betfair + SofaScore xGD workflow (no Streamlit).

## Quick Start

```bash
/Users/sebastiandaws/miniconda3/envs/footy/bin/python xgd_web_app.py --host 127.0.0.1 --port 8090
```

Open: `http://127.0.0.1:8090`

## Project Layout

- `xgd_web_app.py`: compatibility launcher; calls `xgd_app.runtime.main()`
- `xgd_app/runtime.py`: app startup and HTTP server lifecycle
- `xgd_app/app_state.py`: central app state and service wiring
- `xgd_app/web/handler.py`: HTTP routes (`/api/*`) and static asset serving
- `xgd_app/services/`: business logic split by area
- `xgd_app/data/`: SofaScore + historical Betfair data loading
- `xgd_app/markets/`: handicap and goal-line market parsing helpers
- `webapp/`: frontend assets (`index.html`, `app.js`, `styles.css`)
- `app_data/`: selected leagues, generated leagues list, mappings, saved games, and path defaults
- `betfair_scraper.py` and `xgd_form_model.py`: local modules loaded by `AppState`

## Credentials

The app reads credentials in this order:
1. CLI args
2. Environment vars (`BETFAIR_*`)
3. `betfair_credentials.py` in this folder
4. `external_paths.betfair_credentials` from `app_data/default_paths.json`

Use `betfair_credentials.example.py` as a template.

## Data

Default SofaScore DB path:
- `./sofascore_local.db` if present
- otherwise `external_paths.sofascore_db` from `app_data/default_paths.json`

You can override with `--db-path`.

## Default Paths

Cross-project paths are configured in `app_data/default_paths.json`.
Update that file to point this app at shared data or modules in other repositories.

## Notes

- `betfair_credentials.py` should be gitignored if it contains secrets.
- `app_data/selected_leagues.txt` controls which competitions are fetched from Betfair.
