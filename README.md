# Football Handicap Viewer

Web app version of the Betfair + Database xGD workflow (no Streamlit).

## Quick Start

Python 3.10+ is required. Python 3.11 is recommended.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python xgd_web_app.py --host 127.0.0.1 --port 8090
```

Open: `http://127.0.0.1:8090`

On Windows, activate the virtual environment with:

```powershell
.\.venv\Scripts\Activate.ps1
```

## Conda Launcher Script

Use `launch_app.cmd` to start the app via `conda run`:

This is the single launcher entrypoint for Windows shells (CMD, PowerShell, VS Code terminal, and Git Bash on Windows).
By default it auto-loads config from `app_data/launcher_config.json`.
The launcher now prints startup stages, and the app prints a clear `Fully running` line when the server is live.
Install the packages from `requirements.txt` into the selected conda environment before launching.

Examples:

```powershell
# PowerShell/CMD (no flags needed when app_data/launcher_config.json is set)
.\launch_app.cmd
```

```powershell
# Direct env + app args
.\launch_app.cmd --conda-env footy -- --host 127.0.0.1 --port 8090
```

```powershell
# Optional explicit config override
.\launch_app.cmd --config .\app_data\launcher_config.example.json
```

Config keys used by the launcher:
- `launcher.conda_env` or `launcher.conda.env` (or `launcher.conda.path`)
- `launcher.conda_exe` (optional)
- `launcher.app_args` (optional)
- `launcher.env_vars` (optional extra env vars)

The launcher auto-discovers conda when possible (`--conda-exe` still overrides).

## Project Layout

- `xgd_web_app.py`: compatibility launcher; calls `xgd_app.runtime.main()`
- `launcher/launch_app_impl.py`: internal Python launcher used by `launch_app.cmd`
- `xgd_app/runtime.py`: app startup and HTTP server lifecycle
- `xgd_app/app_state.py`: central app state and service wiring
- `xgd_app/web/handler.py`: HTTP routes (`/api/*`) and static asset serving
- `xgd_app/services/`: business logic split by area
- `xgd_app/data/`: database + historical Betfair data loading
- `xgd_app/markets/`: handicap and goal-line market parsing helpers
- `xgd_app/integrations/`: local Betfair scraper and form model modules loaded by `AppState`
- `webapp/`: frontend assets (`index.html`, `app.js`, `styles.css`)
- `scripts/`: utility scripts for local data inspection
- `app_data/`: selected leagues, generated leagues list, mappings, saved games, and path defaults
- `requirements.txt`: Python package dependencies

## Credentials

The app reads credentials in this order:
1. CLI args
2. Environment vars (`BETFAIR_*`)
3. `betfair_credentials.py` in this folder
4. `external_paths.betfair_credentials` from `app_data/default_paths.json`

Use `betfair_credentials.example.py` as a template.
Do not commit real credentials. Local credential/config files such as `betfair_credentials.py` and `app_data/launcher_config.json` are ignored by git.

## Data

Source DB is taken from launcher/app args (`--db-path`), typically via `app_data/launcher_config.json`.

Optional separate events DB can be set with `--match-events-db-path`. This lets you use one DB for match/xG rows (for example FotMob) and another DB for timed `match_events` (for example SofaScore corners/events).

Example:
- `--db-path ../Sofascore_scraper/fotmob_local.db`
- `--match-events-db-path ../Sofascore_scraper/sofascore_local.db`

If you run without launcher args, the app still falls back to local defaults.

## Default Paths

Cross-project paths are configured in `app_data/default_paths.json`.
Update that file to point this app at shared data or modules in other repositories.

## App Navigation

1. Launch the app and open `http://127.0.0.1:8090`.
2. Use the top row tabs to switch between `Games`, `Modelling Prices`, `HC Rankings`, `Teams`, `Matchup`, and `Mapping`.
3. Click a game row (or team button) to open the right-side details panel.
4. Use `Close` in the panel header to return to the table view.

Use the `Settings` button in the top right to open the settings sidebar. It contains:
- `Refresh Betfair Odds`: reloads upcoming game/price data.
- `xG Threshold`: changes highlight and HC/xG decision thresholds used across views.
- `xG Mode`: switches between xG and NPxG where supported.
- `Highlight`: toggles xGD/HC highlighting.
- `No-HC Games`: show or hide games with missing handicap prices.
- `Theme`: switches between light and dark mode.
- `Hard Refresh xGD`: recomputes xGD data from source rows.
- `Exit App`: shuts down the local app server.

## Tabs Overview

### Games
- Main working view for upcoming and historical fixtures.
- Sub-tabs switch between `Games` and `Saved Games`.
- `Upcoming` / `Historical` switch controls mode.
- Day navigation: `Previous Day`, `Next Day`, `Today`/`Latest`, plus date jump input.
- Filters: leagues, tier, sort, team search, model edge, and pricing period.
- Pricing period can be switched between `Season`, `Last 5`, and `Last 3`.
- Historical mode shows summary cells at the top of the `HC Bet` and `Goals Bet` columns for the visible rows.
- Clicking a fixture opens a details panel with sub-tabs:
  - `xGD`: matchup, venue-based and general form.
  - `Stats`: corner/card stats and gamestate-based numbers.
  - `Shots`: shot maps and shot-level detail when available.
  - `Pricing`: model price outputs with the same `Season` / `Last 5` / `Last 3` selector.
  - `HC Perf`: season handicap performance tables for both teams.
- `Save` in the details panel adds/removes the game from `Saved Games`.

### Modelling Prices
- Compares Betfair prices with model prices for upcoming fixtures.
- Supports `Season`, `Last 5`, and `Last 3` pricing periods.
- Shows xGD as the same three-value stack used in the Games page.
- Applies the xGD highlighting logic to the xGD column.
- Uses the model edge threshold from the Games toolbar/settings context when highlighting price differences.

### HC Rankings
- League-level handicap performance ranking table by team.
- Controls:
  - venue sub-tabs: `General`, `Home`, `Away`
  - `Rank By`: `Result`, `xG`, `PnL (For)`, `PnL (Against)`
  - `Season` selector
  - `League` selector
- Clicking a team opens that team’s page for deeper season-level review.

### Teams
- Directory of teams available from current data.
- Search by team name.
- Clicking a team opens the team details panel.

Team details panel:
- Header selectors: `Season` and `League`
- Sub-tabs:
  - `xG Games`: game-by-game xG and recent form context
  - `Stats`: aggregate team metrics
  - `HC Perf`: handicap performance and summary tables

### Matchup
- Manual matchup tool for checking two teams without needing a Betfair market row.
- Enter home team, away team, and competition, then run the matchup to view xGD and pricing outputs.
- Uses the same source database and mapping logic as the main Games views.

### Mapping
- Used to resolve Betfair vs Database naming mismatches.
- Sub-tabs:
  - `Teams`: map unmatched teams to Database names.
  - `Competitions`: map unmatched competition names.
- Includes save/delete actions and bulk save for selected rows.
- Mapping files are source-specific by DB stem:
  - `app_data/manual_team_mappings.<db_stem>.json`
  - `app_data/manual_competition_mappings.<db_stem>.json`
  - Example for FotMob: `manual_team_mappings.fotmob_local.json`
- In hybrid mode (`--db-path` + `--match-events-db-path`), the app uses both DBs' mapping files to align matches across sources before applying event-based gamestate stats.

## Notes

- `betfair_credentials.py` should be gitignored if it contains secrets.
- `app_data/selected_leagues.txt` controls which competitions are fetched from Betfair.
- Backup SQL files matching `app_data/*backup*.sql` are ignored by git.
- Generated local artifacts such as `__pycache__`, `.DS_Store`, launcher config, saved games, and historical data should stay out of commits.
