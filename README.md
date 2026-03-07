# Betfair xGD Web App

Web app version of the Betfair + SofaScore xGD workflow (no Streamlit), built in the same style as `betfair_app.py`.

## Run

```bash
/Users/sebastiandaws/miniconda3/envs/footy/bin/python xgd_web_app.py --host 127.0.0.1 --port 8090
```

Open: `http://127.0.0.1:8090`

## Credentials

The app reads credentials in this order:
1. CLI args
2. Environment vars (`BETFAIR_*`)
3. `betfair_credentials.py` in this folder
4. `../Bot Finder/betfair_credentials.py` fallback

Use `betfair_credentials.example.py` as a template.

## Data

Default SofaScore DB path:
- `./sofascore_local.db` if present
- otherwise fallback `../Sofascore_scraper/sofascore_local.db`

You can override with `--db-path`.

## Repo Notes

- `betfair_credentials.py` should be gitignored if it contains secrets.
- `betfair_scraper.py` and `wyscout_data.py` are bundled locally so this folder can become its own repo.
