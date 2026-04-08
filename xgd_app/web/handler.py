"""HTTP handler implementation for the xGD app."""

from __future__ import annotations

import json
import math
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pandas as pd

WEBAPP_DIR = Path(__file__).resolve().parents[2] / "webapp"


def _clamp_int(value: Any, default: int, min_value: int, max_value: int) -> int:
    try:
        out = int(value)
    except Exception:
        out = int(default)
    return max(min_value, min(max_value, out))


def _clamp_float(value: Any, default: float, min_value: float, max_value: float) -> float:
    try:
        out = float(value)
    except Exception:
        out = float(default)
    if not math.isfinite(out):
        out = float(default)
    return max(float(min_value), min(float(max_value), out))


def _sanitize_for_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_sanitize_for_json(v) for v in value]
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return _sanitize_for_json(value.item())
        except Exception:
            return str(value)
    return value


class AppHandler(BaseHTTPRequestHandler):
    state: Any

    @staticmethod
    def _is_client_disconnect_error(exc: BaseException) -> bool:
        if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
            return True
        if isinstance(exc, OSError):
            return int(getattr(exc, "errno", -1)) in {32, 54, 104}
        return False

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            return self._serve_static("index.html", "text/html; charset=utf-8")
        if path == "/app.js":
            return self._serve_static("app.js", "application/javascript; charset=utf-8")
        if path == "/styles.css":
            return self._serve_static("styles.css", "text/css; charset=utf-8")
        if path == "/api/games":
            query = parse_qs(parsed.query)
            mode = (query.get("mode") or ["upcoming"])[0]
            load_more_raw = (query.get("load_more") or ["0"])[0]
            load_more_historical = str(load_more_raw or "").strip().lower() in {"1", "true", "yes", "on"}
            return self._serve_games(mode=mode, load_more_historical=load_more_historical)
        if path == "/api/game-xgd":
            query = parse_qs(parsed.query)
            market_id = (query.get("market_id") or [""])[0]
            recent_n_raw = (query.get("recent_n") or ["5"])[0]
            recent_n = _clamp_int(recent_n_raw, default=5, min_value=1, max_value=20)
            venue_recent_n_raw = (query.get("venue_recent_n") or ["5"])[0]
            venue_recent_n = _clamp_int(venue_recent_n_raw, default=5, min_value=1, max_value=20)
            return self._serve_game_xgd(market_id, recent_n, venue_recent_n)
        if path == "/api/game-hcperf":
            query = parse_qs(parsed.query)
            market_id = (query.get("market_id") or [""])[0]
            return self._serve_game_hc_performance(market_id)
        if path == "/api/manual-mappings":
            return self._serve_manual_mappings()
        if path == "/api/saved-games":
            return self._serve_saved_games()
        if path == "/api/team-hc-rankings":
            query = parse_qs(parsed.query)
            xg_threshold_raw = (query.get("xg_threshold") or ["0.1"])[0]
            xg_threshold = _clamp_float(xg_threshold_raw, default=0.1, min_value=0.0, max_value=5.0)
            return self._serve_team_hc_rankings(xg_threshold=xg_threshold)
        if path == "/api/team-hc-rankings/details":
            query = parse_qs(parsed.query)
            team_name = (query.get("team") or [""])[0]
            competition_name = (query.get("competition") or [""])[0]
            return self._serve_team_hc_ranking_details(team_name=team_name, competition_name=competition_name)
        if path == "/api/team-page":
            query = parse_qs(parsed.query)
            team_name = (query.get("team") or [""])[0]
            competition_name = (query.get("competition") or [""])[0]
            return self._serve_team_page(team_name=team_name, competition_name=competition_name)
        if path == "/api/teams":
            return self._serve_teams_directory()

        self._json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/api/historical-day-xgd":
            return self._serve_historical_day_xgd()
        if path == "/api/rescan-closing-prices":
            return self._serve_rescan_closing_prices()
        if path == "/api/manual-mappings":
            return self._upsert_manual_mapping()
        if path == "/api/manual-mappings/bulk":
            return self._bulk_upsert_manual_mappings()
        if path == "/api/manual-mappings/delete":
            return self._delete_manual_mapping()
        if path == "/api/manual-competition-mappings":
            return self._upsert_manual_competition_mapping()
        if path == "/api/manual-competition-mappings/delete":
            return self._delete_manual_competition_mapping()
        if path == "/api/saved-games":
            return self._save_game()
        if path == "/api/saved-games/delete":
            return self._unsave_game()
        if path == "/api/exit-app":
            return self._exit_app()

        self._json({"error": "Not found"}, status=HTTPStatus.NOT_FOUND)

    def _serve_games(self, mode: str = "upcoming", load_more_historical: bool = False) -> None:
        try:
            payload = self.state.list_games_grouped_by_day(
                mode=mode,
                load_more_historical=bool(load_more_historical),
            )
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_game_xgd(self, market_id: str, recent_n: int, venue_recent_n: int) -> None:
        if not market_id:
            self._json({"error": "market_id is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = self.state.get_game_xgd(market_id, recent_n=recent_n, venue_recent_n=venue_recent_n)
            self._json(payload)
        except KeyError:
            self._json({"error": "Market not found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_game_hc_performance(self, market_id: str) -> None:
        if not market_id:
            self._json({"error": "market_id is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            verbose_raw = (query.get("verbose") or ["0"])[0]
            verbose = str(verbose_raw or "").strip().lower() in {"1", "true", "yes", "on"}
            payload = self.state.get_game_hc_performance(market_id, verbose=verbose)
            self._json(payload)
        except KeyError:
            self._json({"error": "Market not found"}, status=HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_historical_day_xgd(self) -> None:
        payload = self._read_json_body()
        day_iso = str(payload.get("date", "")).strip()
        if not day_iso:
            self._json({"error": "date is required (YYYY-MM-DD)"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            out = self.state.calculate_historical_day_xgd(day_iso=day_iso)
            self._json({"ok": True, **out})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_rescan_closing_prices(self) -> None:
        try:
            out = self.state.rescan_historical_closing_prices()
            self._json({"ok": True, **out})
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_manual_mappings(self) -> None:
        try:
            payload = self.state.list_manual_team_mappings()
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_saved_games(self) -> None:
        try:
            payload = self.state.list_saved_games_grouped_by_day()
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_team_hc_rankings(self, xg_threshold: float = 0.1) -> None:
        try:
            payload = self.state.get_team_hc_rankings(xg_push_threshold=xg_threshold)
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_team_hc_ranking_details(self, team_name: str, competition_name: str) -> None:
        team_text = str(team_name or "").strip()
        if not team_text:
            self._json({"error": "team is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = self.state.get_team_hc_ranking_details(
                team_name=team_text,
                competition_name=str(competition_name or "").strip() or None,
            )
            self._json(payload)
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_team_page(self, team_name: str, competition_name: str) -> None:
        team_text = str(team_name or "").strip()
        if not team_text:
            self._json({"error": "team is required"}, status=HTTPStatus.BAD_REQUEST)
            return
        try:
            payload = self.state.get_team_page(
                team_name=team_text,
                competition_name=str(competition_name or "").strip() or None,
            )
            self._json(payload)
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _serve_teams_directory(self) -> None:
        try:
            payload = self.state.list_teams_directory()
            self._json(payload)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _save_game(self) -> None:
        payload = self._read_json_body()
        market_id = str(payload.get("market_id", "")).strip()
        try:
            out = self.state.save_game(market_id=market_id)
            self._json({"ok": True, **out})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _unsave_game(self) -> None:
        payload = self._read_json_body()
        market_id = str(payload.get("market_id", "")).strip()
        try:
            out = self.state.unsave_game(market_id=market_id)
            self._json({"ok": True, **out})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _exit_app(self) -> None:
        server = getattr(self, "server", None) or getattr(self.__class__, "http_server", None)
        if server is None:
            self._json({"error": "Server shutdown is unavailable."}, status=HTTPStatus.SERVICE_UNAVAILABLE)
            return
        print("[xgd_web_app] Exit App requested from web UI. Shutting down...", flush=True)
        self._json({"ok": True, "message": "Shutdown requested"})
        threading.Timer(0.15, server.shutdown).start()

    def _upsert_manual_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        sofa_name = str(payload.get("sofa_name", "")).strip()
        try:
            self.state.upsert_manual_team_mapping(raw_name=raw_name, sofa_name=sofa_name)
            self._json({"ok": True})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _bulk_upsert_manual_mappings(self) -> None:
        payload = self._read_json_body()
        mappings = payload.get("mappings", [])
        try:
            saved_count = self.state.upsert_manual_team_mappings_bulk(mappings=mappings)
            self._json({"ok": True, "saved_count": int(saved_count)})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _delete_manual_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        try:
            deleted = self.state.delete_manual_team_mapping(raw_name=raw_name)
            self._json({"ok": True, "deleted": bool(deleted)})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _upsert_manual_competition_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        sofa_name = str(payload.get("sofa_name", "")).strip()
        try:
            self.state.upsert_manual_competition_mapping(raw_name=raw_name, sofa_name=sofa_name)
            self._json({"ok": True})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _delete_manual_competition_mapping(self) -> None:
        payload = self._read_json_body()
        raw_name = str(payload.get("raw_name", "")).strip()
        try:
            deleted = self.state.delete_manual_competition_mapping(raw_name=raw_name)
            self._json({"ok": True, "deleted": bool(deleted)})
        except ValueError as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.BAD_REQUEST)
        except Exception as exc:
            self._json({"error": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def _read_json_body(self) -> dict[str, Any]:
        length_raw = self.headers.get("Content-Length", "0")
        try:
            length = max(0, int(length_raw))
        except Exception:
            length = 0
        if length <= 0:
            return {}
        body = self.rfile.read(length)
        if not body:
            return {}
        try:
            parsed = json.loads(body.decode("utf-8"))
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}

    def _serve_static(self, relative_path: str, content_type: str) -> None:
        path = WEBAPP_DIR / relative_path
        if not path.exists():
            self._json({"error": f"Missing asset: {path}"}, status=HTTPStatus.NOT_FOUND)
            return
        data = path.read_bytes()
        try:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except Exception as exc:
            if self._is_client_disconnect_error(exc):
                return
            raise

    def _json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        safe_payload = _sanitize_for_json(payload)
        body = json.dumps(safe_payload, allow_nan=False).encode("utf-8")
        try:
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except Exception as exc:
            if self._is_client_disconnect_error(exc):
                return
            raise

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


__all__ = ["AppHandler"]
