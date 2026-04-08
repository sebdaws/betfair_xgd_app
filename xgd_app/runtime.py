"""Runtime entrypoint."""

from __future__ import annotations

from http.server import ThreadingHTTPServer

from xgd_app.app_state import AppState
from xgd_app.core import parse_args
from xgd_app.web.handler import AppHandler


def main() -> None:
    print("[xgd_web_app] Stage 1/3: Parsing arguments...", flush=True)
    args = parse_args()

    print("[xgd_web_app] Stage 2/3: Initializing app state and refreshing games...", flush=True)
    state = AppState(args)
    state.refresh_games(force=True)

    print("[xgd_web_app] Stage 3/3: Starting HTTP server...", flush=True)
    AppHandler.state = state
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    AppHandler.http_server = server
    print(
        f"[xgd_web_app] Fully running: Football Handicap Viewer is live at "
        f"http://{args.host}:{args.port}",
        flush=True,
    )
    print("[xgd_web_app] Press Ctrl+C to stop.", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        AppHandler.http_server = None
        server.server_close()
        print("[xgd_web_app] HTTP server stopped. App exiting.", flush=True)


__all__ = ["AppState", "main"]


if __name__ == "__main__":
    main()
