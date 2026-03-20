"""Runtime entrypoint."""

from __future__ import annotations

from http.server import ThreadingHTTPServer

from xgd_app.app_state import AppState
from xgd_app.core import parse_args
from xgd_app.web.handler import AppHandler


def main() -> None:
    args = parse_args()
    state = AppState(args)
    state.refresh_games(force=True)

    AppHandler.state = state
    server = ThreadingHTTPServer((args.host, args.port), AppHandler)
    print(f"Football Handicap Viewer running on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


__all__ = ["AppState", "main"]


if __name__ == "__main__":
    main()
