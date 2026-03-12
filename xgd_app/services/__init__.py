"""Service layer package."""

__all__ = ["GamesService", "HistoricalService", "MappingService", "GameXgdService"]


def __getattr__(name: str):
    if name == "GamesService":
        from .games import GamesService

        return GamesService
    if name == "HistoricalService":
        from .historical import HistoricalService

        return HistoricalService
    if name == "MappingService":
        from .mappings import MappingService

        return MappingService
    if name == "GameXgdService":
        from .game_xgd import GameXgdService

        return GameXgdService
    raise AttributeError(name)
