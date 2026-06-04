"""
Data-source adapter selection (spec §3).

Choose the implementation via the DATA_SOURCE env var, defaulting to `mock`
so the app is buildable and testable offline:

    DATA_SOURCE=mock   (default) — seeded local data, no network
    DATA_SOURCE=live             — best-effort SuperBru/ESPN scraping

Business logic should depend only on the interfaces in base.py and obtain the
concrete adapter through these accessors.
"""

import os
from functools import lru_cache

from .base import PlayerSource, FixtureSource, LineupSource, ScoreSource


def data_source() -> str:
    return os.getenv('DATA_SOURCE', 'mock').lower()


@lru_cache(maxsize=None)
def _adapter(kind: str):
    if kind == 'live':
        from .live import LiveAdapter
        return LiveAdapter()
    from .mock import MockAdapter
    return MockAdapter()


def get_adapter():
    """The single adapter object implementing all four data-source interfaces."""
    return _adapter(data_source())


def get_player_source() -> PlayerSource:
    return get_adapter()


def get_fixture_source() -> FixtureSource:
    return get_adapter()


def get_lineup_source() -> LineupSource:
    return get_adapter()


def get_score_source() -> ScoreSource:
    return get_adapter()
