"""
Data-source abstraction (spec §3).

Business logic must never call SuperBru/ESPN directly. Instead it depends on
these four small interfaces — one per data type from §4 — and the concrete
adapter is selected at runtime via the DATA_SOURCE env var (see __init__.py):

  PlayerSource   §4.1  full player pool per competition (SuperBru)
  FixtureSource  §4.2  season fixture list per competition (ESPN)
  LineupSource   §4.3  weekly match-day lineups joined to players (ESPN)
  ScoreSource    §4.4  per-player round scoring data (SuperBru)

The mock adapter is the contract of record (§8.6): every field the live
adapter must eventually populate appears on these records, so the app runs
end-to-end offline.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Record types — the shape every adapter returns
# ---------------------------------------------------------------------------

@dataclass
class PlayerRecord:
    name: str                 # SuperBru "Last,F" format
    team: str                 # real club/franchise name
    position: str             # one of leagues.POSITIONS (PR/HK/LK/LF/SH/FH/MID/OBK)
    price: float = 0.0


@dataclass
class MatchRecord:
    home: str                 # real team name
    away: str
    kickoff: str              # ISO-8601 UTC


@dataclass
class RoundRecord:
    round_number: int
    first_kickoff: str        # ISO-8601 UTC
    last_kickoff: str         # ISO-8601 UTC
    matches: list[MatchRecord] = field(default_factory=list)


@dataclass
class LineupEntry:
    player_name: str          # SuperBru "Last,F" format
    real_team: str
    jersey: int | None
    status: str               # 'S' starting, 'B' bench, 'O' out (spec §4.3)

    @property
    def is_bench(self) -> int:
        return 1 if self.status == 'B' else 0


@dataclass
class ScoreRecord:
    name: str                 # SuperBru "Last,F" format
    team: str
    position: str
    total_points: float       # cumulative season total (matches existing schema)
    price: float = 0.0
    kicking: float = 0.0
    points_per_game: str = ''
    popularity: str = ''
    form: str = ''


# ---------------------------------------------------------------------------
# Interfaces
# ---------------------------------------------------------------------------

class PlayerSource(ABC):
    @abstractmethod
    def fetch_players(self, competition: str) -> list[PlayerRecord]:
        """Full draftable player pool for a competition (spec §4.1)."""


class FixtureSource(ABC):
    @abstractmethod
    def fetch_rounds(self, competition: str) -> list[RoundRecord]:
        """Full season fixture list grouped into rounds (spec §4.2)."""


class LineupSource(ABC):
    @abstractmethod
    def fetch_lineups(self, competition: str, round_number: int) -> list[LineupEntry]:
        """Match-day lineups for one round, with S/B/O status (spec §4.3)."""


class ScoreSource(ABC):
    @abstractmethod
    def fetch_player_scores(self, competition: str, round_number: int) -> list[ScoreRecord]:
        """Per-player scoring data for one round (spec §4.4)."""
