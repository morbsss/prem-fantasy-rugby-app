"""
Mock data-source adapter (spec §3) — the contract of record (§8.6).

Reads the committed seed JSON (api/datasource/seed/<competition>.json) and
derives per-round scores and match-day lineups deterministically, so the whole
app runs end-to-end with no network access. Re-running always yields identical
data.

A player only accrues points in rounds where the round's lineup places him in
the 23-man match-day squad (status S or B); when he's out (O) the round adds 0,
mirroring real fantasy scoring.
"""

import json
import hashlib
from functools import lru_cache
from pathlib import Path

from .base import (
    PlayerSource, FixtureSource, LineupSource, ScoreSource,
    PlayerRecord, RoundRecord, MatchRecord, LineupEntry, ScoreRecord,
)

SEED_DIR = Path(__file__).parent / 'seed'

STARTERS_PER_TEAM = 15
BENCH_PER_TEAM = 8
SQUAD_PER_TEAM = STARTERS_PER_TEAM + BENCH_PER_TEAM   # 23

# Positions whose round points are partly kicking (used to populate `kicking`).
KICKERS = {'FH', 'SH', 'OBK'}


def _h(*parts) -> int:
    return int(hashlib.sha1('|'.join(str(p) for p in parts).encode()).hexdigest(), 16)


@lru_cache(maxsize=None)
def _load(competition: str) -> dict:
    path = SEED_DIR / f'{competition}.json'
    if not path.exists():
        raise FileNotFoundError(
            f'Mock seed missing: {path}. Run `python -m api.datasource.generate_seed`.'
        )
    return json.loads(path.read_text(encoding='utf-8'))


def _players_by_team(competition: str) -> dict[str, list[dict]]:
    by_team: dict[str, list[dict]] = {}
    for p in _load(competition)['players']:
        by_team.setdefault(p['team'], []).append(p)
    return by_team


def _matchday_ids(competition: str, round_number: int) -> dict[str, str]:
    """player_id → status ('S'/'B') for everyone in a match-day squad this round.

    Each real team's pool is ordered by a per-round hash; the first 15 start,
    the next 8 are bench, the rest are out (absent from this map).
    """
    status: dict[str, str] = {}
    for team, pool in _players_by_team(competition).items():
        ordered = sorted(pool, key=lambda p: _h(p['id'], round_number))
        for idx, p in enumerate(ordered):
            if idx < STARTERS_PER_TEAM:
                status[p['id']] = 'S'
            elif idx < SQUAD_PER_TEAM:
                status[p['id']] = 'B'
    return status


def _round_points(player: dict, round_number: int) -> tuple[float, float]:
    """(points, kicking) a player scores in a single round, deterministically."""
    rate, vol = player['rate'], player['vol']
    # variation in [-vol, +vol] of the base rate
    frac = (_h(player['id'], 'pts', round_number) % 1000) / 1000.0   # 0..1
    variation = (frac - 0.5) * 2 * vol
    pts = max(0.0, round(rate * (1 + variation), 1))
    kicking = round(pts * 0.4, 1) if player['position'] in KICKERS else 0.0
    return pts, kicking


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------

class MockAdapter(PlayerSource, FixtureSource, LineupSource, ScoreSource):
    """Single object implementing all four data-source interfaces off seed files."""

    # --- §4.1 players -----------------------------------------------------
    def fetch_players(self, competition: str) -> list[PlayerRecord]:
        return [
            PlayerRecord(name=p['name'], team=p['team'],
                         position=p['position'], price=p['price'])
            for p in _load(competition)['players']
        ]

    # --- §4.2 fixtures ----------------------------------------------------
    def fetch_rounds(self, competition: str) -> list[RoundRecord]:
        rounds = []
        for r in _load(competition)['rounds']:
            rounds.append(RoundRecord(
                round_number=r['round'],
                first_kickoff=r['first_kickoff'],
                last_kickoff=r['last_kickoff'],
                matches=[MatchRecord(home=m['home'], away=m['away'],
                                     kickoff=r['first_kickoff']) for m in r['matches']],
            ))
        return rounds

    # --- §4.3 lineups -----------------------------------------------------
    def fetch_lineups(self, competition: str, round_number: int) -> list[LineupEntry]:
        status = _matchday_ids(competition, round_number)
        entries: list[LineupEntry] = []
        for team, pool in _players_by_team(competition).items():
            ordered = sorted(pool, key=lambda p: _h(p['id'], round_number))
            for idx, p in enumerate(ordered):
                st = status.get(p['id'], 'O')
                jersey = idx + 1 if idx < SQUAD_PER_TEAM else None
                entries.append(LineupEntry(
                    player_name=p['name'], real_team=team,
                    jersey=jersey, status=st,
                ))
        return entries

    # --- §4.4 scores ------------------------------------------------------
    def fetch_player_scores(self, competition: str, round_number: int) -> list[ScoreRecord]:
        # Cumulative totals through `round_number` — only rounds the player was
        # in a match-day squad accrue points.
        squads = {r: _matchday_ids(competition, r) for r in range(1, round_number + 1)}
        records: list[ScoreRecord] = []
        for p in _load(competition)['players']:
            total = kick_total = 0.0
            for r in range(1, round_number + 1):
                if p['id'] in squads[r]:
                    pts, kick = _round_points(p, r)
                    total += pts
                    kick_total += kick
            records.append(ScoreRecord(
                name=p['name'], team=p['team'], position=p['position'],
                total_points=round(total, 1), price=p['price'],
                kicking=round(kick_total, 1),
                points_per_game=str(round(total / max(1, round_number), 1)),
                popularity='', form='',
            ))
        return records

    # --- helpers used by the seed script ---------------------------------
    def fantasy_teams(self, competition: str) -> list[str]:
        return list(_load(competition)['fantasy_teams'])

    def n_rounds(self, competition: str) -> int:
        return _load(competition)['n_rounds']
