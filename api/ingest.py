"""
Ingestion jobs (spec §4) — league-aware, idempotent, adapter-driven.

These pull through the data-source adapters (api/datasource), so they run
offline against the mock adapter and against SuperBru/ESPN when
DATA_SOURCE=live. Every write is an upsert keyed to make re-runs idempotent,
and every row carries its league_id so the two leagues stay isolated.

Layering (spec §3): the scheduler decides *when*; these functions do the
ingestion; the domain/API layers read the resulting tables.
"""

from datetime import datetime, timezone

from .db import execute as _exec, DB_TYPE
from .datasource import get_fixture_source, get_lineup_source, get_score_source


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scalar(row):
    if row is None:
        return None
    if isinstance(row, dict):
        return next(iter(row.values()))
    return row[0]


# ---------------------------------------------------------------------------
# §4.2 fixtures → rounds
# ---------------------------------------------------------------------------

def ingest_rounds(conn, league_id: int, competition: str) -> int:
    rounds = get_fixture_source().fetch_rounds(competition)
    for rd in rounds:
        _exec(conn,
              'INSERT INTO rounds (round_number, first_kickoff, last_kickoff, league_id) '
              'VALUES (?, ?, ?, ?) '
              'ON CONFLICT (league_id, round_number) DO UPDATE SET '
              '  first_kickoff = excluded.first_kickoff, '
              '  last_kickoff  = excluded.last_kickoff',
              (rd.round_number, rd.first_kickoff, rd.last_kickoff, league_id))
    conn.commit()
    return len(rounds)


def round_kickoffs(competition: str, round_number: int) -> list[str]:
    """Per-match kickoff ISO strings for one round (for live detection §4.2)."""
    for rd in get_fixture_source().fetch_rounds(competition):
        if rd.round_number == round_number:
            return [m.kickoff for m in rd.matches] or [rd.first_kickoff]
    return []


# ---------------------------------------------------------------------------
# §4.3 lineups → match_lineups (status S/B; O = absent from squad, no row)
# ---------------------------------------------------------------------------

def ingest_lineups(conn, league_id: int, competition: str, round_number: int) -> int:
    entries = get_lineup_source().fetch_lineups(competition, round_number)
    now = _now()
    written = 0
    for e in entries:
        if e.status == 'O' or not e.player_name:
            continue
        _exec(conn,
              'INSERT INTO match_lineups '
              '  (round, player_name, real_team, jersey, is_bench, scraped_at, league_id) '
              'VALUES (?, ?, ?, ?, ?, ?, ?) '
              'ON CONFLICT (round, player_name, real_team) DO UPDATE SET '
              '  jersey = excluded.jersey, is_bench = excluded.is_bench, '
              '  scraped_at = excluded.scraped_at, league_id = excluded.league_id',
              (round_number, e.player_name, e.real_team, e.jersey, e.is_bench, now, league_id))
        written += 1
    conn.commit()
    return written


# ---------------------------------------------------------------------------
# §4.4 player scoring → weekly_stats
# ---------------------------------------------------------------------------

def _player_id(conn, name: str, team: str, position: str, league_id: int) -> int:
    _exec(conn,
          'INSERT INTO players (name, team, position, league_id) VALUES (?, ?, ?, ?) '
          'ON CONFLICT (name, team, position) DO UPDATE SET team = excluded.team',
          (name, team, position, league_id))
    cur = _exec(conn,
                'SELECT player_id FROM players WHERE name = ? AND team = ? AND position = ?',
                (name, team, position))
    return _scalar(cur.fetchone())


def ingest_player_scores(conn, league_id: int, competition: str,
                         round_number: int, finalize: bool = False) -> int:
    """Upsert one round of cumulative player scores. `finalize` is the Monday
    authoritative pass (spec §4.4) — same write path, overwriting live values."""
    scores = get_score_source().fetch_player_scores(competition, round_number)
    now = _now()
    written = 0
    for s in scores:
        if not s.name:
            continue
        pid = _player_id(conn, s.name, s.team, s.position, league_id)
        _exec(conn,
              'INSERT INTO weekly_stats '
              '  (player_id, round, total_points, price, kicking, points_per_game, '
              '   popularity, form, scraped_at, league_id) '
              'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
              'ON CONFLICT (player_id, round) DO UPDATE SET '
              '  total_points = excluded.total_points, price = excluded.price, '
              '  kicking = excluded.kicking, points_per_game = excluded.points_per_game, '
              '  popularity = excluded.popularity, form = excluded.form, '
              '  scraped_at = excluded.scraped_at, league_id = excluded.league_id',
              (pid, round_number, s.total_points, s.price, s.kicking, s.points_per_game,
               s.popularity, s.form, now, league_id))
        written += 1
    conn.commit()
    return written
