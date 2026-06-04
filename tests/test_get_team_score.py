"""Per-team round scoring SQL (spec §5.4): cumulative deltas, captain doubling,
kicker/kicking handling. Runs against an in-memory SQLite DB."""

import sqlite3
import pytest

from api.competition import get_team_score


@pytest.fixture
def conn():
    c = sqlite3.connect(':memory:')
    c.row_factory = sqlite3.Row
    c.executescript('''
        CREATE TABLE weekly_stats (player_id INT, round INT, total_points REAL, kicking REAL);
        CREATE TABLE team_selections (team_name TEXT, player_id INT, round INT,
                                      is_captain INT, is_kicker INT);
        CREATE TABLE team_front_row (team_name TEXT, round INT, club TEXT, league_id INT);
    ''')
    # Round 1 baselines and round 2 cumulative totals.
    ws = [
        # player, round, total, kicking
        (1, 1, 10, 2), (1, 2, 25, 5),    # non-kicker:  base 15, kick 3
        (2, 1, 10, 4), (2, 2, 30, 10),   # kicker:      base 20, kick 6
        (3, 1, 5, 1),  (3, 2, 20, 4),    # captain:     base 15, kick 3
    ]
    c.executemany('INSERT INTO weekly_stats VALUES (?,?,?,?)', ws)
    sel = [
        ('T', 1, 2, 0, 0),   # non-kicker, non-captain
        ('T', 2, 2, 0, 1),   # kicker
        ('T', 3, 2, 1, 0),   # captain
    ]
    c.executemany('INSERT INTO team_selections VALUES (?,?,?,?,?)', sel)
    c.commit()
    return c


def test_team_score_combines_captain_kicker_and_deltas(conn):
    # non-kicker: base 15 - kick 3 = 12
    # kicker:     base 20 (kicking kept)
    # captain:    (base 15 - kick 3) * 2 = 24
    assert get_team_score(conn, 'T', 2) == pytest.approx(12 + 20 + 24)


def test_empty_team_scores_zero(conn):
    assert get_team_score(conn, 'Nobody', 2) == 0.0
