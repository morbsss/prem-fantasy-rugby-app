"""Playoff progression (spec §5.5): two-legged aggregate semis + single final.

Championship advances semi *winners* (final winner = Champion); Sacko advances
semi *losers* (final loser = Sacko / wooden spoon).
"""

from api import competition as comp
from api.competition import Team, build_playoffs, SEMI_LEG1, SEMI_LEG2, FINAL_ROUND


def _table():
    return [Team(name=f'T{i}') for i in range(1, 9)]   # T1 (top) .. T8 (bottom)


# (team, round) → score. Unspecified pairs score 0.
SCORES = {
    # Championship semis: T1 & T2 win their aggregates
    ('T1', SEMI_LEG1): 50, ('T1', SEMI_LEG2): 50,
    ('T4', SEMI_LEG1): 10, ('T4', SEMI_LEG2): 10,
    ('T2', SEMI_LEG1): 40, ('T2', SEMI_LEG2): 40,
    ('T3', SEMI_LEG1): 10, ('T3', SEMI_LEG2): 10,
    # Championship final: T1 beats T2
    ('T1', FINAL_ROUND): 60, ('T2', FINAL_ROUND): 10,
    # Sacko semis (losers advance): T8 loses to T5, T7 loses to T6
    ('T5', SEMI_LEG1): 50, ('T5', SEMI_LEG2): 50,
    ('T8', SEMI_LEG1): 10, ('T8', SEMI_LEG2): 10,
    ('T6', SEMI_LEG1): 50, ('T6', SEMI_LEG2): 50,
    ('T7', SEMI_LEG1): 10, ('T7', SEMI_LEG2): 10,
    # Sacko final: T7 loses → wooden spoon
    ('T8', FINAL_ROUND): 60, ('T7', FINAL_ROUND): 10,
}


def _patch(monkeypatch):
    monkeypatch.setattr(comp, 'get_team_score',
                        lambda conn, t, r: float(SCORES.get((t, r), 0.0)))


def test_championship_winners_advance_and_crown_champion(monkeypatch):
    _patch(monkeypatch)
    po = build_playoffs(conn=None, table=_table(), max_round=FINAL_ROUND)
    champ = po['championship']
    assert champ['seeds'] == ['T1', 'T2', 'T3', 'T4']        # 1v4, 2v3 seeding
    assert {champ['semis'][0]['home'], champ['semis'][0]['away']} == {'T1', 'T4'}
    assert champ['semis'][0]['winner'] == 'T1'               # aggregate winner advances
    assert champ['semis'][1]['winner'] == 'T2'
    assert champ['final']['champion'] == 'T1'


def test_aggregate_is_sum_of_two_legs(monkeypatch):
    _patch(monkeypatch)
    po = build_playoffs(conn=None, table=_table(), max_round=FINAL_ROUND)
    semi = po['championship']['semis'][0]   # T1 vs T4
    assert semi['home_agg'] == 100 and semi['away_agg'] == 20


def test_sacko_losers_advance_and_loser_takes_spoon(monkeypatch):
    _patch(monkeypatch)
    po = build_playoffs(conn=None, table=_table(), max_round=FINAL_ROUND)
    sacko = po['sacko']
    assert sacko['seeds'] == ['T5', 'T6', 'T7', 'T8']
    # The team that LOSES the aggregate advances in the Sacko bracket.
    assert sacko['semis'][0]['winner'] == 'T8'    # T8 lost to T5 → advances
    assert sacko['semis'][1]['winner'] == 'T7'    # T7 lost to T6 → advances
    assert sacko['final']['champion'] == 'T7'     # final loser = wooden spoon


def test_provisional_before_finals_played(monkeypatch):
    _patch(monkeypatch)
    po = build_playoffs(conn=None, table=_table(), max_round=SEMI_LEG1)  # finals not reached
    assert po['championship']['final']['champion'] is None
    assert po['championship']['final']['played'] is False


def test_no_sacko_with_fewer_than_eight_teams(monkeypatch):
    _patch(monkeypatch)
    po = build_playoffs(conn=None, table=_table()[:6], max_round=FINAL_ROUND)
    assert po['championship'] is not None
    assert po['sacko'] is None
