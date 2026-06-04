"""Scoring, standings, tiebreak and bye rules (spec §5.2–5.4)."""

import pytest

from api import competition as comp
from api.competition import Team, calculate_table, _apply_result


# ---------------------------------------------------------------------------
# Matchup scoring + bonus points (spec §5.4)
# ---------------------------------------------------------------------------

def _result(hs, aw):
    h, a = Team('H'), Team('A')
    _apply_result(h, a, hs, aw)
    return h, a


def test_win_awards_four_points():
    h, a = _result(20, 10)
    assert h.won == 1 and h.league_points >= comp.WIN_PTS
    assert a.lost == 1


def test_winning_bonus_at_margin_27():
    h, a = _result(27, 0)   # win by exactly 27 → winner BP
    assert h.league_points == comp.WIN_PTS + comp.BP_PTS
    assert h.bonus_points == comp.BP_PTS


def test_no_winning_bonus_below_27():
    h, a = _result(26, 0)
    assert h.league_points == comp.WIN_PTS
    assert h.bonus_points == 0


def test_losing_bonus_at_margin_11():
    h, a = _result(11, 0)   # loser within 11 → losing BP (and winner no BP, margin<27)
    assert a.league_points == comp.BP_PTS      # loss(0) + losing bonus
    assert a.lost == 1 and a.bonus_points == comp.BP_PTS
    assert h.league_points == comp.WIN_PTS


def test_no_losing_bonus_above_11():
    h, a = _result(12, 0)
    assert a.league_points == 0 and a.bonus_points == 0


def test_big_win_gives_winner_bp_and_loser_none():
    h, a = _result(40, 5)   # margin 35: winner BP, loser margin>11 no BP
    assert h.league_points == comp.WIN_PTS + comp.BP_PTS
    assert a.league_points == 0


def test_tie_splits_two_each():
    h, a = _result(15, 15)
    assert h.drawn == 1 and a.drawn == 1
    assert h.league_points == comp.DRAW_PTS and a.league_points == comp.DRAW_PTS


# ---------------------------------------------------------------------------
# Standings tiebreak — higher Points For, NOT points difference (spec §5.4)
# ---------------------------------------------------------------------------

def test_tiebreak_uses_points_for_not_diff(monkeypatch):
    # A and C both finish on 4 league points. A has higher Points For (20 vs 18)
    # but LOWER points difference (8 vs 16). Spec ranks by Points For → A above C.
    scores = {'A': 20, 'B': 12, 'C': 18, 'D': 2}
    monkeypatch.setattr(comp, 'get_team_score', lambda conn, t, wk: scores[t])
    fixtures = [
        (1, 'A', False, 'B', False),
        (1, 'C', False, 'D', False),
    ]
    table = calculate_table(fixtures, conn=None, max_round=1)
    names = [t.name for t in table]
    a, c = next(t for t in table if t.name == 'A'), next(t for t in table if t.name == 'C')
    assert a.league_points == c.league_points == comp.WIN_PTS
    assert a.points_for > c.points_for and a.points_diff < c.points_diff
    assert names.index('A') < names.index('C')   # PF tiebreak, not PD


# ---------------------------------------------------------------------------
# Byes — score the average of the other teams that round (spec §5.2)
# ---------------------------------------------------------------------------

def test_bye_scores_average_of_other_teams(monkeypatch):
    # A vs B played (30, 10) → average 20. C is on a bye and should be scored
    # against 20. C scores 25 → C wins its "bye match".
    scores = {'A': 30, 'B': 10, 'C': 25}
    monkeypatch.setattr(comp, 'get_team_score', lambda conn, t, wk: scores[t])
    fixtures = [
        (1, 'A', False, 'B', False),
        (1, 'C', False, 'Bye', False),
    ]
    table = calculate_table(fixtures, conn=None, max_round=1)
    c = next(t for t in table if t.name == 'C')
    assert c.points_against == pytest.approx(20.0)   # average of A and B
    assert c.points_for == pytest.approx(25.0)
    assert c.won == 1
    assert 'Bye' not in [t.name for t in table]


def test_bye_loss_when_below_average(monkeypatch):
    scores = {'A': 30, 'B': 10, 'C': 5}     # avg 20, C below → loss
    monkeypatch.setattr(comp, 'get_team_score', lambda conn, t, wk: scores[t])
    fixtures = [(1, 'A', False, 'B', False), (1, 'C', False, 'Bye', False)]
    table = calculate_table(fixtures, conn=None, max_round=1)
    c = next(t for t in table if t.name == 'C')
    assert c.lost == 1
    assert c.league_points == 0   # losing margin 15 > 11 → no losing bonus


def test_bye_loss_within_11_gets_losing_bonus(monkeypatch):
    scores = {'A': 30, 'B': 10, 'C': 12}    # avg 20, C below by 8 (≤11) → losing BP
    monkeypatch.setattr(comp, 'get_team_score', lambda conn, t, wk: scores[t])
    fixtures = [(1, 'A', False, 'B', False), (1, 'C', False, 'Bye', False)]
    table = calculate_table(fixtures, conn=None, max_round=1)
    c = next(t for t in table if t.name == 'C')
    assert c.lost == 1 and c.league_points == comp.BP_PTS
