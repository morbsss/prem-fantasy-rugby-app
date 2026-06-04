"""Snake-draft engine + starting-team validity (front row = club unit model)."""

from api import draft as d
from api.leagues import (
    validate_roster, ROSTER_SIZE, BENCH_COUNT, STARTER_COUNT,
    DRAFT_PICKS_PER_TEAM, INDIVIDUAL_POSITIONS,
)


# ---------------------------------------------------------------------------
# Snake order (15 picks/team: 1 FR unit + 14 individuals)
# ---------------------------------------------------------------------------

def test_snake_sequence_reverses_each_round():
    assert d.snake_sequence(['A', 'B', 'C'], 2) == ['A', 'B', 'C', 'C', 'B', 'A']


def test_team_on_clock():
    order = ['A', 'B', 'C']
    assert d.team_on_clock(order, 1) == 'A'
    assert d.team_on_clock(order, 4) == 'C'   # snake turn
    assert d.team_on_clock(order, 6) == 'A'


def test_total_picks_is_15_per_team():
    order = ['A', 'B', 'C']
    assert DRAFT_PICKS_PER_TEAM == 15
    assert d.total_picks(order) == 3 * 15


# ---------------------------------------------------------------------------
# Individual squad validity: 10 starters + 4 bench (front row is separate)
# ---------------------------------------------------------------------------

# 10 individual starters: 1 LK, 2 LF, 1 SH, 1 FH, 2 MID, 3 OBK
VALID_STARTERS = ['LK', 'LF', 'LF', 'SH', 'FH', 'MID', 'MID', 'OBK', 'OBK', 'OBK']
VALID_BENCH = ['LK', 'LF', 'MID', 'OBK']   # 4 bench (no props/hookers)


def test_constants_are_14_individuals_4_bench():
    assert STARTER_COUNT == 10 and BENCH_COUNT == 4 and ROSTER_SIZE == 14


def test_validate_roster_accepts_legal_14():
    sel = [(p, False) for p in VALID_STARTERS] + [(p, True) for p in VALID_BENCH]
    ok, msg = validate_roster(sel)
    assert ok and msg is None


def test_validate_roster_rejects_props_or_hookers():
    # Props/hookers belong to the club front-row unit, not the individual squad.
    sel = [(p, False) for p in (['PR'] + VALID_STARTERS[1:])] + [(p, True) for p in VALID_BENCH]
    assert validate_roster(sel)[0] is False


def test_validate_roster_rejects_wrong_bench_count():
    sel = [(p, False) for p in VALID_STARTERS] + [(p, True) for p in VALID_BENCH[:-1]]  # 3 bench
    assert validate_roster(sel)[0] is False


def test_validate_roster_rejects_unfillable_lineup():
    starters = ['LK', 'LF', 'LF', 'SH', 'FH', 'MID', 'MID', 'OBK', 'OBK', 'LK']  # only 2 OBK
    sel = [(p, False) for p in starters] + [(p, True) for p in VALID_BENCH]
    assert validate_roster(sel)[0] is False


# ---------------------------------------------------------------------------
# Auto-draft: 1 FR unit + 14 individuals
# ---------------------------------------------------------------------------

def _individual_pool():
    pool, pid = [], 0
    for pos in INDIVIDUAL_POSITIONS:
        for _ in range(6):
            pool.append({'id': pid, 'position': pos, 'rank': 100 - pid}); pid += 1
    return pool


def _fr_clubs():
    return [{'club': f'Club{i}', 'rank': 50 - i} for i in range(6)]


def test_auto_draft_builds_fr_plus_14():
    avail = {p['id']: p for p in _individual_pool()}
    fr = _fr_clubs()
    owned, has_fr = [], False
    fr_taken = None
    for _ in range(DRAFT_PICKS_PER_TEAM):
        pick = d.auto_pick(list(avail.values()), fr, [p['position'] for p in owned], has_fr)
        assert pick is not None
        if pick['type'] == 'fr':
            has_fr = True; fr_taken = pick['club']
            fr = [c for c in fr if c['club'] != pick['club']]
        else:
            owned.append(avail.pop(pick['player']['id']))
    assert has_fr and fr_taken is not None
    assert len(owned) == ROSTER_SIZE          # 14 individuals
    starters, bench = d.choose_starting_xi(owned)
    assert len(starters) == STARTER_COUNT and len(bench) == BENCH_COUNT
    sel = [(p['position'], False) for p in starters] + [(p['position'], True) for p in bench]
    assert validate_roster(sel)[0]


def test_unmet_starter_needs_individual_only():
    needs = d.unmet_starter_needs(['OBK', 'OBK', 'OBK', 'FH'])
    assert needs['OBK'] == 0 and needs['FH'] == 0
    assert needs['MID'] == 2 and needs['LK'] == 1 and 'PR' not in needs
