"""Round-robin fixture generation (spec §5.2–5.3)."""

from collections import Counter

from api.competition import generate_regular_fixtures, REGULAR_ROUNDS


def test_generation_is_deterministic():
    teams = ['A', 'B', 'C', 'D']
    assert generate_regular_fixtures(teams) == generate_regular_fixtures(teams)


def test_even_league_has_no_byes_and_full_slate():
    teams = ['A', 'B', 'C', 'D']
    fx = generate_regular_fixtures(teams, n_rounds=6)
    weeks = {w for (w, *_rest) in fx}
    assert weeks == set(range(1, 7))
    for w in weeks:
        wk = [f for f in fx if f[0] == w]
        assert len(wk) == 2                       # 4 teams → 2 matches
        names = [t for (_, h, _, a, _) in wk for t in (h, a)]
        assert 'Bye' not in names
        assert sorted(names) == ['A', 'B', 'C', 'D']   # each team once, no self-play


def test_no_team_plays_itself():
    fx = generate_regular_fixtures(['A', 'B', 'C', 'D', 'E', 'F'], n_rounds=REGULAR_ROUNDS)
    assert all(h != a for (_, h, _, a, _) in fx)


def test_odd_league_rotates_byes_evenly():
    # 3 teams over one full cycle (3 rounds) → each team sits out exactly once.
    teams = ['A', 'B', 'C']
    fx = generate_regular_fixtures(teams, n_rounds=3)
    byes = Counter()
    for (_, h, _, a, _) in fx:
        if a == 'Bye':
            byes[h] += 1
        elif h == 'Bye':
            byes[a] += 1
    assert byes == Counter({'A': 1, 'B': 1, 'C': 1})


def test_default_is_fifteen_regular_rounds():
    fx = generate_regular_fixtures(['A', 'B', 'C', 'D'])
    assert max(w for (w, *_r) in fx) == REGULAR_ROUNDS
