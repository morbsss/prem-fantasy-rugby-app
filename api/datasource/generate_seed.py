"""
Generate the mock seed JSON files (spec §3 "from local files").

Run once to (re)produce api/datasource/seed/<competition>.json:

    python -m api.datasource.generate_seed

The output is committed and is the contract of record for the mock adapter.
Generation is fully deterministic (seeded by competition + index, hashed with
sha1) so re-running yields byte-identical files. Per-round player scores and
match-day lineups are NOT stored here — the mock adapter derives them
deterministically from each player's `rate`/`vol` profile, which keeps the
files small and human-inspectable.
"""

import json
import hashlib
from pathlib import Path

SEED_DIR = Path(__file__).parent / 'seed'

N_ROUNDS = 18                      # 15 regular + 2 semi legs + final (matches competition.py)
SEASON = '2026-27'

# Real teams per competition (name, abbreviation).
REAL_TEAMS = {
    'premiership': [
        ('Bath', 'BAT'), ('Bristol Bears', 'BRI'), ('Exeter Chiefs', 'EXE'),
        ('Gloucester', 'GLO'), ('Harlequins', 'HAR'), ('Leicester Tigers', 'LEI'),
        ('Northampton Saints', 'NOR'), ('Sale Sharks', 'SAL'), ('Saracens', 'SAR'),
        ('Newcastle Red Bulls', 'NEW'),
    ],
    'super_rugby': [
        ('Blues', 'BLU'), ('Chiefs', 'CHI'), ('Crusaders', 'CRU'),
        ('Highlanders', 'HIG'), ('Hurricanes', 'HUR'), ('Moana Pasifika', 'MOA'),
        ('ACT Brumbies', 'BRU'), ('Queensland Reds', 'RED'), ('NSW Waratahs', 'WAR'),
        ('Western Force', 'FOR'), ('Fijian Drua', 'DRU'),
    ],
}

# Mock fantasy teams per league slug (8 = even / OFDS, 9 = odd / meatyboys to
# exercise the rotating-bye logic in §5.2).
FANTASY_TEAMS = {
    'ofds': [
        'Dulwich Panthers', 'Bread XV', 'Chessums Cheerleaders', 'London WaspCester',
        'George XV', 'Seldom', 'Pizza Morahana', 'Dirty Ruckers',
    ],
    'meatyboys': [
        'Auckland Anchors', 'Canterbury Crushers', 'Wellington Warriors',
        'Otago Outlaws', 'Waikato Wreckers', 'Pasifika Pythons',
        'Brumby Bruisers', 'Reef Sharks', 'Drua Dynamos',
    ],
}

# Season start (round 1 first kickoff) per competition. Chosen so most of the
# season is "in the past" relative to mid-2026 and standings populate.
SEASON_START = {
    'premiership': '2026-02-27',   # Fri
    'super_rugby': '2026-02-13',   # Fri
}

# Each real team carries this position spread (sums to 26 → deep enough for an
# 8–10 team draft of 17 players each, with free agents left over).
SQUAD_SHAPE = {'PR': 4, 'HK': 2, 'LK': 3, 'LF': 5, 'SH': 2, 'FH': 2, 'MID': 3, 'OBK': 5}

# Base scoring rate (points/round) by position — backs out-score forwards a bit.
BASE_RATE = {'PR': 4.5, 'HK': 5.0, 'LK': 5.5, 'LF': 6.0,
             'SH': 7.0, 'FH': 8.0, 'MID': 6.5, 'OBK': 7.0}

SURNAMES = [
    'Smith', 'Jones', 'Williams', 'Brown', 'Taylor', 'Davies', 'Wilson', 'Evans',
    'Thomas', 'Roberts', 'Walker', 'Wright', 'Robinson', 'Thompson', 'White',
    'Hughes', 'Edwards', 'Green', 'Lewis', 'Wood', 'Harris', 'Martin', 'Clarke',
    'Carter', 'Phillips', 'Watson', 'Turner', 'Hill', 'Moore', 'Cooper', 'Ward',
    'Morgan', 'King', 'Bennett', 'Price', 'Cole', 'Shaw', 'Bell', 'Murphy',
    'Reid', 'Kelly', 'Burns', 'Fox', 'Stone', 'Marsh', 'Vermeulen', 'du Toit',
    'van Rhyn', 'Kolbe', 'Mapimpi', 'Falesolo', 'Tuipulotu', 'Latu', 'Naholo',
    'Ioane', 'Havili', 'Reece', 'Barrett', 'Mauala', 'Sititi', 'Fihaki',
    'Nawaqanitawase', 'Suaalii', 'Petaia', 'Lolesio', 'Gordon', 'Wilkin',
    'Holloway', 'Frost', 'Salakaia-Loto', 'Tupou', 'Bell', 'Slipper',
]
INITIALS = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'R', 'S', 'T', 'W']


def _h(*parts) -> int:
    """Deterministic non-negative int from the given parts (stable across runs)."""
    key = '|'.join(str(p) for p in parts)
    return int(hashlib.sha1(key.encode()).hexdigest(), 16)


def _make_players(competition: str) -> list[dict]:
    players: list[dict] = []
    used: set[str] = set()
    pid = 0
    for team_name, _abbr in REAL_TEAMS[competition]:
        for pos, count in SQUAD_SHAPE.items():
            for k in range(count):
                seed = _h(competition, team_name, pos, k)
                surname = SURNAMES[seed % len(SURNAMES)]
                initial = INITIALS[(seed // 7) % len(INITIALS)]
                name = f'{surname},{initial}'
                # Resolve collisions by walking the initial list.
                bump = 0
                while name in used:
                    bump += 1
                    initial = INITIALS[(seed // 7 + bump) % len(INITIALS)]
                    name = f'{surname},{initial}'
                used.add(name)
                rate = round(BASE_RATE[pos] + (seed % 50) / 10.0, 1)   # rate..rate+5
                vol = round(0.2 + (seed % 7) / 10.0, 2)
                price = round(4.0 + rate * 0.6, 1)
                players.append({
                    'id': f'{competition[:3]}{pid:03d}',
                    'name': name, 'team': team_name, 'position': pos,
                    'rate': rate, 'vol': vol, 'price': price,
                })
                pid += 1
    return players


def _make_rounds(competition: str) -> list[dict]:
    from datetime import datetime, timedelta
    start = datetime.fromisoformat(SEASON_START[competition])
    teams = [t[0] for t in REAL_TEAMS[competition]]
    rounds = []
    for r in range(1, N_ROUNDS + 1):
        week0 = start + timedelta(weeks=r - 1)
        # Premiership: Fri 19:00 → Sun 15:00 UTC. Super Rugby plays in AEST/NZST,
        # which lands earlier in UTC: Fri 07:00 → Sun 06:00 UTC.
        if competition == 'premiership':
            first = week0.replace(hour=19, minute=0)
            last = (week0 + timedelta(days=2)).replace(hour=15, minute=0)
        else:
            first = week0.replace(hour=7, minute=0)
            last = (week0 + timedelta(days=2)).replace(hour=6, minute=0)
        # Pair teams up for the round (rotate so pairings vary; purely cosmetic —
        # fantasy fixtures are generated independently in competition.py).
        rot = teams[r % len(teams):] + teams[:r % len(teams)]
        matches = []
        for i in range(len(rot) // 2):
            matches.append({'home': rot[i], 'away': rot[len(rot) - 1 - i]})
        rounds.append({
            'round': r,
            'first_kickoff': first.isoformat() + 'Z',
            'last_kickoff': last.isoformat() + 'Z',
            'matches': matches,
        })
    return rounds


def build(competition: str, league_slug: str) -> dict:
    return {
        'competition': competition,
        'league_slug': league_slug,
        'season': SEASON,
        'n_rounds': N_ROUNDS,
        'real_teams': [{'name': n, 'abbr': a} for n, a in REAL_TEAMS[competition]],
        'fantasy_teams': FANTASY_TEAMS[league_slug],
        'players': _make_players(competition),
        'rounds': _make_rounds(competition),
    }


def main() -> None:
    SEED_DIR.mkdir(parents=True, exist_ok=True)
    for competition, league_slug in (('premiership', 'ofds'), ('super_rugby', 'meatyboys')):
        data = build(competition, league_slug)
        out = SEED_DIR / f'{competition}.json'
        out.write_text(json.dumps(data, indent=2), encoding='utf-8')
        print(f'Wrote {out}  ({len(data["players"])} players, {len(data["rounds"])} rounds)')


if __name__ == '__main__':
    main()
