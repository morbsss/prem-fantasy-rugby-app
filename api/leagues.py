"""
League registry and roster rules for Meatyboys Rugby Fantasy.

Two leagues run concurrently and independently (spec §1, §5.1):

  meatyboys  → Super Rugby Pacific   → existing repo theme  → Australia/Sydney
  ofds       → English Premiership   → red / blue / white   → Europe/London

This module is pure configuration — no DB, no network — so it can be imported
by the schema layer, the data-source adapters, the draft engine, and the UI
without creating import cycles.
"""

# ---------------------------------------------------------------------------
# League definitions
# ---------------------------------------------------------------------------

LEAGUES = {
    'meatyboys': {
        'slug':        'meatyboys',
        'name':        'meatyboys',
        'brand':       'Meatyboys',
        'competition': 'super_rugby',
        'comp_name':   'Super Rugby Pacific',
        # "existing repo colours" — the forest/cream/amber design system already
        # in base.html is the default theme.
        'theme':       'forest',
        'timezone':    'Australia/Sydney',
        # Live-source identifiers. The mock adapter is the contract of record
        # (spec §3, §8.6); these are best-effort hooks for the live adapter.
        'espn_league_id':   '270557',   # ESPN Super Rugby Pacific (best-effort)
        'superbru_table':   None,        # no confirmed SuperBru table id yet
    },
    'ofds': {
        'slug':        'ofds',
        'name':        'Owen Farrell Disappreciation Society',
        'brand':       'OFDS',
        'competition': 'premiership',
        'comp_name':   'English Premiership',
        'theme':       'union',          # red / blue / white
        'timezone':    'Europe/London',
        'espn_league_id':   '267979',   # ESPN Gallagher Premiership
        'superbru_table':   '2017',
    },
}

# The competition each league mirrors, keyed for convenience.
COMPETITION_BY_LEAGUE = {k: v['competition'] for k, v in LEAGUES.items()}

DEFAULT_LEAGUE = 'ofds'   # the league the original single-league data belongs to


def get_league(slug: str) -> dict:
    """Return the config for a league slug, or raise KeyError."""
    return LEAGUES[slug]


def league_slugs() -> list[str]:
    return list(LEAGUES.keys())


# ---------------------------------------------------------------------------
# Roster rules — front row is a CLUB UNIT, the rest are individual players
# ---------------------------------------------------------------------------
#
# A squad = ONE club front-row unit (e.g. "Leicester FR") + 14 individual
# players. Props (PR) and hookers (HK) are NOT owned individually — they only
# score via the front-row unit, whose scoring players come from the club's real
# matchday lineup (status S/B). The individual squad is unconstrained (any 14);
# the starting-team composition is enforced only on save (validate_roster):
#
#   Front Row unit  (1 club → fills 3 of the 13 starting spots)
#   Lock            1   (LK)
#   Loose Forwards  2   (LF)
#   Half Back       1   (SH)
#   Fly Half        1   (FH)
#   Midfielders     2   (MID)
#   Outside Backs   3   (OBK)
#   -----------------------------
#   Individual starters  10   (+ front-row unit = 13 starting spots)
#   Bench (any individual) 4
#   Individual squad      14   (+ 1 front-row unit)

# Positions owned only via the club front-row unit (never drafted individually).
FR_POSITIONS = ('PR', 'HK')
# The front-row unit fills this many of the 13 starting spots (display only).
FRONT_ROW_SPOTS = 3

# Individual (non-front-row) positions and their starting-slot requirements.
INDIVIDUAL_POSITIONS = ['LK', 'LF', 'SH', 'FH', 'MID', 'OBK']

SLOT_POSITIONS: dict[str, set[str]] = {
    'LK':  {'LK'},
    'LF':  {'LF'},
    'SH':  {'SH'},
    'FH':  {'FH'},
    'MID': {'MID'},
    'OBK': {'OBK'},
}

STARTER_SLOTS: dict[str, int] = {
    'LK': 1, 'LF': 2, 'SH': 1, 'FH': 1, 'MID': 2, 'OBK': 3,
}

BENCH_COUNT = 4
STARTER_COUNT = sum(STARTER_SLOTS.values())          # 10 individual starters
ROSTER_SIZE = STARTER_COUNT + BENCH_COUNT            # 14 individual players (+ 1 FR unit)
DRAFT_PICKS_PER_TEAM = ROSTER_SIZE + 1               # 15 (14 individuals + 1 FR unit)

# All recognised player position codes.
POSITIONS = ['PR', 'HK', 'LK', 'LF', 'SH', 'FH', 'MID', 'OBK']

# Reverse map: position code → the starting slots it is eligible to fill.
SLOTS_FOR_POSITION: dict[str, set[str]] = {pos: set() for pos in POSITIONS}
for _slot, _positions in SLOT_POSITIONS.items():
    for _pos in _positions:
        SLOTS_FOR_POSITION.setdefault(_pos, set()).add(_slot)


def eligible_slots(position: str) -> set[str]:
    """Starting slots a player of `position` can fill (empty set ⇒ bench only)."""
    return SLOTS_FOR_POSITION.get(position, set())


def starter_demand_by_position() -> dict[str, int]:
    """
    Minimum number of players of each position needed to fill the starting XI.
    Slots that accept multiple positions (Front Row = PR|HK) are attributed to
    their first listed position for demand-estimation purposes; the draft
    engine validates the real eligibility graph, this is only a sizing hint.
    """
    demand: dict[str, int] = {pos: 0 for pos in POSITIONS}
    for slot, count in STARTER_SLOTS.items():
        # Attribute to the alphabetically-first eligible position as a hint.
        primary = sorted(SLOT_POSITIONS[slot])[0]
        demand[primary] += count
    return demand


# ---------------------------------------------------------------------------
# Roster feasibility (used by the draft engine and the squad-save validator)
# ---------------------------------------------------------------------------

def _slot_units() -> list[set[str]]:
    """STARTER_SLOTS expanded into one allowed-position set per starting place
    (length == STARTER_COUNT, i.e. 11)."""
    units: list[set[str]] = []
    for slot, count in STARTER_SLOTS.items():
        units.extend(SLOT_POSITIONS[slot] for _ in range(count))
    return units


def assign_starters(positions: list[str]) -> list[int] | None:
    """Try to place `positions` (one per starter) into the starting slots.

    Returns a list mapping each starting-place index → the index in `positions`
    assigned to it, or None if no perfect matching exists. Bipartite matching
    via augmenting paths; inputs are tiny (11 players, 11 places).
    """
    units = _slot_units()
    if len(positions) != len(units):
        return None
    place_to_player = [-1] * len(units)

    def augment(player_idx: int, seen: list[bool]) -> bool:
        for j, allowed in enumerate(units):
            if positions[player_idx] in allowed and not seen[j]:
                seen[j] = True
                if place_to_player[j] == -1 or augment(place_to_player[j], seen):
                    place_to_player[j] = player_idx
                    return True
        return False

    for i in range(len(positions)):
        if not augment(i, [False] * len(units)):
            return None
    return place_to_player


def can_fill_starters(positions: list[str]) -> bool:
    """True if exactly these starter positions can fill the starting slots."""
    return assign_starters(positions) is not None


def validate_roster(selections: list[tuple[str, bool]]) -> tuple[bool, str | None]:
    """Validate the 14 INDIVIDUAL squad players (the front-row unit is separate).

    `selections` is a list of (position, is_bench) for the non-front-row players.
    Requires exactly ROSTER_SIZE (14) individuals, BENCH_COUNT (4) bench, and a
    startable 10 that fills LK/LF/SH/FH/MID/OBK. Props/hookers must not appear
    here (they belong to the club front-row unit).
    """
    if any(pos in FR_POSITIONS for pos, _ in selections):
        return False, 'Props and hookers are part of the club front-row unit, not the squad.'
    if len(selections) != ROSTER_SIZE:
        return False, f'Squad must be {ROSTER_SIZE} outfield players (got {len(selections)})'
    bench = [pos for pos, is_bench in selections if is_bench]
    starters = [pos for pos, is_bench in selections if not is_bench]
    if len(bench) != BENCH_COUNT:
        return False, f'Need exactly {BENCH_COUNT} bench players (got {len(bench)})'
    if len(starters) != STARTER_COUNT:
        return False, f'Need exactly {STARTER_COUNT} outfield starters (got {len(starters)})'
    if not can_fill_starters(starters):
        return False, 'Starting line-up does not satisfy the position requirements'
    return True, None


def starter_minimums() -> list[tuple[frozenset[str], int]]:
    """Mandatory draft minimums: (allowed positions, count) per starting slot.

    A 17-man roster is valid iff it contains players covering every starting
    slot's minimum; the remaining BENCH_COUNT picks are any position.
    """
    return [(frozenset(SLOT_POSITIONS[slot]), count) for slot, count in STARTER_SLOTS.items()]
