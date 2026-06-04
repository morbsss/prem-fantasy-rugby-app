"""
Snake-draft engine — pure logic, no DB or network.

Teams draft in snake order (1..N, N..1, 1..N, ...) for DRAFT_PICKS_PER_TEAM
picks: one club front-row UNIT + 14 individual players (api/leagues.py). Absent
users are auto-drafted. These functions operate on plain data so they're
unit-testable; the DB-backed orchestration lives in api/index.py.
"""

from .leagues import (
    STARTER_SLOTS, SLOT_POSITIONS, BENCH_COUNT, ROSTER_SIZE, STARTER_COUNT,
    DRAFT_PICKS_PER_TEAM,
)

TOTAL_PICKS_PER_TEAM = DRAFT_PICKS_PER_TEAM


def snake_sequence(order: list[str], rounds: int = DRAFT_PICKS_PER_TEAM) -> list[str]:
    """The full pick order: forward on odd rounds, reversed on even ones."""
    seq: list[str] = []
    for r in range(rounds):
        seq.extend(order if r % 2 == 0 else list(reversed(order)))
    return seq


def total_picks(order: list[str]) -> int:
    return len(order) * DRAFT_PICKS_PER_TEAM


def team_on_clock(order: list[str], pick_number: int) -> str | None:
    """Team for a 1-based pick number, or None if out of range / draft over."""
    seq = snake_sequence(order)
    if pick_number < 1 or pick_number > len(seq):
        return None
    return seq[pick_number - 1]


def draft_round_of_pick(order: list[str], pick_number: int) -> int:
    """1-based snake round a pick falls in."""
    return ((pick_number - 1) // len(order)) + 1 if order else 0


# ---------------------------------------------------------------------------
# Roster needs + auto-pick
# ---------------------------------------------------------------------------

def unmet_starter_needs(owned_positions: list[str]) -> dict[str, int]:
    """Per-slot count still required to guarantee a startable XI.

    Greedily attributes each owned player to a slot whose minimum is unmet;
    players that can't satisfy any remaining minimum are treated as bench/free.
    """
    remaining = dict(STARTER_SLOTS)
    for pos in owned_positions:
        for slot in remaining:
            if remaining[slot] > 0 and pos in SLOT_POSITIONS[slot]:
                remaining[slot] -= 1
                break
    return remaining


def _can_help(position: str, remaining: dict[str, int]) -> bool:
    return any(rem > 0 and position in SLOT_POSITIONS[slot]
               for slot, rem in remaining.items())


def auto_pick(available: list[dict], available_fr_clubs: list[dict],
              owned_positions: list[str], has_fr: bool) -> dict | None:
    """Choose the next draft entity for a team.

    Returns {'type': 'player', 'player': <dict>} for an individual, or
    {'type': 'fr', 'club': <name>} for a club front-row unit, or None.

    Priority: fill mandatory individual starter slots (by rank), then grab the
    front-row unit, then best-available individuals for the bench.
    """
    ind = sorted(available, key=lambda p: (-p.get('rank', 0), str(p['id'])))
    fr = sorted(available_fr_clubs, key=lambda c: (-c.get('rank', 0), c['club']))
    remaining = unmet_starter_needs(owned_positions)

    if sum(remaining.values()) > 0:
        helper = next((p for p in ind if _can_help(p['position'], remaining)), None)
        if helper is not None:
            return {'type': 'player', 'player': helper}
    if not has_fr and fr:
        return {'type': 'fr', 'club': fr[0]['club']}
    if ind:
        return {'type': 'player', 'player': ind[0]}
    if not has_fr and fr:
        return {'type': 'fr', 'club': fr[0]['club']}
    return None


def choose_starting_xi(roster: list[dict]) -> tuple[list[dict], list[dict]]:
    """Split the 14 INDIVIDUAL squad players into (starters, bench).

    `roster` items are dicts with 'position'. Assigns the highest-'rank' players
    into open starting slots (LK/LF/SH/FH/MID/OBK); once every slot is filled the
    remainder (4) become bench. The front-row unit is handled separately.
    """
    ranked = sorted(roster, key=lambda p: (-p.get('rank', 0), str(p.get('id', ''))))
    remaining = dict(STARTER_SLOTS)
    starters: list[dict] = []
    bench: list[dict] = []
    for p in ranked:
        placed = False
        if len(starters) < STARTER_COUNT:
            for slot in remaining:
                if remaining[slot] > 0 and p['position'] in SLOT_POSITIONS[slot]:
                    remaining[slot] -= 1
                    starters.append(p)
                    placed = True
                    break
        if not placed:
            bench.append(p)
    return starters, bench
