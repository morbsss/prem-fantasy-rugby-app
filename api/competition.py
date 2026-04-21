"""
Fantasy Rugby Competition Table.

Reads fixtures from fixtures.csv and calculates weekly team scores
from team_selections + weekly_stats in prem_rugby_25_26.db.

Scoring:
  Win  = 4 league pts
  Draw = 2 league pts each
  Loss = 0 league pts
  Winning BP  = +1 if winning margin >= 81
  Losing BP   = +1 if losing margin <= 18
  Bye  = 2 league pts (no match played)
"""

import re
import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field

DB_PATH      = 'prem_rugby_25_26.db'
FIXTURES_CSV = 'fixtures.csv'

WIN_PTS          = 4
DRAW_PTS         = 2
LOSS_PTS         = 0
BP_PTS           = 1
WINNER_BP_MARGIN = 27   # winner gets BP if margin >= this
LOSER_BP_MARGIN  = 11   # loser gets BP if margin <= this


def _get_placeholder(conn):
    """Return the appropriate placeholder for the database type."""
    try:
        import psycopg2
        if isinstance(conn, psycopg2.extensions.connection):
            return '%s'
    except ImportError:
        pass
    return '?'


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Team:
    name:           str
    played:         int   = 0
    won:            int   = 0
    drawn:          int   = 0
    lost:           int   = 0
    points_for:     float = 0.0
    points_against: float = 0.0
    bonus_points:   int   = 0
    league_points:  int   = 0

    @property
    def points_diff(self) -> float:
        return self.points_for - self.points_against


# ---------------------------------------------------------------------------
# Fixture parsing
# ---------------------------------------------------------------------------

def parse_fixtures(path: str) -> list[tuple[int, str, bool, str, bool]]:
    """
    Returns list of (week, home_team, home_bp, away_team, away_bp).
    'Bye' is kept as a team name — handled separately in scoring.
    """
    fixtures = []
    current_week = None

    with open(path, newline='', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip('\n')
            cols = [c.strip() for c in line.split(',')]

            week_match = re.match(r'Week (\d+)', cols[0])
            if week_match:
                current_week = int(week_match.group(1))
                continue

            if current_week is None or not any(cols):
                continue

            raw_home = cols[0]
            raw_away = cols[4] if len(cols) > 4 else ''

            if not raw_home or not raw_away:
                continue

            home_bp = raw_home.endswith(' BP')
            away_bp = raw_away.endswith(' BP')
            home    = raw_home.removesuffix(' BP').strip()
            away    = raw_away.removesuffix(' BP').strip()

            fixtures.append((current_week, home, home_bp, away, away_bp))

    return fixtures


# ---------------------------------------------------------------------------
# Score lookup
# ---------------------------------------------------------------------------

def get_team_score(conn, team_name: str, round_num: int) -> float:
    """
    Sum weekly points for all players in team_name's selection for round_num.
    - Points are cumulative, so each player's score = this round - previous round.
    - kick_delta is computed for every player (kicking is included in total_points).
    - Kicker (is_kicker=1): base_delta kept as-is (kicking already included).
    - Non-kicker: kick_delta subtracted (kicking not credited to non-kickers).
    - Captain (is_captain=1): (base_delta - kick_delta) * 2.
    - MAX() + GROUP BY guards against duplicate weekly_stats rows.
    """
    cursor = conn.cursor()
    placeholder = _get_placeholder(conn)
    cursor.execute(f'''
        SELECT COALESCE(SUM(
            CASE
                WHEN is_captain = 1 THEN (base_delta - kick_delta) * 2
                WHEN is_kicker  = 1 THEN base_delta
                ELSE base_delta - kick_delta
            END), 0)
        FROM (
            SELECT
                ts.is_captain,
                ts.is_kicker,
                MAX(ws_curr.total_points) - COALESCE(MAX(ws_prev.total_points), 0)
                    AS base_delta,
                CASE WHEN MAX(ws_prev.kicking) IS NULL THEN 0
                     ELSE COALESCE(CAST(MAX(ws_curr.kicking) AS REAL), 0)
                            - COALESCE(CAST(MAX(ws_prev.kicking) AS REAL), 0)
                END AS kick_delta
            FROM team_selections ts
            JOIN weekly_stats ws_curr
                ON ws_curr.player_id = ts.player_id AND ws_curr.round = ts.round
            LEFT JOIN weekly_stats ws_prev
                ON ws_prev.player_id = ts.player_id AND ws_prev.round = ts.round - 1
            WHERE ts.team_name = {placeholder} AND ts.round = {placeholder}
            GROUP BY ts.player_id, ts.is_captain, ts.is_kicker
        )
    ''', (team_name, round_num))
    row = cursor.fetchone()
    cursor.close()
    return float(row[0]) if row else 0.0


# ---------------------------------------------------------------------------
# Table calculation
# ---------------------------------------------------------------------------

def calculate_table(
    fixtures: list[tuple[int, str, bool, str, bool]],
    conn: sqlite3.Connection,
    max_round: int | None = None,
) -> list[Team]:
    teams: dict[str, Team] = {}

    # Group fixtures by week for two-pass bye processing
    weeks: dict[int, list] = defaultdict(list)
    for fix in fixtures:
        if max_round is None or fix[0] <= max_round:
            weeks[fix[0]].append(fix)

    for week, week_fixtures in sorted(weeks.items()):
        # Register all teams
        for _, home, _, away, _ in week_fixtures:
            for t in (home, away):
                if t != 'Bye' and t not in teams:
                    teams[t] = Team(name=t)

        # Pass 1: score all non-bye matches
        played_scores: dict[str, float] = {}
        for _, home, _, away, _ in week_fixtures:
            if home == 'Bye' or away == 'Bye':
                continue
            hs = get_team_score(conn, home, week)
            aw = get_team_score(conn, away, week)
            played_scores[home] = hs
            played_scores[away] = aw

        # Bye score = average of all non-bye team scores that week
        bye_score = (
            sum(played_scores.values()) / len(played_scores)
            if played_scores else 0.0
        )

        # Pass 2a: process regular matches
        for _, home, _, away, _ in week_fixtures:
            if home == 'Bye' or away == 'Bye':
                continue
            hs = played_scores[home]
            aw = played_scores[away]
            if hs == 0 and aw == 0:
                continue

            teams[home].played         += 1
            teams[away].played         += 1
            teams[home].points_for     += hs
            teams[home].points_against += aw
            teams[away].points_for     += aw
            teams[away].points_against += hs

            _apply_result(teams[home], teams[away], hs, aw)

        # Pass 2b: process bye matches vs the week average
        for _, home, _, away, _ in week_fixtures:
            if home != 'Bye' and away != 'Bye':
                continue
            team_name = home if away == 'Bye' else away
            ts = get_team_score(conn, team_name, week)
            if ts == 0 and bye_score == 0:
                continue

            teams[team_name].played         += 1
            teams[team_name].points_for     += ts
            teams[team_name].points_against += bye_score

            margin = abs(ts - bye_score)
            if ts > bye_score:
                teams[team_name].won           += 1
                teams[team_name].league_points += WIN_PTS
                if margin >= WINNER_BP_MARGIN:
                    teams[team_name].league_points += BP_PTS
                    teams[team_name].bonus_points  += BP_PTS
            elif ts < bye_score:
                teams[team_name].lost          += 1
                if margin <= LOSER_BP_MARGIN:
                    teams[team_name].league_points += BP_PTS
                    teams[team_name].bonus_points  += BP_PTS
            else:
                teams[team_name].drawn         += 1
                teams[team_name].league_points += DRAW_PTS

    return sorted(
        teams.values(),
        key=lambda t: (t.league_points, t.points_diff),
        reverse=True,
    )


def _apply_result(home: Team, away: Team, hs: float, aw: float) -> None:
    """Apply win/draw/loss + bonus points between two teams."""
    if hs > aw:
        margin = hs - aw
        home.won           += 1
        away.lost          += 1
        home.league_points += WIN_PTS
        if margin >= WINNER_BP_MARGIN:
            home.league_points += BP_PTS
            home.bonus_points  += BP_PTS
        if margin <= LOSER_BP_MARGIN:
            away.league_points += BP_PTS
            away.bonus_points  += BP_PTS
    elif aw > hs:
        margin = aw - hs
        away.won           += 1
        home.lost          += 1
        away.league_points += WIN_PTS
        if margin >= WINNER_BP_MARGIN:
            away.league_points += BP_PTS
            away.bonus_points  += BP_PTS
        if margin <= LOSER_BP_MARGIN:
            home.league_points += BP_PTS
            home.bonus_points  += BP_PTS
    else:
        home.drawn         += 1
        away.drawn         += 1
        home.league_points += DRAW_PTS
        away.league_points += DRAW_PTS


# ---------------------------------------------------------------------------
# Display
# ---------------------------------------------------------------------------

def display_table(table: list[Team]) -> None:
    print(f'\n{"=" * 82}')
    print(f'  FANTASY RUGBY — COMPETITION TABLE')
    print(f'{"=" * 82}')
    print(f'  {"#":>2}  {"Team":<35} {"P":>3} {"W":>3} {"D":>3} {"L":>3}'
          f' {"PF":>7} {"PA":>7} {"PD":>7} {"Pts":>4}')
    print(f'  {"-" * 78}')
    for i, t in enumerate(table, 1):
        pd_str = f'+{t.points_diff:.1f}' if t.points_diff >= 0 else f'{t.points_diff:.1f}'
        print(
            f'  {i:>2}. {t.name:<33} {t.played:>3} {t.won:>3} {t.drawn:>3} {t.lost:>3}'
            f' {t.points_for:>7.1f} {t.points_against:>7.1f} {pd_str:>7} {t.league_points:>4}'
        )
    print()


def display_results(
    fixtures: list[tuple[int, str, bool, str, bool]],
    conn: sqlite3.Connection,
    max_round: int | None = None,
) -> None:
    current_week = None
    for week, home, home_bp, away, away_bp in fixtures:
        if max_round is not None and week > max_round:
            break
        if week != current_week:
            current_week = week
            print(f'\n  --- Week {week} ---')
        if away == 'Bye':
            print(f'  {home} — BYE')
            continue
        if home == 'Bye':
            print(f'  {away} — BYE')
            continue
        hs = get_team_score(conn, home, week)
        as_ = get_team_score(conn, away, week)
        if hs == 0 and as_ == 0:
            print(f'  {home} vs {away} — no data')
        else:
            margin = abs(hs - as_)
            winner = home if hs > as_ else (away if as_ > hs else None)
            loser  = away if hs > as_ else (home if as_ > hs else None)
            w_bp   = winner and margin >= WINNER_BP_MARGIN
            l_bp   = loser  and margin <= LOSER_BP_MARGIN
            h_tag  = ' (BP)' if (home == winner and w_bp) or (home == loser and l_bp) else ''
            a_tag  = ' (BP)' if (away == winner and w_bp) or (away == loser and l_bp) else ''
            print(f'  {home}{h_tag} {hs:.1f} – {as_:.1f} {away}{a_tag}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    fixtures = parse_fixtures(FIXTURES_CSV)
    print(f'Loaded {len(fixtures)} fixtures.')

    with sqlite3.connect(DB_PATH) as conn:
        max_round = conn.execute('SELECT MAX(round) FROM weekly_stats').fetchone()[0]
        print(f'Stats available up to round {max_round}.\n')

        display_results(fixtures, conn, max_round)
        table = calculate_table(fixtures, conn, max_round)

    display_table(table)


if __name__ == '__main__':
    main()
