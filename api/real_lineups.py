#!/usr/bin/env python3
"""
Scrape match lineups from an ESPN rugby lineups page.

Usage:
    python scripts/scrape_lineup.py <espn_url>  [--db PATH] [--dry-run]

Examples:
    python scripts/scrape_lineup.py https://www.espn.com/rugby/lineups/_/gameId/603052/league/267979
    python scripts/scrape_lineup.py https://www.espn.com/rugby/lineups/_/gameId/603052/league/267979 --db prem_rugby_26_27.db

The script fetches both teams' lineups from ESPN's API and prints them.
Pass --db to also upsert the players into the local SQLite players table (useful
for seeding next-season player data). Pass --dry-run to skip the DB write.
"""

import os
import sys
import re
import json
import sqlite3
import argparse
from datetime import date
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

LEAGUE_ID   = '267979'
LEAGUE_ABBR = 'Prem Rugby'
ROUND_GAP_DAYS = 4  # days gap that separates one round from the next

# ESPN position abbreviations -> app position codes
# App positions: PR, HK, LK, LF, SH, FH, MID, OBK
POSITION_MAP = {
    'P':   'PR',   # Prop (loosehead/tighthead)
    'LP':  'PR',   # Loosehead Prop
    'TP':  'PR',   # Tighthead Prop
    'PR':  'PR',
    'H':   'HK',   # Hooker
    'HK':  'HK',
    'L':   'LK',   # Lock
    'LK':  'LK',
    '2R':  'LK',   # Second Row
    'FL':  'LF',   # Flanker
    'LF':  'LF',
    'OF':  'LF',   # Openside Flanker
    'BF':  'LF',   # Blindside Flanker
    'N8':  'LF',   # Number 8 (loose forward)
    '8':   'LF',
    'SH':  'SH',   # Scrum Half
    'FH':  'FH',   # Fly Half
    'SO':  'FH',   # Stand Off
    'C':   'MID',  # Centre
    'IC':  'MID',  # Inside Centre
    'OC':  'MID',  # Outside Centre
    'MID': 'MID',
    'W':   'OBK',  # Wing
    'WI':  'OBK',
    'LW':  'OBK',  # Left Wing
    'RW':  'OBK',  # Right Wing
    'FB':  'OBK',  # Fullback
    'OBK': 'OBK',
    'R':   None,   # Replacement (bench) — position determined by jersey number
}


def parse_espn_url(raw):
    """Extract (game_id, league_id) from an ESPN URL or bare game ID."""
    m = re.search(r'gameId[=/](\d+)(?:[/_]league[=/](\d+))?', raw)
    if m:
        return m.group(1), m.group(2) or '267979'
    if raw.isdigit():
        return raw, '267979'
    raise ValueError(f'Cannot parse ESPN URL or game ID from: {raw!r}')


def fetch_json(url):
    """GET a URL and return parsed JSON. Sends browser-like headers."""
    req = Request(url, headers={
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/json, text/javascript, */*',
    })
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode('utf-8'))


def format_name(full_name):
    """Convert 'First Last' → 'Last,F' to match SuperBru player name format.

    Treats everything after the first word as the surname so that particles
    like 'van', 'de', 'du' are preserved (e.g. 'Ernst van Rhyn' → 'van Rhyn,E').
    Apostrophes are stripped to match SuperBru's normalisation
    (e.g. "Ma'asi-White" → 'Maasi-White').
    """
    parts = full_name.strip().split()
    if len(parts) < 2:
        return full_name
    first = parts[0]
    last = ' '.join(parts[1:]).replace("'", '')
    return f'{last},{first[0].upper()}'


def map_position(espn_code):
    """Return app position code for an ESPN position code, or None if unknown."""
    if not espn_code:
        return None
    return POSITION_MAP.get(espn_code.upper().strip())


def extract_lineups(data):
    """
    Parse ESPN API JSON and return a list of team dicts:
      [{ name, abbreviation, home_away, players: [{name, jersey, espn_pos, position, is_bench}] }]
    """
    rosters = data.get('rosters', [])
    if not rosters:
        # Some events nest under 'header' or use a different key — check a few
        raise ValueError(
            'No "rosters" key found in ESPN API response. '
            'The event may not have lineups published yet, or the URL/game ID may be wrong.'
        )

    teams = []
    for block in rosters:
        team_info = block.get('team', {})
        team_name = (
            team_info.get('displayName')
            or team_info.get('name')
            or 'Unknown'
        )
        abbreviation = team_info.get('abbreviation', '')
        home_away = block.get('homeAway', '')

        players = []
        for p in block.get('roster', []):
            athlete = p.get('athlete', {})
            full_name = (
                athlete.get('fullName')
                or athlete.get('displayName')
                or ''
            ).strip()

            # Jersey number
            raw_jersey = p.get('jersey')
            try:
                jersey = int(raw_jersey) if raw_jersey is not None else None
            except (ValueError, TypeError):
                jersey = None

            # Position — may be on the player record or nested inside athlete
            pos_obj = p.get('position') or athlete.get('position') or {}
            espn_pos = (
                pos_obj.get('abbreviation')
                or pos_obj.get('name')
                or ''
            ).upper().strip()
            app_pos = map_position(espn_pos)

            # Jersey number is the most reliable way to determine bench status.
            # ESPN's 'starter' flag is False for all players on completed matches,
            # so we can't trust it — always use jersey number instead.
            is_bench = jersey is not None and jersey > 15

            players.append({
                'name':      full_name,
                'jersey':    jersey,
                'espn_pos':  espn_pos,
                'position':  app_pos,
                'is_bench':  is_bench,
            })

        # Sort by jersey number so output is in shirt-number order
        players.sort(key=lambda x: x['jersey'] if x['jersey'] is not None else 99)

        teams.append({
            'name':         team_name,
            'abbreviation': abbreviation,
            'home_away':    home_away,
            'players':      players,
        })

    return teams


def print_lineups(teams):
    """Pretty-print both teams' lineups to stdout."""
    for team in teams:
        ha = f"  [{team['home_away'].upper()}]" if team['home_away'] else ''
        print(f"\n{'=' * 55}")
        print(f"  {team['name']}{ha}  ({team['abbreviation']})")
        print(f"{'=' * 55}")
        print(f"  {'#':<4} {'Player':<28} {'ESPN':<8} {'App Pos'}")
        print(f"  {'-' * 4} {'-' * 28} {'-' * 8} {'-' * 7}")

        starters    = [p for p in team['players'] if not p['is_bench']]
        replacements = [p for p in team['players'] if p['is_bench']]

        for p in starters:
            pos = p['position'] or '?'
            print(f"  {p['jersey'] or '?':<4} {p['name']:<28} {p['espn_pos']:<8} {pos}")

        if replacements:
            print(f"  {'--- Replacements ---'}")
            for p in replacements:
                pos = p['position'] or '?'
                print(f"  {p['jersey'] or '?':<4} {p['name']:<28} {p['espn_pos']:<8} {pos}")

    # Summary of unmapped positions
    all_players = [p for t in teams for p in t['players']]
    unmapped = [p for p in all_players if p['espn_pos'] and not p['position']]
    if unmapped:
        codes = sorted({p['espn_pos'] for p in unmapped})
        print(f"\n  Note: unmapped ESPN position codes (add to POSITION_MAP if needed): {', '.join(codes)}")


def insert_into_db(teams, db_path, round_num):
    """
    Upsert real-match lineup data into the match_lineups table.
    Names are stored in 'Last,F' format to match SuperBru player records.
    """
    from datetime import datetime, timezone

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Create table if it doesn't exist (mirrors api/db.py ensure_schema)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS match_lineups (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            round INTEGER NOT NULL,
            player_name TEXT NOT NULL,
            real_team TEXT NOT NULL,
            jersey INTEGER,
            is_bench INTEGER NOT NULL DEFAULT 0,
            scraped_at TEXT NOT NULL,
            UNIQUE(round, player_name, real_team)
        )
    ''')

    now = datetime.now(timezone.utc).isoformat()
    inserted = 0
    updated = 0

    for team in teams:
        real_team = team['name']
        for p in team['players']:
            if not p['name']:
                continue
            player_name = format_name(p['name'])
            cur.execute('''
                INSERT INTO match_lineups (round, player_name, real_team, jersey, is_bench, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(round, player_name, real_team) DO UPDATE SET
                    jersey    = excluded.jersey,
                    is_bench  = excluded.is_bench,
                    scraped_at = excluded.scraped_at
            ''', (round_num, player_name, real_team, p['jersey'], 1 if p['is_bench'] else 0, now))
            if cur.lastrowid and cur.rowcount:
                inserted += 1
            else:
                updated += 1

    conn.commit()
    cur.close()
    conn.close()

    total = inserted + updated
    print(f"\nDatabase: {db_path}")
    print(f"  {total} lineup entries written for round {round_num} (format: Last,F)")


def get_round_events(round_num, end_year=2026):
    """
    Fetch the full season scoreboard, verify it's Prem Rugby, group matches
    into rounds by date proximity, and return the events for round_num.
    """
    start_date = f'{end_year - 1}0901'
    end_date   = f'{end_year}0731'
    url = (
        f'https://site.api.espn.com/apis/site/v2/sports/rugby'
        f'/{LEAGUE_ID}/scoreboard?dates={start_date}-{end_date}&limit=200'
    )
    data = fetch_json(url)

    # Confirm we're looking at the right league
    leagues = data.get('leagues', [])
    prem = next((l for l in leagues if l.get('abbreviation') == LEAGUE_ABBR), None)
    if not prem:
        raise ValueError(f'{LEAGUE_ABBR!r} league not found in ESPN response')

    events = sorted(data.get('events', []), key=lambda e: e.get('date', ''))

    # Group events into rounds: a gap > ROUND_GAP_DAYS between match dates = new round
    rounds = []
    current_round = []
    prev_date = None

    for event in events:
        try:
            cur_date = date.fromisoformat(event.get('date', '')[:10])
        except ValueError:
            continue
        if prev_date and (cur_date - prev_date).days > ROUND_GAP_DAYS:
            if current_round:
                rounds.append(current_round)
            current_round = []
        current_round.append(event)
        prev_date = cur_date

    if current_round:
        rounds.append(current_round)

    if round_num < 1 or round_num > len(rounds):
        raise ValueError(f'Round {round_num} not found — season has {len(rounds)} rounds')

    return rounds[round_num - 1]


def main():
    round_num = 15

    print(f'Fetching {LEAGUE_ABBR} season fixtures...')
    try:
        events = get_round_events(round_num)
    except (URLError, HTTPError) as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)

    print(f'Round {round_num}: {len(events)} matches\n')

    all_teams = []
    for event in events:
        game_id = event['id']
        comp    = event['competitions'][0]
        home    = next((c for c in comp['competitors'] if c['homeAway'] == 'home'), {})
        away    = next((c for c in comp['competitors'] if c['homeAway'] == 'away'), {})
        home_name = home.get('team', {}).get('displayName', '?')
        away_name = away.get('team', {}).get('displayName', '?')
        match_date = event.get('date', '')[:10]
        status     = event.get('status', {}).get('type', {}).get('description', '')

        print(f'\n{"#" * 60}')
        print(f'  {home_name} vs {away_name}  ({match_date})  [{status}]')
        print(f'{"#" * 60}')

        summary_url = (
            f'https://site.api.espn.com/apis/site/v2/sports/rugby'
            f'/{LEAGUE_ID}/summary?event={game_id}'
        )
        try:
            summary = fetch_json(summary_url)
            teams = extract_lineups(summary)
            print_lineups(teams)
            all_teams.extend(teams)
        except ValueError as e:
            print(f'  Lineups not yet available: {e}')

    if all_teams:
        db_path = os.getenv('DB_PATH', 'prem_rugby_25_26.db')
        insert_into_db(all_teams, db_path, round_num)


if __name__ == '__main__':
    main()
