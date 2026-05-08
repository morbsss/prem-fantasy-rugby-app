"""
Fetch the Premiership Rugby season schedule from ESPN and upsert first/last
kickoff times for each round into the local rounds table.

Usage:
    python api/sync_rounds.py              # current season (end year 2026)
    python api/sync_rounds.py --year 2027  # next season

The rounds table drives the pick-lockout logic in the app: selections lock
the moment the first match of each round kicks off (all times UTC).
"""

import os
import json
import sqlite3
import argparse
from datetime import date, datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError

LEAGUE_ID      = '267979'
LEAGUE_ABBR    = 'Prem Rugby'
ROUND_GAP_DAYS = 4


def fetch_json(url):
    req = Request(url, headers={
        'User-Agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/124.0.0.0 Safari/537.36'
        ),
        'Accept': 'application/json',
    })
    with urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode('utf-8'))


def _group_into_rounds(events):
    """Group a sorted list of ESPN events into rounds by date gap."""
    rounds, current, prev_date = [], [], None
    for event in events:
        try:
            cur_date = date.fromisoformat(event.get('date', '')[:10])
        except ValueError:
            continue
        if prev_date and (cur_date - prev_date).days > ROUND_GAP_DAYS:
            if current:
                rounds.append(current)
            current = []
        current.append(event)
        prev_date = cur_date
    if current:
        rounds.append(current)
    return rounds


def fetch_rounds(end_year=2026):
    """Return list of (round_num, first_kickoff_utc, last_kickoff_utc, matches)."""
    start_date = f'{end_year - 1}0901'
    end_date   = f'{end_year}0731'
    url = (
        f'https://site.api.espn.com/apis/site/v2/sports/rugby'
        f'/{LEAGUE_ID}/scoreboard?dates={start_date}-{end_date}&limit=200'
    )
    data = fetch_json(url)

    leagues = data.get('leagues', [])
    if not any(l.get('abbreviation') == LEAGUE_ABBR for l in leagues):
        raise ValueError(f'{LEAGUE_ABBR!r} not found in ESPN response')

    events = sorted(data.get('events', []), key=lambda e: e.get('date', ''))
    grouped = _group_into_rounds(events)

    result = []
    for i, rnd in enumerate(grouped, 1):
        kickoffs = []
        matches  = []
        for event in rnd:
            dt_str = event.get('date', '')
            if not dt_str:
                continue
            dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            kickoffs.append(dt)
            comp = event['competitions'][0]
            home = next((c for c in comp['competitors'] if c['homeAway'] == 'home'), {})
            away = next((c for c in comp['competitors'] if c['homeAway'] == 'away'), {})
            matches.append({
                'home': home.get('team', {}).get('displayName', '?'),
                'home_abbr': home.get('team', {}).get('abbreviation', '?'),
                'away': away.get('team', {}).get('displayName', '?'),
                'away_abbr': away.get('team', {}).get('abbreviation', '?'),
                'kickoff': dt.isoformat(),
                'status': event.get('status', {}).get('type', {}).get('description', ''),
            })
        if not kickoffs:
            continue
        result.append((
            i,
            min(kickoffs).isoformat(),
            max(kickoffs).isoformat(),
            matches,
        ))
    return result


def upsert_rounds(db_path, rounds):
    conn = sqlite3.connect(db_path)
    cur  = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS rounds (
            round_number INTEGER PRIMARY KEY,
            first_kickoff TEXT NOT NULL,
            last_kickoff  TEXT NOT NULL
        )
    ''')
    for round_num, first_ko, last_ko, _ in rounds:
        cur.execute('''
            INSERT INTO rounds (round_number, first_kickoff, last_kickoff)
            VALUES (?, ?, ?)
            ON CONFLICT(round_number) DO UPDATE SET
                first_kickoff = excluded.first_kickoff,
                last_kickoff  = excluded.last_kickoff
        ''', (round_num, first_ko, last_ko))
    conn.commit()
    conn.close()


def main():
    parser = argparse.ArgumentParser(description='Sync ESPN round schedule to local DB')
    parser.add_argument('--year', type=int, default=2026,
                        help='Season end year (default: 2026)')
    parser.add_argument('--db', default=None,
                        help='DB path (default: DB_PATH env var or prem_rugby_25_26.db)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print rounds without writing to DB')
    args = parser.parse_args()

    db_path = args.db or os.getenv('DB_PATH', 'prem_rugby_25_26.db')

    print(f'Fetching {LEAGUE_ABBR} {args.year - 1}/{args.year} schedule...\n')
    try:
        rounds = fetch_rounds(args.year)
    except (URLError, HTTPError) as e:
        print(f'Network error: {e}')
        raise SystemExit(1)

    for round_num, first_ko, last_ko, matches in rounds:
        first_dt = datetime.fromisoformat(first_ko)
        last_dt  = datetime.fromisoformat(last_ko)
        print(f'Round {round_num:>2}  '
              f'lock {first_dt.strftime("%a %d %b %H:%M UTC")}  '
              f'->  ends {last_dt.strftime("%a %d %b %H:%M UTC")}')
        for m in matches:
            ko = datetime.fromisoformat(m['kickoff'])
            print(f'         {ko.strftime("%a %d %b %H:%M UTC")}  '
                  f'{m["home_abbr"]} vs {m["away_abbr"]}  [{m["status"]}]')
        print()

    if not args.dry_run:
        upsert_rounds(db_path, rounds)
        print(f'Written {len(rounds)} rounds to {db_path}')
    else:
        print('(dry-run — nothing written)')


if __name__ == '__main__':
    main()
