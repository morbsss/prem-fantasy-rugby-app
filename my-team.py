"""
Scrapes superbru fantasy team selections for rounds 1-11 and stores them in SQLite.

Usage:
    python my-team.py              # scrape all rounds and persist to DB
    python my-team.py --debug      # also saves raw HTML for round 1 of each team
"""

import argparse
import re
import sqlite3
import sys
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup as bs

from parameters import team_id

LOGIN_URL = 'https://www.superbru.com/login'
TEAM_URL  = 'https://www.superbru.com/premrugbyfantasy/play_points.php'
DB_PATH   = 'prem_rugby_25_26_test.db'

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/116.0.0.0 Safari/537.36'
    ),
}


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _find_endpoint_in_js(text: str) -> tuple[str, str] | None:
    """
    Search JS for a fetch/$.post/$.ajax call to a login endpoint.
    Returns (endpoint_url, full_js_text) so the caller can also extract keys.
    """
    matches = re.findall(
        r'''(?:fetch|\.post)\s*\(\s*['"]([^'"]+)['"]'''
        r'''|(?:url)\s*[:=]\s*['"]([^'"]+)['"]''',
        text,
    )
    for groups in matches:
        match = next((g for g in groups if g), None)
        if not match:
            continue
        if re.search(r'\b(login|auth|signin|sign[_-]in)\b', match.lower()):
            url = match if match.startswith('http') else 'https://www.superbru.com' + match
            return url, text
    return None


def _extract_post_keys(js_text: str, endpoint: str) -> tuple[str | None, str | None]:
    """
    Given the JS that contains a login endpoint, extract the email and password
    key names from the data object passed to that endpoint.
    Returns (email_key, password_key) — either may be None if not found.
    """
    path = endpoint.replace('https://www.superbru.com', '')
    idx = js_text.find(path)
    if idx == -1:
        return None, None

    window = js_text[max(0, idx - 600): idx + 600]
    data_match = re.search(r'data\s*[:=]\s*\{([^}]+)\}', window)
    if not data_match:
        return None, None

    keys = re.findall(r'''['"]?([\w-]+)['"]?\s*:''', data_match.group(1))
    email_key = next((k for k in keys if 'email' in k.lower() or 'user' in k.lower()), None)
    pw_key    = next((k for k in keys if 'pass'  in k.lower() or 'pwd'  in k.lower()), None)
    return email_key, pw_key


def get_login_fields(session: requests.Session) -> tuple[dict, str, str, str]:
    """
    Fetch the login page.
    Returns (hidden_fields, post_url, email_key, password_key).
    """
    resp = session.get(LOGIN_URL, headers=HEADERS)
    resp.raise_for_status()
    soup = bs(resp.text, 'html.parser')

    form = soup.find('form')
    if not form:
        raise RuntimeError('Could not find a login form on the page.')

    hidden_fields = {}
    for inp in form.find_all('input', type='hidden'):
        if inp.get('name'):
            hidden_fields[inp['name']] = inp.get('value', '')

    un_id = next(
        (inp['id'] for inp in form.find_all('input', type='email') if inp.get('id')),
        'email-superbru',
    )
    pw_id = next(
        (inp['id'] for inp in form.find_all('input', type='password') if inp.get('id')),
        'password-superbru',
    )

    post_url  = None
    js_source = ''

    for script in soup.find_all('script', src=False):
        result = _find_endpoint_in_js(script.string or '')
        if result:
            post_url, js_source = result
            break

    if not post_url:
        for script in soup.find_all('script', src=True):
            src = script['src']
            if 'superbru' not in src and not src.startswith('/'):
                continue
            if not src.startswith('http'):
                src = 'https://www.superbru.com' + src
            try:
                js_resp = session.get(src, headers=HEADERS, timeout=10)
                if len(js_resp.content) > 50_000:
                    print(f'Skipping large file ({len(js_resp.content) // 1024} KB): {src}')
                    continue
                result = _find_endpoint_in_js(js_resp.text)
                if result:
                    post_url, js_source = result
                    print(f'Found endpoint in: {src}')
                    break
            except requests.RequestException:
                continue

    if not post_url:
        post_url = LOGIN_URL
        print('Warning: could not find login endpoint in JS — falling back to login page URL.')

    email_key, pw_key = _extract_post_keys(js_source, post_url)

    if not email_key:
        email_key = un_id.replace('-superbru', '')
    if not pw_key:
        pw_key = pw_id.replace('-superbru', '')

    print(f'Login endpoint    : {post_url}')
    print(f'Email key         : {email_key}')
    print(f'Password key      : {pw_key}')
    return hidden_fields, post_url, email_key, pw_key


def login(session: requests.Session, username: str, password: str) -> None:
    hidden_fields, post_url, email_key, pw_key = get_login_fields(session)

    payload = {**hidden_fields, email_key: username, pw_key: password}
    print(f'Payload fields : {list(payload.keys())}')

    ajax_headers = {
        **HEADERS,
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': LOGIN_URL,
        'Origin': 'https://www.superbru.com',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
    }

    resp = session.post(post_url, data=payload, headers=ajax_headers, allow_redirects=True)
    print(f'Response       : {resp.status_code} — {resp.url}')
    resp.raise_for_status()

    try:
        body = resp.json()
        print(f'Response body  : {body}')
        if isinstance(body, dict):
            if (body.get('redirect') or body.get('success')
                    or body.get('result') == 'success'
                    or body.get('status') in ('ok', 'success', '1', 1)):
                print('Logged in successfully.')
                return
            err = body.get('error') or body.get('message') or body.get('msg') or 'unknown error'
            raise RuntimeError(f'Login failed — {err}')
    except requests.exceptions.JSONDecodeError:
        pass

    if 'login' in resp.url.lower():
        raise RuntimeError('Login failed — check your username and password.')

    print('Logged in successfully.')


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def setup_selections_table(conn: sqlite3.Connection) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS team_selections (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            round       INTEGER NOT NULL,
            team_name   TEXT NOT NULL,
            player_id   INTEGER NOT NULL REFERENCES players(player_id),
            is_captain  INTEGER NOT NULL DEFAULT 0,
            is_kicker   INTEGER NOT NULL DEFAULT 0,
            is_bench    INTEGER NOT NULL DEFAULT 0,
            jersey      INTEGER,
            scraped_at  TEXT NOT NULL,
            UNIQUE(round, team_name, player_id)
        )
    ''')


def get_player_id(conn: sqlite3.Connection, last_name: str, position: str | None) -> int | None:
    if '...' in str(last_name) or '…' in str(last_name):
        last_name   = last_name.replace('...', '').replace('…', '').strip()
        where_query = f'{last_name}%'
    else:
        where_query = f'{last_name},%'

    if position:
        row = conn.execute(
            "SELECT player_id FROM players WHERE name LIKE ? AND position = ?",
            (where_query, position),
        ).fetchone()
        if row:
            return row[0]
    # Fall back to last name only
    row = conn.execute(
        "SELECT player_id FROM players WHERE name LIKE ?",
        (where_query,),
    ).fetchone()
    if row:
        print(f'  Warning: "{last_name}" matched by name only — position check failed.')
        return row[0]
    return None


def get_current_round(conn: sqlite3.Connection) -> int:
    """Return the latest round scraped into weekly_stats."""
    row = conn.execute('SELECT MAX(round) FROM weekly_stats').fetchone()
    return row[0] if row[0] is not None else 1


_JERSEY_POSITION = {
    1: 'PR', 3: 'PR',
    2: 'HK',
    4: 'LK', 5: 'LK',
    6: 'LF', 7: 'LF', 8: 'LF',
    9: 'SH',
    10: 'FH',
    11: 'OBK', 14: 'OBK', 15: 'OBK',
    12: 'MID', 13: 'MID',
}


def upsert_selection(conn: sqlite3.Connection, round_num: int, team_name: str,
                     player_id: int, is_captain: bool, is_kicker: bool,
                     scraped_at: str, is_bench: bool = False,
                     jersey: int | None = None) -> None:
    conn.execute('''
        INSERT INTO team_selections
            (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(round, team_name, player_id) DO UPDATE SET
            is_captain = excluded.is_captain,
            is_kicker  = excluded.is_kicker,
            is_bench   = excluded.is_bench,
            jersey     = excluded.jersey,
            scraped_at = excluded.scraped_at
    ''', (round_num, team_name, player_id,
          int(is_captain), int(is_kicker), int(is_bench), jersey, scraped_at))


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def fetch_team_page(session: requests.Session, round_num: int, ppl_id: str) -> tuple[bs, str]:
    url = f'{TEAM_URL}?ppl={ppl_id}&r={round_num}'
    resp = session.get(url, headers=HEADERS)
    resp.raise_for_status()
    return bs(resp.text, 'html.parser'), resp.text


def parse_team(soup: bs) -> list[tuple[int, str, list[str]]]:
    """
    Returns a sorted list of (jersey_num, player_name, flags) for jersey numbers 1-15.
    flags contains 'C' (captain) and/or 'K' (kicker) if detected in the parent element.
    """
    players = []

    for block in soup.find_all('div', class_='playerNumberName'):
        num_el  = block.find('span', class_='number')
        name_el = block.find('div', class_='name')
        if not num_el or not name_el:
            continue

        try:
            num = int(num_el.get_text(strip=True))
        except ValueError:
            continue

        if num > 15:
            continue

        name  = ' '.join(re.sub(r'^[A-Z]\s+', '', name_el.get_text(strip=True)).split())
        flags = []
        parent = block.parent
        if parent:
            for el in parent.find_all(True):
                t   = el.get_text(strip=True)
                cls = ' '.join(el.get('class', [])).lower()
                if (t == 'C' or 'captain' in cls) and 'C' not in flags:
                    flags.append('C')
                if (t == 'K' or 'kicker' in cls or 'kick' in cls) and 'K' not in flags:
                    flags.append('K')

        players.append((num, name, flags))

    return sorted(players, key=lambda x: x[0])


def display_team(round_num: int, starters: list[tuple[int, str, list[str]]], team_name: str) -> None:
    print(f'\n  {team_name} — ROUND {round_num}')
    print('  ' + '-' * 30)
    for num, name, flags in starters:
        flag_str = f' [{", ".join(sorted(flags))}]' if flags else ''
        print(f'  {num:2}. {name}{flag_str}')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description='Scrape superbru fantasy teams for rounds 1-11.')
    parser.add_argument('--debug', action='store_true', help='Save raw HTML for round 1 of each team')
    args = parser.parse_args()

    username = 'andrewmorbey1@gmail.com'
    password = 'sally35l'

    session = requests.Session()

    try:
        login(session, username, password)
    except RuntimeError as e:
        print(f'Error: {e}', file=sys.stderr)
        sys.exit(1)

    with sqlite3.connect(DB_PATH) as conn:
        setup_selections_table(conn)
        round_num = get_current_round(conn)
        print(f'Scraping team selections for round {round_num}.')

        for team_manager, ppl_id in team_id.items():
            soup, raw_html = fetch_team_page(session, round_num, ppl_id)

            if args.debug:
                fname = f'debug_r{round_num}_{team_manager}.html'
                with open(fname, 'w', encoding='utf-8') as f:
                    f.write(raw_html)
                print(f'  Raw HTML saved to {fname}')

            starters = parse_team(soup)

            if not starters:
                print(f'  [{team_manager}] No data found — skipping.')
                continue

            scraped_at = datetime.now(timezone.utc).isoformat()
            for num, name, flags in starters:
                player_id = get_player_id(conn, name, _JERSEY_POSITION.get(num))
                if player_id is None:
                    print(f'  Warning: "{name}" not found in players table — skipping.')
                    continue
                upsert_selection(conn, round_num, team_manager, player_id,
                                 'C' in flags, 'K' in flags, scraped_at, jersey=num)
            conn.commit()
            print(f'  [{team_manager}] {len(starters)} players saved.')


if __name__ == '__main__':
    main()
