"""
Vercel Cron handler for player data scraping.

Runs every Tuesday at 12:00 UTC via Vercel Cron.
Endpoint: GET /api/cron/player-data

Scrapes player statistics from SuperBru and updates the database.
"""

import os
from datetime import datetime, timezone
from flask import Flask, jsonify, request
import requests as req_lib
import urllib3
from bs4 import BeautifulSoup

from ..db import get_connection, ensure_schema, execute as db_execute, DB_TYPE

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

CRON_SECRET = os.getenv('CRON_SECRET', '')

POSITION_MAP = {1: 'PR', 2: 'HK', 3: 'LK', 4: 'LF', 5: 'SH', 6: 'FH', 7: 'MID', 8: 'OBK'}
SUPERBRU_URL = 'https://www.superbru.com/premiershiprugbyfantasy/ajax/f_write_player_stats.php?'
SCRAPE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
}


def _cron_auth_ok():
    if not CRON_SECRET:
        return True
    return request.headers.get('Authorization') == f'Bearer {CRON_SECRET}'


def _to_float(val):
    try:
        return float(str(val).replace('£', '').replace('m', '').strip())
    except (ValueError, TypeError):
        return 0.0


def _to_price(val):
    try:
        return float(str(val).replace('£', '').replace('m', '').strip()) * 1_000_000
    except (ValueError, TypeError):
        return 0.0


def _get_next_round(conn) -> int:
    cur = db_execute(conn, 'SELECT MAX(round) FROM weekly_stats')
    row = cur.fetchone()
    cur.close()
    if DB_TYPE == 'postgres':
        result = row['max'] if row and row.get('max') else None
    else:
        result = row[0] if row else None
    return 1 if result is None else result + 1


@app.route('/api/cron/player-data')
def player_data_cron():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401

    conn = get_connection()
    ensure_schema(conn)
    round_num = _get_next_round(conn)
    scraped_at = datetime.now(timezone.utc).isoformat()

    players = []
    try:
        for page in range(1, 9):
            resp = req_lib.get(
                f'{SUPERBRU_URL}pg={page}&tbl=2017',
                headers=SCRAPE_HEADERS,
                timeout=10,
                verify=False,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            tbl = soup.find('tbody')
            if not tbl:
                continue
            for row in tbl.find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) < 8:
                    cells.insert(5, '0')
                players.append({
                    'team':         cells[0],
                    'name':         cells[1][:-1] if cells[1] else '',
                    'position':     POSITION_MAP[page],
                    'total_points': _to_float(cells[3]),
                    'price':        _to_price(cells[4]),
                    'kicking':      _to_float(cells[5]),
                    'ppg':          cells[6],
                    'popularity':   cells[7],
                    'form':         cells[8] if len(cells) > 8 else '',
                })
    except Exception as e:
        conn.close()
        return jsonify({'error': f'Scrape failed: {e}'}), 500

    upserted = 0
    for p in players:
        if not p['name']:
            continue

        db_execute(conn, '''
            INSERT INTO players (name, team, position)
            VALUES (?, ?, ?)
            ON CONFLICT (name, team, position) DO UPDATE SET
                team = excluded.team, position = excluded.position
        ''', (p['name'], p['team'], p['position'])).close()

        cur = db_execute(conn,
            'SELECT player_id FROM players WHERE name = ? AND team = ? AND position = ?',
            (p['name'], p['team'], p['position']),
        )
        row = cur.fetchone()
        cur.close()
        player_id = row['player_id'] if isinstance(row, dict) else row[0]

        db_execute(conn, '''
            INSERT INTO weekly_stats
                (player_id, round, total_points, price, kicking,
                 points_per_game, popularity, form, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (player_id, round) DO UPDATE SET
                total_points    = excluded.total_points,
                price           = excluded.price,
                kicking         = excluded.kicking,
                points_per_game = excluded.points_per_game,
                popularity      = excluded.popularity,
                form            = excluded.form,
                scraped_at      = excluded.scraped_at
        ''', (
            player_id, round_num,
            p['total_points'], p['price'], p['kicking'],
            p['ppg'], p['popularity'], p['form'], scraped_at,
        )).close()
        upserted += 1

    if round_num > 1:
        db_execute(conn, '''
            INSERT INTO team_selections
                (round, team_name, player_id, is_captain, is_kicker,
                 is_bench, jersey, scraped_at)
            SELECT ?, team_name, player_id, is_captain, is_kicker,
                   is_bench, jersey, ?
            FROM team_selections
            WHERE round = ?
            ON CONFLICT (round, team_name, player_id) DO NOTHING
        ''', (round_num, scraped_at, round_num - 1)).close()

    conn.commit()
    conn.close()
    return jsonify({'status': 'ok', 'round': round_num, 'players_upserted': upserted})


if __name__ == '__main__':
    app.run(debug=True, port=5001)
