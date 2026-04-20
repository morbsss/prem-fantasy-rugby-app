"""
Vercel Cron handler for player data scraping.

Runs every Tuesday at 12:00 UTC via Vercel Cron.
Endpoint: POST /api/cron/player-data

Scrapes player statistics from SuperBru and updates the database.
"""

import os
import sys
from datetime import datetime
from flask import Flask, jsonify

import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup as bs

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, DB_TYPE

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

# Position mapping
POSITION_MAP = {1: 'PR', 2: 'HK', 3: 'LK', 4: 'LF', 5: 'SH', 6: 'FH', 7: 'MID', 8: 'OBK'}

# Request headers
REQ_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/116.0.0.0 Safari/537.36'
    ),
}


def setup_database(conn) -> None:
    """Create tables if they don't exist."""
    cursor = conn.cursor()

    if DB_TYPE == 'postgres':
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                player_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                team TEXT,
                position TEXT,
                UNIQUE(name, team, position)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weekly_stats (
                id SERIAL PRIMARY KEY,
                player_id INTEGER NOT NULL REFERENCES players(player_id),
                round INTEGER NOT NULL,
                total_points REAL,
                price REAL,
                kicking TEXT,
                points_per_game TEXT,
                popularity TEXT,
                form TEXT,
                scraped_at TEXT NOT NULL,
                UNIQUE(player_id, round)
            )
        ''')
    else:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                player_id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                team TEXT,
                position TEXT,
                UNIQUE(name, team, position)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS weekly_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                player_id INTEGER NOT NULL REFERENCES players(player_id),
                round INTEGER NOT NULL,
                total_points REAL,
                price REAL,
                kicking TEXT,
                points_per_game TEXT,
                popularity TEXT,
                form TEXT,
                scraped_at TEXT NOT NULL,
                UNIQUE(player_id, round)
            )
        ''')

    conn.commit()
    cursor.close()


def get_next_round(conn) -> int:
    """Return max(round) + 1, or 0 if database has no data."""
    cursor = conn.cursor()
    cursor.execute('SELECT MAX(round) FROM weekly_stats')
    row = cursor.fetchone()
    cursor.close()

    if DB_TYPE == 'postgres':
        result = row['max'] if row and row.get('max') else None
    else:
        result = row[0] if row else None

    return 0 if result is None else result + 1


def upsert_player(conn, name: str, team: str, position: str) -> int:
    """Insert player if new; return player_id."""
    cursor = conn.cursor()

    if DB_TYPE == 'postgres':
        cursor.execute(
            'INSERT INTO players (name, team, position) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING',
            (name, team, position),
        )
    else:
        cursor.execute(
            'INSERT OR IGNORE INTO players (name, team, position) VALUES (?, ?, ?)',
            (name, team, position),
        )

    if DB_TYPE == 'postgres':
        cursor.execute(
            'SELECT player_id FROM players WHERE name = %s AND team = %s AND position = %s',
            (name, team, position),
        )
    else:
        cursor.execute(
            'SELECT player_id FROM players WHERE name = ? AND team = ? AND position = ?',
            (name, team, position),
        )

    row = cursor.fetchone()
    cursor.close()

    if DB_TYPE == 'postgres':
        return row['player_id'] if row else None
    else:
        return row[0] if row else None


def upsert_weekly_stats(conn, player_id: int, round_num: int, total_points, price,
                       kicking: str, ppg: str, popularity: str, form: str, scraped_at: str) -> None:
    """Insert or update stats for a player/round."""
    cursor = conn.cursor()

    if DB_TYPE == 'postgres':
        cursor.execute('''
            INSERT INTO weekly_stats
                (player_id, round, total_points, price, kicking, points_per_game,
                 popularity, form, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(player_id, round) DO UPDATE SET
                total_points = EXCLUDED.total_points,
                price = EXCLUDED.price,
                kicking = EXCLUDED.kicking,
                points_per_game = EXCLUDED.points_per_game,
                popularity = EXCLUDED.popularity,
                form = EXCLUDED.form,
                scraped_at = EXCLUDED.scraped_at
        ''', (player_id, round_num, total_points, price, kicking, ppg, popularity, form, scraped_at))
    else:
        cursor.execute('''
            INSERT INTO weekly_stats
                (player_id, round, total_points, price, kicking, points_per_game,
                 popularity, form, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(player_id, round) DO UPDATE SET
                total_points = excluded.total_points,
                price = excluded.price,
                kicking = excluded.kicking,
                points_per_game = excluded.points_per_game,
                popularity = excluded.popularity,
                form = excluded.form,
                scraped_at = excluded.scraped_at
        ''', (player_id, round_num, total_points, price, kicking, ppg, popularity, form, scraped_at))

    conn.commit()
    cursor.close()


def scrape_player_data() -> dict:
    """Scrape player data from SuperBru API."""
    try:
        # Get current round
        conn = get_connection()
        setup_database(conn)
        current_round = get_next_round(conn)
        conn.close()

        label = 'preseason' if current_round == 0 else f'round {current_round}'

        # Scrape data
        player_list = []
        base_url = 'https://www.superbru.com/premiershiprugbyfantasy/ajax/f_write_player_stats.php?'

        for i in range(1, 9):
            url = f'{base_url}pg={i}&tbl=2017'
            session = requests.session()
            response = session.get(url, headers=REQ_HEADERS, verify=False, timeout=10)
            response.raise_for_status()

            soup = bs(response.text, 'html.parser')
            tbl = soup.find('tbody')

            if not tbl:
                continue

            players = tbl.find_all('tr')
            for player in players:
                stats = player.find_all('td')
                playerdata = [stat.get_text() for stat in stats]
                if len(playerdata) < 9:
                    playerdata.insert(5, float(0))
                playerdata[2] = POSITION_MAP[i]
                player_list.append(playerdata)

        if not player_list:
            return {
                'status': 'error',
                'message': 'No player data scraped',
                'round': current_round,
                'count': 0
            }

        # Create DataFrame
        col_names = ['Team', 'Player', 'Position', 'TotalPoints', 'Price',
                     'Kicking', 'PointsPerGame', 'Popularity', 'Form']
        df = pd.DataFrame(player_list, columns=col_names)

        # Clean data
        df['Player'] = df['Player'].str[:-1]
        df['TotalPoints'] = pd.to_numeric(df['TotalPoints'], errors='coerce')
        df['Price'] = (
            df['Price']
            .str.replace('£', '', regex=False)
            .str.replace('m', '', regex=False)
        )
        df['Kicking'] = pd.to_numeric(df['Kicking'], errors='coerce')
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce') * 1_000_000

        # Persist to database
        conn = get_connection()
        setup_database(conn)
        scraped_at = datetime.utcnow().isoformat()

        for _, row in df.iterrows():
            try:
                player_id = upsert_player(conn, row['Player'], row['Team'], row['Position'])
                if player_id:
                    upsert_weekly_stats(
                        conn, player_id, current_round,
                        row['TotalPoints'], row['Price'],
                        row['Kicking'], row['PointsPerGame'],
                        row['Popularity'], row['Form'],
                        scraped_at,
                    )
            except Exception as e:
                conn.close()
                raise e

        conn.close()

        return {
            'status': 'success',
            'message': f'{label.capitalize()} data scraped and saved',
            'round': current_round,
            'count': len(df)
        }

    except Exception as e:
        return {
            'status': 'error',
            'message': str(e),
            'round': None,
            'count': 0
        }


@app.route('/api/cron/player-data', methods=['POST'])
def player_data_cron():
    """Scheduled player data scraper endpoint."""
    try:
        result = scrape_player_data()

        if result['status'] == 'success':
            return jsonify({
                'status': 'success',
                'message': result['message'],
                'timestamp': datetime.utcnow().isoformat(),
                'round': result['round'],
                'players_updated': result['count']
            }), 200
        else:
            return jsonify({
                'status': 'error',
                'message': result['message'],
                'timestamp': datetime.utcnow().isoformat(),
            }), 500

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500


if __name__ == '__main__':
    app.run(debug=True, port=5001)
