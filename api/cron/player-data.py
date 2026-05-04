"""
Vercel Cron handler for player data scraping.

Runs every Tuesday at 12:00 UTC via Vercel Cron.
Endpoint: POST /api/cron/player-data

Scrapes player statistics from SuperBru and updates the database.
"""

import os
from datetime import datetime
from flask import Flask, jsonify

import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup as bs

from ..db import get_connection, DB_TYPE

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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS team_selections (
                id SERIAL PRIMARY KEY,
                round INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                player_id INTEGER NOT NULL REFERENCES players(player_id),
                is_captain INTEGER NOT NULL DEFAULT 0,
                is_kicker INTEGER NOT NULL DEFAULT 0,
                is_bench INTEGER NOT NULL DEFAULT 0,
                jersey INTEGER,
                scraped_at TEXT NOT NULL,
                UNIQUE(round, team_name, player_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                team_name TEXT UNIQUE,
                created_at TEXT NOT NULL
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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS team_selections (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                round INTEGER NOT NULL,
                team_name TEXT NOT NULL,
                player_id INTEGER NOT NULL REFERENCES players(player_id),
                is_captain INTEGER NOT NULL DEFAULT 0,
                is_kicker INTEGER NOT NULL DEFAULT 0,
                is_bench INTEGER NOT NULL DEFAULT 0,
                jersey INTEGER,
                scraped_at TEXT NOT NULL,
                UNIQUE(round, team_name, player_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                team_name TEXT UNIQUE,
                created_at TEXT NOT NULL
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
            'INSERT INTO players (name, team, position) VALUES (%s, %s, %s) ON CONFLICT (name, team, position) DO NOTHING',
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


def copy_team_selections_to_next_round(conn, previous_round: int, current_round: int) -> dict:
    """Copy team selections from previous round to current round for all teams."""
    cursor = conn.cursor()

    try:
        # Get all teams from the previous round
        if DB_TYPE == 'postgres':
            cursor.execute('''
                SELECT DISTINCT team_name FROM team_selections WHERE round = %s
            ''', (previous_round,))
        else:
            cursor.execute('''
                SELECT DISTINCT team_name FROM team_selections WHERE round = ?
            ''', (previous_round,))

        teams = [row['team_name'] if isinstance(row, dict) else row[0] for row in cursor.fetchall()]

        if not teams:
            cursor.close()
            return {'status': 'info', 'message': 'No teams to copy', 'teams_copied': 0}

        # Copy picks from previous round to current round for each team
        copied_count = 0
        scraped_at = datetime.utcnow().isoformat()

        for team_name in teams:
            if DB_TYPE == 'postgres':
                cursor.execute('''
                    INSERT INTO team_selections
                        (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
                    SELECT %s, team_name, player_id, is_captain, is_kicker, is_bench, jersey, %s
                    FROM team_selections
                    WHERE team_name = %s AND round = %s
                    ON CONFLICT(round, team_name, player_id) DO NOTHING
                ''', (current_round, scraped_at, team_name, previous_round))
            else:
                cursor.execute('''
                    INSERT OR IGNORE INTO team_selections
                        (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
                    SELECT ?, team_name, player_id, is_captain, is_kicker, is_bench, jersey, ?
                    FROM team_selections
                    WHERE team_name = ? AND round = ?
                ''', (current_round, scraped_at, team_name, previous_round))

            copied_count += cursor.rowcount

        conn.commit()
        cursor.close()

        return {
            'status': 'success',
            'message': f'Copied {copied_count} picks from round {previous_round} to round {current_round}',
            'teams_copied': len(teams),
            'picks_copied': copied_count
        }

    except Exception as e:
        cursor.close()
        return {'status': 'error', 'message': str(e), 'teams_copied': 0}


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

        # Copy team selections from previous round to new round (if not preseason)
        copy_result = None
        if current_round > 0:
            copy_result = copy_team_selections_to_next_round(conn, current_round - 1, current_round)

        conn.close()

        # Build response message
        response_msg = f'{label.capitalize()} data scraped and saved'
        if copy_result and copy_result['status'] == 'success':
            response_msg += f" ({copy_result['message']})"

        return {
            'status': 'success',
            'message': response_msg,
            'round': current_round,
            'count': len(df),
            'team_copy': copy_result
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
