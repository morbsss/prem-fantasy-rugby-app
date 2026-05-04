import sqlite3
import requests
import urllib3
import pandas as pd
from bs4 import BeautifulSoup as bs
from datetime import datetime as dt

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

DB_PATHS = [
    'prem_rugby_25_26.db',
    'prem_rugby_25_26_test.db',
]

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

def setup_database(conn: sqlite3.Connection) -> None:
    conn.execute('''
        CREATE TABLE IF NOT EXISTS players (
            player_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name      TEXT    NOT NULL,
            team      TEXT,
            position  TEXT,
            UNIQUE(name, team, position)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS weekly_stats (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id       INTEGER NOT NULL REFERENCES players(player_id),
            round           INTEGER NOT NULL,
            total_points    REAL,
            price           REAL,
            kicking         TEXT,
            points_per_game TEXT,
            popularity      TEXT,
            form            TEXT,
            scraped_at      TEXT    NOT NULL,
            UNIQUE(player_id, round)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS team_selections (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            round       INTEGER NOT NULL,
            team_name   TEXT    NOT NULL,
            player_id   INTEGER NOT NULL REFERENCES players(player_id),
            is_captain  INTEGER NOT NULL DEFAULT 0,
            is_kicker   INTEGER NOT NULL DEFAULT 0,
            is_bench    INTEGER NOT NULL DEFAULT 0,
            jersey      INTEGER,
            scraped_at  TEXT    NOT NULL,
            UNIQUE(round, team_name, player_id)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id         INTEGER PRIMARY KEY AUTOINCREMENT,
            username        TEXT UNIQUE NOT NULL,
            password_hash   TEXT NOT NULL,
            team_name       TEXT UNIQUE,
            created_at      TEXT NOT NULL
        )
    ''')
    conn.commit()


def upsert_player(conn: sqlite3.Connection, name: str, team: str, position: str) -> int:
    """Insert player if new; always refresh team/position. Returns player_id."""
    conn.execute(
        'INSERT OR IGNORE INTO players (name, team, position) VALUES (?, ?, ?)',
        (name, team, position),
    )
    conn.execute(
        'UPDATE players SET team = ?, position = ? WHERE name = ? AND team = ? AND position = ?',
        (team, position, name, team, position),
    )
    row = conn.execute(
        'SELECT player_id FROM players WHERE name = ? AND team = ? AND position = ?',
        (name, team, position),
    ).fetchone()
    return row[0]


def upsert_weekly_stats(
    conn: sqlite3.Connection,
    player_id: int,
    round_num: int,
    total_points,
    price,
    kicking: str,
    ppg: str,
    popularity: str,
    form: str,
    scraped_at: str,
) -> None:
    """Insert or overwrite stats for a player/round combination."""
    conn.execute(
        '''
        INSERT INTO weekly_stats
            (player_id, round, total_points, price, kicking, points_per_game,
             popularity, form, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(player_id, round) DO UPDATE SET
            total_points    = excluded.total_points,
            price           = excluded.price,
            kicking         = excluded.kicking,
            points_per_game = excluded.points_per_game,
            popularity      = excluded.popularity,
            form            = excluded.form,
            scraped_at      = excluded.scraped_at
        ''',
        (player_id, round_num, total_points, price, kicking, ppg, popularity, form, scraped_at),
    )


def get_next_round(conn: sqlite3.Connection) -> int:
    """Return max(round) + 1, or 0 if the database has no data yet (preseason)."""
    row = conn.execute('SELECT MAX(round) FROM weekly_stats').fetchone()
    return 0 if row[0] is None else row[0] + 1


def copy_team_selections_to_next_round(conn: sqlite3.Connection, previous_round: int, current_round: int) -> dict:
    """Copy team selections from previous round to current round for all teams."""
    try:
        # Get all teams from the previous round
        teams = [row[0] for row in conn.execute(
            'SELECT DISTINCT team_name FROM team_selections WHERE round = ?',
            (previous_round,)
        ).fetchall()]

        if not teams:
            return {'status': 'info', 'message': 'No teams to copy', 'teams_copied': 0, 'picks_copied': 0}

        # Copy picks from previous round to current round for each team
        copied_count = 0
        scraped_at = dt.now().isoformat()

        for team_name in teams:
            conn.execute('''
                INSERT OR IGNORE INTO team_selections
                    (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
                SELECT ?, team_name, player_id, is_captain, is_kicker, is_bench, jersey, ?
                FROM team_selections
                WHERE team_name = ? AND round = ?
            ''', (current_round, scraped_at, team_name, previous_round))

            copied_count += conn.total_changes

        conn.commit()

        return {
            'status': 'success',
            'message': f'Copied {copied_count} picks from round {previous_round} to round {current_round}',
            'teams_copied': len(teams),
            'picks_copied': copied_count
        }

    except Exception as e:
        return {'status': 'error', 'message': str(e), 'teams_copied': 0, 'picks_copied': 0}


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

POSITION_MAP = {1: 'PR', 2: 'HK', 3: 'LK', 4: 'LF', 5: 'SH', 6: 'FH', 7: 'MID', 8: 'OBK'}

req_headers = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/116.0.0.0 Safari/537.36'
    ),
}

# ---------------------------------------------------------------------------
# Determine round from DB
# ---------------------------------------------------------------------------

with sqlite3.connect(DB_PATHS[0]) as conn:
    setup_database(conn)
    Round = get_next_round(conn)

label = 'preseason' if Round == 0 else f'round {Round}'
print(f'Scraping {label}...')
print('NOTE: superbru player stats always reflect the current snapshot.')
print('      Run this script immediately after each round\'s matches conclude.\n')

player_list = []
baseURL = 'https://www.superbru.com/premiershiprugbyfantasy/ajax/f_write_player_stats.php?'
print('Connected to Player Data... Extracting Data...')

for i in range(1, 9):
    session = requests.session()
    url = f'{baseURL}pg={i}&tbl=2017'
    response = session.get(url, headers=req_headers, verify=False)
    soup = bs(response.text, 'html.parser')
    tbl = soup.find('tbody')

    players = tbl.find_all('tr')
    print(f'On Page {i}')
    for player in players:
        stats = player.find_all('td')
        playerdata = [stat.get_text() for stat in stats]
        if len(playerdata) < 9:
            playerdata.insert(5, float(0))
        playerdata[2] = POSITION_MAP[i]
        player_list.append(playerdata)

# ---------------------------------------------------------------------------
# Clean up into a DataFrame
# ---------------------------------------------------------------------------

col_names = ['Team', 'Player', 'Position', 'TotalPoints', 'Price',
             'Kicking', 'PointsPerGame', 'Popularity', 'Form']

df = pd.DataFrame(player_list, columns=col_names)

df['Player'] = df['Player'].str[:-1]
df['TotalPoints'] = pd.to_numeric(df['TotalPoints'], errors='coerce')
df['Price'] = (
    df['Price']
    .str.replace('£', '', regex=False)
    .str.replace('m', '', regex=False)
)
df['Kicking'] = df['Kicking'].astype(float)
df['Price'] = pd.to_numeric(df['Price'], errors='coerce') * 1_000_000

# ---------------------------------------------------------------------------
# Persist to SQLite
# ---------------------------------------------------------------------------

scraped_at = dt.now().isoformat()

for db_path in DB_PATHS:
    with sqlite3.connect(db_path) as conn:
        setup_database(conn)
        for _, row in df.iterrows():
            player_id = upsert_player(conn, row['Player'], row['Team'], row['Position'])
            upsert_weekly_stats(
                conn, player_id, Round,
                row['TotalPoints'], row['Price'],
                row['Kicking'], row['PointsPerGame'],
                row['Popularity'], row['Form'],
                scraped_at,
            )
        conn.commit()
        print(f'{label.capitalize()} data saved to {db_path} — {len(df)} players stored.')

        # Copy team selections from previous round to current round (if not preseason)
        if Round > 0:
            copy_result = copy_team_selections_to_next_round(conn, Round - 1, Round)
            if copy_result['status'] == 'success':
                print(f"  Team selections: {copy_result['message']}")
            elif copy_result['status'] == 'info':
                print(f"  Team selections: {copy_result['message']}")
            else:
                print(f"  Team selections error: {copy_result['message']}")
