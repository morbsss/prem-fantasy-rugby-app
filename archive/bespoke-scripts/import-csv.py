"""
One-off script to import historical CSV data into prem_rugby_25_26.db.

CSV columns: ID (ignored), Player, Team, Position, Price, Total Points, Game Week
Missing stat fields (Kicking, PointsPerGame, Popularity, Form) are stored as NULL.
"""

import sqlite3
import pandas as pd
from datetime import datetime as dt

CSV_PATH = 'full-player-data.csv'
DB_PATH  = 'prem_rugby_25_26.db'


# ---------------------------------------------------------------------------
# Database helpers (mirrors player-data.py)
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
    conn.commit()


def upsert_player(conn: sqlite3.Connection, name: str, team: str, position: str) -> int:
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
    scraped_at: str,
) -> None:
    conn.execute(
        '''
        INSERT INTO weekly_stats
            (player_id, round, total_points, price,
             kicking, points_per_game, popularity, form, scraped_at)
        VALUES (?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
        ON CONFLICT(player_id, round) DO UPDATE SET
            total_points = excluded.total_points,
            price        = excluded.price,
            scraped_at   = excluded.scraped_at
        ''',
        (player_id, round_num, total_points, price, scraped_at),
    )


# ---------------------------------------------------------------------------
# Load & validate CSV
# ---------------------------------------------------------------------------

df = pd.read_csv(CSV_PATH)

# Drop the old ID column and rename to match DB conventions
df = df.drop(columns=['ID'])
df = df.rename(columns={
    'Total Points': 'total_points',
    'Game Week':    'round',
})

# Coerce numeric columns; anything unparseable becomes NaN → stored as NULL
df['total_points'] = pd.to_numeric(df['total_points'], errors='coerce')
df['Price']        = pd.to_numeric(df['Price'],        errors='coerce')

print(f'Loaded {len(df)} rows across {df["round"].nunique()} game week(s).')

# ---------------------------------------------------------------------------
# Import into SQLite
# ---------------------------------------------------------------------------

imported_at = dt.now().isoformat()
inserted = 0
skipped  = 0

with sqlite3.connect(DB_PATH) as conn:
    setup_database(conn)

    for _, row in df.iterrows():
        name = str(row['Player']).strip()
        if not name:
            skipped += 1
            continue

        player_id = upsert_player(conn, name, str(row['Team']), str(row['Position']))
        upsert_weekly_stats(
            conn,
            player_id,
            int(row['round']),
            row['total_points'] if pd.notna(row['total_points']) else None,
            row['Price']        if pd.notna(row['Price'])        else None,
            imported_at,
        )
        inserted += 1

    conn.commit()

print(f'Done — {inserted} rows imported, {skipped} skipped.')
print(f'Data saved to {DB_PATH}.')
