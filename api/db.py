"""
Database abstraction layer supporting SQLite (local) and PostgreSQL (Vercel).

Environment variables:
  DB_TYPE=sqlite or postgres (default: sqlite)
  DB_PATH=path/to/db.db (SQLite only, default: prem_rugby_25_26_test.db)
  DATABASE_URL=postgres://... (PostgreSQL only, required for Vercel)
"""

import os
import sqlite3


DB_TYPE = os.getenv('DB_TYPE', 'sqlite').lower()


def _convert_query_placeholders(query: str) -> str:
    """Convert SQLite ? placeholders to PostgreSQL %s placeholders."""
    if DB_TYPE == 'postgres':
        # Replace ? with %s, but avoid replacing ? inside strings
        parts = []
        in_string = False
        quote_char = None
        i = 0
        while i < len(query):
            char = query[i]
            if char in ('"', "'") and (i == 0 or query[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    quote_char = char
                elif char == quote_char:
                    in_string = False
                    quote_char = None

            if not in_string and char == '?':
                parts.append('%s')
            else:
                parts.append(char)
            i += 1
        return ''.join(parts)
    return query


def get_connection():
    """Return a database connection (SQLite or PostgreSQL) based on DB_TYPE."""
    if DB_TYPE == 'postgres':
        return _get_postgres_connection()
    else:
        return _get_sqlite_connection()


def _get_sqlite_connection():
    """SQLite connection for local development."""
    db_path = os.getenv('DB_PATH', 'prem_rugby_25_26.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def _get_postgres_connection():
    """PostgreSQL connection for Vercel production."""
    import psycopg2
    from psycopg2.extras import RealDictCursor

    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise ValueError('DATABASE_URL environment variable not set for PostgreSQL mode')

    conn = psycopg2.connect(database_url)
    conn.cursor_factory = RealDictCursor
    return conn


def execute(conn, query: str, params=None):
    """Execute a query and return cursor. Handles both SQLite and Postgres."""
    cursor = conn.cursor()
    converted_query = _convert_query_placeholders(query)
    if params:
        cursor.execute(converted_query, params)
    else:
        cursor.execute(converted_query)
    return cursor


def fetchone(cursor):
    """Fetch one row from cursor, handling both DB types."""
    row = cursor.fetchone()
    if row is None:
        return None
    # Convert to dict-like object for consistency
    if isinstance(row, dict):
        return row
    return dict(row) if row else None


def fetchall(cursor):
    """Fetch all rows from cursor, handling both DB types."""
    rows = cursor.fetchall()
    if not rows:
        return []
    # Convert to list of dicts for consistency
    if isinstance(rows[0], dict):
        return rows
    return [dict(r) if r else {} for r in rows]


def ensure_schema(conn):
    """Create tables if they don't exist (compatible with both SQLite and PostgreSQL)."""
    cursor = conn.cursor()

    # Players table
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

    # Weekly stats table
    if DB_TYPE == 'postgres':
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

    # Team selections table
    if DB_TYPE == 'postgres':
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
    else:
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

    # Users table
    if DB_TYPE == 'postgres':
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
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                team_name TEXT UNIQUE,
                created_at TEXT NOT NULL
            )
        ''')

    conn.commit()

    # Migrate existing team_selections tables that predate the is_bench / jersey columns.
    if DB_TYPE == 'postgres':
        cursor.execute('ALTER TABLE team_selections ADD COLUMN IF NOT EXISTS is_bench INTEGER NOT NULL DEFAULT 0')
        cursor.execute('ALTER TABLE team_selections ADD COLUMN IF NOT EXISTS jersey INTEGER')
    else:
        existing_cols = {row[1] for row in cursor.execute('PRAGMA table_info(team_selections)')}
        if 'is_bench' not in existing_cols:
            cursor.execute('ALTER TABLE team_selections ADD COLUMN is_bench INTEGER NOT NULL DEFAULT 0')
        if 'jersey' not in existing_cols:
            cursor.execute('ALTER TABLE team_selections ADD COLUMN jersey INTEGER')

    conn.commit()
    cursor.close()
