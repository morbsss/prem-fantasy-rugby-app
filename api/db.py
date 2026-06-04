"""
Database abstraction layer supporting SQLite (local) and PostgreSQL (Vercel).

Environment variables:
  DB_TYPE=sqlite or postgres (default: sqlite)
  DB_PATH=path/to/db.db (SQLite only, default: prem_rugby_25_26_test.db)
  DATABASE_URL=postgres://... (PostgreSQL only, required for Vercel)
"""

import os
import sqlite3

from .leagues import LEAGUES, DEFAULT_LEAGUE


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

    # Rounds table — stores first/last kickoff per round (populated by sync_rounds.py).
    # No sole PK on round_number: round numbers repeat across leagues, so
    # uniqueness is enforced per-league via idx_rounds_league_round (added in
    # _ensure_league_schema). Legacy DBs keep their original round_number PK.
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS rounds (
            round_number INTEGER NOT NULL,
            first_kickoff TEXT NOT NULL,
            last_kickoff TEXT NOT NULL
        )
    ''')

    # Match lineups table (populated by real_lineups.py)
    if DB_TYPE == 'postgres':
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS match_lineups (
                id SERIAL PRIMARY KEY,
                round INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                real_team TEXT NOT NULL,
                jersey INTEGER,
                is_bench INTEGER NOT NULL DEFAULT 0,
                scraped_at TEXT NOT NULL,
                UNIQUE(round, player_name, real_team)
            )
        ''')
    else:
        cursor.execute('''
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

    # Two-league support (spec §1, §5.1) + draft engine tables.
    _ensure_league_schema(conn, cursor)

    conn.commit()
    cursor.close()


# ---------------------------------------------------------------------------
# Two-league + draft schema
# ---------------------------------------------------------------------------

# Tables that gain a league_id discriminator. Existing single-league rows are
# backfilled to DEFAULT_LEAGUE (the original Premiership / OFDS data).
_LEAGUE_SCOPED_TABLES = (
    'players', 'weekly_stats', 'team_selections', 'rounds', 'match_lineups', 'users',
)


def _column_exists(cursor, table: str, column: str) -> bool:
    if DB_TYPE == 'postgres':
        cursor.execute(
            'SELECT 1 FROM information_schema.columns '
            'WHERE table_name = %s AND column_name = %s',
            (table, column),
        )
        return cursor.fetchone() is not None
    return column in {row[1] for row in cursor.execute(f'PRAGMA table_info({table})')}


def _ensure_league_schema(conn, cursor) -> None:
    """Create the leagues + draft tables, add league_id to scoped tables, and
    seed the two leagues. Idempotent and non-destructive: existing rows are
    backfilled to the default (Premiership / OFDS) league."""
    serial = 'SERIAL PRIMARY KEY' if DB_TYPE == 'postgres' else 'INTEGER PRIMARY KEY AUTOINCREMENT'

    # Leagues registry table — mirrors api/leagues.py so the DB is queryable.
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS leagues (
            league_id {serial},
            slug TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            competition TEXT NOT NULL,
            theme TEXT NOT NULL,
            timezone TEXT NOT NULL,
            commissioner_user_id INTEGER,
            draft_at TEXT,
            draft_order TEXT,
            season_start TEXT
        )
    ''')

    # Draft order + live-draft state, one row per league.
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS draft_state (
            league_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL DEFAULT 'pending',
            current_pick INTEGER NOT NULL DEFAULT 0,
            started_at TEXT,
            completed_at TEXT,
            pick_deadline TEXT
        )
    ''')

    # One row per drafted entity — a player_id, OR a club front-row unit (fr_club).
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS draft_picks (
            id {serial},
            league_id INTEGER NOT NULL,
            pick_number INTEGER NOT NULL,
            round_number INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            player_id INTEGER,
            fr_club TEXT,
            is_auto INTEGER NOT NULL DEFAULT 0,
            picked_at TEXT,
            UNIQUE(league_id, pick_number)
        )
    ''')

    # Each fantasy team's owned club front-row unit, per round (spec follow-up:
    # front row is drafted/owned as a club unit, scored from the real matchday).
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS team_front_row (
            id {serial},
            league_id INTEGER NOT NULL,
            team_name TEXT NOT NULL,
            round INTEGER NOT NULL,
            club TEXT NOT NULL,
            scraped_at TEXT,
            UNIQUE(league_id, team_name, round)
        )
    ''')

    # Trades log — free-agent pickups and inter-team (user↔user) trades.
    # free_agent: from_team picks up in_player_id and drops out_player_id (→ FA),
    #             status 'completed' immediately.
    # player_trade: from_team offers out_player_id for to_team's in_player_id,
    #               status 'pending' until the responder accepts/rejects.
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS trades (
            id {serial},
            league_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            status TEXT NOT NULL,
            from_team TEXT NOT NULL,
            to_team TEXT,
            out_player_id INTEGER,
            in_player_id INTEGER,
            created_at TEXT NOT NULL,
            resolved_at TEXT
        )
    ''')

    # Ingestion job-run log (spec §3: each job logs its run; drives cadence
    # + finalize idempotency in the scheduler).
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS job_runs (
            id {serial},
            league_id INTEGER NOT NULL,
            job TEXT NOT NULL,
            round_number INTEGER,
            status TEXT NOT NULL,
            detail TEXT,
            run_at TEXT NOT NULL
        )
    ''')

    conn.commit()

    # Seed the two leagues (idempotent upsert on slug).
    for cfg in LEAGUES.values():
        cursor.execute('SELECT league_id FROM leagues WHERE slug = ?'.replace('?', _ph()),
                       (cfg['slug'],))
        if cursor.fetchone() is None:
            cursor.execute(
                'INSERT INTO leagues (slug, name, competition, theme, timezone) '
                f'VALUES ({_ph()}, {_ph()}, {_ph()}, {_ph()}, {_ph()})',
                (cfg['slug'], cfg['name'], cfg['competition'], cfg['theme'], cfg['timezone']),
            )
    conn.commit()

    default_league_id = _league_id_for_slug(cursor, DEFAULT_LEAGUE)

    # Add league_id to each scoped table and backfill existing rows.
    for table in _LEAGUE_SCOPED_TABLES:
        if not _column_exists(cursor, table, 'league_id'):
            cursor.execute(f'ALTER TABLE {table} ADD COLUMN league_id INTEGER')
        cursor.execute(
            f'UPDATE {table} SET league_id = {_ph()} WHERE league_id IS NULL',
            (default_league_id,),
        )
    conn.commit()

    # Per-league uniqueness for rounds (round numbers repeat across leagues).
    # Works on both fresh DBs and legacy DBs that already have league_id backfilled.
    cursor.execute(
        'CREATE UNIQUE INDEX IF NOT EXISTS idx_rounds_league_round '
        'ON rounds (league_id, round_number)'
    )

    # Email-based auth (spec §6.1). Added as a column so legacy username rows
    # keep working; new sign-ups populate it.
    if not _column_exists(cursor, 'users', 'email'):
        cursor.execute('ALTER TABLE users ADD COLUMN email TEXT')
    # Per-pick draft clock (spec §6.2 auto-draft).
    if not _column_exists(cursor, 'draft_state', 'pick_deadline'):
        cursor.execute('ALTER TABLE draft_state ADD COLUMN pick_deadline TEXT')
    # Club front-row unit picks (front-row redesign).
    if not _column_exists(cursor, 'draft_picks', 'fr_club'):
        cursor.execute('ALTER TABLE draft_picks ADD COLUMN fr_club TEXT')
    conn.commit()


def _ph() -> str:
    return '%s' if DB_TYPE == 'postgres' else '?'


def _league_id_for_slug(cursor, slug: str) -> int:
    cursor.execute(f'SELECT league_id FROM leagues WHERE slug = {_ph()}', (slug,))
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f'League {slug!r} not seeded')
    return row['league_id'] if isinstance(row, dict) else row[0]
