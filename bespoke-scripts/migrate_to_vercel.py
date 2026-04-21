#!/usr/bin/env python3
"""
Migrate data from local SQLite to Vercel Postgres.

Usage:
  1. Get DATABASE_URL from Vercel dashboard (Storage tab → Postgres → .env.local)
  2. Run: DATABASE_URL="postgres://..." python migrate_to_vercel.py --source prem_rugby_25_26.db
"""

import os
import sys
import sqlite3
import argparse

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)


def migrate_data(source_db: str, database_url: str):
    """Migrate all data from SQLite to PostgreSQL."""

    # Connect to local SQLite
    print(f"Connecting to SQLite: {source_db}")
    sqlite_conn = sqlite3.connect(source_db)
    sqlite_conn.row_factory = sqlite3.Row
    sqlite_cursor = sqlite_conn.cursor()

    # Connect to Vercel Postgres
    print(f"Connecting to Vercel Postgres...")
    try:
        postgres_conn = psycopg2.connect(database_url)
        postgres_cursor = postgres_conn.cursor()
    except psycopg2.Error as e:
        print(f"ERROR: Could not connect to Postgres: {e}")
        print("Make sure DATABASE_URL is correct and set from Vercel dashboard")
        sqlite_conn.close()
        sys.exit(1)

    print("✓ Connected to both databases\n")

    try:
        # Create schema in Postgres (if not exists)
        print("Creating schema in Postgres...")
        postgres_cursor.execute('''
            CREATE TABLE IF NOT EXISTS players (
                player_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                team TEXT,
                position TEXT,
                UNIQUE(name, team, position)
            )
        ''')
        postgres_cursor.execute('''
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
        postgres_cursor.execute('''
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
        postgres_conn.commit()
        print("✓ Schema created\n")

        # Migrate players
        print("Migrating players...")
        sqlite_cursor.execute('SELECT * FROM players')
        players = sqlite_cursor.fetchall()

        for player in players:
            postgres_cursor.execute(
                'INSERT INTO players (player_id, name, team, position) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING',
                (player['player_id'], player['name'], player['team'], player['position'])
            )
        postgres_conn.commit()
        print(f"✓ Migrated {len(players)} players\n")

        # Migrate weekly_stats
        print("Migrating weekly_stats...")
        sqlite_cursor.execute('SELECT * FROM weekly_stats')
        stats = sqlite_cursor.fetchall()

        for stat in stats:
            postgres_cursor.execute(
                '''INSERT INTO weekly_stats
                   (player_id, round, total_points, price, kicking, points_per_game, popularity, form, scraped_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING''',
                (stat['player_id'], stat['round'], stat['total_points'], stat['price'],
                 stat['kicking'], stat['points_per_game'], stat['popularity'], stat['form'],
                 stat['scraped_at'])
            )
        postgres_conn.commit()
        print(f"✓ Migrated {len(stats)} weekly stats\n")

        # Migrate team_selections
        print("Migrating team_selections...")
        sqlite_cursor.execute('SELECT * FROM team_selections')
        selections = sqlite_cursor.fetchall()

        for selection in selections:
            postgres_cursor.execute(
                '''INSERT INTO team_selections
                   (round, team_name, player_id, is_captain, is_kicker, scraped_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT DO NOTHING''',
                (selection['round'], selection['team_name'], selection['player_id'],
                 selection['is_captain'], selection['is_kicker'], selection['scraped_at'])
            )
        postgres_conn.commit()
        print(f"✓ Migrated {len(selections)} team selections\n")

        print("=" * 50)
        print("✓ MIGRATION COMPLETE")
        print("=" * 50)
        print(f"Migrated:")
        print(f"  • {len(players)} players")
        print(f"  • {len(stats)} weekly stats records")
        print(f"  • {len(selections)} team selections")

    except Exception as e:
        print(f"ERROR during migration: {e}")
        postgres_conn.rollback()
        sys.exit(1)
    finally:
        sqlite_cursor.close()
        sqlite_conn.close()
        postgres_cursor.close()
        postgres_conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Migrate data from local SQLite to Vercel Postgres',
        epilog='Example: DATABASE_URL="postgres://..." python migrate_to_vercel.py --source prem_rugby_25_26.db'
    )
    parser.add_argument('--source', default='prem_rugby_25_26_test.db',
                       help='Path to local SQLite database (default: prem_rugby_25_26_test.db)')

    args = parser.parse_args()

    # Get DATABASE_URL from environment
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        print("ERROR: DATABASE_URL environment variable not set")
        print("\nTo get DATABASE_URL:")
        print("  1. Go to https://vercel.com/dashboard")
        print("  2. Select your project")
        print("  3. Click 'Storage' tab")
        print("  4. Click your Postgres database")
        print("  5. Click '.env.local' and copy DATABASE_URL")
        print("  6. Run: DATABASE_URL='your-url-here' python migrate_to_vercel.py")
        sys.exit(1)

    # Check if source database exists
    if not os.path.exists(args.source):
        print(f"ERROR: SQLite database not found: {args.source}")
        sys.exit(1)

    migrate_data(args.source, database_url)
