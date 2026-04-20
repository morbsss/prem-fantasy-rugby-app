"""
One-off migration: deduplicate weekly_stats and enforce UNIQUE(player_id, round).

The original table was created before the constraint existed, so the CSV import
inserted many rows per player per round. This script:
  1. Keeps only the row with the MAX total_points per (player_id, round)
  2. Rebuilds weekly_stats with the UNIQUE constraint applied
"""

import sqlite3

DB_PATH = 'prem_rugby_25_26.db'

with sqlite3.connect(DB_PATH) as conn:
    before = conn.execute('SELECT COUNT(*) FROM weekly_stats').fetchone()[0]
    print(f'Rows before: {before}')

    conn.executescript('''
        -- Step 1: create clean table with UNIQUE constraint
        CREATE TABLE IF NOT EXISTS weekly_stats_clean (
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
        );

        -- Step 2: insert one row per (player_id, round) keeping MAX total_points
        INSERT INTO weekly_stats_clean
            (player_id, round, total_points, price, kicking, points_per_game,
             popularity, form, scraped_at)
        SELECT
            player_id,
            round,
            MAX(total_points),
            MAX(price),
            MAX(kicking),
            MAX(points_per_game),
            MAX(popularity),
            MAX(form),
            MAX(scraped_at)
        FROM weekly_stats
        GROUP BY player_id, round;

        -- Step 3: swap tables
        DROP TABLE weekly_stats;
        ALTER TABLE weekly_stats_clean RENAME TO weekly_stats;
    ''')

    after = conn.execute('SELECT COUNT(*) FROM weekly_stats').fetchone()[0]
    print(f'Rows after : {after}')
    print(f'Removed    : {before - after} duplicate rows')
