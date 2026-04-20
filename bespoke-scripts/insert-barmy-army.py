"""
One-off script to insert team selections for "Eddie Jones's Barmy Army", rounds 1-18.
Players are fixed across all rounds.
"""

import sqlite3
from datetime import datetime, timezone

DB_PATH   = 'prem_rugby_25_26.db'
TEAM_NAME = "Eddie Jones's Barmy Army"
ROUNDS    = range(1, 19)

# (db_name, is_captain, is_kicker)
PLAYERS = [
    ('Chessum,L',            False, False),
    ('Barbeary,A',           False, False),
    ('Janse van Rensburg,B', False, False),
    ('Redpath,C',            False, False),
    ('Chick,C',              False, False),
    ('Heyes,J',              False, False),
    ('Varney,S',             False, False),
    ('Clarke,E',             False, False),
    ('Haydon-Wood,W',        False, True ),
    ('Turner,G',             False, False),
    ('McArthur,A',           True,  False),
    ('Wyatt,T',              False, False),
    ('Redshaw,B',            False, False),
]


def get_player_id(conn: sqlite3.Connection, name: str) -> int | None:
    # Exact match
    row = conn.execute(
        'SELECT player_id FROM players WHERE name = ?', (name,)
    ).fetchone()
    if row:
        return row[0]
    # Fall back to last name portion only
    last = name.split(',')[0]
    row = conn.execute(
        'SELECT player_id FROM players WHERE name LIKE ?', (f'{last},%',)
    ).fetchone()
    if row:
        print(f'  Warning: "{name}" matched by last name only — verify manually.')
        return row[0]
    return None


def upsert_selection(conn, round_num, player_id, is_captain, is_kicker, scraped_at):
    conn.execute('''
        INSERT INTO team_selections
            (round, team_name, player_id, is_captain, is_kicker, scraped_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(round, team_name, player_id) DO UPDATE SET
            is_captain = excluded.is_captain,
            is_kicker  = excluded.is_kicker,
            scraped_at = excluded.scraped_at
    ''', (round_num, TEAM_NAME, player_id, int(is_captain), int(is_kicker), scraped_at))


def main():
    scraped_at = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(DB_PATH) as conn:
        # Resolve all player IDs up front
        resolved = []
        for name, is_captain, is_kicker in PLAYERS:
            player_id = get_player_id(conn, name)
            if player_id is None:
                print(f'  Error: "{name}" not found in players table — skipping.')
                continue
            resolved.append((player_id, is_captain, is_kicker, name))

        print(f'Resolved {len(resolved)}/{len(PLAYERS)} players.')

        # Insert for every round
        for round_num in ROUNDS:
            for player_id, is_captain, is_kicker, _ in resolved:
                upsert_selection(conn, round_num, player_id, is_captain, is_kicker, scraped_at)

        conn.commit()

    print(f'Done — {len(resolved)} players x {len(list(ROUNDS))} rounds inserted.')


if __name__ == '__main__':
    main()
