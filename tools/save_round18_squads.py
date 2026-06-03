"""
One-off: save the four finalists' squads into round 18 for the Grand Final.

  - Chessums Cheerleaders / London WaspCester / George XV: copy each team's
    latest saved selection forward to round 18 (captain/kicker/bench/jersey kept).
  - Eddie Jones's Barmy Army: replace round 18 with the explicit 23 below.

Players are resolved by NAME (+ team/position to disambiguate duplicates), never
by player_id, so the same script works on local SQLite and prod Postgres.

Run:
  local : DB_TYPE=sqlite  python -m tools.save_round18_squads
  prod  : DB_TYPE=postgres DATABASE_URL=postgres://... python -m tools.save_round18_squads
"""

import os
from datetime import datetime, timezone

from api.db import get_connection, ensure_schema, execute, DB_TYPE

TARGET_ROUND = 18
COPY_FORWARD_TEAMS = ['Chessums Cheerleaders', 'London WaspCester', 'George XV']

BARMY_TEAM = "Eddie Jones's Barmy Army"
BARMY_CAPTAIN = 'McArthur,A'
BARMY_KICKER  = 'Haydon-Wood,W'

# (name, team filter|None, position filter|None, jersey, is_bench)
# Jerseys follow the app's SLOT_ORDER formation (1-15 start, 16-23 bench;
# exactly one of each position on the bench). Contested start/bench slots
# settled by season points: Heyes>Clarke (PR), Dun>de Chaves (LK),
# Earl>Gonzalez (LF), Feyi-Waboso>Woods (OBK).
BARMY_SQUAD = [
    # --- starters (jersey 1-15) ---
    ('McArthur,A',           'GLO', 'PR',   1, 0),   # captain
    ('Heyes,J',              None,  None,   3, 0),
    ('Turner,G',             None,  None,   2, 0),
    ('Chessum,L',            None,  None,   4, 0),
    ('Dun,J',                None,  None,   5, 0),
    ('Barbeary,A',           None,  None,   6, 0),
    ('Chick,C',              'NOR', 'LF',   7, 0),
    ('Earl,B',               None,  None,   8, 0),
    ('Varney,S',             'EXE', 'SH',   9, 0),
    ('Haydon-Wood,W',        None,  None,  10, 0),   # kicker
    ('Janse van Rensburg,B', None,  None,  12, 0),
    ('Redpath,C',            None,  None,  13, 0),
    ('Wyatt,T',              None,  None,  11, 0),
    ('Redshaw,B',            'GLO', 'OBK', 14, 0),
    ('Feyi-Waboso,I',        None,  None,  15, 0),
    # --- bench (jersey 16-23) ---
    ('Tuipulotu,K',          None,  None,  16, 1),
    ('Clarke,E',             None,  None,  17, 1),
    ('de Chaves,S',          None,  None,  18, 1),
    ('Gonzalez,JM',          None,  None,  19, 1),
    ('Bracken,C',            None,  None,  20, 1),
    ('MacGinty,AJ',          None,  None,  21, 1),
    ('Waghorn,B',            None,  None,  22, 1),
    ('Woods,Ja',             None,  None,  23, 1),
]

SQUAD_QUOTAS = {'PR': 3, 'HK': 2, 'LK': 3, 'LF': 4, 'SH': 2, 'FH': 2, 'MID': 3, 'OBK': 4}


def _val(row, key, idx):
    if row is None:
        return None
    if isinstance(row, dict):
        return row.get(key)
    return row[idx]


def resolve(conn, name, team, pos):
    q = 'SELECT player_id, position FROM players WHERE name = ?'
    params = [name]
    if team:
        q += ' AND team = ?'; params.append(team)
    if pos:
        q += ' AND position = ?'; params.append(pos)
    cur = execute(conn, q, tuple(params))
    rows = cur.fetchall()
    cur.close()
    return [(_val(r, 'player_id', 0), _val(r, 'position', 1)) for r in rows]


def main():
    conn = get_connection()
    ensure_schema(conn)
    now = datetime.now(timezone.utc).isoformat()

    print(f'== DB_TYPE={DB_TYPE} | target round {TARGET_ROUND} ==\n')

    # ---- 1. Copy the three teams' latest squad forward ---------------------
    for team in COPY_FORWARD_TEAMS:
        cur = execute(conn,
            'SELECT MAX(round) AS m FROM team_selections WHERE team_name = ? AND round < ?',
            (team, TARGET_ROUND))
        src = _val(cur.fetchone(), 'm', 0)
        cur.close()
        if not src:
            print(f'!! {team}: no prior selection found — SKIPPED')
            continue
        execute(conn, 'DELETE FROM team_selections WHERE team_name = ? AND round = ?',
                (team, TARGET_ROUND)).close()
        execute(conn, '''
            INSERT INTO team_selections
                (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
            SELECT ?, team_name, player_id, is_captain, is_kicker, is_bench, jersey, ?
            FROM team_selections WHERE team_name = ? AND round = ?
        ''', (TARGET_ROUND, now, team, src)).close()
        print(f'   {team}: copied round {src} -> {TARGET_ROUND}')

    # ---- 2. Resolve + validate Barmy Army squad ----------------------------
    print()
    resolved = []
    pos_counts = {}
    errors = []
    for name, team, pos, jersey, is_bench in BARMY_SQUAD:
        matches = resolve(conn, name, team, pos)
        if len(matches) != 1:
            errors.append(f'{name} (team={team}, pos={pos}) -> {len(matches)} matches: {matches}')
            continue
        pid, position = matches[0]
        pos_counts[position] = pos_counts.get(position, 0) + 1
        resolved.append((pid, name, position, jersey, is_bench))

    if errors:
        print('!! Could not uniquely resolve these players — ABORTING Barmy write:')
        for e in errors:
            print('   -', e)
        conn.close()
        return

    quota_ok = pos_counts == SQUAD_QUOTAS
    print(f'   Barmy positions: {pos_counts}')
    print(f'   Quota valid: {quota_ok}')
    if not quota_ok:
        print('!! Position quotas do not match — ABORTING Barmy write.')
        conn.close()
        return

    # ---- 3. Write Barmy round 18 -------------------------------------------
    execute(conn, 'DELETE FROM team_selections WHERE team_name = ? AND round = ?',
            (BARMY_TEAM, TARGET_ROUND)).close()
    for pid, name, position, jersey, is_bench in resolved:
        execute(conn, '''
            INSERT INTO team_selections
                (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            TARGET_ROUND, BARMY_TEAM, pid,
            1 if name == BARMY_CAPTAIN else 0,
            1 if name == BARMY_KICKER  else 0,
            is_bench, jersey, now,
        )).close()

    starters = sum(1 for r in resolved if r[4] == 0)
    bench    = sum(1 for r in resolved if r[4] == 1)
    print(f'   {BARMY_TEAM}: wrote {len(resolved)} players '
          f'({starters} starters + {bench} bench), C={BARMY_CAPTAIN}, K={BARMY_KICKER}')

    conn.commit()
    conn.close()
    print('\nDone.')


if __name__ == '__main__':
    main()
