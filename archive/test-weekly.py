# import sqlite3
# conn = sqlite3.connect('prem_rugby_25_26.db')
# print(conn.execute('''
#     SELECT COALESCE(SUM(
#         CASE WHEN is_captain = 1
#              THEN base_delta * 2
#              ELSE  base_delta + kick_delta
#         END), 0)
#     FROM (
#         SELECT ts.is_captain,
#             MAX(ws_curr.total_points) - COALESCE(MAX(ws_prev.total_points), 0) AS base_delta,
#             CASE WHEN ts.is_kicker = 1
#                  THEN COALESCE(CAST(MAX(ws_curr.kicking) AS REAL), 0)
#                     - COALESCE(CAST(MAX(ws_prev.kicking) AS REAL), 0)
#                  ELSE 0 END AS kick_delta
#         FROM team_selections ts
#         JOIN weekly_stats ws_curr ON ws_curr.player_id = ts.player_id AND ws_curr.round = ts.round
#         LEFT JOIN weekly_stats ws_prev ON ws_prev.player_id = ts.player_id AND ws_prev.round = ts.round - 1
#         WHERE ts.team_name = 'Bread XV' AND ts.round = 8
#         GROUP BY ts.player_id, ts.is_captain, ts.is_kicker
#     )
# ''').fetchone()[0])
# conn.close()

import sqlite3
conn = sqlite3.connect('prem_rugby_25_26_test.db')
rows = conn.execute('''
    SELECT p.name,
        CASE
            WHEN is_captain = 1 THEN (base_delta - kick_delta) * 2
            WHEN is_kicker  = 1 THEN base_delta
            ELSE base_delta - kick_delta
        END AS score,
        kick_delta,
        ts.is_captain,
        ts.is_kicker
    FROM (
        SELECT ts.player_id, ts.is_captain, ts.is_kicker,
            MAX(ws_curr.total_points) - COALESCE(MAX(ws_prev.total_points), 0) AS base_delta,
            COALESCE(CAST(MAX(ws_curr.kicking) AS REAL), 0) 
                - COALESCE(CAST(MAX(ws_prev.kicking) AS REAL), 0)
            AS kick_delta
        FROM team_selections ts
        JOIN weekly_stats ws_curr ON ws_curr.player_id = ts.player_id AND ws_curr.round = ts.round
        LEFT JOIN weekly_stats ws_prev ON ws_prev.player_id = ts.player_id AND ws_prev.round = ts.round - 1
        WHERE ts.team_name = "Eddie Jones's Barmy Army" AND ts.round = 12
        GROUP BY ts.player_id, ts.is_captain, ts.is_kicker
    ) ts
    JOIN players p ON p.player_id = ts.player_id
    ORDER BY score DESC
''').fetchall()

for name, score, kick_pts, is_captain, is_kicker in rows:
    flags = ' (C)' if is_captain else ' (K)' if is_kicker else ''
    print(f'{name}{flags}:                  {score}                 {kick_pts}')

conn.close()