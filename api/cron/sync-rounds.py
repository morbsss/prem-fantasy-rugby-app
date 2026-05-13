"""
Vercel Cron handler for syncing round schedule from ESPN.

Runs every Monday at 09:00 UTC via Vercel Cron.
Endpoint: GET /api/cron/sync-rounds

Fetches the Premiership Rugby season schedule from ESPN and upserts
first/last kickoff times per round into the database.
"""

import os
from flask import Flask, jsonify, request

from ..db import get_connection, ensure_schema, execute as db_execute
from ..sync_rounds import fetch_rounds

app = Flask(__name__)

CRON_SECRET = os.getenv('CRON_SECRET', '')


def _cron_auth_ok():
    if not CRON_SECRET:
        return True
    return request.headers.get('Authorization') == f'Bearer {CRON_SECRET}'


@app.route('/api/cron/sync-rounds')
def sync_rounds_cron():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        rounds = fetch_rounds()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    conn = get_connection()
    ensure_schema(conn)

    for round_num, first_ko, last_ko, _ in rounds:
        db_execute(conn, '''
            INSERT INTO rounds (round_number, first_kickoff, last_kickoff)
            VALUES (?, ?, ?)
            ON CONFLICT (round_number) DO UPDATE SET
                first_kickoff = excluded.first_kickoff,
                last_kickoff  = excluded.last_kickoff
        ''', (round_num, first_ko, last_ko)).close()

    conn.commit()
    conn.close()
    return jsonify({'status': 'ok', 'rounds_synced': len(rounds)})


if __name__ == '__main__':
    app.run(debug=True, port=5002)
