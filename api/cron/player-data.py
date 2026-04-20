"""
Vercel Cron handler for player data scraping.

Runs every Tuesday at 12:00 UTC via Vercel Cron.
Endpoint: POST /api/cron/player-data
"""

from datetime import datetime
import os
from flask import Flask, jsonify

app = Flask(__name__)


@app.route('/api/cron/player-data', methods=['POST'])
def player_data_cron():
    """Scheduled player data scraper endpoint."""
    try:
        # TODO: Import scraper logic and run here
        # This is a placeholder for the actual player-data.py scraper

        return jsonify({
            'status': 'success',
            'message': 'Player data scraper executed',
            'timestamp': datetime.utcnow().isoformat(),
            'round': None,  # TODO: Return actual round
            'players_updated': 0  # TODO: Return actual count
        }), 200

    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 500


if __name__ == '__main__':
    app.run(debug=True, port=5001)
