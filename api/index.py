"""
Fantasy Draft Web App — run with: python -m flask --app api.index run
Or on Vercel: automatically deployed as serverless function at /

DEPLOYMENT CONFIGURATION
========================
Control pick locking behavior via environment variables:

  ALLOW_UNRESTRICTED_EDITS
  - 'true': All picks can be edited anytime (development mode)
  - 'false' or unset: Picks locked Friday 19:30-Tuesday 23:59 UTC (production mode)

  DB_TYPE
  - 'sqlite': Uses local SQLite database (development, default)
  - 'postgres': Uses Vercel Postgres (production)

  Example (local development):
    DB_TYPE=sqlite ALLOW_UNRESTRICTED_EDITS=true python -m flask --app api.index run

  Example (Vercel production):
    DB_TYPE=postgres (auto-configured)
    DATABASE_URL=postgres://... (auto-injected by Vercel)
"""

import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, render_template, request, session, redirect
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .db import get_connection, ensure_schema, DB_TYPE
from .auth import create_user, authenticate_user, get_available_teams
from .competition import (
    parse_fixtures, calculate_table, get_team_score,
    WINNER_BP_MARGIN, LOSER_BP_MARGIN,
)

app = Flask(__name__, template_folder='templates')

# Configure session
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV', 'development') == 'production'
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Get database type and fixture path
DB_TYPE_LOCAL = os.getenv('DB_TYPE', 'sqlite').lower()

# For Vercel: fixtures.csv is in api/data/
# For local: bespoke-scripts/fixtures.csv
api_data = os.path.join(os.path.dirname(__file__), 'data', 'fixtures.csv')
bespoke_data = os.path.join(os.path.dirname(__file__), '..', 'bespoke-scripts', 'fixtures.csv')

if os.path.exists(api_data):
    FIXTURES_CSV = api_data
elif os.path.exists(bespoke_data):
    FIXTURES_CSV = bespoke_data
else:
    FIXTURES_CSV = api_data  # Default to api/data/

# Squad composition rules
SQUAD_QUOTAS  = {'PR': 3, 'HK': 2, 'LK': 3, 'LF': 4, 'SH': 2, 'FH': 2, 'MID': 3, 'OBK': 4}
SQUAD_STARTERS = {'PR': 2, 'HK': 1, 'LK': 2, 'LF': 3, 'SH': 1, 'FH': 1, 'MID': 2, 'OBK': 3}
TOTAL_SQUAD   = sum(SQUAD_QUOTAS.values())   # 23


def _convert_placeholders(query: str) -> str:
    """Convert ? to %s for PostgreSQL if needed."""
    if DB_TYPE == 'postgres':
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
            if not in_string and char == '?':
                parts.append('%s')
            else:
                parts.append(char)
            i += 1
        return ''.join(parts)
    return query


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_db():
    """Get a database connection using the abstraction layer."""
    conn = get_connection()
    return conn


class _CursorWrapper:
    """Wrapper that auto-converts ? to %s for PostgreSQL."""
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, params=None):
        converted = _convert_placeholders(query)
        if params:
            self._cursor.execute(converted, params)
        else:
            self._cursor.execute(converted)
        return self

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    def close(self):
        return self._cursor.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _get_cursor(conn):
    """Get a wrapped cursor that handles placeholder conversion."""
    return _CursorWrapper(conn.cursor())


def get_next_round(conn) -> int:
    cursor = _get_cursor(conn)
    cursor.execute('SELECT MAX(round) FROM weekly_stats')
    row = cursor.fetchone()
    cursor.close()
    if DB_TYPE == 'postgres':
        result = row['max'] if row and row.get('max') else None
    else:
        result = row[0] if row else None
    return 1 if result is None else result + 1


def get_last_round(conn) -> int:
    cursor = _get_cursor(conn)
    cursor.execute('SELECT MAX(round) FROM weekly_stats')
    row = cursor.fetchone()
    cursor.close()
    if DB_TYPE == 'postgres':
        result = row['max'] if row and row.get('max') else None
    else:
        result = row[0] if row else None
    return result or 1


ALLOW_UNRESTRICTED_EDITS = os.getenv('ALLOW_UNRESTRICTED_EDITS', 'false').lower() == 'true'


def _round_kickoffs(conn, round_num):
    """Return (first_kickoff, last_kickoff) datetimes for round_num, or (None, None)."""
    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT first_kickoff, last_kickoff FROM rounds WHERE round_number = ?',
        (round_num,)
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None, None
    first_str = row['first_kickoff'] if isinstance(row, dict) else row[0]
    last_str  = row['last_kickoff']  if isinstance(row, dict) else row[1]
    return datetime.fromisoformat(first_str), datetime.fromisoformat(last_str)


def is_locked(conn=None) -> bool:
    """Picks lock at the first kickoff of the round being picked for."""
    if ALLOW_UNRESTRICTED_EDITS:
        return False
    _owned = conn is None
    if _owned:
        conn = get_db()
        ensure_schema(conn)
    next_round = get_next_round(conn)
    first_ko, _ = _round_kickoffs(conn, next_round)
    if _owned:
        conn.close()
    if first_ko is None:
        return False
    return datetime.now(timezone.utc) >= first_ko


def next_lock_time(conn, next_round) -> str:
    """ISO string of when picks lock: first kickoff of next_round."""
    first_ko, _ = _round_kickoffs(conn, next_round)
    if first_ko:
        return first_ko.isoformat()
    # Fallback: next Friday 19:30 UTC
    now = datetime.now(timezone.utc)
    days_until_friday = (4 - now.weekday()) % 7
    friday = (now + timedelta(days=days_until_friday)).replace(
        hour=19, minute=30, second=0, microsecond=0)
    if friday <= now:
        friday += timedelta(days=7)
    return friday.isoformat()


def reopen_time(conn, next_round) -> str:
    """ISO string of estimated unlock: last kickoff of next_round (round ends)."""
    _, last_ko = _round_kickoffs(conn, next_round)
    if last_ko:
        return last_ko.isoformat()
    # Fallback: next Tuesday 23:59 UTC
    now = datetime.now(timezone.utc)
    days_since_friday = (now.weekday() - 4) % 7
    last_friday = (now - timedelta(days=days_since_friday)).replace(
        hour=19, minute=30, second=0, microsecond=0)
    return (last_friday + timedelta(days=4)).replace(
        hour=23, minute=59, second=0, microsecond=0).isoformat()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

# AUTH ENDPOINTS

@app.route('/auth')
def auth_page():
    if session.get('user_id'):
        return redirect('/')
    return render_template('auth.html', current_page='auth')


@app.route('/api/auth/register', methods=['POST'])
def register():
    """Register a new user."""
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')
    team_name = data.get('team_name', '').strip()

    if not username or len(username) < 3:
        return jsonify({'error': 'Username must be at least 3 characters'}), 400

    if not password or len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400

    if not team_name:
        return jsonify({'error': 'Team selection required'}), 400

    conn = get_db()
    ensure_schema(conn)
    result = create_user(conn, username, password, team_name)
    conn.close()

    if 'error' in result:
        return jsonify(result), 400

    session['user_id'] = result['user_id']
    session['username'] = result['username']
    session['team_name'] = result['team_name']

    return jsonify({'status': 'success', 'message': 'Account created', **result}), 201


@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login user."""
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')

    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400

    conn = get_db()
    ensure_schema(conn)
    result = authenticate_user(conn, username, password)
    conn.close()

    if 'error' in result:
        return jsonify(result), 401

    session['user_id'] = result['user_id']
    session['username'] = result['username']
    session['team_name'] = result['team_name']

    return jsonify({'status': 'success', 'message': 'Logged in', **result}), 200


@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout user."""
    session.clear()
    return jsonify({'status': 'success', 'message': 'Logged out'}), 200


@app.route('/api/auth/user')
def get_user():
    """Get current logged-in user."""
    if not session.get('user_id'):
        return jsonify({'error': 'Not logged in'}), 401

    return jsonify({
        'user_id': session['user_id'],
        'username': session['username'],
        'team_name': session['team_name'],
    }), 200


@app.route('/api/auth/teams')
def list_teams():
    """Get available teams for registration."""
    conn = get_db()
    ensure_schema(conn)
    teams = get_available_teams(conn)
    conn.close()
    return jsonify(teams), 200


# MAIN ROUTES

@app.route('/')
def index():
    return render_template('index.html', current_page='squad')


@app.route('/api/state')
def state():
    conn = get_db()
    ensure_schema(conn)

    last_round = get_last_round(conn)
    next_round = last_round + 1

    cursor = _get_cursor(conn)
    cursor.execute("""
        WITH team_latest AS (
            SELECT team_name, MAX(round) AS latest_round
            FROM team_selections
            GROUP BY team_name
        ),
        current_picks AS (
            SELECT ts.player_id, MIN(ts.team_name) AS team_name
            FROM team_selections ts
            JOIN team_latest tl
                ON ts.team_name = tl.team_name AND ts.round = tl.latest_round
            GROUP BY ts.player_id
        ),
        lineup_round AS (
            SELECT MAX(round) AS max_round FROM match_lineups
        )
        SELECT
            p.player_id,
            p.name,
            p.position,
            p.team AS real_team,
            ws.price,
            ws.total_points - COALESCE(ws_prev.total_points, 0) AS last_round_score,
            cp.team_name AS fantasy_team,
            CASE
                WHEN ml.player_name IS NOT NULL AND ml.is_bench = 0 THEN 'S'
                WHEN ml.player_name IS NOT NULL AND ml.is_bench = 1 THEN 'B'
                ELSE NULL
            END AS lineup_status
        FROM players p
        JOIN weekly_stats ws
            ON ws.player_id = p.player_id AND ws.round = ?
        LEFT JOIN weekly_stats ws_prev
            ON ws_prev.player_id = p.player_id AND ws_prev.round = ?
        LEFT JOIN current_picks cp ON cp.player_id = p.player_id
        LEFT JOIN match_lineups ml
            ON REPLACE(p.name, '''', '') = ml.player_name
            AND ml.round = (SELECT max_round FROM lineup_round)
        ORDER BY p.position, ws.total_points DESC
    """, (last_round, last_round - 1))
    players = [dict(r) for r in cursor.fetchall()]
    for p in players:
        if p.get('last_round_score') is not None:
            p['last_round_score'] = round(p['last_round_score'], 1)
    cursor.close()

    cursor = _get_cursor(conn)
    cursor.execute('''
        SELECT DISTINCT team_name FROM team_selections ORDER BY team_name
    ''')
    teams = [r['team_name'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
    cursor.close()

    locked  = is_locked(conn)
    cutoff  = next_lock_time(conn, next_round)
    reopen  = reopen_time(conn, next_round)
    conn.close()
    return jsonify({
        'round':       next_round,
        'last_round':  last_round,
        'is_locked':   locked,
        'cutoff':      cutoff,
        'reopen':      reopen,
        'players':     players,
        'teams':       teams,
        'quotas':      SQUAD_QUOTAS,
        'starters':    SQUAD_STARTERS,
        'total_squad': TOTAL_SQUAD,
    })


@app.route('/api/my-picks')
def my_picks():
    """Load the logged-in user's squad using their session team — no URL team_name matching."""
    team_name = session.get('team_name')
    if not team_name:
        return jsonify({'error': 'Not logged in'}), 401

    conn = get_db()
    ensure_schema(conn)
    cursor = _get_cursor(conn)
    cursor.execute('''
        SELECT
            p.player_id, p.name, p.position, p.team AS real_team,
            ts.is_captain, ts.is_kicker, ts.is_bench, ts.jersey
        FROM team_selections ts
        JOIN players p ON p.player_id = ts.player_id
        WHERE ts.team_name = ?
          AND ts.round = (
              SELECT MAX(round) FROM team_selections WHERE team_name = ?
          )
        ORDER BY ts.is_bench, ts.jersey
    ''', (team_name, team_name))
    picks = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    next_round = get_next_round(conn)
    conn.close()
    return jsonify({'team_name': team_name, 'picks': picks, 'round': next_round})


@app.route('/api/team-view')
def get_team_view():
    """View another team's picks. Uses query param to avoid Vercel path-variable routing issues."""
    team_name = request.args.get('name', '').strip()
    if not team_name:
        return jsonify({'error': 'name param required'}), 400

    conn = get_db()
    ensure_schema(conn)

    cursor = _get_cursor(conn)
    cursor.execute('''
        SELECT
            p.player_id, p.name, p.position, p.team AS real_team,
            ts.is_captain, ts.is_kicker, ts.is_bench, ts.jersey
        FROM team_selections ts
        JOIN players p ON p.player_id = ts.player_id
        WHERE ts.team_name = ?
          AND ts.round = (
              SELECT MAX(round) FROM team_selections WHERE team_name = ?
          )
        ORDER BY ts.is_bench, ts.jersey
    ''', (team_name, team_name))

    picks = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    next_round = get_next_round(conn)
    conn.close()

    return jsonify({
        'team_name': team_name,
        'round':     next_round,
        'picks':     picks,
    })


@app.route('/api/team/<team_name>')
def get_team(team_name):
    """Path-variable route kept for backward compat (save picks still uses /api/team/<name>/picks)."""
    conn = get_db()
    ensure_schema(conn)

    cursor = _get_cursor(conn)
    cursor.execute('''
        SELECT
            p.player_id, p.name, p.position, p.team AS real_team,
            ts.is_captain, ts.is_kicker, ts.is_bench, ts.jersey
        FROM team_selections ts
        JOIN players p ON p.player_id = ts.player_id
        WHERE ts.team_name = ?
          AND ts.round = (
              SELECT MAX(round) FROM team_selections WHERE team_name = ?
          )
        ORDER BY ts.is_bench, ts.jersey
    ''', (team_name, team_name))

    picks = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    next_round = get_next_round(conn)
    conn.close()

    return jsonify({'team_name': team_name, 'round': next_round, 'picks': picks})



@app.route('/api/team/<team_name>/picks', methods=['POST'])
def save_picks(team_name):
    # Check user is logged in
    user_id = session.get('user_id')
    if not user_id:
        return jsonify({'error': 'Not logged in'}), 401

    # Look up user's team from database using user_id (not session)
    conn = get_db()
    ensure_schema(conn)
    cursor = _get_cursor(conn)
    cursor.execute('SELECT team_name FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    cursor.close()

    # Fall back to session team_name when users table has no matching row
    user_team = (row['team_name'] if isinstance(row, dict) else row[0]) if row else session.get('team_name')
    if not user_team:
        conn.close()
        return jsonify({'error': 'No team associated with your account'}), 401

    # Use user_team from database, ignore URL team_name - this prevents any typo/mismatch issues
    if is_locked(conn):
        conn.close()
        return jsonify({'error': 'Deadline has passed — picks are locked until next round.'}), 403

    data = request.get_json()
    player_ids = data.get('player_ids', [])
    bench_ids  = set(data.get('bench_ids', []))
    jerseys    = data.get('jerseys', {})   # {player_id: jersey_num}
    captain_id = data.get('captain_id')
    kicker_id  = data.get('kicker_id')

    if not player_ids:
        conn.close()
        return jsonify({'error': 'No players selected.'}), 400

    next_round = get_next_round(conn)

    # Validate position quotas
    cursor = _get_cursor(conn)
    placeholders = ','.join(['?'] * len(player_ids))
    cursor.execute(
        f'SELECT player_id, position FROM players WHERE player_id IN ({placeholders})',
        player_ids,
    )
    pos_rows = [dict(r) for r in cursor.fetchall()]
    pos_map = {r['player_id']: r['position'] for r in pos_rows}
    pos_counts = {}
    for pos in pos_map.values():
        pos_counts[pos] = pos_counts.get(pos, 0) + 1

    errors = []
    for pos, required in SQUAD_QUOTAS.items():
        actual = pos_counts.get(pos, 0)
        if actual != required:
            errors.append(f'{pos}: need {required}, got {actual}')
    if errors:
        cursor.close()
        conn.close()
        return jsonify({'error': f'Invalid squad: {"; ".join(errors)}'}), 400

    # Check none of the selected players are claimed by a different team
    cursor.execute(f'''
        SELECT p.name, ts.team_name
        FROM team_selections ts
        JOIN players p ON p.player_id = ts.player_id
        WHERE ts.round = ?
          AND ts.player_id IN ({placeholders})
          AND ts.team_name != ?
    ''', [next_round, *player_ids, user_team])
    conflicts = [dict(r) for r in cursor.fetchall()]

    if conflicts:
        msgs = [f"{r['name']} (already in {r['team_name']})" for r in conflicts]
        cursor.close()
        conn.close()
        return jsonify({'error': f"Player conflict: {', '.join(msgs)}"}), 409

    now = datetime.now(timezone.utc).isoformat()

    # Replace this team's picks for the round
    cursor.execute(
        'DELETE FROM team_selections WHERE team_name = ? AND round = ?',
        (user_team, next_round),
    )
    for pid in player_ids:
        jnum = jerseys.get(str(pid)) or jerseys.get(pid)
        cursor.execute('''
            INSERT INTO team_selections
                (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            next_round, user_team, pid,
            1 if pid == captain_id else 0,
            1 if pid == kicker_id  else 0,
            1 if pid in bench_ids  else 0,
            jnum,
            now,
        ))
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'status': 'saved', 'round': next_round, 'count': len(player_ids)})



# ---------------------------------------------------------------------------
# Competition routes
# ---------------------------------------------------------------------------

@app.route('/competition')
def competition():
    return render_template('competition.html', current_page='competition')


@app.route('/fixtures')
def fixtures():
    return render_template('fixtures.html', current_page='fixtures')


@app.route('/api/competition')
def competition_data():
    fixtures  = parse_fixtures(FIXTURES_CSV)
    conn      = get_db()
    max_round = get_last_round(conn)

    table = calculate_table(fixtures, conn, max_round)

    # Group fixtures by week for two-pass bye handling
    week_map: dict[int, list] = defaultdict(list)
    for fix in fixtures:
        week_map[fix[0]].append(fix)

    all_weeks: dict[int, list] = {}
    for week in sorted(week_map.keys()):
        all_weeks[week] = []
        wf = week_map[week]

        # Pass 1: compute all non-bye scores so we can derive the bye average
        non_bye_scores: dict[str, float] = {}
        if week <= max_round:
            for _, home, _hbp, away, _abp in wf:
                if home == 'Bye' or away == 'Bye':
                    continue
                non_bye_scores[home] = get_team_score(conn, home, week)
                non_bye_scores[away] = get_team_score(conn, away, week)
        bye_avg = (
            sum(non_bye_scores.values()) / len(non_bye_scores)
            if non_bye_scores else 0.0
        )

        # Pass 2: build match entries
        for _, home, _hbp, away, _abp in wf:
            if home == 'Bye' or away == 'Bye':
                team = home if away == 'Bye' else away
                if week <= max_round:
                    ts      = get_team_score(conn, team, week)
                    no_data = ts == 0 and bye_avg == 0
                    margin  = abs(ts - bye_avg)
                    t_wins  = ts > bye_avg
                    t_loses = ts < bye_avg
                    t_bp    = (t_wins  and margin >= WINNER_BP_MARGIN) or \
                              (t_loses and margin <= LOSER_BP_MARGIN)
                    all_weeks[week].append({
                        'is_bye': True, 'played': not no_data,
                        'team': team,
                        'team_score': round(ts, 1),
                        'bye_score':  round(bye_avg, 1),
                        'wins': t_wins, 'loses': t_loses,
                        'team_bp': t_bp,
                    })
                else:
                    all_weeks[week].append({'is_bye': True, 'played': False, 'team': team})
                continue

            if week <= max_round:
                hs      = non_bye_scores.get(home, 0.0)
                aw      = non_bye_scores.get(away, 0.0)
                no_data = hs == 0 and aw == 0
                h_bp = a_bp = False
                if not no_data and hs != aw:
                    margin = abs(hs - aw)
                    h_wins = hs > aw
                    h_bp = (h_wins and margin >= WINNER_BP_MARGIN) or (not h_wins and margin <= LOSER_BP_MARGIN)
                    a_bp = (not h_wins and margin >= WINNER_BP_MARGIN) or (h_wins and margin <= LOSER_BP_MARGIN)
                all_weeks[week].append({
                    'is_bye': False, 'played': not no_data,
                    'home': home, 'away': away,
                    'home_score': round(hs, 1), 'away_score': round(aw, 1),
                    'home_wins': hs > aw, 'away_wins': aw > hs,
                    'home_bp': h_bp, 'away_bp': a_bp,
                })
            else:
                all_weeks[week].append({'is_bye': False, 'played': False, 'home': home, 'away': away})

    conn.close()
    return jsonify({
        'max_round': max_round,
        'table': [{
            'name': t.name, 'played': t.played,
            'won': t.won, 'drawn': t.drawn, 'lost': t.lost,
            'points_for': round(t.points_for, 1),
            'points_against': round(t.points_against, 1),
            'points_diff': round(t.points_diff, 1),
            'bonus_points': t.bonus_points,
            'league_points': t.league_points,
        } for t in table],
        'results': [
            {'week': w, 'matches': m}
            for w, m in sorted(all_weeks.items())
        ],
    })


# ---------------------------------------------------------------------------
# Cron helpers
# ---------------------------------------------------------------------------

CRON_SECRET = os.getenv('CRON_SECRET', '')

_SUPERBRU_POS = {1: 'PR', 2: 'HK', 3: 'LK', 4: 'LF', 5: 'SH', 6: 'FH', 7: 'MID', 8: 'OBK'}
_SUPERBRU_URL = 'https://www.superbru.com/premiershiprugbyfantasy/ajax/f_write_player_stats.php?'
_SCRAPE_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/124.0.0.0 Safari/537.36'
    ),
}


def _cron_auth_ok() -> bool:
    """Accept requests from Vercel's cron runner. Skip check when no secret is configured (local dev)."""
    if not CRON_SECRET:
        return True
    return request.headers.get('Authorization') == f'Bearer {CRON_SECRET}'


def _to_float(val):
    try:
        return float(str(val).replace('£', '').replace('m', '').strip())
    except (ValueError, TypeError):
        return 0.0


def _to_price(val):
    try:
        return float(str(val).replace('£', '').replace('m', '').strip()) * 1_000_000
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Cron: sync round schedule from ESPN
# ---------------------------------------------------------------------------

@app.route('/api/cron/sync-rounds')
def cron_sync_rounds():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401

    try:
        from .sync_rounds import fetch_rounds
        rounds = fetch_rounds()
    except Exception as e:
        return jsonify({'error': str(e)}), 500

    conn = get_db()
    ensure_schema(conn)
    cursor = _get_cursor(conn)
    for round_num, first_ko, last_ko, _ in rounds:
        cursor.execute('''
            INSERT INTO rounds (round_number, first_kickoff, last_kickoff)
            VALUES (?, ?, ?)
            ON CONFLICT (round_number) DO UPDATE SET
                first_kickoff = excluded.first_kickoff,
                last_kickoff  = excluded.last_kickoff
        ''', (round_num, first_ko, last_ko))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'status': 'ok', 'rounds_synced': len(rounds)})


# ---------------------------------------------------------------------------
# Cron: fetch ESPN lineups for the current round
# ---------------------------------------------------------------------------

@app.route('/api/cron/lineups')
def cron_lineups():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401

    from .real_lineups import fetch_json, get_round_events, extract_lineups, format_name

    conn = get_db()
    ensure_schema(conn)
    next_round = get_next_round(conn)

    try:
        events = get_round_events(next_round)
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e), 'round': next_round}), 500

    cursor = _get_cursor(conn)
    now = datetime.now(timezone.utc).isoformat()
    written = 0
    skipped = []

    for event in events:
        game_id = event['id']
        comp = event['competitions'][0]
        home = next((c for c in comp['competitors'] if c['homeAway'] == 'home'), {})
        away = next((c for c in comp['competitors'] if c['homeAway'] == 'away'), {})
        label = f"{home.get('team',{}).get('abbreviation','?')} v {away.get('team',{}).get('abbreviation','?')}"

        summary_url = (
            f'https://site.api.espn.com/apis/site/v2/sports/rugby'
            f'/267979/summary?event={game_id}'
        )
        try:
            summary = fetch_json(summary_url)
            teams   = extract_lineups(summary)
        except Exception:
            skipped.append(label)
            continue

        for team in teams:
            for p in team['players']:
                if not p['name']:
                    continue
                cursor.execute('''
                    INSERT INTO match_lineups
                        (round, player_name, real_team, jersey, is_bench, scraped_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (round, player_name, real_team) DO UPDATE SET
                        jersey     = excluded.jersey,
                        is_bench   = excluded.is_bench,
                        scraped_at = excluded.scraped_at
                ''', (
                    next_round, format_name(p['name']), team['name'],
                    p['jersey'], 1 if p['is_bench'] else 0, now,
                ))
                written += 1

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({
        'status':          'ok',
        'round':           next_round,
        'entries_written': written,
        'no_lineup_yet':   skipped,
    })


# ---------------------------------------------------------------------------
# Cron: scrape SuperBru player stats for the completed round
# ---------------------------------------------------------------------------

@app.route('/api/cron/player-data')
def cron_player_data():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401

    import requests
    from bs4 import BeautifulSoup

    conn = get_db()
    ensure_schema(conn)
    round_num  = get_next_round(conn)
    scraped_at = datetime.now(timezone.utc).isoformat()

    # Scrape all 8 position pages from SuperBru
    players = []
    try:
        for page in range(1, 9):
            resp = requests.get(
                f'{_SUPERBRU_URL}pg={page}&tbl=2017',
                headers=_SCRAPE_HEADERS,
                timeout=10,
                verify=False,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            tbl  = soup.find('tbody')
            if not tbl:
                continue
            for row in tbl.find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) < 8:
                    cells.insert(5, '0')
                players.append({
                    'team':          cells[0],
                    'name':          cells[1][:-1] if cells[1] else '',
                    'position':      _SUPERBRU_POS[page],
                    'total_points':  _to_float(cells[3]),
                    'price':         _to_price(cells[4]),
                    'kicking':       _to_float(cells[5]),
                    'ppg':           cells[6],
                    'popularity':    cells[7],
                    'form':          cells[8] if len(cells) > 8 else '',
                })
    except Exception as e:
        conn.close()
        return jsonify({'error': f'Scrape failed: {e}'}), 500

    # Persist to DB
    cursor   = _get_cursor(conn)
    upserted = 0

    for p in players:
        if not p['name']:
            continue

        # Upsert player record
        cursor.execute('''
            INSERT INTO players (name, team, position)
            VALUES (?, ?, ?)
            ON CONFLICT (name, team, position) DO UPDATE SET
                team     = excluded.team,
                position = excluded.position
        ''', (p['name'], p['team'], p['position']))

        cursor.execute(
            'SELECT player_id FROM players WHERE name = ? AND team = ? AND position = ?',
            (p['name'], p['team'], p['position']),
        )
        row       = cursor.fetchone()
        player_id = row['player_id'] if isinstance(row, dict) else row[0]

        # Upsert weekly stats
        cursor.execute('''
            INSERT INTO weekly_stats
                (player_id, round, total_points, price, kicking,
                 points_per_game, popularity, form, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (player_id, round) DO UPDATE SET
                total_points    = excluded.total_points,
                price           = excluded.price,
                kicking         = excluded.kicking,
                points_per_game = excluded.points_per_game,
                popularity      = excluded.popularity,
                form            = excluded.form,
                scraped_at      = excluded.scraped_at
        ''', (
            player_id, round_num,
            p['total_points'], p['price'], p['kicking'],
            p['ppg'], p['popularity'], p['form'], scraped_at,
        ))
        upserted += 1

    # Copy last round's picks forward as defaults for teams that haven't updated yet
    if round_num > 1:
        cursor.execute('''
            INSERT INTO team_selections
                (round, team_name, player_id, is_captain, is_kicker,
                 is_bench, jersey, scraped_at)
            SELECT ?, team_name, player_id, is_captain, is_kicker,
                   is_bench, jersey, ?
            FROM team_selections
            WHERE round = ?
            ON CONFLICT (round, team_name, player_id) DO NOTHING
        ''', (round_num, scraped_at, round_num - 1))

    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'status': 'ok', 'round': round_num, 'players_upserted': upserted})


# ---------------------------------------------------------------------------

if __name__ == '__main__':
    app.run(debug=True, port=5000)
