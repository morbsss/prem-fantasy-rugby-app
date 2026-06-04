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
from .leagues import (
    DEFAULT_LEAGUE, LEAGUES, ROSTER_SIZE, BENCH_COUNT, STARTER_SLOTS,
    SLOT_POSITIONS, validate_roster, INDIVIDUAL_POSITIONS, FR_POSITIONS,
    FRONT_ROW_SPOTS,
)
from . import draft as draft_engine
from . import scheduler, ingest
from .auth import create_user, authenticate_user, get_available_teams
from .competition import (
    calculate_table, get_team_score,
    get_league_teams, generate_regular_fixtures,
    build_playoffs, playoff_fixtures, standings_progression, REGULAR_ROUNDS,
    WINNER_BP_MARGIN, LOSER_BP_MARGIN,
)

app = Flask(__name__, template_folder='templates')

# Configure session
app.secret_key = os.getenv('SECRET_KEY', 'dev-secret-key-change-in-production')
app.config['SESSION_COOKIE_HTTPONLY'] = True
app.config['SESSION_COOKIE_SECURE'] = os.getenv('FLASK_ENV', 'development') == 'production'
app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# Get database type
DB_TYPE_LOCAL = os.getenv('DB_TYPE', 'sqlite').lower()

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


# ---------------------------------------------------------------------------
# Current-league resolution (two-league support, spec §1/§5.1)
# ---------------------------------------------------------------------------

def _league_id_by_slug(conn, slug):
    cursor = _get_cursor(conn)
    cursor.execute('SELECT league_id FROM leagues WHERE slug = ?', (slug,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return row['league_id'] if isinstance(row, dict) else row[0]


def current_league_id(conn):
    """Resolve the league the current request operates on.

    Precedence: ?league=<slug> override → session league_slug (set at onboarding)
    → the logged-in user's league → DEFAULT_LEAGUE. Always returns a valid id.
    """
    slug = request.args.get('league') or session.get('league_slug')
    if slug and slug in LEAGUES:
        lid = _league_id_by_slug(conn, slug)
        if lid:
            return lid
    user_id = session.get('user_id')
    if user_id:
        cursor = _get_cursor(conn)
        cursor.execute('SELECT league_id FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        cursor.close()
        if row:
            lid = row['league_id'] if isinstance(row, dict) else row[0]
            if lid:
                return lid
    return _league_id_by_slug(conn, DEFAULT_LEAGUE)


def _league_meta(conn, league_id):
    """slug/name/competition/theme for a league_id (for theming the UI)."""
    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT slug, name, competition, theme FROM leagues WHERE league_id = ?',
        (league_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return dict(row) if not isinstance(row, dict) else row


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


def get_next_round(conn, league_id=None) -> int:
    """The round users are currently picking for (or playing in).

    Time-based: smallest round_number in `rounds` whose last_kickoff is in
    the future. Falls back to MAX(weekly_stats.round) + 1 when the rounds
    table is empty or every scheduled round has finished. Scoped to `league_id`.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    cursor = _get_cursor(conn)
    if league_id is None:
        cursor.execute(
            'SELECT round_number FROM rounds WHERE last_kickoff > ? '
            'ORDER BY round_number ASC LIMIT 1',
            (now_iso,)
        )
    else:
        cursor.execute(
            'SELECT round_number FROM rounds WHERE last_kickoff > ? AND league_id = ? '
            'ORDER BY round_number ASC LIMIT 1',
            (now_iso, league_id)
        )
    row = cursor.fetchone()
    cursor.close()
    if row:
        return row['round_number'] if isinstance(row, dict) else row[0]
    return _round_after_last_scraped(conn, league_id)


def _round_after_last_scraped(conn, league_id=None) -> int:
    """MAX(weekly_stats.round) + 1, or 1 if empty. Used by the player-data
    cron to label the round whose stats it's about to record."""
    cursor = _get_cursor(conn)
    if league_id is None:
        cursor.execute('SELECT MAX(round) FROM weekly_stats')
    else:
        cursor.execute('SELECT MAX(round) FROM weekly_stats WHERE league_id = ?', (league_id,))
    row = cursor.fetchone()
    cursor.close()
    if DB_TYPE == 'postgres':
        result = row['max'] if row and row.get('max') else None
    else:
        result = row[0] if row else None
    return 1 if result is None else result + 1


def get_last_round(conn, league_id=None) -> int:
    cursor = _get_cursor(conn)
    if league_id is None:
        cursor.execute('SELECT MAX(round) FROM weekly_stats')
    else:
        cursor.execute('SELECT MAX(round) FROM weekly_stats WHERE league_id = ?', (league_id,))
    row = cursor.fetchone()
    cursor.close()
    if DB_TYPE == 'postgres':
        result = row['max'] if row and row.get('max') else None
    else:
        result = row[0] if row else None
    return result or 1


ALLOW_UNRESTRICTED_EDITS = os.getenv('ALLOW_UNRESTRICTED_EDITS', 'false').lower() == 'true'


def _round_kickoffs(conn, round_num, league_id=None):
    """Return (first_kickoff, last_kickoff) datetimes for round_num, or (None, None)."""
    cursor = _get_cursor(conn)
    if league_id is None:
        cursor.execute(
            'SELECT first_kickoff, last_kickoff FROM rounds WHERE round_number = ?',
            (round_num,)
        )
    else:
        cursor.execute(
            'SELECT first_kickoff, last_kickoff FROM rounds '
            'WHERE round_number = ? AND league_id = ?',
            (round_num, league_id)
        )
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None, None
    first_str = row['first_kickoff'] if isinstance(row, dict) else row[0]
    last_str  = row['last_kickoff']  if isinstance(row, dict) else row[1]
    return datetime.fromisoformat(first_str), datetime.fromisoformat(last_str)


def is_locked(conn=None, league_id=None) -> bool:
    """Picks lock at the first kickoff of the round being picked for."""
    if ALLOW_UNRESTRICTED_EDITS:
        return False
    _owned = conn is None
    if _owned:
        conn = get_db()
        ensure_schema(conn)
    next_round = get_next_round(conn, league_id)
    first_ko, _ = _round_kickoffs(conn, next_round, league_id)
    if _owned:
        conn.close()
    if first_ko is None:
        return False
    return datetime.now(timezone.utc) >= first_ko


def next_lock_time(conn, next_round, league_id=None) -> str:
    """ISO string of when picks lock: first kickoff of next_round."""
    first_ko, _ = _round_kickoffs(conn, next_round, league_id)
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


def reopen_time(conn, next_round, league_id=None) -> str:
    """ISO string of estimated unlock: last kickoff of next_round (round ends)."""
    _, last_ko = _round_kickoffs(conn, next_round, league_id)
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


def _slug_for_league_id(conn, league_id):
    meta = _league_meta(conn, league_id)
    return meta['slug'] if meta else None


@app.context_processor
def inject_theme():
    """Theme + brand for every rendered page, from the session's league (§7).

    No league yet (anonymous / mid-selection) → neutral default; the onboarding
    screen flips to grayscale client-side until a league is chosen.
    """
    slug = session.get('league_slug')
    league = LEAGUES.get(slug) if slug else None
    if league:
        return {
            'theme': league['theme'], 'brand': league['brand'],
            'brand_sub': league['comp_name'], 'league_slug': slug,
        }
    return {'theme': 'forest', 'brand': 'Meatyboys', 'brand_sub': 'Rugby Fantasy',
            'league_slug': None}


@app.route('/api/leagues')
def list_leagues():
    """The two joinable leagues, for the onboarding chooser (spec §6.1)."""
    return jsonify([
        {
            'slug': cfg['slug'], 'name': cfg['name'], 'brand': cfg['brand'],
            'competition': cfg['competition'], 'comp_name': cfg['comp_name'],
            'theme': cfg['theme'],
        }
        for cfg in LEAGUES.values()
    ]), 200


@app.route('/api/auth/register', methods=['POST'])
def register():
    """Register a new user: email + password, joined to a chosen league (§6.1)."""
    data = request.get_json()
    email = data.get('email', '').strip().lower()
    password = data.get('password', '')
    team_name = data.get('team_name', '').strip()
    league_slug = data.get('league', '').strip()

    if not email or '@' not in email:
        return jsonify({'error': 'A valid email is required'}), 400
    if not password or len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    if league_slug not in LEAGUES:
        return jsonify({'error': 'Please choose a league to join'}), 400
    if not team_name:
        return jsonify({'error': 'Please name your team'}), 400

    conn = get_db()
    ensure_schema(conn)
    league_id = _league_id_by_slug(conn, league_slug)
    result = create_user(conn, email, password, team_name, league_id)

    if 'error' in result:
        conn.close()
        return jsonify(result), 400

    # Commissioner = league creator (§8.5): first registrant claims the role.
    cursor = _get_cursor(conn)
    cursor.execute(
        'UPDATE leagues SET commissioner_user_id = ? '
        'WHERE league_id = ? AND commissioner_user_id IS NULL',
        (result['user_id'], league_id),
    )
    conn.commit()
    cursor.close()
    conn.close()

    session['user_id'] = result['user_id']
    session['username'] = result['username']
    session['team_name'] = result['team_name']
    session['league_slug'] = league_slug

    return jsonify({'status': 'success', 'message': 'Account created', **result}), 201


@app.route('/api/auth/login', methods=['POST'])
def login():
    """Login by email (or legacy username) + password."""
    data = request.get_json()
    identifier = (data.get('email') or data.get('username') or '').strip().lower()
    password = data.get('password', '')

    if not identifier or not password:
        return jsonify({'error': 'Email and password required'}), 400

    conn = get_db()
    ensure_schema(conn)
    result = authenticate_user(conn, identifier, password)
    slug = _slug_for_league_id(conn, result['league_id']) if 'league_id' in result else None
    conn.close()

    if 'error' in result:
        return jsonify(result), 401

    session['user_id'] = result['user_id']
    session['username'] = result['username']
    session['team_name'] = result['team_name']
    if slug:
        session['league_slug'] = slug

    return jsonify({'status': 'success', 'message': 'Logged in', **result}), 200


@app.route('/api/auth/logout', methods=['POST'])
def logout():
    """Logout user."""
    session.clear()
    return jsonify({'status': 'success', 'message': 'Logged out'}), 200


@app.route('/api/auth/user')
def get_user():
    """Get current logged-in user, including their league for theming."""
    if not session.get('user_id'):
        return jsonify({'error': 'Not logged in'}), 401

    slug = session.get('league_slug')
    league = LEAGUES.get(slug) if slug else None
    return jsonify({
        'user_id': session['user_id'],
        'username': session['username'],
        'team_name': session['team_name'],
        'league': {
            'slug': slug, 'name': league['name'], 'brand': league['brand'],
            'theme': league['theme'], 'comp_name': league['comp_name'],
        } if league else None,
    }), 200


@app.route('/api/auth/teams')
def list_teams():
    """Existing teams in a league (and whether claimed). ?league=<slug> to scope."""
    conn = get_db()
    ensure_schema(conn)
    league_id = current_league_id(conn)
    teams = get_available_teams(conn, league_id)
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

    league_id  = current_league_id(conn)
    last_round = get_last_round(conn, league_id)
    next_round = get_next_round(conn, league_id)

    cursor = _get_cursor(conn)
    cursor.execute("""
        WITH team_latest AS (
            SELECT team_name, MAX(round) AS latest_round
            FROM team_selections
            WHERE league_id = ?
            GROUP BY team_name
        ),
        current_picks AS (
            SELECT ts.player_id, MIN(ts.team_name) AS team_name
            FROM team_selections ts
            JOIN team_latest tl
                ON ts.team_name = tl.team_name AND ts.round = tl.latest_round
            WHERE ts.league_id = ?
            GROUP BY ts.player_id
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
            AND ml.round = ? AND ml.league_id = p.league_id
        WHERE p.league_id = ?
        ORDER BY p.position, ws.total_points DESC
    """, (league_id, league_id, last_round, last_round - 1, next_round, league_id))
    players = [dict(r) for r in cursor.fetchall()]
    for p in players:
        if p.get('last_round_score') is not None:
            p['last_round_score'] = round(p['last_round_score'], 1)
    cursor.close()

    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT DISTINCT team_name FROM team_selections WHERE league_id = ? ORDER BY team_name',
        (league_id,),
    )
    teams = [r['team_name'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
    cursor.close()

    locked  = is_locked(conn, league_id)
    cutoff  = next_lock_time(conn, next_round, league_id)
    reopen  = reopen_time(conn, next_round, league_id)
    league  = _league_meta(conn, league_id)
    conn.close()
    return jsonify({
        'league':      league,
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
        # 17-man snake-draft roster shape (spec §6.2) — supersedes the 23-man
        # quotas above as the squad UI is rebuilt (milestone 9).
        'roster_size':   ROSTER_SIZE,
        'bench_count':   BENCH_COUNT,
        'starter_slots': STARTER_SLOTS,
        'slot_positions': {k: sorted(v) for k, v in SLOT_POSITIONS.items()},
    })


@app.route('/api/players')
def api_players():
    """Player Hub feed (spec §7). Filters (query params):
      round=N   points as of round N (default = latest round)
      metric=total|form   total points to the round, or form = avg of the last
                          3 rounds' points up to the round
    Also returns the available rounds and the league's fantasy teams for the
    Player Hub filters."""
    conn = get_db()
    ensure_schema(conn)
    league_id = current_league_id(conn)
    metric = (request.args.get('metric') or 'total').lower()
    round_param = request.args.get('round', type=int)
    cursor = _get_cursor(conn)

    cursor.execute('SELECT DISTINCT round FROM weekly_stats WHERE league_id = ? ORDER BY round',
                   (league_id,))
    rounds = [r['round'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
    max_round = rounds[-1] if rounds else 0
    target = round_param if (round_param and round_param in rounds) else max_round

    # Cumulative totals per player for every round up to the target.
    cursor.execute('SELECT player_id, round, total_points FROM weekly_stats '
                   'WHERE league_id = ? AND round <= ?', (league_id, target))
    cum: dict[int, dict[int, float]] = {}
    for r in cursor.fetchall():
        d = dict(r) if not isinstance(r, dict) else r
        cum.setdefault(d['player_id'], {})[d['round']] = d['total_points'] or 0.0

    def total_at(pid, rnd):
        rows = cum.get(pid, {})
        past = [rr for rr in rows if rr <= rnd]
        return rows[max(past)] if past else 0.0

    # Current fantasy ownership (squad round = MAX round <= next_round per team).
    next_round = get_next_round(conn, league_id)
    cursor.execute('''
        WITH team_round AS (
            SELECT team_name, MAX(round) AS r FROM team_selections
            WHERE league_id = ? AND round <= ? GROUP BY team_name
        )
        SELECT ts.player_id, MIN(ts.team_name) AS team_name
        FROM team_selections ts JOIN team_round tr
          ON ts.team_name = tr.team_name AND ts.round = tr.r
        WHERE ts.league_id = ? GROUP BY ts.player_id
    ''', (league_id, next_round, league_id))
    owner = {(r['player_id'] if isinstance(r, dict) else r[0]):
             (r['team_name'] if isinstance(r, dict) else r[1]) for r in cursor.fetchall()}

    def metric_value(pid):
        total = total_at(pid, target)
        if metric == 'form':
            base = total_at(pid, target - 3)
            n = min(3, target) or 1
            return round((total - base) / n, 1)
        return round(total, 1)

    # Individual players (NO props/hookers — those are part of the club FR unit).
    cursor.execute(
        f'SELECT player_id, name, position, team FROM players '
        f'WHERE league_id = ? AND position IN ({_IND_PH})',
        (league_id, *INDIVIDUAL_POSITIONS))
    players = []
    for r in cursor.fetchall():
        d = dict(r) if not isinstance(r, dict) else r
        pid = d['player_id']
        players.append({
            'player_id': pid, 'name': d['name'], 'position': d['position'],
            'real_team': d['team'], 'value': metric_value(pid), 'total_points': round(total_at(pid, target), 1),
            'fantasy_team': owner.get(pid), 'is_fr': False,
        })

    # Club front-row UNITS — one "<club> FR" entry per club (props/hookers never
    # shown individually). Owner = the team that drafted that club's front row.
    cursor.execute(
        "SELECT team, player_id FROM players WHERE league_id = ? AND position IN ('PR','HK')",
        (league_id,))
    club_fr_players: dict[str, list[int]] = {}
    for r in cursor.fetchall():
        d = dict(r) if not isinstance(r, dict) else r
        club_fr_players.setdefault(d['team'], []).append(d['player_id'])
    cursor.execute('SELECT club, team_name FROM team_front_row tfr WHERE league_id = ? '
                   'AND round = (SELECT MAX(round) FROM team_front_row WHERE league_id = ? AND team_name = tfr.team_name)',
                   (league_id, league_id))
    fr_owner = {(r['club'] if isinstance(r, dict) else r[0]):
                (r['team_name'] if isinstance(r, dict) else r[1]) for r in cursor.fetchall()}
    for club, pids in club_fr_players.items():
        val = round(sum(metric_value(pid) for pid in pids), 1)
        players.append({
            'player_id': None, 'name': f'{club} FR', 'position': 'FR',
            'real_team': club, 'value': val, 'total_points': val,
            'fantasy_team': fr_owner.get(club), 'is_fr': True,
        })

    cursor.close()
    players.sort(key=lambda p: p['value'], reverse=True)
    league = _league_meta(conn, league_id)
    teams = sorted({t for t in owner.values() if t} | {t for t in fr_owner.values() if t})
    conn.close()
    return jsonify({'league': league, 'players': players, 'rounds': rounds,
                    'round': target, 'metric': metric, 'teams': teams})


@app.route('/players')
def players_page():
    return render_template('player_hub.html', current_page='players')


@app.route('/transfers')
def transfers_page():
    return render_template('transfers.html', current_page='transfers')


@app.route('/matchup')
def matchup_page():
    return render_template('matchup.html', current_page='matchup')


@app.route('/api/my-picks')
def my_picks():
    """Load the logged-in user's squad using their session team — no URL team_name matching."""
    team_name = session.get('team_name')
    if not team_name:
        return jsonify({'error': 'Not logged in'}), 401

    conn = get_db()
    ensure_schema(conn)
    league_id = current_league_id(conn)
    next_round = get_next_round(conn, league_id)
    # Load the squad for the round currently being edited (next_round); if it
    # has no selections yet, fall back to the latest earlier round. Editing
    # (save_picks / transfers) writes to next_round, so this keeps the view and
    # the save target aligned.
    cursor = _get_cursor(conn)
    cursor.execute('''
        SELECT
            p.player_id, p.name, p.position, p.team AS real_team,
            ts.is_captain, ts.is_kicker, ts.is_bench, ts.jersey
        FROM team_selections ts
        JOIN players p ON p.player_id = ts.player_id
        WHERE ts.team_name = ?
          AND ts.round = (
              SELECT MAX(round) FROM team_selections
              WHERE team_name = ? AND round <= ?
          )
        ORDER BY ts.is_bench, ts.jersey
    ''', (team_name, team_name, next_round))
    picks = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    fr = _team_front_row_view(conn, league_id, team_name, next_round)
    conn.close()
    return jsonify({'team_name': team_name, 'picks': picks, 'round': next_round,
                    'fr_club': fr['club'], 'fr_players': fr['players']})


def _team_front_row_view(conn, league_id, team_name, next_round):
    """The team's owned club front-row unit + its current matchday front-rowers."""
    cursor = _get_cursor(conn)
    cursor.execute('SELECT club FROM team_front_row '
                   'WHERE team_name = ? AND round = (SELECT MAX(round) FROM team_front_row '
                   '  WHERE team_name = ? AND round <= ?)',
                   (team_name, team_name, next_round))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return {'club': None, 'players': []}
    club = row['club'] if isinstance(row, dict) else row[0]
    players = _fr_unit_players(conn, league_id, club, next_round)
    return {'club': club, 'players': players}


def _fr_unit_players(conn, league_id, club, round_num):
    """Club's PR/HK players in the round's matchday squad (S/B); falls back to all
    of the club's front-rowers when there's no lineup data."""
    cursor = _get_cursor(conn)
    cursor.execute('SELECT COUNT(*) AS c FROM match_lineups WHERE round = ? AND real_team = ?',
                   (round_num, club))
    crow = cursor.fetchone()
    has_lineup = ((crow['c'] if isinstance(crow, dict) else crow[0]) or 0) > 0
    if has_lineup:
        cursor.execute("""
            SELECT p.name, p.position, ml.is_bench
            FROM players p
            JOIN match_lineups ml ON ml.round = ? AND ml.real_team = p.team
              AND REPLACE(p.name, '''', '') = ml.player_name
            WHERE p.league_id = ? AND p.team = ? AND p.position IN ('PR','HK')
            ORDER BY ml.is_bench, p.position
        """, (round_num, league_id, club))
        rows = [dict(r) for r in cursor.fetchall()]
        out = [{'name': r['name'], 'position': r['position'],
                'status': 'B' if r['is_bench'] else 'S'} for r in rows]
    else:
        cursor.execute('''SELECT name, position FROM players
                          WHERE league_id = ? AND team = ? AND position IN ('PR','HK')
                          ORDER BY position, name''', (league_id, club))
        out = [{'name': (r['name'] if isinstance(r, dict) else r[0]),
                'position': (r['position'] if isinstance(r, dict) else r[1]),
                'status': '—'} for r in cursor.fetchall()]
    cursor.close()
    return out


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
    league_id = current_league_id(conn)
    next_round = get_next_round(conn, league_id)
    fr = _team_front_row_view(conn, league_id, team_name, next_round)
    conn.close()

    return jsonify({
        'team_name': team_name,
        'round':     next_round,
        'picks':     picks,
        'fr_club':   fr['club'],
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
    cursor.execute('SELECT team_name, league_id FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    cursor.close()

    # Fall back to session team_name when users table has no matching row
    if row:
        rowd = row if isinstance(row, dict) else {'team_name': row[0], 'league_id': row[1]}
        user_team = rowd['team_name']
        league_id = rowd['league_id']
    else:
        user_team = session.get('team_name')
        league_id = None
    if league_id is None:
        league_id = current_league_id(conn)
    if not user_team:
        conn.close()
        return jsonify({'error': 'No team associated with your account'}), 401

    # Use user_team from database, ignore URL team_name - this prevents any typo/mismatch issues
    if is_locked(conn, league_id):
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

    next_round = get_next_round(conn, league_id)

    # Validate the 17-man roster (spec §6.2): exactly ROSTER_SIZE players,
    # BENCH_COUNT bench, and a startable XV from the non-bench players.
    cursor = _get_cursor(conn)
    placeholders = ','.join(['?'] * len(player_ids))
    cursor.execute(
        f'SELECT player_id, position FROM players WHERE player_id IN ({placeholders})',
        player_ids,
    )
    pos_map = {r['player_id'] if isinstance(r, dict) else r[0]:
               (r['position'] if isinstance(r, dict) else r[1])
               for r in cursor.fetchall()}

    selections = [(pos_map.get(pid, '?'), pid in bench_ids) for pid in player_ids]
    ok, msg = validate_roster(selections)
    if not ok:
        cursor.close()
        conn.close()
        return jsonify({'error': f'Invalid squad: {msg}'}), 400

    # Check none of the selected players are claimed by a different team
    cursor.execute(f'''
        SELECT p.name, ts.team_name
        FROM team_selections ts
        JOIN players p ON p.player_id = ts.player_id
        WHERE ts.round = ?
          AND ts.league_id = ?
          AND ts.player_id IN ({placeholders})
          AND ts.team_name != ?
    ''', [next_round, league_id, *player_ids, user_team])
    conflicts = [dict(r) for r in cursor.fetchall()]

    if conflicts:
        msgs = [f"{r['name']} (already in {r['team_name']})" for r in conflicts]
        cursor.close()
        conn.close()
        return jsonify({'error': f"Player conflict: {', '.join(msgs)}"}), 409

    now = datetime.now(timezone.utc).isoformat()

    # Replace this team's picks for the round
    cursor.execute(
        'DELETE FROM team_selections WHERE team_name = ? AND round = ? AND league_id = ?',
        (user_team, next_round, league_id),
    )
    for pid in player_ids:
        jnum = jerseys.get(str(pid)) or jerseys.get(pid)
        cursor.execute('''
            INSERT INTO team_selections
                (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at, league_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            next_round, user_team, pid,
            1 if pid == captain_id else 0,
            1 if pid == kicker_id  else 0,
            1 if pid in bench_ids  else 0,
            jnum,
            now,
            league_id,
        ))
    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({'status': 'saved', 'round': next_round, 'count': len(player_ids)})


# ---------------------------------------------------------------------------
# Trades — free-agent pickups + inter-team (user↔user) trades
# ---------------------------------------------------------------------------

def _session_team(conn):
    """(user_id, team, league_id) for the logged-in user, or None."""
    uid = session.get('user_id')
    if not uid:
        return None
    cursor = _get_cursor(conn)
    cursor.execute('SELECT team_name, league_id FROM users WHERE user_id = ?', (uid,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    rd = dict(row) if not isinstance(row, dict) else row
    return {'user_id': uid, 'team': rd['team_name'], 'league_id': rd['league_id']}


def _roster_ids(conn, league_id, team, rnd):
    cursor = _get_cursor(conn)
    cursor.execute('SELECT player_id FROM team_selections WHERE league_id = ? AND team_name = ? AND round = ?',
                   (league_id, team, rnd))
    ids = [r['player_id'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
    cursor.close()
    return ids


def _player_position(conn, player_id):
    cursor = _get_cursor(conn)
    cursor.execute('SELECT position FROM players WHERE player_id = ?', (player_id,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return row['position'] if isinstance(row, dict) else row[0]


def _owner_of(conn, league_id, player_id, rnd):
    """Fantasy team owning a player this round, or None if free agent."""
    cursor = _get_cursor(conn)
    cursor.execute('SELECT team_name FROM team_selections WHERE league_id = ? AND player_id = ? AND round = ?',
                   (league_id, player_id, rnd))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return row['team_name'] if isinstance(row, dict) else row[0]


def _roster_round(conn, league_id, team, next_round):
    """The round a team's current squad lives on — MAX(round <= next_round) —
    matching what /api/my-picks loads, so trades write where the squad is shown."""
    cursor = _get_cursor(conn)
    cursor.execute('SELECT MAX(round) AS r FROM team_selections '
                   'WHERE league_id = ? AND team_name = ? AND round <= ?',
                   (league_id, team, next_round))
    row = cursor.fetchone()
    cursor.close()
    r = (row['r'] if isinstance(row, dict) else row[0]) if row else None
    return r if r is not None else next_round


def _swap_player(conn, league_id, team, rnd, out_id, in_id):
    """In-place 1-for-1 swap in a team's squad — the incoming player inherits the
    outgoing player's lineup slot (bench/jersey/captain/kicker). Squad stays any
    composition; the starting-XV rules are enforced separately on the Squad page."""
    cursor = _get_cursor(conn)
    cursor.execute('UPDATE team_selections SET player_id = ? '
                   'WHERE league_id = ? AND team_name = ? AND round = ? AND player_id = ?',
                   (in_id, league_id, team, rnd, out_id))
    cursor.close()


def _record_trade(conn, league_id, ttype, status, from_team, to_team, out_id, in_id, resolved=False):
    now = datetime.now(timezone.utc).isoformat()
    cursor = _get_cursor(conn)
    cursor.execute(
        'INSERT INTO trades (league_id, type, status, from_team, to_team, out_player_id, '
        ' in_player_id, created_at, resolved_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (league_id, ttype, status, from_team, to_team, out_id, in_id, now, now if resolved else None))
    conn.commit()
    cursor.close()


def _enrich_trades(conn, rows):
    """Attach player {name,position} for out/in ids to each trade row."""
    ids = {r[k] for r in rows for k in ('out_player_id', 'in_player_id') if r.get(k)}
    names = {}
    if ids:
        cursor = _get_cursor(conn)
        ph = ','.join(['?'] * len(ids))
        cursor.execute(f'SELECT player_id, name, position FROM players WHERE player_id IN ({ph})', list(ids))
        for r in cursor.fetchall():
            d = dict(r) if not isinstance(r, dict) else r
            names[d['player_id']] = {'name': d['name'], 'position': d['position']}
        cursor.close()
    for r in rows:
        r['out_player'] = names.get(r.get('out_player_id'))
        r['in_player'] = names.get(r.get('in_player_id'))
    return rows


@app.route('/api/trades')
def api_trades():
    """Trade feed for the current league: completed history + my pending offers."""
    conn = get_db()
    ensure_schema(conn)
    league_id = current_league_id(conn)
    st = _session_team(conn)
    my_team = st['team'] if st else None

    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT id, type, status, from_team, to_team, out_player_id, in_player_id, created_at, resolved_at '
        'FROM trades WHERE league_id = ? ORDER BY created_at DESC', (league_id,))
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    _enrich_trades(conn, rows)

    history  = [r for r in rows if r['status'] == 'completed']
    incoming = [r for r in rows if r['status'] == 'pending' and my_team and r['to_team'] == my_team]
    outgoing = [r for r in rows if r['status'] == 'pending' and my_team and r['from_team'] == my_team]
    locked = is_locked(conn, league_id)
    conn.close()
    return jsonify({'my_team': my_team, 'is_locked': locked,
                    'history': history, 'incoming': incoming, 'outgoing': outgoing})


@app.route('/api/trades/free-agent', methods=['POST'])
def trade_free_agent():
    """Immediate free-agent pickup: drop one of your players, add a free agent."""
    conn = get_db()
    ensure_schema(conn)
    st = _session_team(conn)
    if not st:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    league_id, team = st['league_id'], st['team']
    if is_locked(conn, league_id):
        conn.close()
        return jsonify({'error': 'Trades are locked — a game in this round has kicked off.'}), 403

    data = request.get_json() or {}
    drop_id, add_id = data.get('drop_id'), data.get('add_id')
    if _player_position(conn, add_id) in FR_POSITIONS:
        conn.close()
        return jsonify({'error': 'Props and hookers are owned via the club front-row unit, not individually.'}), 400
    rnd = _roster_round(conn, league_id, team, get_next_round(conn, league_id))
    roster = _roster_ids(conn, league_id, team, rnd)
    if drop_id not in roster:
        conn.close()
        return jsonify({'error': 'The player to drop is not on your team.'}), 400
    owner = _owner_of(conn, league_id, add_id, rnd)
    if owner is not None:
        conn.close()
        return jsonify({'error': f'That player is not a free agent (owned by {owner}).'}), 409

    # In-place swap — squad composition is unconstrained (rules apply to the XV).
    _swap_player(conn, league_id, team, rnd, drop_id, add_id)
    conn.commit()
    _record_trade(conn, league_id, 'free_agent', 'completed', team, None, drop_id, add_id, resolved=True)
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/trades/propose', methods=['POST'])
def trade_propose():
    """Propose a 1-for-1 trade to another user's team (pending their response)."""
    conn = get_db()
    ensure_schema(conn)
    st = _session_team(conn)
    if not st:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    league_id, team = st['league_id'], st['team']
    data = request.get_json() or {}
    to_team = (data.get('to_team') or '').strip()
    give_id, receive_id = data.get('give_id'), data.get('receive_id')
    nr = get_next_round(conn, league_id)
    my_rnd = _roster_round(conn, league_id, team, nr)
    their_rnd = _roster_round(conn, league_id, to_team, nr)

    if not to_team or to_team == team:
        conn.close()
        return jsonify({'error': 'Choose another team to trade with.'}), 400
    if _player_position(conn, receive_id) in FR_POSITIONS or _player_position(conn, give_id) in FR_POSITIONS:
        conn.close()
        return jsonify({'error': 'Props and hookers are part of the club front-row unit, not tradeable individually.'}), 400
    if give_id not in _roster_ids(conn, league_id, team, my_rnd):
        conn.close()
        return jsonify({'error': 'The player you are offering is not on your team.'}), 400
    if _owner_of(conn, league_id, receive_id, their_rnd) != to_team:
        conn.close()
        return jsonify({'error': 'The player you want is not on that team.'}), 400

    _record_trade(conn, league_id, 'player_trade', 'pending', team, to_team, give_id, receive_id)
    conn.close()
    return jsonify({'status': 'pending'})


@app.route('/api/trades/respond', methods=['POST'])
def trade_respond():
    """Responder accepts or rejects a pending trade offered to their team."""
    conn = get_db()
    ensure_schema(conn)
    st = _session_team(conn)
    if not st:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    league_id, team = st['league_id'], st['team']
    data = request.get_json() or {}
    trade_id, action = data.get('trade_id'), data.get('action')

    cursor = _get_cursor(conn)
    cursor.execute('SELECT type, status, from_team, to_team, out_player_id, in_player_id '
                   'FROM trades WHERE id = ? AND league_id = ?', (trade_id, league_id))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        conn.close()
        return jsonify({'error': 'Trade not found.'}), 404
    t = dict(row) if not isinstance(row, dict) else row
    if t['status'] != 'pending' or t['to_team'] != team:
        conn.close()
        return jsonify({'error': 'This trade is not awaiting your response.'}), 403

    now = datetime.now(timezone.utc).isoformat()
    if action == 'reject':
        cursor = _get_cursor(conn)
        cursor.execute('UPDATE trades SET status = ?, resolved_at = ? WHERE id = ?',
                       ('rejected', now, trade_id))
        conn.commit(); cursor.close(); conn.close()
        return jsonify({'status': 'rejected'})

    if action != 'accept':
        conn.close()
        return jsonify({'error': 'Unknown action.'}), 400

    if is_locked(conn, league_id):
        conn.close()
        return jsonify({'error': 'Trades are locked — a game in this round has kicked off.'}), 403

    nr = get_next_round(conn, league_id)
    from_team, to_team = t['from_team'], t['to_team']
    out_id, in_id = t['out_player_id'], t['in_player_id']   # from_team gives out_id, receives in_id
    from_rnd = _roster_round(conn, league_id, from_team, nr)
    to_rnd = _roster_round(conn, league_id, to_team, nr)
    if out_id not in _roster_ids(conn, league_id, from_team, from_rnd) or \
       in_id not in _roster_ids(conn, league_id, to_team, to_rnd):
        cursor = _get_cursor(conn)
        cursor.execute('UPDATE trades SET status = ?, resolved_at = ? WHERE id = ?', ('rejected', now, trade_id))
        conn.commit(); cursor.close(); conn.close()
        return jsonify({'error': 'Players are no longer available — trade voided.'}), 409

    # In-place swap on each team's squad (atomic — single commit below).
    _swap_player(conn, league_id, from_team, from_rnd, out_id, in_id)
    _swap_player(conn, league_id, to_team, to_rnd, in_id, out_id)
    cursor = _get_cursor(conn)
    cursor.execute('UPDATE trades SET status = ?, resolved_at = ? WHERE id = ?', ('completed', now, trade_id))
    conn.commit(); cursor.close(); conn.close()
    return jsonify({'status': 'completed'})


@app.route('/api/trades/cancel', methods=['POST'])
def trade_cancel():
    """Proposer cancels their own pending trade offer."""
    conn = get_db()
    ensure_schema(conn)
    st = _session_team(conn)
    if not st:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    trade_id = (request.get_json() or {}).get('trade_id')
    cursor = _get_cursor(conn)
    cursor.execute('SELECT from_team, status FROM trades WHERE id = ? AND league_id = ?',
                   (trade_id, st['league_id']))
    row = cursor.fetchone()
    if not row:
        cursor.close(); conn.close()
        return jsonify({'error': 'Trade not found.'}), 404
    t = dict(row) if not isinstance(row, dict) else row
    if t['from_team'] != st['team'] or t['status'] != 'pending':
        cursor.close(); conn.close()
        return jsonify({'error': 'You can only cancel your own pending offers.'}), 403
    cursor.execute('UPDATE trades SET status = ?, resolved_at = ? WHERE id = ?',
                   ('cancelled', datetime.now(timezone.utc).isoformat(), trade_id))
    conn.commit(); cursor.close(); conn.close()
    return jsonify({'status': 'cancelled'})



# ---------------------------------------------------------------------------
# Draft engine (spec §6.2)
# ---------------------------------------------------------------------------

import json as _json

PICK_SECONDS = 60   # per-pick clock; on expiry a team is auto-drafted


def _ensure_draft_row(conn, league_id):
    cursor = _get_cursor(conn)
    cursor.execute('SELECT league_id FROM draft_state WHERE league_id = ?', (league_id,))
    if cursor.fetchone() is None:
        cursor.execute(
            "INSERT INTO draft_state (league_id, status, current_pick) VALUES (?, 'pending', 0)",
            (league_id,),
        )
        conn.commit()
    cursor.close()


def _get_draft(conn, league_id):
    """Combined draft_state + leagues draft config as a dict."""
    _ensure_draft_row(conn, league_id)
    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT ds.status, ds.current_pick, ds.started_at, ds.completed_at, ds.pick_deadline, '
        '       l.draft_at, l.draft_order, l.commissioner_user_id, l.season_start '
        'FROM draft_state ds JOIN leagues l ON l.league_id = ds.league_id '
        'WHERE ds.league_id = ?',
        (league_id,),
    )
    row = cursor.fetchone()
    cursor.close()
    d = dict(row) if not isinstance(row, dict) else row
    d['draft_order'] = _json.loads(d['draft_order']) if d.get('draft_order') else []
    return d


def _league_team_names(conn, league_id):
    """Teams eligible to draft in a league: registered user teams, falling back
    to any teams already present in team_selections (e.g. mock-seeded data)."""
    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT team_name FROM users WHERE league_id = ? AND team_name IS NOT NULL',
        (league_id,),
    )
    teams = [r['team_name'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
    if not teams:
        cursor.execute(
            'SELECT DISTINCT team_name FROM team_selections WHERE league_id = ?', (league_id,))
        teams = [r['team_name'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()]
    cursor.close()
    return sorted(t for t in teams if t)


def _drafted_player_ids(conn, league_id):
    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT player_id FROM draft_picks WHERE league_id = ? AND player_id IS NOT NULL',
        (league_id,))
    ids = {r['player_id'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()}
    cursor.close()
    return ids


def _team_owned(conn, league_id, team):
    """Players a team has drafted: list of dicts {player_id, name, position}."""
    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT dp.player_id, p.name, p.position '
        'FROM draft_picks dp JOIN players p ON p.player_id = dp.player_id '
        'WHERE dp.league_id = ? AND dp.team_name = ? ORDER BY dp.pick_number',
        (league_id, team))
    rows = [dict(r) for r in cursor.fetchall()]
    cursor.close()
    return rows


_IND_PH = ','.join(['?'] * len(INDIVIDUAL_POSITIONS))


def _available_players(conn, league_id):
    """Undrafted INDIVIDUAL players (excludes props/hookers — those belong to the
    club front-row unit). Ranked by latest total_points."""
    drafted = _drafted_player_ids(conn, league_id)
    cursor = _get_cursor(conn)
    cursor.execute(
        f'SELECT p.player_id, p.name, p.position, p.team AS real_team, '
        f'       COALESCE(MAX(ws.total_points), 0) AS rank '
        f'FROM players p LEFT JOIN weekly_stats ws ON ws.player_id = p.player_id '
        f'WHERE p.league_id = ? AND p.position IN ({_IND_PH}) '
        f'GROUP BY p.player_id, p.name, p.position, p.team ORDER BY rank DESC',
        (league_id, *INDIVIDUAL_POSITIONS))
    out = []
    for r in cursor.fetchall():
        d = dict(r)
        if d['player_id'] in drafted:
            continue
        out.append({'id': d['player_id'], 'name': d['name'], 'position': d['position'],
                    'real_team': d['real_team'], 'rank': d['rank']})
    cursor.close()
    return out


def _fr_club_ranks(conn, league_id):
    """{club: aggregate latest front-row points} — ranks club FR units for drafts."""
    cursor = _get_cursor(conn)
    cursor.execute(
        "SELECT p.team AS club, COALESCE(SUM(ws.total_points), 0) AS rank "
        "FROM players p LEFT JOIN weekly_stats ws ON ws.player_id = p.player_id "
        "  AND ws.round = (SELECT MAX(round) FROM weekly_stats w2 WHERE w2.player_id = p.player_id) "
        "WHERE p.league_id = ? AND p.position IN ('PR','HK') GROUP BY p.team",
        (league_id,))
    ranks = {(r['club'] if isinstance(r, dict) else r[0]): (r['rank'] if isinstance(r, dict) else r[1])
             for r in cursor.fetchall()}
    cursor.close()
    return ranks


def _drafted_fr_clubs(conn, league_id):
    cursor = _get_cursor(conn)
    cursor.execute('SELECT fr_club FROM draft_picks WHERE league_id = ? AND fr_club IS NOT NULL',
                   (league_id,))
    clubs = {r['fr_club'] if isinstance(r, dict) else r[0] for r in cursor.fetchall()}
    cursor.close()
    return clubs


def _available_fr_clubs(conn, league_id):
    """Club front-row units not yet drafted, ranked best-first."""
    ranks = _fr_club_ranks(conn, league_id)
    taken = _drafted_fr_clubs(conn, league_id)
    clubs = [{'club': c, 'rank': ranks[c]} for c in ranks if c and c not in taken]
    clubs.sort(key=lambda c: (-c['rank'], c['club']))
    return clubs


def _team_fr_club(conn, league_id, team):
    """The club front-row unit a team has drafted (or None), from draft_picks."""
    cursor = _get_cursor(conn)
    cursor.execute('SELECT fr_club FROM draft_picks '
                   'WHERE league_id = ? AND team_name = ? AND fr_club IS NOT NULL LIMIT 1',
                   (league_id, team))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return row['fr_club'] if isinstance(row, dict) else row[0]


def _user_context(conn):
    """(user_id, team_name, league_id, is_commissioner) for the session user."""
    user_id = session.get('user_id')
    if not user_id:
        return None
    cursor = _get_cursor(conn)
    cursor.execute('SELECT team_name, league_id FROM users WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    rd = dict(row) if not isinstance(row, dict) else row
    league_id = rd['league_id']
    meta = _get_draft(conn, league_id)
    return {
        'user_id': user_id, 'team_name': rd['team_name'], 'league_id': league_id,
        'is_commissioner': meta.get('commissioner_user_id') == user_id,
    }


def _record_pick_and_advance(conn, league_id, state, team, player_id, fr_club, is_auto):
    order = state['draft_order']
    pick_no = state['current_pick']
    now = datetime.now(timezone.utc)
    cursor = _get_cursor(conn)
    cursor.execute(
        'INSERT INTO draft_picks (league_id, pick_number, round_number, team_name, '
        ' player_id, fr_club, is_auto, picked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (league_id, pick_no, draft_engine.draft_round_of_pick(order, pick_no),
         team, player_id, fr_club, 1 if is_auto else 0, now.isoformat()))
    next_pick = pick_no + 1
    if next_pick > draft_engine.total_picks(order):
        cursor.execute(
            "UPDATE draft_state SET current_pick = ?, status = 'complete', "
            "completed_at = ?, pick_deadline = NULL WHERE league_id = ?",
            (next_pick, now.isoformat(), league_id))
        conn.commit()
        cursor.close()
        _finalize_draft(conn, league_id, order)
        return True
    deadline = (now + timedelta(seconds=PICK_SECONDS)).isoformat()
    cursor.execute(
        'UPDATE draft_state SET current_pick = ?, pick_deadline = ? WHERE league_id = ?',
        (next_pick, deadline, league_id))
    conn.commit()
    cursor.close()
    return False


def _finalize_draft(conn, league_id, order):
    """Materialise drafted squads for the first round: 14 individuals into
    team_selections + the club front-row unit into team_front_row."""
    next_round = get_next_round(conn, league_id)
    now = datetime.now(timezone.utc).isoformat()
    cursor = _get_cursor(conn)
    for team in order:
        roster = _team_owned(conn, league_id, team)   # individuals only
        for p in roster:
            p['rank'] = 0
        starters, bench = draft_engine.choose_starting_xi(roster)
        cursor.execute(
            'DELETE FROM team_selections WHERE league_id = ? AND team_name = ? AND round = ?',
            (league_id, team, next_round))
        jersey = 1
        ordered = [(p, 0) for p in starters] + [(p, 1) for p in bench]
        captain_id = roster[0]['player_id'] if roster else None
        kicker_id = next((p['player_id'] for p in roster
                          if p['position'] in ('FH', 'SH', 'OBK')), captain_id)
        for p, is_bench in ordered:
            cursor.execute(
                'INSERT INTO team_selections (round, team_name, player_id, is_captain, '
                ' is_kicker, is_bench, jersey, scraped_at, league_id) '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (next_round, team, p['player_id'],
                 1 if p['player_id'] == captain_id else 0,
                 1 if p['player_id'] == kicker_id else 0,
                 is_bench, jersey, now, league_id))
            jersey += 1
        # Front-row unit.
        club = _team_fr_club(conn, league_id, team)
        if club:
            cursor.execute('DELETE FROM team_front_row WHERE league_id = ? AND team_name = ? AND round = ?',
                           (league_id, team, next_round))
            cursor.execute('INSERT INTO team_front_row (league_id, team_name, round, club, scraped_at) '
                           'VALUES (?, ?, ?, ?, ?)', (league_id, team, next_round, club, now))
    conn.commit()
    cursor.close()


def _draft_due_for_autopick(state):
    """True if the current pick's clock has expired."""
    if state['status'] != 'live' or not state.get('pick_deadline'):
        return False
    return datetime.now(timezone.utc) >= datetime.fromisoformat(state['pick_deadline'])


@app.route('/draft')
def draft_page():
    return render_template('draft.html', current_page='draft')


@app.route('/api/draft')
def api_draft():
    conn = get_db()
    ensure_schema(conn)
    league_id = current_league_id(conn)
    state = _get_draft(conn, league_id)
    order = state['draft_order'] or _league_team_names(conn, league_id)
    ctx = _user_context(conn)

    on_clock = (draft_engine.team_on_clock(order, state['current_pick'])
                if state['status'] == 'live' else None)

    cursor = _get_cursor(conn)
    cursor.execute(
        'SELECT dp.pick_number, dp.round_number, dp.team_name, dp.is_auto, dp.fr_club, '
        '       p.name, p.position '
        'FROM draft_picks dp LEFT JOIN players p ON p.player_id = dp.player_id '
        'WHERE dp.league_id = ? ORDER BY dp.pick_number', (league_id,))
    board = []
    for r in cursor.fetchall():
        d = dict(r)
        if d.get('fr_club'):
            d['name'] = f"{d['fr_club']} FR"
            d['position'] = 'FR'
        board.append(d)
    cursor.close()

    your_team = ctx['team_name'] if ctx else None
    your_roster = _team_owned(conn, league_id, your_team) if your_team else []
    your_fr = _team_fr_club(conn, league_id, your_team) if your_team else None
    your_needs = draft_engine.unmet_starter_needs([p['position'] for p in your_roster])

    resp = {
        'league': _league_meta(conn, league_id),
        'status': state['status'],
        'draft_at': state['draft_at'],
        'order': order,
        'current_pick': state['current_pick'],
        'total_picks': draft_engine.total_picks(order) if order else 0,
        'on_clock': on_clock,
        'round': draft_engine.draft_round_of_pick(order, state['current_pick']) if order else 0,
        'pick_deadline': state['pick_deadline'],
        'auto_due': _draft_due_for_autopick(state),
        'board': board,
        'your_team': your_team,
        'your_roster': your_roster,
        'your_fr': your_fr,
        'need_fr': bool(your_team) and your_fr is None,
        'your_needs': {k: v for k, v in your_needs.items() if v},
        'is_commissioner': ctx['is_commissioner'] if ctx else False,
        'is_on_clock': bool(ctx and on_clock == ctx['team_name']),
        'available': _available_players(conn, league_id)[:200],
        'available_fr': _available_fr_clubs(conn, league_id),
        'roster_size': ROSTER_SIZE,
        'bench_count': BENCH_COUNT,
    }
    conn.close()
    return jsonify(resp)


@app.route('/api/draft/setup', methods=['POST'])
def api_draft_setup():
    conn = get_db()
    ensure_schema(conn)
    ctx = _user_context(conn)
    if not ctx:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    if not ctx['is_commissioner']:
        conn.close()
        return jsonify({'error': 'Only the commissioner can set up the draft'}), 403

    league_id = ctx['league_id']
    state = _get_draft(conn, league_id)
    if state['status'] != 'pending':
        conn.close()
        return jsonify({'error': 'Draft has already started'}), 409

    data = request.get_json() or {}
    draft_at = (data.get('draft_at') or '').strip()
    order = data.get('order') or []

    league_teams = set(_league_team_names(conn, league_id))
    if set(order) != league_teams or len(order) != len(league_teams):
        conn.close()
        return jsonify({'error': 'Draft order must list every team in the league exactly once'}), 400

    # Draft must complete before the season starts (spec §6.2).
    cursor = _get_cursor(conn)
    cursor.execute('SELECT MIN(first_kickoff) AS s FROM rounds WHERE league_id = ?', (league_id,))
    srow = cursor.fetchone()
    season_start = (srow['s'] if isinstance(srow, dict) else srow[0]) if srow else None
    if draft_at and season_start:
        try:
            if datetime.fromisoformat(draft_at) >= datetime.fromisoformat(season_start):
                cursor.close(); conn.close()
                return jsonify({'error': 'Draft must be scheduled before the season starts'}), 400
        except ValueError:
            cursor.close(); conn.close()
            return jsonify({'error': 'Invalid draft date'}), 400

    cursor.execute(
        'UPDATE leagues SET draft_at = ?, draft_order = ?, season_start = ? WHERE league_id = ?',
        (draft_at or None, _json.dumps(order), season_start, league_id))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'status': 'ok', 'order': order, 'draft_at': draft_at})


@app.route('/api/draft/start', methods=['POST'])
def api_draft_start():
    conn = get_db()
    ensure_schema(conn)
    ctx = _user_context(conn)
    if not ctx:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    if not ctx['is_commissioner']:
        conn.close()
        return jsonify({'error': 'Only the commissioner can start the draft'}), 403

    league_id = ctx['league_id']
    state = _get_draft(conn, league_id)
    if state['status'] == 'complete':
        conn.close()
        return jsonify({'error': 'Draft already complete'}), 409
    if not state['draft_order']:
        conn.close()
        return jsonify({'error': 'Set the draft order first'}), 400

    now = datetime.now(timezone.utc)
    deadline = (now + timedelta(seconds=PICK_SECONDS)).isoformat()
    cursor = _get_cursor(conn)
    cursor.execute(
        "UPDATE draft_state SET status = 'live', current_pick = 1, started_at = ?, "
        "pick_deadline = ? WHERE league_id = ?",
        (now.isoformat(), deadline, league_id))
    conn.commit()
    cursor.close()
    conn.close()
    return jsonify({'status': 'live'})


def _make_pick(conn, league_id, player_id, fr_club, by_user_team, is_auto):
    """Shared pick logic for an individual player OR a club front-row unit.
    Returns (status_code, payload)."""
    state = _get_draft(conn, league_id)
    if state['status'] != 'live':
        return 409, {'error': 'Draft is not live'}
    order = state['draft_order']
    team = draft_engine.team_on_clock(order, state['current_pick'])
    if team is None:
        return 409, {'error': 'Draft is over'}
    if by_user_team is not None and by_user_team != team:
        return 403, {'error': f"It is {team}'s pick, not yours"}

    available = _available_players(conn, league_id)
    fr_clubs = _available_fr_clubs(conn, league_id)
    has_fr = _team_fr_club(conn, league_id, team) is not None
    owned = [p['position'] for p in _team_owned(conn, league_id, team)]
    label = None

    if is_auto or (player_id is None and not fr_club):
        pick = draft_engine.auto_pick(available, fr_clubs, owned, has_fr)
        if not pick:
            return 409, {'error': 'No players available'}
        if pick['type'] == 'fr':
            fr_club = pick['club']; player_id = None; label = f'{fr_club} FR'
        else:
            player_id = pick['player']['id']; label = pick['player']['name']
    elif fr_club:
        if has_fr:
            return 409, {'error': 'You already own a front-row unit'}
        if fr_club not in {c['club'] for c in fr_clubs}:
            return 409, {'error': 'That front row is unavailable or already drafted'}
        player_id = None; label = f'{fr_club} FR'
    else:
        p = next((x for x in available if x['id'] == player_id), None)
        if not p:
            return 409, {'error': 'Player is unavailable or already drafted'}
        label = p['name']

    done = _record_pick_and_advance(conn, league_id, state, team, player_id, fr_club, is_auto)
    return 200, {'status': 'picked', 'team': team, 'player_id': player_id,
                 'fr_club': fr_club, 'player': label, 'complete': done}


@app.route('/api/draft/pick', methods=['POST'])
def api_draft_pick():
    conn = get_db()
    ensure_schema(conn)
    ctx = _user_context(conn)
    if not ctx:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    data = request.get_json() or {}
    player_id = data.get('player_id')
    fr_club = (data.get('fr_club') or '').strip() or None
    code, payload = _make_pick(conn, ctx['league_id'], player_id, fr_club, ctx['team_name'], is_auto=False)
    conn.close()
    return jsonify(payload), code


@app.route('/api/draft/autopick', methods=['POST'])
def api_draft_autopick():
    """Auto-draft the team on the clock. Allowed when the pick clock has
    expired (any participant may trigger it), or by the commissioner / the
    on-clock user at will (spec §6.2 absent-user auto-draft)."""
    conn = get_db()
    ensure_schema(conn)
    ctx = _user_context(conn)
    if not ctx:
        conn.close()
        return jsonify({'error': 'Not logged in'}), 401
    league_id = ctx['league_id']
    state = _get_draft(conn, league_id)
    on_clock = draft_engine.team_on_clock(state['draft_order'], state['current_pick'])
    allowed = (_draft_due_for_autopick(state) or ctx['is_commissioner']
               or ctx['team_name'] == on_clock)
    if not allowed:
        conn.close()
        return jsonify({'error': 'Pick clock has not expired'}), 403
    code, payload = _make_pick(conn, league_id, None, None, None, is_auto=True)
    conn.close()
    return jsonify(payload), code


# ---------------------------------------------------------------------------
# Competition routes
# ---------------------------------------------------------------------------

@app.route('/competition')
def competition():
    return render_template('competition.html', current_page='competition')


@app.route('/fixtures')
def fixtures():
    return render_template('fixtures.html', current_page='fixtures')


@app.route('/finals')
def finals():
    return render_template('finals.html', current_page='finals')


@app.route('/api/competition')
def competition_data():
    conn      = get_db()
    ensure_schema(conn)
    league_id = current_league_id(conn)
    max_round = get_last_round(conn, league_id)

    # Dynamic schedule: generate the regular season from the live team list,
    # then seed playoffs (Championship top-4 + Sacko bottom-4) off the standings.
    teams    = get_league_teams(conn, league_id)
    regular  = generate_regular_fixtures(teams)
    table    = calculate_table(regular, conn, min(max_round, REGULAR_ROUNDS))
    playoffs = build_playoffs(conn, table, max_round)

    # Regular fixtures + resolved playoff pairings drive the per-week results
    # (fixtures page + weekly chart). Playoff weeks simply have no byes.
    fixtures = regular + playoff_fixtures(playoffs)

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

    # Per-round standings for movement arrows + the historical-position line
    # chart (spec §7) — computed before the connection is closed.
    position_history = standings_progression(regular, conn, min(max_round, REGULAR_ROUNDS))

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
        'playoffs': playoffs,
        'position_history': position_history,
        'regular_rounds': REGULAR_ROUNDS,
    })


# ---------------------------------------------------------------------------
# Scheduler + ingestion (spec §3, §4)
# ---------------------------------------------------------------------------

CRON_SECRET = os.getenv('CRON_SECRET', '')


def _cron_auth_ok() -> bool:
    """Accept requests from the cron runner. Skip the check when no secret is
    configured (local dev)."""
    if not CRON_SECRET:
        return True
    return request.headers.get('Authorization') == f'Bearer {CRON_SECRET}'


def _last_run(conn, league_id, job):
    cursor = _get_cursor(conn)
    cursor.execute(
        "SELECT MAX(run_at) AS m FROM job_runs "
        "WHERE league_id = ? AND job = ? AND status = 'ok'",
        (league_id, job))
    row = cursor.fetchone()
    cursor.close()
    if not row:
        return None
    return row['m'] if isinstance(row, dict) else row[0]


def _finalize_done(conn, league_id, round_number):
    cursor = _get_cursor(conn)
    cursor.execute(
        "SELECT 1 FROM job_runs WHERE league_id = ? AND job = 'finalize' "
        "AND round_number = ? AND status = 'ok' LIMIT 1",
        (league_id, round_number))
    done = cursor.fetchone() is not None
    cursor.close()
    return done


def _rounds_known(conn, league_id):
    cursor = _get_cursor(conn)
    cursor.execute('SELECT 1 FROM rounds WHERE league_id = ? LIMIT 1', (league_id,))
    known = cursor.fetchone() is not None
    cursor.close()
    return known


def _log_run(conn, league_id, job, round_number, status, detail):
    cursor = _get_cursor(conn)
    cursor.execute(
        'INSERT INTO job_runs (league_id, job, round_number, status, detail, run_at) '
        'VALUES (?, ?, ?, ?, ?, ?)',
        (league_id, job, round_number, status, str(detail)[:300],
         datetime.now(timezone.utc).isoformat()))
    conn.commit()
    cursor.close()


def _carry_forward_picks(conn, league_id, round_num):
    """Copy a league's previous-round squads forward as defaults so teams that
    haven't edited still field a side."""
    if round_num <= 1:
        return
    cursor = _get_cursor(conn)
    cursor.execute('''
        INSERT INTO team_selections
            (round, team_name, player_id, is_captain, is_kicker, is_bench, jersey, scraped_at, league_id)
        SELECT ?, team_name, player_id, is_captain, is_kicker, is_bench, jersey, ?, league_id
        FROM team_selections
        WHERE round = ? AND league_id = ?
        ON CONFLICT (round, team_name, player_id) DO NOTHING
    ''', (round_num, datetime.now(timezone.utc).isoformat(), round_num - 1, league_id))
    conn.commit()
    cursor.close()


def _run_job(conn, league_id, competition, job, active_round):
    """Execute one ingestion job, returning a short detail string."""
    if job == 'sync_rounds':
        return active_round, f'{ingest.ingest_rounds(conn, league_id, competition)} rounds'
    if job == 'lineups':
        return active_round, f'{ingest.ingest_lineups(conn, league_id, competition, active_round)} entries'
    if job == 'live_scoring':
        n = ingest.ingest_player_scores(conn, league_id, competition, active_round)
        _carry_forward_picks(conn, league_id, active_round)
        return active_round, f'{n} players'
    if job == 'finalize':
        # The just-finished gameweek is the round before the upcoming one.
        fin_round = max(1, active_round - 1)
        n = ingest.ingest_player_scores(conn, league_id, competition, fin_round, finalize=True)
        return fin_round, f'{n} players (final)'
    return active_round, 'noop'


@app.route('/api/cron/tick')
def cron_tick():
    """Single timezone-aware scheduler service (spec §3). On each call it
    decides — per league, in that league's local zone — which ingestion jobs
    are due and runs them. Idempotent; every run is logged to job_runs."""
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401

    conn = get_db()
    ensure_schema(conn)
    now = datetime.now(timezone.utc)
    summary = []

    for slug, cfg in LEAGUES.items():
        league_id = _league_id_by_slug(conn, slug)
        competition = cfg['competition']
        active_round = get_next_round(conn, league_id)

        # Live detection (§4.2): any match in this round within its game window.
        try:
            kickoffs = ingest.round_kickoffs(competition, active_round)
        except Exception:
            kickoffs = []
        live_now = scheduler.match_is_live(now, kickoffs)

        last_runs = {j: _last_run(conn, league_id, j)
                     for j in ('sync_rounds', 'lineups', 'live_scoring')}
        fin_round = max(1, active_round - 1)
        due = scheduler.due_jobs(
            competition, now, cfg['timezone'], last_runs, live_now,
            finalize_done=_finalize_done(conn, league_id, fin_round),
            rounds_known=_rounds_known(conn, league_id),
        )

        ran = []
        for job in due:
            try:
                rnd, detail = _run_job(conn, league_id, competition, job, active_round)
                _log_run(conn, league_id, job, rnd, 'ok', detail)
                ran.append({'job': job, 'round': rnd, 'detail': detail})
            except Exception as e:
                _log_run(conn, league_id, job, active_round, 'error', str(e))
                ran.append({'job': job, 'error': str(e)})

        summary.append({
            'league': slug, 'tz': cfg['timezone'], 'active_round': active_round,
            'live': live_now, 'due': due, 'ran': ran,
        })

    conn.close()
    return jsonify({'now': now.isoformat(), 'leagues': summary})


# Backward-compatible per-job endpoints (single league = default/OFDS), now
# routed through the adapter-based, league-aware ingestion functions so they
# set league_id correctly. Prefer /api/cron/tick going forward.

def _default_league(conn):
    return _league_id_by_slug(conn, DEFAULT_LEAGUE)


@app.route('/api/cron/sync-rounds')
def cron_sync_rounds():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_db()
    ensure_schema(conn)
    league_id = _default_league(conn)
    try:
        n = ingest.ingest_rounds(conn, league_id, LEAGUES[DEFAULT_LEAGUE]['competition'])
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e)}), 500
    _log_run(conn, league_id, 'sync_rounds', None, 'ok', f'{n} rounds')
    conn.close()
    return jsonify({'status': 'ok', 'rounds_synced': n})


@app.route('/api/cron/lineups')
def cron_lineups():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_db()
    ensure_schema(conn)
    league_id = _default_league(conn)
    round_num = get_next_round(conn, league_id)
    try:
        n = ingest.ingest_lineups(conn, league_id, LEAGUES[DEFAULT_LEAGUE]['competition'], round_num)
    except Exception as e:
        conn.close()
        return jsonify({'error': str(e), 'round': round_num}), 500
    _log_run(conn, league_id, 'lineups', round_num, 'ok', f'{n} entries')
    conn.close()
    return jsonify({'status': 'ok', 'round': round_num, 'entries_written': n})


@app.route('/api/cron/player-data')
def cron_player_data():
    if not _cron_auth_ok():
        return jsonify({'error': 'Unauthorized'}), 401
    conn = get_db()
    ensure_schema(conn)
    league_id = _default_league(conn)
    round_num = _round_after_last_scraped(conn, league_id)
    try:
        n = ingest.ingest_player_scores(conn, league_id,
                                        LEAGUES[DEFAULT_LEAGUE]['competition'], round_num)
    except Exception as e:
        conn.close()
        return jsonify({'error': f'Scrape failed: {e}'}), 500
    _carry_forward_picks(conn, league_id, round_num)
    _log_run(conn, league_id, 'live_scoring', round_num, 'ok', f'{n} players')
    conn.close()
    return jsonify({'status': 'ok', 'round': round_num, 'players_upserted': n})


# ---------------------------------------------------------------------------

if __name__ == '__main__':
    app.run(debug=True, port=5000)
