"""Authentication helper functions for user management."""

from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime
from .db import DB_TYPE


def _is_postgres(conn) -> bool:
    return DB_TYPE == 'postgres'


def hash_password(password: str) -> str:
    """Hash a password using PBKDF2."""
    return generate_password_hash(password, method='pbkdf2:sha256')


def verify_password(password: str, hash_str: str) -> bool:
    """Verify a password against its hash."""
    return check_password_hash(hash_str, password)


def create_user(conn, email: str, password: str, team_name: str, league_id) -> dict:
    """Create a new user account (email + password, joined to one league).

    `username` is set to the email for backward compatibility with the legacy
    session/display code, which still reads session['username'].
    """
    ph = '%s' if _is_postgres(conn) else '?'
    cursor = conn.cursor()

    # Email already registered? (username mirrors email, so this also covers it.)
    cursor.execute(f'SELECT user_id FROM users WHERE email = {ph} OR username = {ph}', (email, email))
    if cursor.fetchone():
        cursor.close()
        return {'error': 'An account with that email already exists'}

    # Team name already taken?
    cursor.execute(f'SELECT user_id FROM users WHERE team_name = {ph}', (team_name,))
    if cursor.fetchone():
        cursor.close()
        return {'error': 'That team name is already taken'}

    password_hash = hash_password(password)
    created_at = datetime.utcnow().isoformat()

    try:
        cursor.execute(
            f'INSERT INTO users (username, email, password_hash, team_name, league_id, created_at) '
            f'VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph})',
            (email, email, password_hash, team_name, league_id, created_at),
        )
        conn.commit()

        cursor.execute(
            f'SELECT user_id, username, email, team_name, league_id FROM users WHERE email = {ph}',
            (email,),
        )
        user = cursor.fetchone()
        cursor.close()
        u = user if isinstance(user, dict) else {
            'user_id': user[0], 'username': user[1], 'email': user[2],
            'team_name': user[3], 'league_id': user[4],
        }
        return {
            'user_id': u['user_id'], 'username': u['username'], 'email': u['email'],
            'team_name': u['team_name'], 'league_id': u['league_id'],
        }
    except Exception as e:
        cursor.close()
        conn.rollback()
        return {'error': str(e)}


def authenticate_user(conn, identifier: str, password: str) -> dict:
    """Authenticate by email (or legacy username) + password."""
    ph = '%s' if _is_postgres(conn) else '?'
    cursor = conn.cursor()
    cursor.execute(
        f'SELECT user_id, username, password_hash, team_name, league_id '
        f'FROM users WHERE LOWER(email) = LOWER({ph}) OR LOWER(username) = LOWER({ph})',
        (identifier, identifier),
    )
    user = cursor.fetchone()
    cursor.close()

    if not user:
        return {'error': 'Invalid email or password'}

    if isinstance(user, dict):
        user_id = user['user_id']
        user_password_hash = user['password_hash']
        team_name = user['team_name']
        username_val = user['username']
        league_id = user['league_id']
    else:
        user_id, username_val, user_password_hash, team_name, league_id = user

    if not verify_password(password, user_password_hash):
        return {'error': 'Invalid email or password'}

    return {
        'user_id': user_id,
        'username': username_val,
        'team_name': team_name,
        'league_id': league_id,
    }


def get_available_teams(conn, league_id=None) -> list:
    """Get list of teams (optionally scoped to a league) and whether claimed."""
    ph = '%s' if _is_postgres(conn) else '?'
    cursor = conn.cursor()

    if league_id is None:
        cursor.execute('''
            SELECT DISTINCT ts.team_name, u.username
            FROM team_selections ts
            LEFT JOIN users u ON u.team_name = ts.team_name
            ORDER BY ts.team_name
        ''')
    else:
        cursor.execute(f'''
            SELECT DISTINCT ts.team_name, u.username
            FROM team_selections ts
            LEFT JOIN users u ON u.team_name = ts.team_name
            WHERE ts.league_id = {ph}
            ORDER BY ts.team_name
        ''', (league_id,))

    teams = []
    for row in cursor.fetchall():
        if isinstance(row, dict):
            team_name = row['team_name']
            owner = row['username']
        else:
            team_name = row[0]
            owner = row[1]

        teams.append({
            'name': team_name,
            'owner': owner,
            'available': owner is None,
        })

    cursor.close()
    return teams
