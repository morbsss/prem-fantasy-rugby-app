"""Authentication helper functions for user management."""

from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime


def hash_password(password: str) -> str:
    """Hash a password using PBKDF2."""
    return generate_password_hash(password, method='pbkdf2:sha256')


def verify_password(password: str, hash_str: str) -> bool:
    """Verify a password against its hash."""
    return check_password_hash(hash_str, password)


def create_user(conn, username: str, password: str, team_name: str) -> dict:
    """Create a new user account. Returns user data or error dict."""
    cursor = conn.cursor()

    # Check if username already exists
    if conn.cursor_factory.__name__ == 'RealDictCursor':  # PostgreSQL
        cursor.execute('SELECT user_id FROM users WHERE username = %s', (username,))
    else:  # SQLite
        cursor.execute('SELECT user_id FROM users WHERE username = ?', (username,))

    if cursor.fetchone():
        cursor.close()
        return {'error': 'Username already taken'}

    # Check if team is already claimed
    if conn.cursor_factory.__name__ == 'RealDictCursor':  # PostgreSQL
        cursor.execute('SELECT user_id FROM users WHERE team_name = %s', (team_name,))
    else:  # SQLite
        cursor.execute('SELECT user_id FROM users WHERE team_name = ?', (team_name,))

    if cursor.fetchone():
        cursor.close()
        return {'error': 'Team already claimed by another user'}

    # Create user
    password_hash = hash_password(password)
    created_at = datetime.utcnow().isoformat()

    try:
        if conn.cursor_factory.__name__ == 'RealDictCursor':  # PostgreSQL
            cursor.execute('''
                INSERT INTO users (username, password_hash, team_name, created_at)
                VALUES (%s, %s, %s, %s)
            ''', (username, password_hash, team_name, created_at))
        else:  # SQLite
            cursor.execute('''
                INSERT INTO users (username, password_hash, team_name, created_at)
                VALUES (?, ?, ?, ?)
            ''', (username, password_hash, team_name, created_at))

        conn.commit()

        # Fetch the created user
        if conn.cursor_factory.__name__ == 'RealDictCursor':  # PostgreSQL
            cursor.execute('SELECT user_id, username, team_name FROM users WHERE username = %s', (username,))
        else:  # SQLite
            cursor.execute('SELECT user_id, username, team_name FROM users WHERE username = ?', (username,))

        user = cursor.fetchone()
        cursor.close()

        return {
            'user_id': user['user_id'] if isinstance(user, dict) else user[0],
            'username': user['username'] if isinstance(user, dict) else user[1],
            'team_name': user['team_name'] if isinstance(user, dict) else user[2],
        }
    except Exception as e:
        cursor.close()
        conn.rollback()
        return {'error': str(e)}


def authenticate_user(conn, username: str, password: str) -> dict:
    """Authenticate user and return user data or error."""
    cursor = conn.cursor()

    if conn.cursor_factory.__name__ == 'RealDictCursor':  # PostgreSQL
        cursor.execute(
            'SELECT user_id, username, password_hash, team_name FROM users WHERE username = %s',
            (username,)
        )
    else:  # SQLite
        cursor.execute(
            'SELECT user_id, username, password_hash, team_name FROM users WHERE username = ?',
            (username,)
        )

    user = cursor.fetchone()
    cursor.close()

    if not user:
        return {'error': 'Invalid username or password'}

    # Extract values based on row type
    if isinstance(user, dict):
        user_id = user['user_id']
        user_password_hash = user['password_hash']
        team_name = user['team_name']
        username_val = user['username']
    else:
        user_id = user[0]
        username_val = user[1]
        user_password_hash = user[2]
        team_name = user[3]

    if not verify_password(password, user_password_hash):
        return {'error': 'Invalid username or password'}

    return {
        'user_id': user_id,
        'username': username_val,
        'team_name': team_name,
    }


def get_available_teams(conn) -> list:
    """Get list of available teams (not claimed by any user)."""
    cursor = conn.cursor()

    # Get all teams from team_selections that don't have a user
    if conn.cursor_factory.__name__ == 'RealDictCursor':  # PostgreSQL
        cursor.execute('''
            SELECT DISTINCT ts.team_name, u.username
            FROM team_selections ts
            LEFT JOIN users u ON u.team_name = ts.team_name
            ORDER BY ts.team_name
        ''')
    else:  # SQLite
        cursor.execute('''
            SELECT DISTINCT ts.team_name, u.username
            FROM team_selections ts
            LEFT JOIN users u ON u.team_name = ts.team_name
            ORDER BY ts.team_name
        ''')

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
