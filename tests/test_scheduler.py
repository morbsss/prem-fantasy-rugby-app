"""Timezone/DST-aware scheduler decisions (spec §3, §4.5)."""

from datetime import datetime, timezone, timedelta

from api import scheduler as s

LON = 'Europe/London'
SYD = 'Australia/Sydney'


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def _local(dt, tz):
    return s.to_local(dt, tz)


# ---------------------------------------------------------------------------
# Lineup windows
# ---------------------------------------------------------------------------

def test_premiership_lineup_window_saturday():
    assert s.in_lineup_window(_local(_utc(2026, 7, 11, 14), LON), 'premiership')   # Sat


def test_super_rugby_wednesday_in_thursday_out():
    # Wed 15:00 AEDT == Wed 04:00 UTC (Feb, +11)
    assert s.in_lineup_window(_local(_utc(2026, 2, 18, 4), SYD), 'super_rugby')
    # Thursday is not part of the Super Rugby window
    assert not s.in_lineup_window(_local(_utc(2026, 2, 19, 4), SYD), 'super_rugby')


def test_dst_shifts_the_sunday_cutoff():
    # 17:30 UTC: in BST (summer) that's 18:30 → past the Sun 18:00 cutoff (out);
    # in GMT (winter) that's 17:30 → still inside the window (in).
    summer = _local(_utc(2026, 7, 12, 17, 30), LON)   # Sunday, BST
    winter = _local(_utc(2026, 1, 11, 17, 30), LON)   # Sunday, GMT
    assert s.in_lineup_window(summer, 'premiership') is False
    assert s.in_lineup_window(winter, 'premiership') is True


# ---------------------------------------------------------------------------
# Finalize (Monday 12:00 local) — DST-aware
# ---------------------------------------------------------------------------

def test_finalize_only_monday_noon_local():
    assert s.is_finalize_time(_local(_utc(2026, 7, 13, 11), LON))      # Mon 12:00 BST
    assert not s.is_finalize_time(_local(_utc(2026, 7, 13, 10), LON))  # Mon 11:00 BST
    assert not s.is_finalize_time(_local(_utc(2026, 7, 12, 11), LON))  # Sunday


# ---------------------------------------------------------------------------
# Live-match detection
# ---------------------------------------------------------------------------

def test_match_is_live_within_window():
    ko = _utc(2026, 3, 7, 15).isoformat()
    assert s.match_is_live(_utc(2026, 3, 7, 16), [ko])              # 1h in
    assert not s.match_is_live(_utc(2026, 3, 7, 18), [ko])          # >2h after
    assert not s.match_is_live(_utc(2026, 3, 7, 14, 30), [ko])      # before kickoff


# ---------------------------------------------------------------------------
# due_jobs: windows + cadence + live + finalize together
# ---------------------------------------------------------------------------

def _due(comp, now, tz, last=None, live=False, fin=False, known=True):
    last_runs = {'sync_rounds': '2099-01-01T00:00:00+00:00', 'lineups': last, 'live_scoring': last}
    return s.due_jobs(comp, now, tz, last_runs, live, fin, known)


def test_due_jobs_lineups_in_window():
    assert 'lineups' in _due('premiership', _utc(2026, 7, 11, 14), LON)


def test_due_jobs_cadence_suppresses_recent_lineups():
    recent = _utc(2026, 7, 11, 13, 30).isoformat()   # 30 min ago (< 2h interval)
    assert 'lineups' not in _due('premiership', _utc(2026, 7, 11, 14), LON, last=recent)


def test_due_jobs_live_scoring_only_when_live():
    assert 'live_scoring' not in _due('premiership', _utc(2026, 7, 11, 14), LON, live=False)
    assert 'live_scoring' in _due('premiership', _utc(2026, 7, 11, 14), LON, live=True)


def test_due_jobs_bootstraps_sync_rounds_when_unknown():
    # Even with a recent sync timestamp, an unknown calendar forces sync_rounds.
    last_runs = {'sync_rounds': datetime.now(timezone.utc).isoformat(),
                 'lineups': None, 'live_scoring': None}
    due = s.due_jobs('premiership', _utc(2026, 7, 7, 9), LON, last_runs,
                     live_now=False, finalize_done=False, rounds_known=False)
    assert 'sync_rounds' in due


def test_due_jobs_finalize_once_per_gameweek():
    now = _utc(2026, 7, 13, 11)   # Mon 12:00 BST
    assert 'finalize' in _due('premiership', now, LON, fin=False)
    assert 'finalize' not in _due('premiership', now, LON, fin=True)
