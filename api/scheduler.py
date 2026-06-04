"""
Timezone-aware ingestion scheduler (spec §3 + §4.5) — pure decisions, no DB.

A single scheduler service (the /api/cron/tick endpoint) calls `due_jobs` per
league on each run to decide which ingestion jobs should fire *now*, evaluated
in the league's local IANA timezone so AEST↔AEDT and GMT↔BST switch
automatically. Cadence is enforced by minimum-interval-since-last-run (passed in
by the caller from the job_runs log), which keeps jobs idempotent regardless of
how precisely the platform cron fires.

Job windows (local time, spec §4.3/§4.4):

  Premiership (Europe/London)
    lineups : Thu 14:00 → Sun 18:00, every 2h
    finalize: Mon 12:00 (once per gameweek)

  Super Rugby Pacific (Australia/Sydney)
    lineups : Wed 15:00 (pre-round) + Fri 15:00 → Sun 17:00, every 2h
    finalize: Mon 12:00 (once per gameweek)

  Both
    live_scoring: every 3 min while any match is live (now within a fixture's
                  game window)
    sync_rounds : refresh the fixture calendar, daily
"""

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# Cadence floors (a job won't run again until this long after its last run).
INTERVALS = {
    'sync_rounds':  timedelta(hours=24),
    'lineups':      timedelta(hours=2),
    'live_scoring': timedelta(minutes=3),
    # finalize is once-per-round, gated by the job_runs log, not an interval.
}

# How long after kickoff a match counts as "live" for §4.4 live scraping.
MATCH_WINDOW = timedelta(hours=2)

# weekday(): Mon=0 .. Sun=6
_MON, _TUE, _WED, _THU, _FRI, _SAT, _SUN = range(7)


def to_local(now_utc: datetime, tz_name: str) -> datetime:
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    return now_utc.astimezone(ZoneInfo(tz_name))


def in_lineup_window(local: datetime, competition: str) -> bool:
    """Whether local time falls in the competition's lineup-scrape window."""
    wd, hr = local.weekday(), local.hour
    if competition == 'premiership':
        if wd == _THU:
            return hr >= 14
        if wd in (_FRI, _SAT):
            return True
        if wd == _SUN:
            return hr < 18
        return False
    if competition == 'super_rugby':
        if wd == _WED:
            return hr >= 15
        if wd == _FRI:
            return hr >= 15
        if wd == _SAT:
            return True
        if wd == _SUN:
            return hr < 17
        return False
    return False


def is_finalize_time(local: datetime) -> bool:
    """Monday 12:00+ in local time (spec §4.4 definitive scrape)."""
    return local.weekday() == _MON and local.hour >= 12


def match_is_live(now_utc: datetime, kickoffs_iso: list[str]) -> bool:
    """True if `now` is within MATCH_WINDOW of any kickoff (spec §4.2)."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    for ko in kickoffs_iso:
        try:
            start = datetime.fromisoformat(ko.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if start <= now_utc <= start + MATCH_WINDOW:
            return True
    return False


def _interval_ok(last_run_iso, now_utc: datetime, interval: timedelta) -> bool:
    if not last_run_iso:
        return True
    last = datetime.fromisoformat(last_run_iso)
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    return (now_utc - last) >= interval


def due_jobs(
    competition: str,
    now_utc: datetime,
    tz_name: str,
    last_runs: dict[str, str | None],
    live_now: bool,
    finalize_done: bool,
    rounds_known: bool,
) -> list[str]:
    """Ordered list of jobs to run now for one league.

    `last_runs`     : {job: last_run_iso or None} from the job_runs log.
    `live_now`      : caller-computed (a match is currently live).
    `finalize_done` : finalize already recorded for the target gameweek.
    `rounds_known`  : the rounds calendar exists for this league.
    """
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    local = to_local(now_utc, tz_name)
    due: list[str] = []

    # Keep the fixture calendar fresh (and bootstrap it if missing).
    if not rounds_known or _interval_ok(last_runs.get('sync_rounds'), now_utc, INTERVALS['sync_rounds']):
        due.append('sync_rounds')

    if in_lineup_window(local, competition) and \
            _interval_ok(last_runs.get('lineups'), now_utc, INTERVALS['lineups']):
        due.append('lineups')

    if live_now and _interval_ok(last_runs.get('live_scoring'), now_utc, INTERVALS['live_scoring']):
        due.append('live_scoring')

    if is_finalize_time(local) and not finalize_done:
        due.append('finalize')

    return due
