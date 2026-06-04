"""
Live data-source adapter (spec §3, §8.6 — best-effort).

Wraps the existing ESPN/SuperBru scraping behind the four interfaces. ESPN
publishes no official rugby API and SuperBru must be scraped, so the live
adapter is best-effort: the mock adapter is the contract of record and the app
must keep working when these endpoints can't be reached.

Per-competition source identifiers come from the league registry
(api/leagues.py). Only the Premiership has confirmed endpoints today; Super
Rugby falls back to empty results until its endpoints are wired.
"""

from ..leagues import LEAGUES
from .base import (
    PlayerSource, FixtureSource, LineupSource, ScoreSource,
    PlayerRecord, RoundRecord, MatchRecord, LineupEntry, ScoreRecord,
)

_SUPERBRU_URL = 'https://www.superbru.com/premiershiprugbyfantasy/ajax/f_write_player_stats.php?'
_SUPERBRU_POS = {1: 'PR', 2: 'HK', 3: 'LK', 4: 'LF', 5: 'SH', 6: 'FH', 7: 'MID', 8: 'OBK'}
_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
        '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    ),
}


def _config(competition: str) -> dict:
    for cfg in LEAGUES.values():
        if cfg['competition'] == competition:
            return cfg
    raise KeyError(f'No league configured for competition {competition!r}')


def _to_float(val) -> float:
    try:
        return float(str(val).replace('£', '').replace('m', '').strip())
    except (ValueError, TypeError):
        return 0.0


class LiveAdapter(PlayerSource, FixtureSource, LineupSource, ScoreSource):

    # --- §4.2 fixtures (ESPN) --------------------------------------------
    def fetch_rounds(self, competition: str) -> list[RoundRecord]:
        cfg = _config(competition)
        from ..sync_rounds import fetch_rounds as espn_fetch_rounds
        # sync_rounds is currently Premiership-pinned; future work parametrises
        # it by cfg['espn_league_id']. Treat failures as "no data".
        if cfg['competition'] != 'premiership':
            return []
        out: list[RoundRecord] = []
        for round_num, first_ko, last_ko, matches in espn_fetch_rounds():
            out.append(RoundRecord(
                round_number=round_num, first_kickoff=first_ko, last_kickoff=last_ko,
                matches=[MatchRecord(home=m['home'], away=m['away'],
                                     kickoff=m['kickoff']) for m in matches],
            ))
        return out

    # --- §4.3 lineups (ESPN) ---------------------------------------------
    def fetch_lineups(self, competition: str, round_number: int) -> list[LineupEntry]:
        cfg = _config(competition)
        if cfg['competition'] != 'premiership':
            return []
        from ..real_lineups import (
            fetch_json, get_round_events, extract_lineups, format_name,
        )
        events = get_round_events(round_number)
        entries: list[LineupEntry] = []
        for event in events:
            game_id = event['id']
            summary_url = (
                f'https://site.api.espn.com/apis/site/v2/sports/rugby'
                f'/{cfg["espn_league_id"]}/summary?event={game_id}'
            )
            try:
                teams = extract_lineups(fetch_json(summary_url))
            except Exception:
                continue
            for team in teams:
                for p in team['players']:
                    if not p['name']:
                        continue
                    entries.append(LineupEntry(
                        player_name=format_name(p['name']), real_team=team['name'],
                        jersey=p['jersey'], status='B' if p['is_bench'] else 'S',
                    ))
        return entries

    # --- §4.1 players / §4.4 scores (SuperBru) ---------------------------
    def _scrape_superbru(self, competition: str) -> list[dict]:
        cfg = _config(competition)
        table = cfg.get('superbru_table')
        if not table:
            return []
        import requests
        from bs4 import BeautifulSoup
        rows = []
        for page in range(1, 9):
            resp = requests.get(
                f'{_SUPERBRU_URL}pg={page}&tbl={table}',
                headers=_HEADERS, timeout=10, verify=False,
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')
            tbl = soup.find('tbody')
            if not tbl:
                continue
            for row in tbl.find_all('tr'):
                cells = [td.get_text(strip=True) for td in row.find_all('td')]
                if len(cells) < 8:
                    cells.insert(5, '0')
                rows.append({
                    'team': cells[0],
                    'name': cells[1][:-1] if cells[1] else '',
                    'position': _SUPERBRU_POS[page],
                    'total_points': _to_float(cells[3]),
                    'price': _to_float(cells[4]) * 1_000_000,
                    'kicking': _to_float(cells[5]),
                    'points_per_game': cells[6],
                    'popularity': cells[7],
                    'form': cells[8] if len(cells) > 8 else '',
                })
        return rows

    def fetch_players(self, competition: str) -> list[PlayerRecord]:
        return [
            PlayerRecord(name=r['name'], team=r['team'],
                         position=r['position'], price=r['price'])
            for r in self._scrape_superbru(competition) if r['name']
        ]

    def fetch_player_scores(self, competition: str, round_number: int) -> list[ScoreRecord]:
        # SuperBru exposes season totals (not per-round), matching the existing
        # cron behaviour; round_number is accepted for interface parity.
        return [
            ScoreRecord(
                name=r['name'], team=r['team'], position=r['position'],
                total_points=r['total_points'], price=r['price'], kicking=r['kicking'],
                points_per_game=r['points_per_game'], popularity=r['popularity'],
                form=r['form'],
            )
            for r in self._scrape_superbru(competition) if r['name']
        ]
