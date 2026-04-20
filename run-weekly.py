# """
# Weekly automation script — run every Tuesday.

# Order:
#   1. player-data.py  — scrapes new round player stats into weekly_stats
#   2. my-team.py      — scrapes team selections for the new round
#   3. competition.py  — prints the updated league table
# """

# import subprocess
# import sys
# from pathlib import Path

# SCRIPTS = [
#     'player-data.py',
#     'my-team.py',
#     'competition.py',
# ]

# DIR = Path(__file__).parent


# def run(script: str) -> bool:
#     print(f'\n{"="*50}')
#     print(f'  Running {script}')
#     print(f'{"="*50}')
#     result = subprocess.run(
#         [sys.executable, DIR / script],
#         cwd=DIR,
#     )
#     if result.returncode != 0:
#         print(f'\nERROR: {script} exited with code {result.returncode}', file=sys.stderr)
#         return False
#     return True


# if __name__ == '__main__':
#     for script in SCRIPTS:
#         if not run(script):
#             print('Stopping — fix the error above before continuing.', file=sys.stderr)
#             sys.exit(1)
#     print('\nWeekly update complete.')
