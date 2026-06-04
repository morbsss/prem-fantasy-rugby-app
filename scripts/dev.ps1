# Local dev / test helper for Meatyboys Rugby Fantasy.
# Run from the project root:
#
#   .\scripts\dev.ps1                 # start the Flask dev server (default)
#   .\scripts\dev.ps1 run             # same as above
#   .\scripts\dev.ps1 reset-draft     # wipe rosters/picks/trades -> fresh draft on the current DB
#   .\scripts\dev.ps1 seed-mock       # (re)build the 2-league offline mock DB (mock_fantasy.db)
#   .\scripts\dev.ps1 logins          # print the local test team logins
#   .\scripts\dev.ps1 help
#
# Reads env from .env.local (DB_PATH, DB_TYPE, ALLOW_UNRESTRICTED_EDITS, ...).
# DATA_SOURCE defaults to 'mock' so nothing hits the live SuperBru/ESPN endpoints.

param([string]$Task = "run")

$ErrorActionPreference = "Stop"

# Prefer the project virtualenv's Python if present.
$py = if (Test-Path ".venv\Scripts\python.exe") { ".venv\Scripts\python.exe" } else { "python" }

function Load-DotEnv($envFile) {
    if (-not (Test-Path $envFile)) {
        Write-Error "Missing $envFile - copy .env.production.example, rename it, and fill in dev values."
        exit 1
    }
    Get-Content $envFile | Where-Object { $_ -match "^\s*[^#]\S+=.*" } | ForEach-Object {
        $parts = $_ -split "=", 2
        [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
    }
    # Keep local testing fully offline unless explicitly overridden.
    if (-not $env:DATA_SOURCE) { $env:DATA_SOURCE = "mock" }
}

function Show-Logins {
    # List the local @test.local team accounts in the configured DB (best-effort).
    $code = @'
import os, sqlite3
path = os.getenv("DB_PATH", "prem_rugby_26_27.db")
if not os.path.exists(path):
    print("  (no DB at " + path + ")"); raise SystemExit
try:
    c = sqlite3.connect(path)
    rows = c.execute("SELECT email, team_name FROM users WHERE email LIKE '%@test.local' ORDER BY user_id").fetchall()
    comm = c.execute("""SELECT u.email FROM leagues l JOIN users u ON u.user_id=l.commissioner_user_id""").fetchall()
    c.close()
except Exception as e:
    print("  (could not read users: %s)" % e); raise SystemExit
if not rows:
    print("  (no @test.local accounts - run: .\\scripts\\dev.ps1 reset-draft is fine, but team users come from the seeded 26_27 DB)")
else:
    commset = {e for (e,) in comm}
    for email, team in rows:
        tag = "  <- commissioner" if email in commset else ""
        print("   %-34s %s%s" % (email, team, tag))
    print("   password (all): draft2627")
'@
    $code | & $py -
}

switch ($Task.ToLower()) {

    "run" {
        Load-DotEnv ".env.local"
        Write-Host ("DB: {0} / {1}  |  DATA_SOURCE: {2}  |  unrestricted edits: {3}" -f `
            $env:DB_TYPE, $env:DB_PATH, $env:DATA_SOURCE, $env:ALLOW_UNRESTRICTED_EDITS) -ForegroundColor Cyan
        Write-Host "Test logins:" -ForegroundColor DarkCyan
        Show-Logins
        Write-Host "Server: http://127.0.0.1:5000  (Ctrl+C to stop)" -ForegroundColor Cyan
        & $py -m flask --app api.index run --debug --port 5000
    }

    "reset-draft" {
        Load-DotEnv ".env.local"
        Write-Host ("Resetting draft on {0} ..." -f $env:DB_PATH) -ForegroundColor Yellow
        $code = @'
from api.db import get_connection, ensure_schema
conn = get_connection(); ensure_schema(conn); cur = conn.cursor()
for t in ("draft_picks", "team_selections", "trades", "team_front_row"):
    cur.execute("DELETE FROM " + t)
cur.execute("UPDATE draft_state SET status='pending', current_pick=0, started_at=NULL, completed_at=NULL, pick_deadline=NULL")
cur.execute("UPDATE leagues SET draft_order=NULL, draft_at=NULL")
conn.commit(); conn.close()
print("Done: rosters, picks and trades cleared; draft set back to pending.")
'@
        $code | & $py -
    }

    "seed-mock" {
        Load-DotEnv ".env.local"
        if (-not $env:MOCK_DB_PATH) { $env:MOCK_DB_PATH = "mock_fantasy.db" }
        $env:DB_PATH = $env:MOCK_DB_PATH
        Write-Host ("Building 2-league mock DB at {0} ..." -f $env:DB_PATH) -ForegroundColor Yellow
        & $py -m api.datasource.generate_seed
        & $py -m api.seed_mock
        Write-Host "Done. Run it with:  DB_PATH set to $($env:DB_PATH) (edit .env.local or use this DB)." -ForegroundColor Yellow
    }

    "logins" {
        Load-DotEnv ".env.local"
        Show-Logins
    }

    default {
        Write-Host @"
Meatyboys Rugby Fantasy - local dev helper

  .\scripts\dev.ps1                start the Flask dev server (mock data, unrestricted edits)
  .\scripts\dev.ps1 reset-draft    wipe rosters/picks/trades on the current DB -> fresh draft
  .\scripts\dev.ps1 seed-mock      (re)build the 2-league offline mock DB
  .\scripts\dev.ps1 logins         print the local test team logins
  .\scripts\dev.ps1 help           show this help

Current DB is taken from .env.local (DB_PATH). To test a draft:
  1) .\scripts\dev.ps1 reset-draft
  2) .\scripts\dev.ps1
  3) open http://127.0.0.1:5000/draft and sign in as the commissioner
"@ -ForegroundColor Gray
    }
}
