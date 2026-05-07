# Start Flask pointing at the production Postgres database (for local prod testing).
# Requires .env.production with real credentials (gitignored).
# Run from the project root: .\scripts\prod.ps1

$envFile = ".env.production"
if (-not (Test-Path $envFile)) {
    Write-Error "Missing $envFile — copy .env.production.example, rename it, and fill in real values."
    exit 1
}

Get-Content $envFile | Where-Object { $_ -match "^\s*[^#]\S+=.*" } | ForEach-Object {
    $parts = $_ -split "=", 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
}

Write-Host "Starting prod-mode server (DB: $env:DB_TYPE, locked: $env:ALLOW_UNRESTRICTED_EDITS)" -ForegroundColor Yellow
python -m flask --app api.index run --port 5000
