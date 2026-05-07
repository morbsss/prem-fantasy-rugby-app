# Start the Flask dev server with local environment variables.
# Run from the project root: .\scripts\dev.ps1

$envFile = ".env.local"
if (-not (Test-Path $envFile)) {
    Write-Error "Missing $envFile - copy .env.production.example, rename it, and fill in dev values."
    exit 1
}

# Load each KEY=VALUE line from .env.local into the current process environment
Get-Content $envFile | Where-Object { $_ -match "^\s*[^#]\S+=.*" } | ForEach-Object {
    $parts = $_ -split "=", 2
    [System.Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim())
}

$msg = 'Starting dev server (DB: ' + $env:DB_TYPE + ' / ' + $env:DB_PATH + ', unrestricted: ' + $env:ALLOW_UNRESTRICTED_EDITS + ')'
Write-Host $msg -ForegroundColor Cyan
python -m flask --app api.index run --debug --port 5000
