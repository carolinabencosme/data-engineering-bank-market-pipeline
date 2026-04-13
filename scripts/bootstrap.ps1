Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Log {
    param(
        [Parameter(Mandatory = $true)][string]$Level,
        [Parameter(Mandatory = $true)][string]$Message
    )
    Write-Host "[$Level] $Message"
}

function Write-Info {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Log -Level 'INFO' -Message $Message
}

function Write-ErrorLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Error "[ERROR] $Message"
}

function Require-Command {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Hint = ''
    )

    if (-not (Get-Command -Name $Name -ErrorAction SilentlyContinue)) {
        Write-ErrorLog "Missing required command: $Name"
        if ($Hint) {
            Write-ErrorLog $Hint
        }
        throw "Missing command: $Name"
    }
}

function Test-DockerDaemon {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker daemon is not reachable. Ensure Docker Desktop/Engine is running.'
    }
}

function Test-DockerCompose {
    docker compose version *> $null
    if ($LASTEXITCODE -ne 0) {
        throw "Docker Compose v2 is not available via 'docker compose'."
    }
}

function Invoke-HealthCheck {
    $healthScript = Join-Path -Path $PSScriptRoot -ChildPath 'health-check.ps1'

    if (-not (Test-Path -Path $healthScript)) {
        throw "Health-check script not found at $healthScript"
    }

    Write-Info 'Running health-check script...'
    & $healthScript
}

function Ensure-AirbyteRunning {
    Write-Info 'Checking Airbyte status with abctl...'

    $statusOutput = & abctl local status 2>&1
    $statusExit = $LASTEXITCODE

    if ($statusExit -eq 0) {
        $statusText = ($statusOutput | Out-String)
        if ($statusText -match 'running|healthy|up') {
            Write-Info 'Airbyte is already running.'
            return
        }

        Write-Info 'Airbyte appears installed but not running. Starting Airbyte...'
        & abctl local start
        return
    }

    $statusText = ($statusOutput | Out-String)
    if ($statusText -match 'not\s+found|no\s+local\s+installation|not\s+installed') {
        Write-Info 'No local Airbyte installation detected. Creating Airbyte local installation...'
        & abctl local install
        return
    }

    Write-Info 'Unable to confirm Airbyte status; attempting idempotent start before install.'
    & abctl local start *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Info 'Airbyte start succeeded.'
        return
    }

    Write-Info 'Airbyte start failed; attempting install.'
    & abctl local install
}

function Main {
    Write-Info 'Running bootstrap preflight checks...'

    Require-Command -Name 'docker' -Hint 'Install Docker and ensure it is on PATH.'
    Test-DockerDaemon
    Test-DockerCompose
    Require-Command -Name 'abctl' -Hint 'Install Airbyte abctl and ensure it is on PATH.'

    Write-Info 'Starting core services (postgres, clickhouse, airflow, dbt)...'
    & docker compose up -d postgres clickhouse airflow dbt

    Ensure-AirbyteRunning

    Invoke-HealthCheck

    Write-Info 'Bootstrap completed successfully.'
}

try {
    Main
}
catch {
    Write-ErrorLog $_.Exception.Message
    exit 1
}