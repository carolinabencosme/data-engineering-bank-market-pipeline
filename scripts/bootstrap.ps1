Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Write-Log {
    param(
        [Parameter(Mandatory = $true)][ValidateSet('INFO', 'WARN', 'ERROR')][string]$Level,
        [Parameter(Mandatory = $true)][string]$Message
    )

    $color = switch ($Level) {
        'INFO'  { 'Cyan' }
        'WARN'  { 'Yellow' }
        'ERROR' { 'Red' }
    }

    Write-Host "[$Level] $Message" -ForegroundColor $color
}

function Write-Info {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Log -Level 'INFO' -Message $Message
}

function Write-WarnLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Log -Level 'WARN' -Message $Message
}

function Write-ErrorLog {
    param([Parameter(Mandatory = $true)][string]$Message)
    Write-Log -Level 'ERROR' -Message $Message
}

function Require-Command {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [string]$Hint = ''
    )

    if (-not (Get-Command -Name $Name -ErrorAction SilentlyContinue)) {
        $message = "Missing required command: $Name"
        if ($Hint) {
            $message = "$message. $Hint"
        }
        throw $message
    }
}

function ConvertTo-ArgumentString {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments
    )

    $escaped = foreach ($arg in $Arguments) {
        if ($arg -match '[\s"]') {
            '"' + ($arg -replace '"', '\"') + '"'
        }
        else {
            $arg
        }
    }

    return ($escaped -join ' ')
}

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter()][string[]]$Arguments = @(),
        [switch]$AllowNonZeroExitCode
    )

    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = $FilePath
    $psi.Arguments = ConvertTo-ArgumentString -Arguments $Arguments
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi

    try {
        [void]$process.Start()

        $stdOut = $process.StandardOutput.ReadToEnd()
        $stdErr = $process.StandardError.ReadToEnd()

        $process.WaitForExit()

        $stdoutLines = @()
        if (-not [string]::IsNullOrWhiteSpace($stdOut)) {
            $stdoutLines = @($stdOut -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        }

        $stderrLines = @()
        if (-not [string]::IsNullOrWhiteSpace($stdErr)) {
            $stderrLines = @($stdErr -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
        }

        return [PSCustomObject]@{
            ExitCode    = $process.ExitCode
            StdOutLines = $stdoutLines
            StdErrLines = $stderrLines
            StdOutText  = ($stdoutLines -join [Environment]::NewLine)
            StdErrText  = ($stderrLines -join [Environment]::NewLine)
            Success     = ($process.ExitCode -eq 0 -or $AllowNonZeroExitCode.IsPresent)
        }
    }
    finally {
        $process.Dispose()
    }
}

function Get-CommandOutputText {
    param(
        [Parameter(Mandatory = $true)]$Result
    )

    $details = @()
    if ($Result.StdOutText) { $details += $Result.StdOutText }
    if ($Result.StdErrText) { $details += $Result.StdErrText }

    if ($details.Count -gt 0) {
        return ($details -join [Environment]::NewLine)
    }

    return 'No additional details were returned.'
}

function Test-DockerDaemon {
    Write-Info 'Checking Docker daemon availability...'

    $result = Invoke-NativeCommand -FilePath 'docker' -Arguments @('info')

    if ($result.ExitCode -ne 0) {
        throw "Docker daemon is not reachable. Ensure Docker Desktop/Engine is running.`n$(Get-CommandOutputText -Result $result)"
    }

    foreach ($warning in $result.StdErrLines) {
        Write-WarnLog $warning
    }

    Write-Info 'Docker daemon is reachable.'
}

function Test-DockerCompose {
    Write-Info 'Checking Docker Compose availability...'

    $result = Invoke-NativeCommand -FilePath 'docker' -Arguments @('compose', 'version')

    if ($result.ExitCode -ne 0) {
        throw "Docker Compose v2 is not available via 'docker compose'.`n$(Get-CommandOutputText -Result $result)"
    }

    Write-Info 'Docker Compose is available.'
}

function Get-RepoRoot {
    return (Resolve-Path -LiteralPath (Join-Path -Path $PSScriptRoot -ChildPath '..')).Path
}

function Get-DotEnvVariables {
    param(
        [Parameter(Mandatory = $true)][string]$LiteralPath
    )

    $result = @{}

    foreach ($rawLine in Get-Content -LiteralPath $LiteralPath) {
        $line = $rawLine.Trim()
        if ([string]::IsNullOrWhiteSpace($line) -or $line.StartsWith('#')) {
            continue
        }

        $idx = $line.IndexOf('=')
        if ($idx -lt 1) {
            continue
        }

        $key = $line.Substring(0, $idx).Trim()
        $val = $line.Substring($idx + 1).Trim()

        if (($val.Length -ge 2) -and (($val.StartsWith('"') -and $val.EndsWith('"')) -or ($val.StartsWith("'") -and $val.EndsWith("'")))) {
            $val = $val.Substring(1, $val.Length - 2)
        }

        $result[$key] = $val
    }

    return $result
}

function Assert-RequiredDotEnvKeys {
    param(
        [Parameter(Mandatory = $true)][hashtable]$EnvVars
    )

    $required = @(
        'POSTGRES_USER',
        'POSTGRES_DB',
        'POSTGRES_PASSWORD',
        'AIRFLOW_DB_USER',
        'AIRFLOW_DB_PASSWORD',
        'AIRFLOW_DB_NAME',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN'
    )

    $missing = @()
    foreach ($key in $required) {
        if (-not $EnvVars.ContainsKey($key) -or [string]::IsNullOrWhiteSpace([string]$EnvVars[$key])) {
            $missing += $key
        }
    }

    if ($missing.Count -gt 0) {
        throw "Missing or empty required keys in .env: $($missing -join ', '). Fill them using .env.example as a guide."
    }
}

function Invoke-DockerCompose {
    param(
        [Parameter(Mandatory = $true)][string[]]$Arguments,
        [switch]$AllowNonZeroExitCode
    )

    return (Invoke-NativeCommand -FilePath 'docker' -Arguments (@('compose') + $Arguments) -AllowNonZeroExitCode:$AllowNonZeroExitCode)
}

function Wait-PostgresReady {
    param(
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    Write-Info "Waiting for Postgres to accept connections (timeout ${TimeoutSeconds}s)..."

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-DockerCompose -Arguments @(
            'exec', '-T', 'postgres', 'bash', '-lc',
            'export PGPASSWORD="${POSTGRES_PASSWORD?}"; pg_isready -h 127.0.0.1 -U "${POSTGRES_USER?}" -d "${POSTGRES_DB?}"'
        ) -AllowNonZeroExitCode
        if ($result.ExitCode -eq 0) {
            Write-Info 'Postgres is accepting connections.'
            return
        }

        Start-Sleep -Seconds 2
    }

    throw (
        "Postgres did not become reachable within ${TimeoutSeconds}s.`n" +
        "Check: docker compose ps postgres`n" +
        "Logs: docker compose logs --tail=200 postgres"
    )
}

function Test-PostgresPipelineRole {
    Write-Info 'Validating pipeline database connectivity (POSTGRES_USER -> POSTGRES_DB)...'

    $result = Invoke-DockerCompose -Arguments @(
        'exec', '-T', 'postgres', 'bash', '-lc',
        'set -euo pipefail; export PGPASSWORD="${POSTGRES_PASSWORD?}"; psql -h 127.0.0.1 -U "${POSTGRES_USER?}" -d "${POSTGRES_DB?}" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null'
    ) -AllowNonZeroExitCode

    if ($result.ExitCode -ne 0) {
        throw (
            "Pipeline Postgres authentication or database access failed (user=`$POSTGRES_USER, db=`$POSTGRES_DB).`n" +
            "Typical causes: wrong POSTGRES_PASSWORD in .env, or the Postgres volume predates init scripts (recreate with: docker compose down -v).`n" +
            "$(Get-CommandOutputText -Result $result)"
        )
    }

    Write-Info 'Pipeline Postgres connectivity OK.'
}

function Escape-SqlLiteralForBash {
    param([string]$Value)

    if ([string]::IsNullOrEmpty($Value)) {
        return ''
    }

    return $Value.Replace("'", "''")
}

function Wrap-BashSingleQuotedArgument {
    param([string]$Inner)

    return "'" + ($Inner.Replace("'", "'\''")) + "'"
}

function Assert-AirflowPostgresObjectsExist {
    Write-Info 'Checking that Airflow metadata role and database exist (postgres init scripts)...'

    $repoRoot = Get-RepoRoot
    $dotenvPath = Join-Path -Path $repoRoot -ChildPath '.env'
    $vars = Get-DotEnvVariables -LiteralPath $dotenvPath

    $eu = Escape-SqlLiteralForBash -Value ([string]$vars['AIRFLOW_DB_USER'])
    $ed = Escape-SqlLiteralForBash -Value ([string]$vars['AIRFLOW_DB_NAME'])

    $checkSql = "SELECT CASE WHEN EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$eu') AND EXISTS (SELECT 1 FROM pg_database WHERE datname = '$ed') THEN 0 ELSE 1 END;"
    $sqlArg = Wrap-BashSingleQuotedArgument -Inner $checkSql

    $remoteSh = 'set -eu; export PGPASSWORD="${POSTGRES_PASSWORD}"; r=$(psql -h 127.0.0.1 -U "${POSTGRES_USER}" -d "${POSTGRES_DB}" -v ON_ERROR_STOP=1 -tAc ' + $sqlArg + ' | tr -d "[:space:]"); if [ "$r" = "0" ]; then exit 0; elif [ "$r" = "1" ]; then echo "MISSING_AIRFLOW_PROVISIONING check_sql_returned=1" >&2; exit 2; else echo "MISSING_AIRFLOW_PROVISIONING unexpected_r=${r:-empty}" >&2; exit 2; fi'

    $result = Invoke-DockerCompose -Arguments @('exec', '-T', 'postgres', 'sh', '-lc', $remoteSh) -AllowNonZeroExitCode

    if ($result.ExitCode -eq 0) {
        Write-Info 'Airflow metadata role and database are present in Postgres.'
        return
    }

    if ($result.ExitCode -eq 2) {
        throw (
            "Airflow metadata is not provisioned in Postgres: the role (AIRFLOW_DB_USER) and/or database (AIRFLOW_DB_NAME) is missing or the check failed.`n`n" +
            "This usually means the Postgres data volume was created before infra/postgres/init ran, so /docker-entrypoint-initdb.d never ran.`n`n" +
            "Fix (from the repository root; destroys Postgres data):`n" +
            "  docker compose down -v`n" +
            "  docker compose up -d`n`n" +
            "Then re-run bootstrap. Confirm init ran: docker compose logs postgres | Select-String -Pattern 'Airflow metadata|CREATE DATABASE'`n`n" +
            "Details:`n$(Get-CommandOutputText -Result $result)"
        )
    }

    throw (
        "Could not verify Airflow metadata provisioning in Postgres.`n" +
        "$(Get-CommandOutputText -Result $result)"
    )
}

function Test-PostgresAirflowMetadataRole {
    Write-Info 'Validating Airflow metadata database connectivity (AIRFLOW_DB_USER -> AIRFLOW_DB_NAME)...'

    $result = Invoke-DockerCompose -Arguments @(
        'exec', '-T', 'postgres', 'bash', '-lc',
        'set -euo pipefail; export PGPASSWORD="${AIRFLOW_DB_PASSWORD?}"; psql -h 127.0.0.1 -U "${AIRFLOW_DB_USER?}" -d "${AIRFLOW_DB_NAME?}" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null'
    ) -AllowNonZeroExitCode

    if ($result.ExitCode -ne 0) {
        throw (
            "Airflow metadata Postgres authentication or database access failed (user=`$AIRFLOW_DB_USER, db=`$AIRFLOW_DB_NAME).`n" +
            "Typical causes: AIRFLOW_DB_* not provisioned (recreate Postgres volume after adding infra/postgres/init), or password mismatch vs AIRFLOW__DATABASE__SQL_ALCHEMY_CONN.`n" +
            "$(Get-CommandOutputText -Result $result)"
        )
    }

    Write-Info 'Airflow metadata Postgres connectivity OK.'
}

function Wait-AirflowInitCompleted {
    param(
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    Write-Info "Waiting for airflow-init to finish (timeout ${TimeoutSeconds}s)..."

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-DockerCompose -Arguments @('ps', '-a', '--format', 'json') -AllowNonZeroExitCode
        if ($result.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($result.StdOutText)) {
            Start-Sleep -Seconds 3
            continue
        }

        $record = $null
        foreach ($line in ($result.StdOutText -split "`r?`n")) {
            if ([string]::IsNullOrWhiteSpace($line)) {
                continue
            }

            try {
                $candidate = $line | ConvertFrom-Json
            }
            catch {
                continue
            }

            if ($candidate.Service -eq 'airflow-init') {
                $record = $candidate
                break
            }
        }

        if ($null -eq $record) {
            Start-Sleep -Seconds 3
            continue
        }

        $state = [string]$record.State
        if ($state -match '(?i)exited') {
            $exitCode = [int]$record.ExitCode
            if ($exitCode -eq 0) {
                Write-Info 'airflow-init completed successfully.'
                return
            }

            throw (
                "airflow-init failed (exit code $exitCode). This usually indicates Airflow cannot migrate its metadata database.`n" +
                "Check: docker compose logs --tail=200 airflow-init`n" +
                "Validate AIRFLOW__DATABASE__SQL_ALCHEMY_CONN matches AIRFLOW_DB_* and that the airflow database exists."
            )
        }

        Start-Sleep -Seconds 3
    }

    throw (
        "Timed out waiting for airflow-init to complete.`n" +
        "Check: docker compose ps airflow-init`n" +
        "Logs: docker compose logs --tail=200 airflow-init"
    )
}

function Test-AirflowMetadataDbCheck {
    param(
        [Parameter(Mandatory = $true)][int]$TimeoutSeconds
    )

    Write-Info "Running 'airflow db check' (timeout ${TimeoutSeconds}s)..."

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastFailure = $null
    while ((Get-Date) -lt $deadline) {
        $result = Invoke-DockerCompose -Arguments @(
            'exec', '-T', 'airflow-webserver', 'airflow', 'db', 'check'
        ) -AllowNonZeroExitCode

        if ($result.ExitCode -eq 0) {
            Write-Info "Airflow metadata DB check OK."
            return
        }

        $lastFailure = $result
        Start-Sleep -Seconds 4
    }

    $details = if ($null -ne $lastFailure) {
        Get-CommandOutputText -Result $lastFailure
    }
    else {
        'No additional details were returned.'
    }

    throw (
        "airflow db check did not succeed before timeout.`n" +
        "This indicates Airflow cannot use its configured metadata database.`n" +
        "Check: docker compose logs --tail=200 airflow-webserver`n" +
        "$details"
    )
}

function Test-PostgresProvisioning {
    Wait-PostgresReady -TimeoutSeconds 120
    Test-PostgresPipelineRole
    Assert-AirflowPostgresObjectsExist
    Test-PostgresAirflowMetadataRole
    Wait-AirflowInitCompleted -TimeoutSeconds 600
    Test-AirflowMetadataDbCheck -TimeoutSeconds 180
}

function Test-RequiredFiles {
    Write-Info 'Validating required environment files...'

    $requiredFiles = @(
        '.env',
        'infra/postgres/postgres.env',
        'infra/clickhouse/clickhouse.env',
        'infra/airflow/airflow.env',
        'infra/dbt/dbt.env'
    )

    $missingFiles = @()

    foreach ($file in $requiredFiles) {
        $fullPath = Join-Path -Path $PSScriptRoot -ChildPath "..\$file"
        if (-not (Test-Path -Path $fullPath -PathType Leaf)) {
            $missingFiles += $file
        }
    }

    if ($missingFiles.Count -gt 0) {
        $missingList = ($missingFiles | ForEach-Object { " - $_" }) -join [Environment]::NewLine
        $suggestedCommand = @(
            "Copy-Item .env.example .env",
            "Copy-Item infra/postgres/postgres.env.example infra/postgres/postgres.env",
            "Copy-Item infra/clickhouse/clickhouse.env.example infra/clickhouse/clickhouse.env",
            "Copy-Item infra/airflow/airflow.env.example infra/airflow/airflow.env",
            "Copy-Item infra/dbt/dbt.env.example infra/dbt/dbt.env"
        ) -join '; '

        throw "Missing required environment file(s):`n$missingList`nCreate them from templates with:`n$suggestedCommand"
    }

    Write-Info 'All required environment files are present.'
}

function Start-CoreServices {
    Write-Info 'Starting core services (postgres, clickhouse, airflow-init, airflow-webserver, airflow-scheduler, dbt)...'

    $result = Invoke-NativeCommand -FilePath 'docker' -Arguments @(
        'compose', 'up', '-d',
        'postgres',
        'clickhouse',
        'airflow-init',
        'airflow-webserver',
        'airflow-scheduler',
        'dbt'
    )

    if ($result.ExitCode -ne 0) {
        throw "Failed to start core services with Docker Compose.`n$(Get-CommandOutputText -Result $result)"
    }

    Write-Info 'Core services started successfully.'
}

function Test-AirbyteStatusHealthy {
    param([Parameter(Mandatory = $true)][string]$StatusText)

    return $StatusText -match '(?i)\b(deployed|running|healthy|up|available)\b'
}

function Test-AirbyteNotInstalledMessage {
    param([Parameter(Mandatory = $true)][string]$StatusText)

    return $StatusText -match '(?i)not\s+found|no\s+local\s+installation|not\s+installed'
}

function Ensure-AirbyteRunning {
    Write-Info 'Checking Airbyte status with abctl...'

    $statusResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'status') -AllowNonZeroExitCode
    $statusText = @($statusResult.StdOutText, $statusResult.StdErrText) -join [Environment]::NewLine

    if ($statusResult.ExitCode -eq 0 -and (Test-AirbyteStatusHealthy -StatusText $statusText)) {
        Write-Info 'Airbyte is already running.'
        return
    }

    if (Test-AirbyteNotInstalledMessage -StatusText $statusText) {
        Write-Info 'No local Airbyte installation detected. Installing Airbyte locally...'
        $installResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'install')

        if ($installResult.ExitCode -ne 0) {
            throw "Airbyte installation failed.`n$(Get-CommandOutputText -Result $installResult)"
        }

        Write-Info 'Airbyte installation completed.'
        return
    }

    Write-Info 'Airbyte appears installed but is not running. Starting Airbyte (skipping reinstall)...'
    $startResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'start')

    if ($startResult.ExitCode -eq 0) {
        Write-Info 'Airbyte started successfully.'
        return
    }

    throw (
        "Airbyte start failed. A local installation already exists; bootstrap does not run `abctl local install` again. " +
        "Fix the issue or run `abctl local status` / `abctl local logs` manually.`n$(Get-CommandOutputText -Result $startResult)"
    )
}

function Invoke-HealthCheck {
    $healthScript = Join-Path -Path $PSScriptRoot -ChildPath 'check-health.ps1'

    if (-not (Test-Path -Path $healthScript)) {
        throw "Health-check script not found at $healthScript"
    }

    Write-Info 'Running health-check script...'
    & powershell -ExecutionPolicy Bypass -File $healthScript

    if ($LASTEXITCODE -ne 0) {
        throw 'Health-check script reported a failure.'
    }
}

function Main {
    $repoRoot = Get-RepoRoot
    Set-Location -LiteralPath $repoRoot

    Write-Info 'Running bootstrap preflight checks...'

    $dotenvPath = Join-Path -Path $repoRoot -ChildPath '.env'
    $envVars = Get-DotEnvVariables -LiteralPath $dotenvPath
    Assert-RequiredDotEnvKeys -EnvVars $envVars

    Test-RequiredFiles -RepoRoot $repoRoot

    Require-Command -Name 'docker' -Hint 'Install Docker and ensure it is on PATH.'
    Require-Command -Name 'abctl' -Hint 'Install Airbyte abctl and ensure it is on PATH.'

    Test-DockerDaemon
    Test-DockerCompose

    Start-CoreServices
    Test-PostgresProvisioning
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
