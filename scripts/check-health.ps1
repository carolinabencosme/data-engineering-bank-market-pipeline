Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$script:HealthCheckResults = New-Object System.Collections.Generic.List[object]
$CriticalFailures = 0

# Servicios que sí deben permanecer corriendo
$ExpectedRunningServices = @('postgres', 'clickhouse', 'airflow-webserver', 'airflow-scheduler', 'dbt')

function Add-Result {
    param(
        [Parameter(Mandatory = $true)][string]$Service,
        [Parameter(Mandatory = $true)][bool]$Passed,
        [Parameter(Mandatory = $true)][string]$Detail
    )

    $status = if ($Passed) { 'PASS' } else { 'FAIL' }
    $script:HealthCheckResults.Add([PSCustomObject]@{
        Service = $Service
        Check   = $status
        Detail  = $Detail
    })

    if (-not $Passed) {
        $script:CriticalFailures++
    }
}

function Invoke-Captured {
    param([Parameter(Mandatory = $true)][scriptblock]$Action)

    $previousEap = $ErrorActionPreference
    $hasNativePreference = $null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -Scope Global -ErrorAction SilentlyContinue)

    if ($hasNativePreference) {
        $previousNative = $global:PSNativeCommandUseErrorActionPreference
    }

    try {
        $ErrorActionPreference = 'Continue'

        if ($hasNativePreference) {
            $global:PSNativeCommandUseErrorActionPreference = $false
        }

        $output = & $Action 2>&1
        $exitCode = $LASTEXITCODE

        return [PSCustomObject]@{
            ExitCode = if ($null -eq $exitCode) { 0 } else { $exitCode }
            Output   = ($output | Out-String).Trim()
        }
    }
    catch {
        return [PSCustomObject]@{
            ExitCode = if ($null -eq $LASTEXITCODE) { 1 } else { $LASTEXITCODE }
            Output   = ($_ | Out-String).Trim()
        }
    }
    finally {
        $ErrorActionPreference = $previousEap

        if ($hasNativePreference) {
            $global:PSNativeCommandUseErrorActionPreference = $previousNative
        }
    }
}

function Get-SummarizedOutput {
    param(
        [string]$Text,
        [int]$MaxLines = 2,
        [int]$MaxChars = 240
    )

    if ([string]::IsNullOrWhiteSpace($Text)) {
        return 'no output'
    }

    $singleLine = ($Text -replace "`r?`n", '; ').Trim()
    if ($singleLine.Length -gt $MaxChars) {
        $singleLine = "$($singleLine.Substring(0, $MaxChars))..."
    }

    $lines = $singleLine -split '; ' | Where-Object { $_.Trim() } | Select-Object -First $MaxLines
    if (-not $lines) {
        return 'no output'
    }

    return ($lines -join '; ')
}

function Test-ComposeServices {
    $compose = Invoke-Captured { docker compose ps --all --format json }

    if ($compose.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($compose.Output)) {
        Add-Result -Service 'compose' -Passed $false -Detail 'Unable to read docker compose ps output'
        return
    }

    $serviceRows = $compose.Output -split "`r?`n" | Where-Object { $_.Trim() }
    $records = @()

    foreach ($row in $serviceRows) {
        try {
            $records += $row | ConvertFrom-Json
        }
        catch {
            Add-Result -Service 'compose' -Passed $false -Detail 'Invalid docker compose ps JSON output'
            return
        }
    }

    $missing = New-Object System.Collections.Generic.List[string]
    $notRunning = New-Object System.Collections.Generic.List[string]

    foreach ($svc in $ExpectedRunningServices) {
        $record = $records | Where-Object { $_.Service -eq $svc } | Select-Object -First 1

        if (-not $record) {
            $missing.Add($svc)
            continue
        }

        if ($record.State -ne 'running') {
            $notRunning.Add("$svc=$($record.State)")
        }
    }

    if ($missing.Count -gt 0 -or $notRunning.Count -gt 0) {
        $parts = @()
        if ($missing.Count -gt 0) { $parts += "missing:$($missing -join ',')" }
        if ($notRunning.Count -gt 0) { $parts += "not-running:$($notRunning -join ',')" }
        Add-Result -Service 'compose' -Passed $false -Detail ($parts -join ' ')
        return
    }

    Add-Result -Service 'compose' -Passed $true -Detail 'expected running services are present'
}

function Test-AirflowInit {
    $compose = Invoke-Captured { docker compose ps --all --format json }

    if ($compose.ExitCode -ne 0 -or [string]::IsNullOrWhiteSpace($compose.Output)) {
        Add-Result -Service 'airflow-init' -Passed $false -Detail 'Unable to inspect airflow-init status'
        return
    }

    $serviceRows = $compose.Output -split "`r?`n" | Where-Object { $_.Trim() }
    $records = @()

    foreach ($row in $serviceRows) {
        try {
            $records += $row | ConvertFrom-Json
        }
        catch {
            Add-Result -Service 'airflow-init' -Passed $false -Detail 'Invalid docker compose ps JSON output'
            return
        }
    }

    $record = $records | Where-Object { $_.Service -eq 'airflow-init' } | Select-Object -First 1

    if (-not $record) {
        Add-Result -Service 'airflow-init' -Passed $false -Detail 'service not found'
        return
    }

    if ($record.State -match 'exited|stopped') {
        Add-Result -Service 'airflow-init' -Passed $true -Detail "completed with state $($record.State)"
    }
    elseif ($record.State -eq 'running') {
        Add-Result -Service 'airflow-init' -Passed $true -Detail 'still running (may still be initializing)'
    }
    else {
        Add-Result -Service 'airflow-init' -Passed $false -Detail "unexpected state: $($record.State)"
    }
}

function Test-PostgresReady {
    $result = Invoke-Captured { docker compose exec -T postgres pg_isready }

    if ($result.ExitCode -eq 0) {
        Add-Result -Service 'postgres' -Passed $true -Detail 'pg_isready ok'
    }
    else {
        Add-Result -Service 'postgres' -Passed $false -Detail "pg_isready failed ($($result.Output -replace "`r?`n", '; '))"
    }
}

function Test-ClickHouse {
    $result = Invoke-Captured { docker compose exec -T clickhouse clickhouse-client --query 'SELECT 1' }
    $normalized = ($result.Output -replace '\s+', '')

    if ($result.ExitCode -eq 0 -and $normalized -eq '1') {
        Add-Result -Service 'clickhouse' -Passed $true -Detail 'SELECT 1 ok'
    }
    else {
        Add-Result -Service 'clickhouse' -Passed $false -Detail "smoke query failed ($($result.Output))"
    }
}

function Test-AirflowWebserver {
    # Webserver often binds after `airflow db check` succeeds; a single curl races connection refused.
    # Waits up to a wall-clock budget (default 30 min), polling every few seconds — not a fixed attempt count.
    # HEALTHCHECK_AIRFLOW_WEBSERVER_MAX_WAIT_SECONDS (default 1800), HEALTHCHECK_AIRFLOW_WEBSERVER_SLEEP_SECONDS (default 5).
    $sleepSeconds = 5
    $rawSleep = $env:HEALTHCHECK_AIRFLOW_WEBSERVER_SLEEP_SECONDS
    if ($rawSleep -match '^\d+$' -and [int]$rawSleep -gt 0) { $sleepSeconds = [int]$rawSleep }

    $maxWaitSeconds = 1800
    $rawWait = $env:HEALTHCHECK_AIRFLOW_WEBSERVER_MAX_WAIT_SECONDS
    if ($rawWait -match '^\d+$' -and [int]$rawWait -gt 0) {
        $maxWaitSeconds = [int]$rawWait
    }

    $start = Get-Date
    $attempt = 0
    $lastResult = $null

    while ($true) {
        $attempt++
        $lastResult = Invoke-Captured {
            docker compose exec -T airflow-webserver curl --silent --show-error --fail http://localhost:8080/health
        }

        if ($lastResult.ExitCode -eq 0 -and $lastResult.Output -match 'healthy') {
            $elapsed = [int]((Get-Date) - $start).TotalSeconds
            $detail = if ($attempt -eq 1) {
                'health endpoint healthy'
            }
            else {
                "health endpoint healthy after $attempt polls (${elapsed}s)"
            }

            Add-Result -Service 'airflow-webserver' -Passed $true -Detail $detail
            return
        }

        $elapsed = ((Get-Date) - $start).TotalSeconds
        if ($elapsed -ge $maxWaitSeconds) {
            break
        }

        Start-Sleep -Seconds $sleepSeconds
    }

    $summary = Get-SummarizedOutput -Text $lastResult.Output
    Add-Result -Service 'airflow-webserver' -Passed $false -Detail "health endpoint not healthy within ${maxWaitSeconds}s ($attempt polls; $summary)"
}

function Test-AirflowScheduler {
    $maxAttempts = 4
    $sleepSeconds = 5
    $transientPattern = 'No alive jobs found|job.*not found|not yet started|scheduler.*starting|connection refused|timed out'
    $successPattern = 'Found one alive job|Found [0-9]+ alive job'
    $lastResult = $null
    $transientDetected = $false

    for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
        $lastResult = Invoke-Captured {
            docker compose exec -T airflow-scheduler bash -lc 'scheduler_host="$(hostname)"; airflow jobs check --job-type SchedulerJob --hostname "$scheduler_host"'
        }

        if ($lastResult.ExitCode -eq 0 -and $lastResult.Output -match $successPattern) {
            $detail = if ($attempt -eq 1) {
                'scheduler responsive'
            } else {
                "scheduler responsive after retry $attempt/$maxAttempts"
            }

            Add-Result -Service 'airflow-scheduler' -Passed $true -Detail $detail
            return
        }

        if ($lastResult.Output -match $transientPattern) {
            $transientDetected = $true
        }

        if ($attempt -lt $maxAttempts) {
            Start-Sleep -Seconds $sleepSeconds
        }
    }

    $summary = Get-SummarizedOutput -Text $lastResult.Output

    if ($transientDetected) {
        Add-Result -Service 'airflow-scheduler' -Passed $false -Detail "scheduler still initializing after $maxAttempts attempts (stderr: $summary)"
    }
    else {
        Add-Result -Service 'airflow-scheduler' -Passed $false -Detail "scheduler check failed (stderr: $summary)"
    }
}

function Test-Dbt {
    $result = Invoke-Captured { docker compose exec -T dbt dbt --version }
    $headline = ($result.Output -split "`r?`n" | Select-Object -First 1)

    if ($result.ExitCode -eq 0) {
        Add-Result -Service 'dbt' -Passed $true -Detail $(if ($headline) { $headline } else { 'dbt available' })
    }
    else {
        Add-Result -Service 'dbt' -Passed $false -Detail 'dbt --version failed'
    }
}

function Test-Airbyte {
    if (-not (Get-Command -Name 'abctl' -ErrorAction SilentlyContinue)) {
        Add-Result -Service 'airbyte' -Passed $false -Detail 'abctl not found'
        return
    }

    $result = Invoke-Captured { abctl local status }

    if ($result.ExitCode -eq 0 -and $result.Output -match '(?i)running|healthy|available|up') {
        Add-Result -Service 'airbyte' -Passed $true -Detail 'abctl local status healthy'
    }
    else {
        Add-Result -Service 'airbyte' -Passed $false -Detail "status unavailable ($($result.Output -replace "`r?`n", '; '))"
    }
}

Test-ComposeServices
Test-AirflowInit
Test-PostgresReady
Test-ClickHouse
Test-AirflowWebserver
Test-AirflowScheduler
Test-Dbt
Test-Airbyte

$script:HealthCheckResults | Format-Table -AutoSize

if ($CriticalFailures -gt 0) {
    exit 1
}

exit 0