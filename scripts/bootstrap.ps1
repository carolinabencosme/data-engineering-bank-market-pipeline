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

function Ensure-AirbyteRunning {
    Write-Info 'Checking Airbyte status with abctl...'

    $statusResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'status') -AllowNonZeroExitCode
    $statusText = @($statusResult.StdOutText, $statusResult.StdErrText) -join [Environment]::NewLine

    if ($statusResult.ExitCode -eq 0 -and $statusText -match '(?i)\b(running|healthy|up)\b') {
        Write-Info 'Airbyte is already running.'
        return
    }

    if ($statusText -match '(?i)not\s+found|no\s+local\s+installation|not\s+installed') {
        Write-Info 'No local Airbyte installation detected. Installing Airbyte locally...'
        $installResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'install')

        if ($installResult.ExitCode -ne 0) {
            throw "Airbyte installation failed.`n$(Get-CommandOutputText -Result $installResult)"
        }

        Write-Info 'Airbyte installation completed.'
        return
    }

    Write-Info 'Airbyte is not confirmed as running. Attempting start...'
    $startResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'start') -AllowNonZeroExitCode

    if ($startResult.ExitCode -eq 0) {
        Write-Info 'Airbyte started successfully.'
        return
    }

    Write-WarnLog 'Airbyte start did not succeed. Attempting fresh install...'
    $installResult = Invoke-NativeCommand -FilePath 'abctl' -Arguments @('local', 'install')

    if ($installResult.ExitCode -ne 0) {
        throw "Airbyte installation failed after start attempt.`n$(Get-CommandOutputText -Result $installResult)"
    }

    Write-Info 'Airbyte installation completed.'
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
    Write-Info 'Running bootstrap preflight checks...'

    Require-Command -Name 'docker' -Hint 'Install Docker and ensure it is on PATH.'
    Require-Command -Name 'abctl' -Hint 'Install Airbyte abctl and ensure it is on PATH.'

    Test-DockerDaemon
    Test-DockerCompose

    Start-CoreServices
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